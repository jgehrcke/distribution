package storage

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/distribution/distribution/v3"
	"github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/opencontainers/go-digest"
)

// TODO(stevvooe): This should configurable in the future.
const blobCacheControlMaxAge = 365 * 24 * time.Hour

// blobServer simply serves blobs from a driver instance using a path function
// to identify paths and a descriptor service to fill in metadata.
type blobServer struct {
	driver   driver.StorageDriver
	statter  distribution.BlobStatter
	pathFn   func(dgst digest.Digest) (string, error)
	redirect bool // allows disabling RedirectURL redirects
}

func (bs *blobServer) ServeBlob(ctx context.Context, w http.ResponseWriter, r *http.Request, dgst digest.Digest) error {
	desc, err := bs.statter.Stat(ctx, dgst)
	if err != nil {
		return err
	}

	path, err := bs.pathFn(desc.Digest)
	if err != nil {
		return err
	}

	if bs.redirect {
		redirectURL, err := bs.driver.RedirectURL(r, path)
		if err != nil {
			return err
		}
		if redirectURL != "" {
			// Redirect to storage URL.
			http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
			return nil
		}
		// Fallback to serving the content directly.
	}

	var content io.ReadSeekCloser
	// Pass *os.File directly so net/http can use the sendfile system call.
	// The buffered fileReader hides the file from net/http.
	if canUseFileFastPath(r) && bs.driver.Name() == "filesystem" {
		rc, err := bs.driver.Reader(ctx, path, 0)
		if err != nil {
			return err
		}
		if f, ok := rc.(*os.File); ok {
			info, err := f.Stat()
			if err != nil {
				f.Close()
				return fmt.Errorf("stat blob %s: %w", desc.Digest, err)
			}
			// ServeContent uses the file size instead of the descriptor size.
			// Blobs must remain immutable while the response uses this open file.
			if info.Size() != desc.Size {
				f.Close()
				return fmt.Errorf("blob %s size mismatch: descriptor %d, file %d", desc.Digest, desc.Size, info.Size())
			}
			content = f
		} else {
			// Use fileReader to preserve seeking when middleware hides *os.File.
			rc.Close()
		}
	}
	if content == nil {
		content, err = newFileReader(ctx, bs.driver, path, desc.Size)
		if err != nil {
			return err
		}
	}
	defer content.Close()

	w.Header().Set("ETag", fmt.Sprintf(`"%s"`, desc.Digest)) // If-None-Match handled by ServeContent
	w.Header().Set("Cache-Control", fmt.Sprintf("max-age=%.f", blobCacheControlMaxAge.Seconds()))

	if w.Header().Get("Docker-Content-Digest") == "" {
		w.Header().Set("Docker-Content-Digest", desc.Digest.String())
	}

	if w.Header().Get("Content-Type") == "" {
		// Set the content type if not already set.
		w.Header().Set("Content-Type", desc.MediaType)
	}

	if w.Header().Get("Content-Length") == "" {
		// Set the content length if not already set.
		w.Header().Set("Content-Length", fmt.Sprint(desc.Size))
	}

	http.ServeContent(w, r, desc.Digest.String(), time.Time{}, content)
	return nil
}

func canUseFileFastPath(r *http.Request) bool {
	// HEAD and failed preconditions need metadata but can return without opening storage.
	if r.Method != http.MethodGet {
		return false
	}
	for _, name := range [...]string{"If-Match", "If-Unmodified-Since", "If-None-Match", "If-Modified-Since"} {
		if r.Header.Get(name) != "" {
			return false
		}
	}
	// If-Range still requires the blob because either outcome sends a body.
	return true
}
