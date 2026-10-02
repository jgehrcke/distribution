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

	br, err := newFileReader(ctx, bs.driver, path, desc.Size)
	if err != nil {
		return err
	}
	defer br.Close()

	var blobReader io.ReadSeeker = br
	if canUseDirectFile(r) && bs.driver.Name() == "filesystem" {
		rc, err := bs.driver.Reader(ctx, path, 0)
		if err != nil {
			return err
		}
		file, ok := rc.(*os.File)
		if !ok {
			// Use the original fileReader when middleware hides the file object.
			rc.Close()
		} else {
			defer file.Close()
			// Keep *os.File visible to net/http so it can use the sendfile system call.
			blobReader = file
		}
	}

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

	http.ServeContent(w, r, desc.Digest.String(), time.Time{}, blobReader)
	return nil
}

func canUseDirectFile(r *http.Request) bool {
	// Only GET requests send blob data; other methods do not need an open file.
	if r.Method != http.MethodGet {
		return false
	}
	// ServeContent may return HTTP 304 or 412 without reading the blob, so keep file opening lazy.
	for _, name := range [...]string{"If-Match", "If-Unmodified-Since", "If-None-Match", "If-Modified-Since"} {
		if r.Header.Get(name) != "" {
			return false
		}
	}
	// If-Range still requires the blob because either outcome sends a body.
	return true
}
