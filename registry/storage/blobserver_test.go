package storage

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/distribution/distribution/v3/internal/dcontext"
	"github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/filesystem"
	"github.com/opencontainers/go-digest"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type fixedBlobStatter struct {
	desc v1.Descriptor
}

func (s fixedBlobStatter) Stat(context.Context, digest.Digest) (v1.Descriptor, error) {
	return s.desc, nil
}

type blobReaderSpy struct {
	driver.StorageDriver
	reads   int
	files   []*os.File
	readErr error
	wrap    func(io.ReadCloser) io.ReadCloser
}

func (d *blobReaderSpy) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	d.reads++
	if d.readErr != nil {
		return nil, d.readErr
	}
	rc, err := d.StorageDriver.Reader(ctx, path, offset)
	if err != nil {
		return rc, err
	}
	if f, ok := rc.(*os.File); ok {
		d.files = append(d.files, f)
	}
	if d.wrap != nil {
		rc = d.wrap(rc)
	}
	return rc, nil
}

// Check sendfile eligibility without depending on the operating system or socket type.
type fileBodyProbe struct {
	*httptest.ResponseRecorder
	sawFile bool
}

func (w *fileBodyProbe) ReadFrom(r io.Reader) (int64, error) {
	if limited, ok := r.(*io.LimitedReader); ok {
		_, w.sawFile = limited.R.(*os.File)
	}
	return io.Copy(w.ResponseRecorder, r)
}

func newFilesystemBlobServer(t *testing.T, body string) (*blobServer, *blobReaderSpy, digest.Digest) {
	t.Helper()
	const path = "/blob"
	content := []byte(body)
	dgst := digest.FromBytes(content)
	fs := filesystem.New(filesystem.DriverParameters{RootDirectory: t.TempDir(), MaxThreads: 25})
	if err := fs.PutContent(context.Background(), path, content); err != nil {
		t.Fatal(err)
	}
	spy := &blobReaderSpy{StorageDriver: fs}
	return &blobServer{
		driver:  spy,
		statter: fixedBlobStatter{desc: v1.Descriptor{Digest: dgst, Size: int64(len(content)), MediaType: "application/octet-stream"}},
		pathFn:  func(digest.Digest) (string, error) { return path, nil },
	}, spy, dgst
}

func assertBlobReadersClosed(t *testing.T, spy *blobReaderSpy) {
	t.Helper()
	for _, f := range spy.files {
		if _, err := f.Stat(); !errors.Is(err, os.ErrClosed) {
			t.Errorf("blob file remains open: Stat error = %v", err)
		}
	}
}

func TestBlobServerFilesystemContent(t *testing.T) {
	for _, tc := range []struct {
		name, method, rangeHeader, precondition, ifRange, wantBody string
		wantStatus, wantReads                                      int
		wantFile                                                   bool
	}{
		{name: "full", method: http.MethodGet, wantBody: "0123456789", wantStatus: http.StatusOK, wantReads: 1, wantFile: true},
		{name: "range", method: http.MethodGet, rangeHeader: "bytes=2-5", wantBody: "2345", wantStatus: http.StatusPartialContent, wantReads: 1, wantFile: true},
		{name: "if-range-match", method: http.MethodGet, rangeHeader: "bytes=2-5", ifRange: "match", wantBody: "2345", wantStatus: http.StatusPartialContent, wantReads: 1, wantFile: true},
		{name: "if-range-mismatch", method: http.MethodGet, rangeHeader: "bytes=2-5", ifRange: `"other"`, wantBody: "0123456789", wantStatus: http.StatusOK, wantReads: 1, wantFile: true},
		{name: "head", method: http.MethodHead, wantStatus: http.StatusOK},
		{name: "not-modified", method: http.MethodGet, precondition: "If-None-Match", wantStatus: http.StatusNotModified},
		{name: "failed-precondition", method: http.MethodGet, precondition: "If-Match", wantStatus: http.StatusPreconditionFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bs, spy, dgst := newFilesystemBlobServer(t, "0123456789")
			req := httptest.NewRequest(tc.method, "/blob", nil)
			req.Header.Set("Range", tc.rangeHeader)
			if tc.ifRange != "" {
				value := tc.ifRange
				if value == "match" {
					value = `"` + dgst.String() + `"`
				}
				req.Header.Set("If-Range", value)
			}
			if tc.precondition == "If-None-Match" {
				req.Header.Set(tc.precondition, `"`+dgst.String()+`"`)
			}
			if tc.precondition == "If-Match" {
				req.Header.Set(tc.precondition, `"other"`)
			}
			probe := &fileBodyProbe{ResponseRecorder: httptest.NewRecorder()}
			ctx, w := dcontext.WithResponseWriter(context.Background(), probe)
			if err := bs.ServeBlob(ctx, w, req, dgst); err != nil {
				t.Fatal(err)
			}
			assertBlobReadersClosed(t, spy)
			if ctx.Value("http.response.written") != int64(len(tc.wantBody)) || ctx.Value("http.response.status") != tc.wantStatus {
				t.Errorf("recorded bytes = %v, status = %v", ctx.Value("http.response.written"), ctx.Value("http.response.status"))
			}
			if probe.Code != tc.wantStatus || probe.Body.String() != tc.wantBody {
				t.Fatalf("response: status %d, body %q", probe.Code, probe.Body.String())
			}
			if spy.reads != tc.wantReads || probe.sawFile != tc.wantFile {
				t.Errorf("Reader calls = %d, file source = %t", spy.reads, probe.sawFile)
			}
			if tc.wantStatus == http.StatusPartialContent && (probe.Header().Get("Content-Range") != "bytes 2-5/10" || probe.Header().Get("Content-Length") != "4") {
				t.Errorf("range headers: %v", probe.Header())
			}
			if tc.method == http.MethodHead && probe.Header().Get("Content-Length") != "10" {
				t.Errorf("HEAD headers: %v", probe.Header())
			}
		})
	}
}

func TestBlobServerFilesystemErrors(t *testing.T) {
	for _, name := range []string{"open", "shorter", "longer"} {
		t.Run(name, func(t *testing.T) {
			bs, spy, dgst := newFilesystemBlobServer(t, "0123456789")
			if name == "open" {
				spy.readErr = os.ErrPermission
			} else {
				body := "short"
				if name == "longer" {
					body = "different size"
				}
				if err := spy.PutContent(context.Background(), "/blob", []byte(body)); err != nil {
					t.Fatal(err)
				}
			}
			w := httptest.NewRecorder()
			err := bs.ServeBlob(context.Background(), w, httptest.NewRequest(http.MethodGet, "/blob", nil), dgst)
			if name == "open" {
				if !errors.Is(err, os.ErrPermission) {
					t.Fatalf("open error = %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "size mismatch") {
				t.Fatalf("size error = %v", err)
			}
			assertBlobReadersClosed(t, spy)
			if len(w.Header()) != 0 || w.Body.Len() != 0 {
				t.Fatal("response written before validation")
			}
		})
	}
}

func TestBlobServerFilesystemWrappedReader(t *testing.T) {
	bs, spy, dgst := newFilesystemBlobServer(t, "0123456789")
	spy.wrap = func(rc io.ReadCloser) io.ReadCloser { return struct{ io.ReadCloser }{rc} }
	probe := &fileBodyProbe{ResponseRecorder: httptest.NewRecorder()}
	ctx, w := dcontext.WithResponseWriter(context.Background(), probe)
	if err := bs.ServeBlob(ctx, w, httptest.NewRequest(http.MethodGet, "/blob", nil), dgst); err != nil {
		t.Fatal(err)
	}
	assertBlobReadersClosed(t, spy)
	if spy.reads != 2 || probe.sawFile || probe.Code != http.StatusOK || probe.Body.String() != "0123456789" || ctx.Value("http.response.written") != int64(10) {
		t.Fatalf("Reader calls = %d, file source = %t, response = %d %q", spy.reads, probe.sawFile, probe.Code, probe.Body.String())
	}
}

func TestBlobServerFilesystemHTTP(t *testing.T) {
	// Exceed net/http's initial buffered copy so the test can reach sendfile on Linux.
	payload := make([]byte, 64*1024)
	// Use varied data so incorrect range offsets change the response body.
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range payload {
		payload[i] = byte(rng.Uint32())
	}
	body := string(payload)
	bs, spy, dgst := newFilesystemBlobServer(t, body)
	type result struct {
		err     error
		status  any
		written any
	}
	results := make(chan result, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, w := dcontext.WithResponseWriter(r.Context(), w)
		err := bs.ServeBlob(ctx, w, r, dgst)
		results <- result{err, ctx.Value("http.response.status"), ctx.Value("http.response.written")}
	}))
	defer server.Close()
	client := server.Client()
	client.Timeout = 5 * time.Second
	for i, tc := range []struct {
		rangeHeader string
		wantBody    string
		wantStatus  int
	}{
		{wantBody: body, wantStatus: http.StatusOK},
		{rangeHeader: "bytes=1025-32768", wantBody: body[1025:32769], wantStatus: http.StatusPartialContent},
		{wantBody: body, wantStatus: http.StatusOK},
	} {
		reused := false
		ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused },
		})
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Range", tc.rangeHeader)
		response, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		gotBody, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || response.StatusCode != tc.wantStatus || string(gotBody) != tc.wantBody {
			t.Fatalf("request %d: status = %d, body length = %d, error = %v", i, response.StatusCode, len(gotBody), err)
		}
		if response.ContentLength != int64(len(tc.wantBody)) || (i > 0 && !reused) {
			t.Errorf("Content-Length = %d, connection reused = %t", response.ContentLength, reused)
		}
		select {
		case got := <-results:
			if got.err != nil || got.status != tc.wantStatus || got.written != int64(len(tc.wantBody)) {
				t.Errorf("ServeBlob result = %+v", got)
			}
		case <-time.After(client.Timeout):
			t.Fatal("blob handler did not finish")
		}
		assertBlobReadersClosed(t, spy)
	}
}
