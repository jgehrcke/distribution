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

// Cover direct-file selection and lazy opening; ServeContent owns the detailed
// conditional and range-header behavior.
func TestBlobServerFilesystemContent(t *testing.T) {
	for _, tc := range []struct {
		name, method, rangeHeader, wantBody string
		precondition, preconditionValue     string
		wantStatus, wantReads               int
		wantFile                            bool
	}{
		{name: "full", method: http.MethodGet, wantBody: "0123456789", wantStatus: http.StatusOK, wantReads: 1, wantFile: true},
		{name: "range", method: http.MethodGet, rangeHeader: "bytes=2-5", wantBody: "2345", wantStatus: http.StatusPartialContent, wantReads: 1, wantFile: true},
		{name: "head", method: http.MethodHead, wantStatus: http.StatusOK},
		{name: "not-modified", method: http.MethodGet, precondition: "If-None-Match", preconditionValue: "match", wantStatus: http.StatusNotModified},
		{name: "failed-precondition", method: http.MethodGet, precondition: "If-Match", preconditionValue: `"other"`, wantStatus: http.StatusPreconditionFailed},
		{name: "if-match-pass", method: http.MethodGet, precondition: "If-Match", preconditionValue: "match", wantBody: "0123456789", wantStatus: http.StatusOK, wantReads: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bs, spy, dgst := newFilesystemBlobServer(t, "0123456789")
			req := httptest.NewRequest(tc.method, "/blob", nil)
			req.Header.Set("Range", tc.rangeHeader)
			if tc.precondition != "" {
				value := tc.preconditionValue
				if value == "match" {
					value = `"` + dgst.String() + `"`
				}
				req.Header.Set(tc.precondition, value)
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

func TestBlobServerFilesystemOpenError(t *testing.T) {
	bs, spy, dgst := newFilesystemBlobServer(t, "0123456789")
	spy.readErr = os.ErrPermission
	w := httptest.NewRecorder()
	err := bs.ServeBlob(context.Background(), w, httptest.NewRequest(http.MethodGet, "/blob", nil), dgst)
	if !errors.Is(err, os.ErrPermission) {
		t.Fatalf("open error = %v", err)
	}
	if len(w.Header()) != 0 || w.Body.Len() != 0 {
		t.Fatal("response written before opening the blob")
	}
}

// A blob file missing after Stat must fail before HTTP 200 is committed;
// the lazy fileReader would serve an empty body with the descriptor length.
func TestBlobServerFilesystemMissingFile(t *testing.T) {
	bs, spy, dgst := newFilesystemBlobServer(t, "0123456789")
	if err := spy.Delete(context.Background(), "/blob"); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	err := bs.ServeBlob(context.Background(), w, httptest.NewRequest(http.MethodGet, "/blob", nil), dgst)
	if !errors.As(err, new(driver.PathNotFoundError)) {
		t.Fatalf("missing file error = %v", err)
	}
	if len(w.Header()) != 0 || w.Body.Len() != 0 {
		t.Fatal("response written for a missing blob file")
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

type nonFilesystemDriver struct{ *blobReaderSpy }

func (nonFilesystemDriver) Name() string { return "non-filesystem" }

func TestBlobServerNonFilesystemReader(t *testing.T) {
	bs, spy, dgst := newFilesystemBlobServer(t, "0123456789")
	spy.wrap = func(rc io.ReadCloser) io.ReadCloser { return struct{ io.ReadCloser }{rc} }
	bs.driver = nonFilesystemDriver{spy}
	probe := &fileBodyProbe{ResponseRecorder: httptest.NewRecorder()}
	if err := bs.ServeBlob(context.Background(), probe, httptest.NewRequest(http.MethodGet, "/blob", nil), dgst); err != nil {
		t.Fatal(err)
	}
	assertBlobReadersClosed(t, spy)
	// Non-filesystem drivers must avoid a speculative Reader call before the lazy read.
	if spy.reads != 1 || probe.sawFile || probe.Code != http.StatusOK || probe.Body.String() != "0123456789" {
		t.Fatalf("Reader calls = %d, file source = %t, response = %d %q", spy.reads, probe.sawFile, probe.Code, probe.Body.String())
	}
}

func TestBlobServerFilesystemHTTP(t *testing.T) {
	for _, tc := range []struct {
		name       string
		tls, http2 bool
	}{
		{name: "http1"},
		{name: "tls-http1", tls: true},
		{name: "tls-http2", tls: true, http2: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testBlobServerFilesystemHTTP(t, tc.tls, tc.http2)
		})
	}
}

func testBlobServerFilesystemHTTP(t *testing.T, useTLS, useHTTP2 bool) {
	t.Helper()
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
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, w := dcontext.WithResponseWriter(r.Context(), w)
		err := bs.ServeBlob(ctx, w, r, dgst)
		results <- result{err, ctx.Value("http.response.status"), ctx.Value("http.response.written")}
	}))
	server.EnableHTTP2 = useHTTP2
	if useTLS {
		server.StartTLS()
	} else {
		server.Start()
	}
	defer server.Close()
	client := server.Client()
	client.Timeout = 5 * time.Second
	wantProtocol := 1
	if useHTTP2 {
		wantProtocol = 2
	}
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
		if response.ProtoMajor != wantProtocol {
			t.Errorf("protocol = %s, want HTTP/%d", response.Proto, wantProtocol)
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

// Exercise multipart's buffered copy path; net/http tests the MIME format.
func TestBlobServerFilesystemMultipartRange(t *testing.T) {
	bs, spy, dgst := newFilesystemBlobServer(t, "0123456789")
	req := httptest.NewRequest(http.MethodGet, "/blob", nil)
	req.Header.Set("Range", "bytes=0-1,8-9")
	w := httptest.NewRecorder()
	ctx, rw := dcontext.WithResponseWriter(context.Background(), w)
	if err := bs.ServeBlob(ctx, rw, req, dgst); err != nil {
		t.Fatal(err)
	}
	assertBlobReadersClosed(t, spy)
	if w.Code != http.StatusPartialContent || spy.reads != 1 {
		t.Fatalf("status = %d, Reader calls = %d", w.Code, spy.reads)
	}
	if !strings.HasPrefix(w.Header().Get("Content-Type"), "multipart/byteranges;") || w.Body.Len() == 0 {
		t.Fatalf("expected multipart response: Content-Type = %q, bytes = %d", w.Header().Get("Content-Type"), w.Body.Len())
	}
	if ctx.Value("http.response.status") != http.StatusPartialContent || ctx.Value("http.response.written") != int64(w.Body.Len()) {
		t.Fatalf("recorded status = %v, bytes = %v", ctx.Value("http.response.status"), ctx.Value("http.response.written"))
	}
}
