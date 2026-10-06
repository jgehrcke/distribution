package registry

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	gorhandlers "github.com/gorilla/handlers"
)

var logTimestamp = regexp.MustCompile(`\[[^]]*\]`)

// TestAccessLogMatchesGorilla compares accessLogHandler with
// CombinedLoggingHandler on the Write path, where Gorilla counts correctly.
func TestAccessLogMatchesGorilla(t *testing.T) {
	for _, tc := range []struct {
		name, target, referer, userAgent, remoteAddr string
		status                                       int
	}{
		{"plain", "/v2/", "", "docker/28.0", "192.0.2.1:1234", http.StatusOK},
		{"status", "/v2/x/blobs/sha256:00", "", "", "192.0.2.1:1234", http.StatusNotFound},
		{"userinfo", "http://alice@example.com/v2/", "", "", "192.0.2.1:1234", http.StatusOK},
		{"ipv6", "/v2/", "", "", "[2001:db8::1]:443", http.StatusOK},
		{"no port", "/v2/", "", "", "192.0.2.1", http.StatusOK},
		{"escaping", "/v2/%22", "a\"b\\c\td\x7fe\x01\xff", "ü   \U0001F600 \x7f\\x7f", "192.0.2.1:1234", http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte("hello"))
			})
			logLine := func(wrap func(io.Writer, http.Handler) http.Handler) string {
				req := httptest.NewRequest(http.MethodGet, tc.target, nil)
				req.RemoteAddr = tc.remoteAddr
				req.Header.Set("Referer", tc.referer)
				req.Header.Set("User-Agent", tc.userAgent)
				var out bytes.Buffer
				wrap(&out, h).ServeHTTP(httptest.NewRecorder(), req)
				// The handlers take their own timestamps.
				return logTimestamp.ReplaceAllString(out.String(), "[ts]")
			}
			got, want := logLine(accessLogHandler), logLine(gorhandlers.CombinedLoggingHandler)
			if want == "" || got != want {
				t.Errorf("log line mismatch\n got: %q\nwant: %q", got, want)
			}
		})
	}
}

// readerFromRecorder records whether ReadFrom reached the underlying writer.
type readerFromRecorder struct {
	*httptest.ResponseRecorder
	readFromCalls int
}

func (r *readerFromRecorder) ReadFrom(src io.Reader) (int64, error) {
	r.readFromCalls++
	return io.Copy(r.ResponseRecorder, src)
}

// TestAccessLogCountsReadFrom checks that bytes copied through io.ReaderFrom
// are logged and that ReadFrom is still delegated to the wrapped writer.
func TestAccessLogCountsReadFrom(t *testing.T) {
	const body = "0123456789"
	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Hide WriteTo so io.Copy uses the writer's ReadFrom, as for *os.File.
		_, _ = io.Copy(w, struct{ io.Reader }{strings.NewReader(body)})
	})
	var out bytes.Buffer
	rec := &readerFromRecorder{ResponseRecorder: httptest.NewRecorder()}
	accessLogHandler(&out, h).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v2/", nil))

	if rec.readFromCalls != 1 {
		t.Errorf("ReadFrom calls = %d, want 1", rec.readFromCalls)
	}
	if rec.Body.String() != body {
		t.Errorf("body = %q, want %q", rec.Body.String(), body)
	}
	if !strings.Contains(out.String(), `" 200 10 "`) {
		t.Errorf("log line does not report status 200 and 10 bytes: %q", out.String())
	}
}
