package dcontext

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/gorilla/handlers"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestWithRequest(t *testing.T) {
	var req http.Request

	start := time.Now()
	req.Method = http.MethodGet
	req.Host = "example.com"
	req.RequestURI = "/test-test"
	req.Header = make(http.Header)
	req.Header.Set("Referer", "foo.com/referer")
	req.Header.Set("User-Agent", "test/0.1")

	ctx := WithRequest(Background(), &req)
	for _, tc := range []struct {
		key      string
		expected any
	}{
		{
			key:      "http.request",
			expected: &req,
		},
		{
			key: "http.request.id",
		},
		{
			key:      "http.request.method",
			expected: req.Method,
		},
		{
			key:      "http.request.host",
			expected: req.Host,
		},
		{
			key:      "http.request.uri",
			expected: req.RequestURI,
		},
		{
			key:      "http.request.referer",
			expected: req.Referer(),
		},
		{
			key:      "http.request.useragent",
			expected: req.UserAgent(),
		},
		{
			key:      "http.request.remoteaddr",
			expected: req.RemoteAddr,
		},
		{
			key: "http.request.startedat",
		},
	} {
		v := ctx.Value(tc.key)

		if v == nil {
			t.Fatalf("value not found for %q", tc.key)
		}

		if tc.expected != nil && v != tc.expected {
			t.Fatalf("%s: %v != %v", tc.key, v, tc.expected)
		}

		// Key specific checks!
		switch tc.key {
		case "http.request.id":
			if _, ok := v.(string); !ok {
				t.Fatalf("request id not a string: %v", v)
			}
		case "http.request.startedat":
			vt, ok := v.(time.Time)
			if !ok {
				t.Fatalf("value not a time: %v", v)
			}

			now := time.Now()
			if vt.After(now) {
				t.Fatalf("time generated too late: %v > %v", vt, now)
			}

			if vt.Before(start) {
				t.Fatalf("time generated too early: %v < %v", vt, start)
			}
		}
	}
}

type testResponseWriter struct {
	flushed bool
	status  int
	written int64
	header  http.Header
}

func (trw *testResponseWriter) Header() http.Header {
	if trw.header == nil {
		trw.header = make(http.Header)
	}

	return trw.header
}

func (trw *testResponseWriter) Write(p []byte) (n int, err error) {
	if trw.status == 0 {
		trw.status = http.StatusOK
	}

	n = len(p)
	trw.written += int64(n)
	return
}

func (trw *testResponseWriter) WriteHeader(status int) {
	trw.status = status
}

func (trw *testResponseWriter) Flush() {
	trw.flushed = true
}

type testReaderFromResponseWriter struct {
	testResponseWriter
	source io.Reader
}

func (trw *testReaderFromResponseWriter) ReadFrom(r io.Reader) (int64, error) {
	trw.source = r
	return io.Copy(&trw.testResponseWriter, r)
}

func TestWithResponseWriter(t *testing.T) {
	trw := testResponseWriter{}
	ctx, rw := WithResponseWriter(Background(), &trw)

	if ctx.Value("http.response") != rw {
		t.Fatalf("response not available in context: %v != %v", ctx.Value("http.response"), rw)
	}

	grw, err := GetResponseWriter(ctx)
	if err != nil {
		t.Fatalf("error getting response writer: %v", err)
	}

	if grw != rw {
		t.Fatalf("unexpected response writer returned: %#v != %#v", grw, rw)
	}

	if ctx.Value("http.response.status") != 0 {
		t.Fatalf("response status should always be a number and should be zero here: %v != 0", ctx.Value("http.response.status"))
	}

	if n, err := rw.Write(make([]byte, 1024)); err != nil {
		t.Fatalf("unexpected error writing: %v", err)
	} else if n != 1024 {
		t.Fatalf("unexpected number of bytes written: %v != %v", n, 1024)
	}

	if ctx.Value("http.response.status") != http.StatusOK {
		t.Fatalf("unexpected response status in context: %v != %v", ctx.Value("http.response.status"), http.StatusOK)
	}

	if ctx.Value("http.response.written") != int64(1024) {
		t.Fatalf("unexpected number reported bytes written: %v != %v", ctx.Value("http.response.written"), 1024)
	}

	// Make sure flush propagates
	rw.(http.Flusher).Flush()

	if !trw.flushed {
		t.Fatal("response writer not flushed")
	}

	// Write another status and make sure context is correct. This normally
	// wouldn't work except for in this contrived testcase.
	rw.WriteHeader(http.StatusBadRequest)

	if ctx.Value("http.response.status") != http.StatusBadRequest {
		t.Fatalf("unexpected response status in context: %v != %v", ctx.Value("http.response.status"), http.StatusBadRequest)
	}
}

func TestWithResponseWriterReadFrom(t *testing.T) {
	copyErr := errors.New("copy failed")
	for _, delegate := range []bool{true, false} {
		name := "fallback"
		if delegate {
			name = "delegate"
		}
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				name       string
				body       string
				status     int
				prefix     string
				readErr    error
				wantStatus int
			}{
				{name: "full", body: "content", wantStatus: http.StatusOK},
				{name: "explicit-status", body: "content", status: http.StatusPartialContent, wantStatus: http.StatusPartialContent},
				{name: "after-write", body: "content", prefix: "abc", wantStatus: http.StatusOK},
				{name: "partial-error", body: "part", readErr: copyErr, wantStatus: http.StatusOK},
				{name: "empty"},
				{name: "error-without-bytes", readErr: copyErr},
			} {
				t.Run(tc.name, func(t *testing.T) {
					trw := &testResponseWriter{}
					rf := &testReaderFromResponseWriter{}
					var destination http.ResponseWriter = trw
					if delegate {
						destination = rf
						trw = &rf.testResponseWriter
					}
					ctx, rw := WithResponseWriter(Background(), destination)
					if tc.status != 0 {
						rw.WriteHeader(tc.status)
					}
					if tc.prefix != "" {
						if _, err := io.WriteString(rw, tc.prefix); err != nil {
							t.Fatal(err)
						}
					}
					var source io.Reader = strings.NewReader(tc.body)
					copySize := int64(len(tc.body))
					if tc.readErr != nil {
						source = io.MultiReader(source, iotest.ErrReader(tc.readErr))
						copySize++
					}
					// Use io.CopyN to exercise ReadFrom even when the source implements WriterTo.
					n, err := io.CopyN(rw, source, copySize)
					if n != int64(len(tc.body)) || !errors.Is(err, tc.readErr) {
						t.Fatalf("CopyN = (%d, %v), want (%d, %v)", n, err, len(tc.body), tc.readErr)
					}
					if delegate {
						limited, ok := rf.source.(*io.LimitedReader)
						if !ok || limited.R != source {
							t.Fatalf("ReadFrom received %T; want the original limited source", rf.source)
						}
					}
					if got := ctx.Value("http.response.status"); got != tc.wantStatus || trw.status != tc.wantStatus {
						t.Errorf("recorded status = %v, writer status = %d, want %d", got, trw.status, tc.wantStatus)
					}
					wantWritten := int64(len(tc.prefix) + len(tc.body))
					if got := ctx.Value("http.response.written"); got != wantWritten || trw.written != wantWritten {
						t.Errorf("recorded bytes = %v, writer bytes = %d, want %d", got, trw.written, wantWritten)
					}
				})
			}
		})
	}
}

func TestWithResponseWriterGorillaLogging(t *testing.T) {
	const body = "0123456789"
	for _, delegate := range []bool{false, true} {
		name := "fallback"
		if delegate {
			name = "delegate"
		}
		t.Run(name, func(t *testing.T) {
			trw := &testResponseWriter{}
			var destination http.ResponseWriter = trw
			if delegate {
				rf := &testReaderFromResponseWriter{}
				destination = rf
				trw = &rf.testResponseWriter
			}
			loggedBytes := -1
			// Match the registry's wrapper order: Gorilla outside WithResponseWriter.
			h := handlers.CustomLoggingHandler(io.Discard, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				ctx, rw := WithResponseWriter(Background(), w)
				rw.WriteHeader(http.StatusOK)
				// CopyN selects ReadFrom even though strings.Reader implements WriterTo.
				n, err := io.CopyN(rw, strings.NewReader(body), int64(len(body)))
				if err != nil || n != int64(len(body)) {
					t.Fatalf("CopyN = (%d, %v), want (%d, nil)", n, err, len(body))
				}
				if got := ctx.Value("http.response.written"); got != int64(len(body)) {
					t.Errorf("Distribution recorded bytes = %v, want %d", got, len(body))
				}
			}), func(_ io.Writer, p handlers.LogFormatterParams) { loggedBytes = p.Size })
			h.ServeHTTP(destination, httptest.NewRequest(http.MethodGet, "/blob", nil))
			if trw.written != int64(len(body)) {
				t.Fatalf("response bytes = %d, want %d", trw.written, len(body))
			}
			if loggedBytes != len(body) {
				t.Errorf("Gorilla logged bytes = %d, want %d (response bytes = %d)", loggedBytes, len(body), trw.written)
			}
		})
	}
}

func TestWithResponseWriterOpenTelemetryAccounting(t *testing.T) {
	const body = "0123456789"
	trw := &testReaderFromResponseWriter{}
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		if err := provider.Shutdown(Background()); err != nil {
			t.Error(err)
		}
	})
	h := otelhttp.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, rw := WithResponseWriter(r.Context(), w)
		rw.WriteHeader(http.StatusOK)
		// CopyN selects ReadFrom even though strings.Reader implements WriterTo.
		if _, err := io.CopyN(rw, strings.NewReader(body), int64(len(body))); err != nil {
			t.Fatal(err)
		}
		if got := ctx.Value("http.response.written"); got != int64(len(body)) || trw.written != int64(len(body)) {
			t.Fatalf("Distribution bytes = %v, response bytes = %d, want %d", got, trw.written, len(body))
		}
	}), "blob", otelhttp.WithMeterProvider(provider))
	h.ServeHTTP(trw, httptest.NewRequest(http.MethodGet, "/blob", nil))
	var metrics metricdata.ResourceMetrics
	if err := reader.Collect(Background(), &metrics); err != nil {
		t.Fatal(err)
	}
	for _, scope := range metrics.ScopeMetrics {
		for _, metric := range scope.Metrics {
			if metric.Name != "http.server.response.body.size" {
				continue
			}
			histogram, ok := metric.Data.(metricdata.Histogram[int64])
			if !ok || len(histogram.DataPoints) != 1 {
				t.Fatalf("unexpected response body histogram: %#v", metric.Data)
			}
			point := histogram.DataPoints[0]
			if point.Count != 1 || point.Sum != int64(len(body)) {
				t.Errorf("OpenTelemetry response body histogram: count=%d sum=%d, want count=1 sum=%d (response bytes = %d)", point.Count, point.Sum, len(body), trw.written)
			}
			return
		}
	}
	t.Fatal("http.server.response.body.size metric missing")
}

func TestWithVars(t *testing.T) {
	var req http.Request
	vars := map[string]string{
		"foo": "asdf",
		"bar": "qwer",
	}

	getVarsFromRequest = func(r *http.Request) map[string]string {
		if r != &req {
			t.Fatalf("unexpected request: %v != %v", r, req)
		}

		return vars
	}

	ctx := WithVars(Background(), &req)
	for _, tc := range []struct {
		key      string
		expected any
	}{
		{
			key:      "vars",
			expected: vars,
		},
		{
			key:      "vars.foo",
			expected: "asdf",
		},
		{
			key:      "vars.bar",
			expected: "qwer",
		},
	} {
		v := ctx.Value(tc.key)

		if !reflect.DeepEqual(v, tc.expected) {
			t.Fatalf("%q: %v != %v", tc.key, v, tc.expected)
		}
	}
}
