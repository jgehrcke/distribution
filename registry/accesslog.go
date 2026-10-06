package registry

import (
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/felixge/httpsnoop"
)

// accessLogHandler logs requests to out in Apache Combined Log Format, like
// gorilla/handlers.CombinedLoggingHandler. Gorilla counts response bytes only
// in Write and logs zero bytes for responses copied through io.ReaderFrom,
// which net/http uses for sendfile. httpsnoop.CaptureMetrics counts both
// paths and still delegates ReadFrom to the wrapped writer.
func accessLogHandler(out io.Writer, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ts, u := time.Now(), *r.URL
		m := httpsnoop.CaptureMetrics(h, w, r)
		_, _ = out.Write(combinedLogLine(r, u, ts, m.Code, m.Written))
	})
}

// combinedLogLine produces the same line as Gorilla's writeCombinedLog.
func combinedLogLine(req *http.Request, u url.URL, ts time.Time, status int, size int64) []byte {
	username := "-"
	if u.User != nil {
		if name := u.User.Username(); name != "" {
			username = name
		}
	}
	host, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		host = req.RemoteAddr
	}
	uri := req.RequestURI
	// CONNECT over HTTP/2 identifies the target with the authority field.
	if req.ProtoMajor == 2 && req.Method == http.MethodConnect {
		uri = req.Host
	}
	if uri == "" {
		uri = u.RequestURI()
	}
	return []byte(host + " - " + username + " [" + ts.Format("02/Jan/2006:15:04:05 -0700") +
		`] "` + req.Method + " " + quoteLogField(uri) + " " + req.Proto + `" ` +
		strconv.Itoa(status) + " " + strconv.FormatInt(size, 10) +
		` "` + quoteLogField(req.Referer()) + `" "` + quoteLogField(req.UserAgent()) + "\"\n")
}

// quoteLogField escapes s like Gorilla's appendQuoted. That matches
// strconv.Quote without the outer quotes, except that Gorilla escapes DEL as
// \u007f where strconv.Quote uses \x7f.
func quoteLogField(s string) string {
	parts := strings.Split(s, "\x7f")
	for i, p := range parts {
		q := strconv.Quote(p)
		parts[i] = q[1 : len(q)-1]
	}
	return strings.Join(parts, `\u007f`)
}
