package ghactions

import (
	"net/http"

	"github.com/buchgr/bazel-remote/v2/cache"
)

func LoggingTransport(l cache.Logger) http.RoundTripper {
	return loggingTransport{
		Logger:     l,
		Underlying: http.DefaultTransport,
	}
}

var _ http.RoundTripper = (*loggingTransport)(nil)

type loggingTransport struct {
	Logger     cache.Logger
	Underlying http.RoundTripper
}

// RoundTrip implements http.RoundTripper.
func (l loggingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	l.Logger.Printf("GHACTIONS - %s - %s", req.Method, req.URL)

	return l.Underlying.RoundTrip(req)
}
