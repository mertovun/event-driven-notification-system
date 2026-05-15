package api

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/mertovun/event-driven-notification-system/internal/observability"
)

// MetricsMiddleware records HTTP request count + latency per (method, route, status).
// Route uses the chi pattern (e.g. /v1/notifications/{id}) so cardinality stays bounded.
func MetricsMiddleware(m *observability.Metrics) func(http.Handler) http.Handler {
	if m == nil {
		return func(next http.Handler) http.Handler { return next }
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Skip operational endpoints to keep cardinality lean.
			switch r.URL.Path {
			case "/livez", "/readyz", "/metrics", "/version", "/openapi.yaml", "/docs":
				next.ServeHTTP(w, r)
				return
			}

			m.HTTPRequestsInFlight.Inc()
			defer m.HTTPRequestsInFlight.Dec()

			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
			start := time.Now()
			next.ServeHTTP(ww, r)
			dur := time.Since(start).Seconds()

			route := chi.RouteContext(r.Context()).RoutePattern()
			if route == "" {
				route = "unknown"
			}
			status := strconv.Itoa(ww.Status())
			m.HTTPRequestsTotal.WithLabelValues(r.Method, route, status).Inc()
			m.HTTPRequestDurationSecs.WithLabelValues(r.Method, route).Observe(dur)
		})
	}
}
