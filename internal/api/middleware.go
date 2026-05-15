package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
)

// ctxKey is a private type to prevent context key collisions.
type ctxKey int

const (
	ctxKeyCorrelationID ctxKey = iota
	ctxKeyLogger
)

const (
	// MaxBodyBytesDefault caps single-resource request bodies.
	MaxBodyBytesDefault int64 = 1 << 20 // 1 MiB
	// MaxBodyBytesBatch caps batch request bodies (up to 1000 items).
	MaxBodyBytesBatch int64 = 4 << 20 // 4 MiB
)

// CorrelationID middleware reads X-Request-Id from the request, generates a UUIDv7
// if missing, attaches it to context, and echoes it on the response.
func CorrelationID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-Id")
		if id == "" {
			if u, err := uuid.NewV7(); err == nil {
				id = u.String()
			} else {
				id = uuid.NewString() // fall back to v4 if v7 ever errors
			}
		}
		w.Header().Set("X-Request-Id", id)
		ctx := context.WithValue(r.Context(), ctxKeyCorrelationID, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// CorrelationIDFrom extracts the correlation id from context, empty string if absent.
func CorrelationIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(ctxKeyCorrelationID).(string)
	return id
}

// WithLogger attaches a base logger to context so handlers can pull a request-scoped logger.
func WithLogger(base *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := context.WithValue(r.Context(), ctxKeyLogger, base)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// LoggerFrom returns the request-scoped logger; if none, the default slog logger.
// The returned logger always carries the correlation_id attribute.
func LoggerFrom(ctx context.Context) *slog.Logger {
	l, ok := ctx.Value(ctxKeyLogger).(*slog.Logger)
	if !ok {
		l = slog.Default()
	}
	if cid := CorrelationIDFrom(ctx); cid != "" {
		l = l.With("correlation_id", cid)
	}
	return l
}

// AccessLog emits one structured log line per request after it completes.
// 5xx → ERROR, 4xx → WARN, otherwise INFO. /livez, /readyz, /metrics suppressed.
func AccessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Suppress health/metrics endpoints to keep log volume sane.
		switch r.URL.Path {
		case "/livez", "/readyz", "/metrics":
			next.ServeHTTP(w, r)
			return
		}

		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)
		dur := time.Since(start)

		logger := LoggerFrom(r.Context())
		level := slog.LevelInfo
		status := ww.Status()
		switch {
		case status >= 500:
			level = slog.LevelError
		case status >= 400:
			level = slog.LevelWarn
		}
		logger.LogAttrs(r.Context(), level, "http request",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", status),
			slog.Int("bytes", ww.BytesWritten()),
			slog.Duration("dur", dur),
		)
	})
}

// Recoverer catches panics, logs at ERROR with stack, and emits a 500 problem details body.
func Recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				logger := LoggerFrom(r.Context())
				logger.LogAttrs(r.Context(), slog.LevelError, "panic recovered",
					slog.Any("panic", rec),
					slog.String("stack", string(debug.Stack())),
				)
				WriteProblem(w, r, http.StatusInternalServerError,
					"about:blank", "Internal Server Error", "an unexpected error occurred")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// MaxBodyBytes wraps the request body with http.MaxBytesReader.
// Use a higher limit on the batch endpoint via BatchSize.
func MaxBodyBytes(limit int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r.Body = http.MaxBytesReader(w, r.Body, limit)
			next.ServeHTTP(w, r)
		})
	}
}

// noStaticFiles is a tiny guard returning 404 for unknown routes that look like file probes.
// Chi's NotFound is used directly; this is a placeholder for future static asset handling.
func noStaticFiles(w http.ResponseWriter, r *http.Request) {
	WriteProblem(w, r, http.StatusNotFound, "about:blank", "Not Found",
		fmt.Sprintf("no route for %s %s", r.Method, r.URL.Path))
}
