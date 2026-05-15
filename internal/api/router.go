package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"runtime"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mertovun/event-driven-notification-system/internal/store/gen"
)

// BuildInfo is injected from main via -ldflags. Surfaces at /version.
type BuildInfo struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildTime string `json:"build_time"`
}

// Deps holds the dependencies handlers need. Wired in main; passed to NewRouter.
type Deps struct {
	Pool      *pgxpool.Pool
	Queries   *gen.Queries
	Logger    *slog.Logger
	BuildInfo BuildInfo
}

// NewRouter builds the Chi router with the middleware chain and operational endpoints.
// Resource handlers (notifications, templates, admin) are mounted by subsequent phases.
func NewRouter(d Deps) http.Handler {
	r := chi.NewRouter()

	// Global middleware chain. Order matters.
	r.Use(CorrelationID)
	r.Use(WithLogger(d.Logger))
	r.Use(AccessLog)
	r.Use(Recoverer)

	// Operational endpoints — no auth, no body-size limit (they receive nothing).
	r.Get("/livez", livezHandler)
	r.Get("/readyz", readyzHandler(d.Pool))
	r.Get("/version", versionHandler(d.BuildInfo))

	// V1 resource routes — auth, body-size limit, then resource handlers.
	r.Route("/v1", func(api chi.Router) {
		api.Use(MaxBodyBytes(MaxBodyBytesDefault))
		api.Use(AuthMiddleware(d.Queries))

		// Smoke endpoint while real routes are wired in steps 2.4–2.8.
		// Returns 200 with the authenticated key's name. Remove once real routes land.
		api.Get("/whoami", func(w http.ResponseWriter, r *http.Request) {
			k, _ := AuthedKeyFrom(r.Context())
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name":   k.Name,
				"scopes": k.Scopes,
			})
		})
	})

	r.NotFound(noStaticFiles)

	return r
}

func livezHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// readyzHandler returns a JSON breakdown of dependency health.
// 200 only when every dependency check passes; 503 otherwise. See docs/06-observability.md §6.
func readyzHandler(pool *pgxpool.Pool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		// Per-check budget; do not let a slow dep block /readyz.
		// Total handler budget is bounded by the chi+server timeouts above.
		checks := map[string]string{"postgres": "ok"}
		ok := true

		if pool != nil {
			pingCtx, cancel := contextWithTimeout(ctx, time.Second)
			defer cancel()
			if err := pool.Ping(pingCtx); err != nil {
				checks["postgres"] = "unreachable: " + err.Error()
				ok = false
			}
		} else {
			checks["postgres"] = "not configured"
			ok = false
		}

		w.Header().Set("Content-Type", "application/json")
		if !ok {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"ok":     ok,
			"checks": checks,
		})
	}
}

func versionHandler(bi BuildInfo) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		body := struct {
			Version   string `json:"version"`
			Commit    string `json:"commit"`
			BuildTime string `json:"build_time"`
			GoVersion string `json:"go_version"`
		}{bi.Version, bi.Commit, bi.BuildTime, runtime.Version()}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(body)
	}
}
