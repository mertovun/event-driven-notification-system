package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"runtime"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/mertovun/event-driven-notification-system/internal/idempotency"
	"github.com/mertovun/event-driven-notification-system/internal/observability"
	"github.com/mertovun/event-driven-notification-system/internal/store/gen"
	"github.com/mertovun/event-driven-notification-system/internal/ws"
)

// BuildInfo is injected from main via -ldflags. Surfaces at /version.
type BuildInfo struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildTime string `json:"build_time"`
}

// AMQPHealthChecker is the readiness contract for the AMQP layer.
// Implemented by *queue.Publisher; satisfied by nil for the api-only mode.
type AMQPHealthChecker interface {
	Ping(ctx context.Context) error
}

// Deps holds the dependencies handlers need. Wired in main; passed to NewRouter.
type Deps struct {
	Pool         *pgxpool.Pool
	Queries      *gen.Queries
	Idempotency  *idempotency.Store
	Redis        *redis.Client
	AMQP         AMQPHealthChecker
	Metrics      *observability.Metrics
	WSHub        *ws.Hub
	Logger       *slog.Logger
	BuildInfo    BuildInfo
	PProfEnabled bool
}

// NewRouter builds the Chi router with the middleware chain and operational endpoints.
// Resource handlers (notifications, templates, admin) are mounted by subsequent phases.
func NewRouter(d Deps) http.Handler {
	r := chi.NewRouter()

	// Global middleware chain. Order matters.
	r.Use(CorrelationID)
	r.Use(WithLogger(d.Logger))
	r.Use(AccessLog)
	r.Use(MetricsMiddleware(d.Metrics))
	r.Use(Recoverer)

	// Operational endpoints — no auth, no body-size limit (they receive nothing).
	r.Get("/livez", livezHandler)
	r.Get("/readyz", readyzHandler(d))
	r.Get("/version", versionHandler(d.BuildInfo))
	if d.Metrics != nil {
		r.Method(http.MethodGet, "/metrics", d.Metrics.Handler())
	}

	// API spec + Swagger UI — no auth so reviewers can browse without a key.
	r.Get("/openapi.yaml", openAPIHandler)
	r.Get("/docs", swaggerHandler)

	// pprof — opt-in via PPROF_ENABLED env var. Production runs leave this off.
	// Loopback-only is enforced by the docker-compose port binding (127.0.0.1).
	if d.PProfEnabled {
		mountPProf(r)
	}

	notifH := newNotificationsHandler(d, d.Idempotency)
	tmplH := newTemplatesHandler(d)
	adminH := newAdminHandler(d, notifH)

	// V1 resource routes — auth, body-size limit, then resource handlers.
	r.Route("/v1", func(api chi.Router) {
		api.Use(MaxBodyBytes(MaxBodyBytesDefault))
		api.Use(AuthMiddleware(d.Queries, d.Redis))

		// notifications — write
		api.Route("/notifications", func(n chi.Router) {
			n.With(RequireScope(ScopeWrite)).Post("/", notifH.create)
			n.With(MaxBodyBytes(MaxBodyBytesBatch), RequireScope(ScopeWrite)).
				Post("/batch", notifH.createBatch)
			n.With(RequireScope(ScopeWrite)).Delete("/{id}", notifH.cancel)
			n.With(RequireScope(ScopeRead)).Get("/{id}", notifH.get)
			n.With(RequireScope(ScopeRead)).Get("/batch/{batchId}", notifH.getByBatch)
			n.With(RequireScope(ScopeRead)).Get("/", notifH.list)
		})

		// websocket — read scope only.
		if d.WSHub != nil {
			api.With(RequireScope(ScopeRead)).
				Get("/ws/notifications", ws.Handler(d.WSHub, ws.DefaultConfig(), d.Logger))
		}

		// admin — all routes require the admin scope.
		api.Route("/admin", func(a chi.Router) {
			a.Use(RequireScope(ScopeAdmin))
			a.Get("/dead-letters", adminH.list)
			a.Get("/dead-letters/{id}", adminH.get)
			a.Post("/dead-letters/{id}/replay", adminH.replay)
			a.Delete("/dead-letters/{id}", adminH.purge)
		})

		// templates
		api.Route("/templates", func(t chi.Router) {
			t.With(RequireScope(ScopeWrite)).Post("/", tmplH.create)
			t.With(RequireScope(ScopeRead)).Get("/", tmplH.list)
			t.With(RequireScope(ScopeRead)).Get("/{id}", tmplH.get)
			t.With(RequireScope(ScopeWrite)).Put("/{id}", tmplH.put)
			t.With(RequireScope(ScopeWrite)).Delete("/{id}", tmplH.del)
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
// 200 only when every wired dependency passes its check; 503 otherwise.
// /livez = process alive (orchestrator restart signal).
// /readyz = dependencies reachable (orchestrator routing signal).
func readyzHandler(d Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		checks := map[string]string{}
		ok := true

		if d.Pool != nil {
			pingCtx, cancel := contextWithTimeout(ctx, time.Second)
			if err := d.Pool.Ping(pingCtx); err != nil {
				checks["postgres"] = "unreachable: " + err.Error()
				ok = false
			} else {
				checks["postgres"] = "ok"
			}
			cancel()
		}

		if d.Redis != nil {
			pingCtx, cancel := contextWithTimeout(ctx, time.Second)
			if err := d.Redis.Ping(pingCtx).Err(); err != nil {
				checks["redis"] = "unreachable: " + err.Error()
				ok = false
			} else {
				checks["redis"] = "ok"
			}
			cancel()
		}

		if d.AMQP != nil {
			pingCtx, cancel := contextWithTimeout(ctx, time.Second)
			if err := d.AMQP.Ping(pingCtx); err != nil {
				checks["rabbitmq"] = "unreachable: " + err.Error()
				ok = false
			} else {
				checks["rabbitmq"] = "ok"
			}
			cancel()
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

// Avoid an unused-import warning on `context` if the handler isn't using it directly.
var _ = context.TODO

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
