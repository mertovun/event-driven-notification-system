// Command notifyd is the event-driven notification system service binary.
// It runs the HTTP API and worker pools as a single process by default;
// --mode selects api / worker / all (default all).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"golang.org/x/sync/errgroup"

	"github.com/mertovun/event-driven-notification-system/internal/api"
	"github.com/mertovun/event-driven-notification-system/internal/config"
	"github.com/mertovun/event-driven-notification-system/internal/events"
	"github.com/mertovun/event-driven-notification-system/internal/idempotency"
	"github.com/mertovun/event-driven-notification-system/internal/observability"
	"github.com/mertovun/event-driven-notification-system/internal/outbox"
	"github.com/mertovun/event-driven-notification-system/internal/provider"
	"github.com/mertovun/event-driven-notification-system/internal/queue"
	"github.com/mertovun/event-driven-notification-system/internal/ratelimit"
	"github.com/mertovun/event-driven-notification-system/internal/scheduler"
	"github.com/mertovun/event-driven-notification-system/internal/store"
	"github.com/mertovun/event-driven-notification-system/internal/store/gen"
	"github.com/mertovun/event-driven-notification-system/internal/worker"
	"github.com/mertovun/event-driven-notification-system/internal/ws"
)

// Injected via -ldflags at build time.
var (
	Version   = "dev"
	Commit    = "none"
	BuildTime = "unknown"
)

func main() {
	// Subcommands run before the long-form flag parsing because they need
	// different argument handling. The distroless final image has no shell
	// or curl, so `notifyd healthcheck` provides a probe that an orchestrator
	// can invoke directly (Docker HEALTHCHECK, k8s liveness exec).
	if len(os.Args) >= 2 && os.Args[1] == "healthcheck" {
		os.Exit(runHealthcheck(os.Args[2:]))
	}

	modeFlag := flag.String("mode", "", "run mode: api | worker | all (overrides MODE env)")
	addrFlag := flag.String("addr", "", "HTTP listen address (overrides HTTP_ADDR env)")
	skipMigrate := flag.Bool("skip-migrate", false, "skip running migrations on boot")
	flag.Parse()

	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(2)
	}
	if *modeFlag != "" {
		cfg.Mode = *modeFlag
	}
	if *addrFlag != "" {
		cfg.HTTPAddr = *addrFlag
	}

	base := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parseLogLevel(cfg.LogLevel)})
	logger := slog.New(observability.WrapHandler(base))
	slog.SetDefault(logger)

	logger.Info("starting notifyd",
		"version", Version,
		"commit", Commit,
		"build_time", BuildTime,
		"mode", cfg.Mode,
		"go", runtime.Version(),
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, logger, *skipMigrate); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, cfg config.Config, logger *slog.Logger, skipMigrate bool) error {
	switch cfg.Mode {
	case "api", "worker", "all":
	default:
		return fmt.Errorf("invalid mode %q (want api | worker | all)", cfg.Mode)
	}

	if skipMigrate {
		logger.Warn("migrations skipped via --skip-migrate flag")
	} else {
		if err := store.Migrate(cfg.DatabaseURL, logger); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}

	pool, err := store.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("pg pool: %w", err)
	}
	defer pool.Close()
	logger.Info("postgres connected", "max_conns", pool.Config().MaxConns)

	q := gen.New(pool)

	metrics := observability.NewMetrics()
	metrics.SetBuildInfo(Version, Commit, runtime.Version())

	// OpenTelemetry tracing — ON by default; falls back to no-op on collector unavailability.
	instanceID, _ := uuid.NewV7()
	otelShutdown, _, err := observability.SetupTracing(ctx, observability.OTelConfig{
		Endpoint:   cfg.OTELEndpoint,
		SampleRate: cfg.OTELSampleRatio,
		Disabled:   cfg.OTELDisabled,
		Service:    "notifyd",
		Version:    Version,
		InstanceID: instanceID.String(),
		Env:        "dev",
	}, logger)
	if err != nil {
		return fmt.Errorf("otel setup: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = otelShutdown(shutdownCtx)
	}()

	redisOpts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return fmt.Errorf("parse redis url: %w", err)
	}
	redisOpts.ReadTimeout = 200 * time.Millisecond
	redisOpts.WriteTimeout = 200 * time.Millisecond
	rdb := redis.NewClient(redisOpts)
	if err := redisotel.InstrumentTracing(rdb); err != nil {
		logger.Warn("redis tracing instrumentation failed", "err", err)
	}
	defer func() { _ = rdb.Close() }()

	pingCtx, cancelPing := context.WithTimeout(ctx, 2*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		cancelPing()
		return fmt.Errorf("redis ping: %w", err)
	}
	cancelPing()
	logger.Info("redis connected")

	idemStore := idempotency.NewStore(rdb, 0)
	eventsPub := events.NewPublisher(rdb, logger)
	wsHub := ws.NewHub(rdb, logger)

	if cfg.DevAPIKey != "" {
		if err := api.SeedDevKey(ctx, q, cfg.DevAPIKey); err != nil {
			return fmt.Errorf("seed dev api key: %w", err)
		}
		logger.Info("dev api key seeded", "prefix", cfg.DevAPIKey[:8])
	}

	// Queue publisher + outbox dispatcher + worker pools run in worker / all mode.
	// Build them before the router so /readyz can include AMQP in the checks.
	var (
		pub      *queue.Publisher
		disp     *outbox.Dispatcher
		schedDsp *scheduler.Dispatcher
		workMgr  *worker.Manager
	)
	if cfg.Mode == "worker" || cfg.Mode == "all" {
		var err error
		pub, err = queue.NewPublisher(ctx, cfg.AMQPURL, logger)
		if err != nil {
			return fmt.Errorf("amqp publisher: %w", err)
		}
		defer func() { _ = pub.Close() }()

		dispID, _ := uuid.NewV7()
		cfgD := outbox.Default(dispID.String())
		cfgD.PollInterval = cfg.OutboxPollInterval
		cfgD.BatchSize = cfg.OutboxBatchSize
		disp = outbox.New(pool, q, pub, metrics, logger, cfgD)

		// Scheduled-notification poller. Shares the main pool — a dedicated
		// pool was tested and made no difference to the bgreader hang
		// documented in KNOWN_ISSUES.md.
		schedID, _ := uuid.NewV7()
		schedDsp = scheduler.New(pool, q, logger, scheduler.Default(schedID.String()))

		// Provider client + rate limiter + worker manager.
		provClient, err := provider.New(cfg.WebhookURL, "notifyd/0.1")
		if err != nil {
			return fmt.Errorf("provider client: %w", err)
		}
		limiter := ratelimit.New(rdb)
		workMgr = worker.NewManager(cfg.AMQPURL, pool, q, rdb, limiter, provClient, pub, metrics, eventsPub, logger, worker.PoolSpec{
			SMSCount:   cfg.WorkerCountSMS,
			EmailCount: cfg.WorkerCountEmail,
			PushCount:  cfg.WorkerCountPush,
		})
	}

	router := api.NewRouter(api.Deps{
		Pool:         pool,
		Queries:      q,
		Idempotency:  idemStore,
		Redis:        rdb,
		AMQP:         amqpChecker(pub),
		Metrics:      metrics,
		WSHub:        wsHub,
		Logger:       logger,
		BuildInfo:    api.BuildInfo{Version: Version, Commit: Commit, BuildTime: BuildTime},
		PProfEnabled: cfg.PProfEnabled,
	})
	tracedRouter := otelhttp.NewHandler(router, "notifyd-http",
		otelhttp.WithFilter(func(r *http.Request) bool {
			switch r.URL.Path {
			case "/livez", "/readyz", "/metrics", "/version", "/openapi.yaml", "/docs":
				return false
			}
			return true
		}),
	)
	srv := newHTTPServer(cfg.HTTPAddr, logger, tracedRouter)

	g, gctx := errgroup.WithContext(ctx)

	if disp != nil {
		g.Go(func() error { return disp.Run(gctx) })
	}
	if schedDsp != nil {
		g.Go(func() error { return schedDsp.Run(gctx) })
	}
	if workMgr != nil {
		g.Go(func() error { return workMgr.Run(gctx) })
	}
	g.Go(func() error { return wsHub.Run(gctx) })

	g.Go(func() error {
		logger.Info("http server listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	})

	g.Go(func() error {
		<-gctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		logger.Info("shutting down")
		return srv.Shutdown(shutdownCtx)
	})

	return g.Wait()
}

func newHTTPServer(addr string, logger *slog.Logger, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadTimeout:       10 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       120 * time.Second,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}
}

// amqpChecker returns nil interface when publisher is nil so /readyz omits the check.
// Otherwise wraps the publisher into the api.AMQPHealthChecker contract.
func amqpChecker(pub *queue.Publisher) api.AMQPHealthChecker {
	if pub == nil {
		return nil
	}
	return pub
}

func parseLogLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// runHealthcheck issues a single GET against /livez on the loopback HTTP
// listener and exits 0 if the response is 200, non-zero otherwise.
//
// Designed for use as a Docker HEALTHCHECK in the distroless final image
// (no shell, no curl available). Inside the container the listener is on
// the container-internal port, which is the HTTP_ADDR config; we default
// to ":8080" to match the listener in newHTTPServer.
//
// Override with the first positional arg, e.g. `notifyd healthcheck http://127.0.0.1:9000/livez`.
func runHealthcheck(args []string) int {
	url := "http://127.0.0.1:8080/livez"
	if len(args) > 0 && args[0] != "" {
		url = args[0]
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck: %v\n", err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "healthcheck: status=%d\n", resp.StatusCode)
		return 1
	}
	return 0
}
