// Command notifyd is the event-driven notification system service binary.
// It runs the HTTP API and worker pools as a single process by default;
// --mode selects api / worker / all (default all).
package main

import (
	"context"
	"encoding/json"
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

	"github.com/go-chi/chi/v5"
	"golang.org/x/sync/errgroup"
)

// Injected via -ldflags at build time.
var (
	Version   = "dev"
	Commit    = "none"
	BuildTime = "unknown"
)

func main() {
	mode := flag.String("mode", "all", "run mode: api | worker | all")
	addr := flag.String("addr", ":8080", "HTTP listen address")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	logger.Info("starting notifyd",
		"version", Version,
		"commit", Commit,
		"build_time", BuildTime,
		"mode", *mode,
		"go", runtime.Version(),
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, *mode, *addr, logger); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, mode, addr string, logger *slog.Logger) error {
	switch mode {
	case "api", "worker", "all":
	default:
		return fmt.Errorf("invalid --mode %q (want api | worker | all)", mode)
	}

	srv := newHTTPServer(addr, logger)

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		logger.Info("http server listening", "addr", addr)
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

func newHTTPServer(addr string, logger *slog.Logger) *http.Server {
	r := chi.NewRouter()
	r.Get("/livez", livezHandler)
	r.Get("/version", versionHandler)

	return &http.Server{
		Addr:              addr,
		Handler:           r,
		ReadTimeout:       10 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       120 * time.Second,
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelError),
	}
}

func livezHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func versionHandler(w http.ResponseWriter, _ *http.Request) {
	body := struct {
		Version   string `json:"version"`
		Commit    string `json:"commit"`
		BuildTime string `json:"build_time"`
		GoVersion string `json:"go_version"`
	}{Version, Commit, BuildTime, runtime.Version()}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(body)
}
