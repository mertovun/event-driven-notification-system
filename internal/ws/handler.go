package ws

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	cws "github.com/coder/websocket"
)

// Config tunes the upgrade handler.
type Config struct {
	HeartbeatInterval time.Duration // server pings every N (default 30s)
	PongDeadline      time.Duration // close if no pong in N (default 10s)
	WriteTimeout      time.Duration // per-message write deadline (default 10s)
	ShutdownTimeout   time.Duration // drain on Close (default 5s)
}

func DefaultConfig() Config {
	return Config{
		HeartbeatInterval: 30 * time.Second,
		PongDeadline:      10 * time.Second,
		WriteTimeout:      10 * time.Second,
		ShutdownTimeout:   5 * time.Second,
	}
}

// Handler returns an HTTP handler that upgrades to WebSocket and bridges to the hub.
// Auth was enforced by the upstream chi middleware; this handler trusts r.Context().
func Handler(hub *Hub, cfg Config, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Parse filter query param.
		filter, err := ParseFilter(r.URL.Query().Get("filter"))
		if err != nil {
			http.Error(w, "bad filter: "+err.Error(), http.StatusBadRequest)
			return
		}

		// Accept the upgrade.
		conn, err := cws.Accept(w, r, &cws.AcceptOptions{
			Subprotocols:       acceptedSubprotocols(r),
			InsecureSkipVerify: true, // we run behind a reverse proxy in prod; origin check belongs there
		})
		if err != nil {
			logger.Warn("ws upgrade failed", "err", err)
			return
		}
		defer func() { _ = conn.Close(cws.StatusNormalClosure, "") }()

		// Subscriber lifecycle.
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		closeOnce := newOnce(func(reason string) {
			_ = conn.Close(cws.StatusPolicyViolation, reason)
			cancel()
		})
		id, recv := hub.Register(filter, closeOnce.Do)
		defer hub.Unregister(id)

		logger.Info("ws connection accepted", "sub_id", id, "filter", filter)

		// Heartbeat + writer goroutine. Reader is the parent goroutine.
		go writePump(ctx, conn, recv, cfg, logger)
		readPump(ctx, conn, cfg, logger)
	}
}

func acceptedSubprotocols(r *http.Request) []string {
	// Allow `bearer.<token>` subprotocol as a fallback for browsers that can't set
	// Authorization headers. (Documented in docs/13 §A3.)
	prot := r.Header.Get("Sec-WebSocket-Protocol")
	if strings.HasPrefix(prot, "bearer.") {
		return []string{prot}
	}
	return nil
}

// writePump sends events from `recv` and pings on Heartbeat ticks.
func writePump(ctx context.Context, conn *cws.Conn, recv <-chan []byte, cfg Config, logger *slog.Logger) {
	ticker := time.NewTicker(cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-recv:
			if !ok {
				return
			}
			writeCtx, cancel := context.WithTimeout(ctx, cfg.WriteTimeout)
			err := conn.Write(writeCtx, cws.MessageText, msg)
			cancel()
			if err != nil {
				logger.Debug("ws write failed", "err", err)
				return
			}
		case <-ticker.C:
			pingCtx, cancel := context.WithTimeout(ctx, cfg.PongDeadline)
			err := conn.Ping(pingCtx)
			cancel()
			if err != nil {
				logger.Debug("ws heartbeat lost", "err", err)
				return
			}
		}
	}
}

// readPump drains incoming frames (we don't expect any, but the lib requires reads).
func readPump(ctx context.Context, conn *cws.Conn, cfg Config, logger *slog.Logger) {
	for {
		_, _, err := conn.Read(ctx)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Debug("ws read ended", "err", err)
			}
			return
		}
	}
}

// once wraps a one-shot close function so we can call it from multiple paths.
type once struct {
	fired bool
	fn    func(string)
}

func newOnce(fn func(string)) *once { return &once{fn: fn} }
func (o *once) Do(reason string) {
	if o.fired {
		return
	}
	o.fired = true
	o.fn(reason)
}
