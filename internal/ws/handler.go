package ws

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	cws "github.com/coder/websocket"
	"github.com/google/uuid"
)

// Subject describes who is on the other end of a WebSocket connection.
// Populated by the SubjectResolver from auth context the upstream auth
// middleware has already verified. OwnerID is the api_key_id; AdminBypass
// is true when the key has the admin scope so the hub's per-event owner
// filter is short-circuited.
type Subject struct {
	OwnerID     *uuid.UUID
	AdminBypass bool
}

// SubjectResolver extracts the WS subject from request context. The auth
// middleware lives in internal/api which can't be imported here (cycle);
// the resolver is the seam.
type SubjectResolver func(ctx context.Context) Subject

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
// Auth was enforced by the upstream chi middleware; this handler trusts
// r.Context() and uses the resolver to pull the subject identity into the
// per-connection Filter so the owner gate is applied on every event.
func Handler(hub *Hub, cfg Config, logger *slog.Logger, resolveSubject SubjectResolver) http.HandlerFunc {
	allowedOrigins := loadAllowedOrigins()
	insecureSkipVerify := len(allowedOrigins) == 0
	return func(w http.ResponseWriter, r *http.Request) {
		// Parse filter query param.
		filter, err := ParseFilter(r.URL.Query().Get("filter"))
		if err != nil {
			http.Error(w, "bad filter: "+err.Error(), http.StatusBadRequest)
			return
		}

		// Pin the subscriber identity to the filter so the hub can apply
		// the per-event owner gate without re-touching auth context on
		// every dispatch. With AdminBypass=false the gate is inclusive
		// only of events whose CreatedBy equals our OwnerID; admin keys
		// short-circuit the gate (ADR-0019).
		subj := resolveSubject(r.Context())
		filter.OwnerID = subj.OwnerID
		filter.AdminBypass = subj.AdminBypass

		// Origin check: opt-in via WS_ALLOWED_ORIGINS env var (comma-separated
		// list of full origins like "https://app.example.com"). When the var
		// is set the upgrader rejects mismatched Origin headers; when empty,
		// we fall back to InsecureSkipVerify=true and assume a reverse proxy
		// is enforcing it. The README documents both modes.
		acceptOpts := &cws.AcceptOptions{
			Subprotocols:       nil, // bearer.<token> subprotocol was unwired; remove the attack surface
			InsecureSkipVerify: insecureSkipVerify,
		}
		if !insecureSkipVerify {
			acceptOpts.OriginPatterns = allowedOrigins
		}
		conn, err := cws.Accept(w, r, acceptOpts)
		if err != nil {
			logger.Warn("ws upgrade failed", "err", err)
			return
		}
		defer func() { _ = conn.Close(cws.StatusNormalClosure, "") }()

		// Subscriber lifecycle. closeOnce can be invoked from two places: the
		// hub's slow-consumer eviction goroutine and the handler's deferred
		// cleanup on read/write error. sync.Once guarantees the underlying
		// conn.Close + cancel() runs exactly once across both paths.
		ctx, cancel := context.WithCancel(r.Context())
		defer cancel()
		var closeOnce sync.Once
		doClose := func(reason string) {
			closeOnce.Do(func() {
				_ = conn.Close(cws.StatusPolicyViolation, reason)
				cancel()
			})
		}
		id, recv := hub.Register(filter, doClose)
		defer hub.Unregister(id)

		logger.Info("ws connection accepted", "sub_id", id, "filter", filter)

		// Heartbeat + writer goroutine. Reader is the parent goroutine.
		go writePump(ctx, conn, recv, cfg, logger)
		readPump(ctx, conn, cfg, logger)
	}
}

// loadAllowedOrigins parses the WS_ALLOWED_ORIGINS env var. Each entry is
// matched as a host pattern by `coder/websocket`; we strip schemes so the
// caller can paste their full origins ("https://app.example.com,https://...")
// without worrying about format. Empty or unset → no allowlist, fall back to
// InsecureSkipVerify=true (with proxy-enforced origin check).
func loadAllowedOrigins() []string {
	raw := strings.TrimSpace(os.Getenv("WS_ALLOWED_ORIGINS"))
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		// Accept either full origin (https://host[:port]) or bare host.
		if u, err := url.Parse(p); err == nil && u.Host != "" {
			out = append(out, u.Host)
		} else {
			out = append(out, p)
		}
	}
	return out
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
func readPump(ctx context.Context, conn *cws.Conn, _ Config, logger *slog.Logger) {
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
