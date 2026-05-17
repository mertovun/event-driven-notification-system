// Package audit holds the background verifier for the admin_audit hash chain.
//
// The chain itself is built by a BEFORE INSERT trigger:
//   - migration 0009 adds prev_hash + row_hash columns and the trigger
//   - migration 0011 wraps the trigger body in pg_advisory_xact_lock to
//     serialize concurrent inserts (otherwise two parallel admin actions
//     can fork the chain without any actual tampering)
//   - migration 0012 makes VerifyAuditChain recompute every row_hash
//     from its contents (otherwise the verifier passes when row_hash
//     is left intact but other columns are edited in place)
//
// VerifyAuditChain (sqlc-generated) is a `COUNT(*)` over the chain looking
// for both linkage breaks (prev_hash != previous row_hash) and content
// breaks (row_hash != sha256(prev_hash || canonical(row))). 0 means the
// chain is intact. A trigger without a running verifier is theater — this
// package is the verifier.
//
// This loop ticks every Config.Interval, runs VerifyAuditChain once, and
// publishes the broken-link count to a Prometheus gauge plus a log line on
// non-zero. The default cadence (5 min) is matched to admin volume; tighten
// in compliance-heavy deployments.
package audit

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/mertovun/event-driven-notification-system/internal/observability"
	"github.com/mertovun/event-driven-notification-system/internal/store/gen"
)

// Config tunes the verifier.
type Config struct {
	// Interval — how often to recheck the chain. 5 min is conservative for
	// the admin volumes a single-tenant service sees; for chain rooms a 30s
	// cadence is fine (the query is a single COUNT over a small table).
	Interval time.Duration
}

func Default() Config {
	return Config{Interval: 5 * time.Minute}
}

// ChainVerifier owns the periodic VerifyAuditChain call.
type ChainVerifier struct {
	cfg     Config
	q       *gen.Queries
	metrics *observability.Metrics
	logger  *slog.Logger
}

func NewChainVerifier(q *gen.Queries, metrics *observability.Metrics, logger *slog.Logger, cfg Config) *ChainVerifier {
	return &ChainVerifier{cfg: cfg, q: q, metrics: metrics, logger: logger}
}

// Run polls VerifyAuditChain until ctx is cancelled. One sample at startup so
// the gauge has a value before the first tick — operators expect /metrics to
// be answerable as soon as /readyz turns green.
func (v *ChainVerifier) Run(ctx context.Context) error {
	v.logger.Info("audit-chain-verifier started", "interval", v.cfg.Interval)

	v.verifyOnce(ctx)

	t := time.NewTicker(v.cfg.Interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			v.verifyOnce(ctx)
		}
	}
}

// VerifyOnce runs a single check and returns the broken-link count. Exposed
// so the admin `GET /v1/admin/audit/verify` endpoint can force a fresh check
// without waiting for the next tick.
func (v *ChainVerifier) VerifyOnce(ctx context.Context) (int64, error) {
	return v.q.VerifyAuditChain(ctx)
}

func (v *ChainVerifier) verifyOnce(ctx context.Context) {
	broken, err := v.q.VerifyAuditChain(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		v.logger.Error("audit-chain-verifier query failed", "err", err)
		return
	}
	if v.metrics != nil && v.metrics.AuditChainBrokenLinks != nil {
		v.metrics.AuditChainBrokenLinks.Set(float64(broken))
	}
	if broken > 0 {
		v.logger.Warn("audit chain has broken links — tamper-evidence triggered",
			"broken_links", broken)
		return
	}
	v.logger.Debug("audit chain intact", "broken_links", 0)
}
