// Package sweeper holds background reclaim loops for notifications stranded
// by a worker crash. Currently a single reclaim path: rows stuck in 'sending'
// after a worker died between MarkSendingCAS and MarkSent.
package sweeper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mertovun/event-driven-notification-system/internal/notification"
	"github.com/mertovun/event-driven-notification-system/internal/queue"
	"github.com/mertovun/event-driven-notification-system/internal/store/gen"
)

// Config tunes the sweeper.
type Config struct {
	// PollInterval — how often to scan for stuck rows. 30s is the default —
	// finer cadence wastes DB cycles, coarser cadence delays recovery.
	PollInterval time.Duration

	// StuckAfter — minimum time a row must have been in 'sending' before
	// we reclaim it. Has to be wider than the worker's handleTimeout (30s)
	// + the in-flight Redis lock TTL (60s) + comfortable margin so we don't
	// reclaim rows being actively processed. 5 minutes is conservative.
	StuckAfter time.Duration
}

func Default() Config {
	return Config{
		PollInterval: 30 * time.Second,
		StuckAfter:   5 * time.Minute,
	}
}

// SendingSweeper reclaims notifications stuck in 'sending' status.
type SendingSweeper struct {
	cfg    Config
	pool   *pgxpool.Pool
	q      *gen.Queries
	logger *slog.Logger
}

func NewSending(pool *pgxpool.Pool, q *gen.Queries, logger *slog.Logger, cfg Config) *SendingSweeper {
	return &SendingSweeper{cfg: cfg, pool: pool, q: q, logger: logger}
}

// Run polls for stuck rows until ctx is cancelled. Each cycle runs the sweep
// in a transaction (notifications row flip + fresh outbox row insert per
// reclaimed notification) so a partial sweep doesn't leave rows in 'queued'
// without a matching outbox entry. Errors log and continue — sweeper is not
// a load-bearing path under normal operation.
func (s *SendingSweeper) Run(ctx context.Context) error {
	s.logger.Info("sending-sweeper started",
		"poll", s.cfg.PollInterval, "stuck_after", s.cfg.StuckAfter)
	t := time.NewTicker(s.cfg.PollInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := s.tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
				s.logger.Error("sending-sweeper tick failed", "err", err)
			}
		}
	}
}

func (s *SendingSweeper) tick(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	qtx := s.q.WithTx(tx)

	rows, err := qtx.SweepStuckSending(ctx, int32(s.cfg.StuckAfter.Seconds()))
	if err != nil {
		return fmt.Errorf("sweep stuck: %w", err)
	}
	if len(rows) == 0 {
		return nil
	}

	// For each reclaimed row, write a fresh outbox entry so the dispatcher
	// re-publishes. The original outbox row was already marked published
	// when the worker started CAS-ing; without a new row the dispatcher
	// would never look at this notification again.
	for _, row := range rows {
		envelope := map[string]any{
			"notification_id": row.ID.String(),
			"channel":         row.Channel,
			"recipient":       row.Recipient,
			"content":         row.Content,
			"priority":        notification.PriorityFromInt(row.Priority),
			"correlation_id":  row.CorrelationID,
		}
		payload, _ := json.Marshal(envelope)
		headers, _ := json.Marshal(map[string]any{
			"x-correlation-id": row.CorrelationID,
			"x-reclaim-source": "sending-sweeper",
		})
		if _, err := qtx.InsertOutbox(ctx, gen.InsertOutboxParams{
			NotificationID: row.ID,
			RoutingKey:     queue.RoutingKey(row.Channel),
			Payload:        payload,
			Headers:        headers,
			Priority:       row.Priority,
		}); err != nil {
			return fmt.Errorf("insert outbox for reclaimed %s: %w", row.ID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	s.logger.Warn("reclaimed notifications stuck in 'sending'",
		"count", len(rows), "stuck_after", s.cfg.StuckAfter)
	return nil
}
