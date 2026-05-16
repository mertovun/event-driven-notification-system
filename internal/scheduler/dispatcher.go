// Package scheduler polls scheduled_notifications and dispatches due rows
// by transitioning them to status=pending and writing an outbox row, which
// the outbox dispatcher then publishes through the normal path.
package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mertovun/event-driven-notification-system/internal/queue"
	"github.com/mertovun/event-driven-notification-system/internal/store/gen"
)

// Config tunes the scheduler.
type Config struct {
	PollInterval time.Duration // default 1s
	BatchSize    int           // default 50
	SchedulerID  string        // identifies this process (UUIDv7 per boot)
	// ClaimTTL — a row claimed by a scheduler that subsequently crashed mid-TX
	// (before the row was deleted) becomes eligible for re-claim after this
	// duration. Mirrors the outbox dispatcher's claim-TTL pattern. 60s matches
	// outbox default; lower values reduce drift but raise the chance of two
	// dispatchers picking up the same row briefly.
	ClaimTTL time.Duration
}

func Default(schedulerID string) Config {
	return Config{
		PollInterval: time.Second,
		BatchSize:    50,
		SchedulerID:  schedulerID,
		ClaimTTL:     60 * time.Second,
	}
}

// Dispatcher claims due scheduled rows and converts them to queued notifications.
type Dispatcher struct {
	cfg    Config
	pool   *pgxpool.Pool
	q      *gen.Queries
	logger *slog.Logger
}

func New(pool *pgxpool.Pool, q *gen.Queries, logger *slog.Logger, cfg Config) *Dispatcher {
	return &Dispatcher{cfg: cfg, pool: pool, q: q, logger: logger}
}

// Run loops until ctx is cancelled.
func (d *Dispatcher) Run(ctx context.Context) error {
	d.logger.Info("scheduler dispatcher started",
		"poll", d.cfg.PollInterval, "batch", d.cfg.BatchSize)
	ticker := time.NewTicker(d.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			d.logger.Info("scheduler dispatcher stopping")
			return nil
		case <-ticker.C:
			if err := d.tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
				d.logger.Error("scheduler tick failed", "err", err)
			}
		}
	}
}

func (d *Dispatcher) tick(ctx context.Context) error {
	sid := d.cfg.SchedulerID
	rows, err := d.q.ClaimDueScheduled(ctx, gen.ClaimDueScheduledParams{
		BatchSize: int32(d.cfg.BatchSize),
		ClaimedBy: &sid,
		ClaimTtl:  int32(d.cfg.ClaimTTL / time.Second),
	})
	if err != nil {
		return fmt.Errorf("claim due scheduled: %w", err)
	}
	if len(rows) == 0 {
		return nil
	}
	d.logger.Debug("scheduler claimed batch", "count", len(rows))
	for _, row := range rows {
		d.dispatchOne(ctx, row.ID)
	}
	return nil
}

func (d *Dispatcher) dispatchOne(ctx context.Context, notifID uuid.UUID) {
	// Transition status + insert outbox row in one TX.
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		d.logger.Error("scheduler begin tx", "id", notifID, "err", err)
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()
	qtx := d.q.WithTx(tx)

	notif, err := qtx.GetNotificationByID(ctx, notifID)
	if err != nil {
		d.logger.Error("scheduler get notification", "id", notifID, "err", err)
		return
	}

	// scheduled → pending via the dedicated MarkScheduledPending query.
	// Going through sqlc keeps DB access uniform and lets `go vet` /
	// query review catch issues — earlier this was a raw `tx.Exec` because
	// the bulk transitions didn't have a matching query.
	if _, err := qtx.MarkScheduledPending(ctx, notifID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Lost the race (cancelled meanwhile); drop quietly.
			_ = qtx.DeleteScheduled(ctx, notifID)
			_ = tx.Commit(ctx)
			return
		}
		d.logger.Error("scheduler CAS scheduled->pending", "id", notifID, "err", err)
		return
	}

	envelope := map[string]any{
		"notification_id": notifID.String(),
		"channel":         notif.Channel,
		"recipient":       notif.Recipient,
		"content":         notif.Content,
		"priority":        priorityLabel(notif.Priority),
		"correlation_id":  notif.CorrelationID,
	}
	payload, _ := json.Marshal(envelope)
	headers, _ := json.Marshal(map[string]any{"x-correlation-id": notif.CorrelationID})
	if _, err := qtx.InsertOutbox(ctx, gen.InsertOutboxParams{
		NotificationID: notifID,
		RoutingKey:     queue.RoutingKey(notif.Channel),
		Payload:        payload,
		Headers:        headers,
		Priority:       notif.Priority,
	}); err != nil {
		d.logger.Error("scheduler insert outbox", "id", notifID, "err", err)
		return
	}

	// Schedule row done its job — remove it so we never re-claim.
	if err := qtx.DeleteScheduled(ctx, notifID); err != nil {
		d.logger.Error("scheduler delete scheduled", "id", notifID, "err", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		d.logger.Error("scheduler commit", "id", notifID, "err", err)
		return
	}
	d.logger.Info("scheduler dispatched", "id", notifID)
}

func priorityLabel(p int16) string {
	switch p {
	case 9:
		return "high"
	case 1:
		return "low"
	default:
		return "normal"
	}
}
