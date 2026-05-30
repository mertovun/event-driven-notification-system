// Package outbox implements the transactional outbox dispatcher (claim+publish loop).
package outbox

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
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/mertovun/event-driven-notification-system/internal/observability"
	"github.com/mertovun/event-driven-notification-system/internal/queue"
	"github.com/mertovun/event-driven-notification-system/internal/store/gen"
)

// Config tunes the dispatcher.
type Config struct {
	PollInterval time.Duration
	BatchSize    int
	ClaimTTL     time.Duration
	MaxAttempts  int
	DispatcherID string // identifies this process (UUIDv7 per boot)
}

// Default returns a Config with sensible starting values: 250ms poll, 50-row batches,
// 60s claim TTL, 10 attempts before terminal failure.
func Default(dispatcherID string) Config {
	return Config{
		PollInterval: 250 * time.Millisecond,
		BatchSize:    50,
		ClaimTTL:     60 * time.Second,
		MaxAttempts:  10,
		DispatcherID: dispatcherID,
	}
}

// Dispatcher claims unpublished outbox rows and publishes them to RabbitMQ.
type Dispatcher struct {
	cfg     Config
	pool    *pgxpool.Pool
	q       *gen.Queries
	pub     *queue.Publisher
	metrics *observability.Metrics
	logger  *slog.Logger
}

func New(pool *pgxpool.Pool, q *gen.Queries, pub *queue.Publisher, metrics *observability.Metrics, logger *slog.Logger, cfg Config) *Dispatcher {
	return &Dispatcher{cfg: cfg, pool: pool, q: q, pub: pub, metrics: metrics, logger: logger}
}

// Run loops until ctx is cancelled. One claim cycle per tick.
// Errors during a tick log and continue; nothing fatal unless ctx is done.
func (d *Dispatcher) Run(ctx context.Context) error {
	d.logger.Info("outbox dispatcher started",
		"poll", d.cfg.PollInterval,
		"batch", d.cfg.BatchSize,
		"claim_ttl", d.cfg.ClaimTTL,
	)
	ticker := time.NewTicker(d.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			d.logger.Info("outbox dispatcher stopping")
			return nil
		case <-ticker.C:
			if err := d.tick(ctx); err != nil && !errors.Is(err, context.Canceled) {
				d.logger.Error("outbox tick failed", "err", err)
			}
		}
	}
}

// tick claims a batch, publishes each row, and marks success / failure.
// Also samples outbox_lag_seconds — the age of the oldest unpublished row —
// so oncall has a single number to alert on when dispatch falls behind producers.
func (d *Dispatcher) tick(ctx context.Context) error {
	defer d.sampleLag(ctx)

	dispID := d.cfg.DispatcherID
	rows, err := d.q.ClaimOutboxBatch(ctx, gen.ClaimOutboxBatchParams{
		ClaimTtlSeconds: int32(d.cfg.ClaimTTL.Seconds()),
		BatchSize:       int32(d.cfg.BatchSize),
		ClaimedBy:       &dispID,
	})
	if err != nil {
		return fmt.Errorf("claim batch: %w", err)
	}
	if len(rows) == 0 {
		return nil
	}

	d.logger.Debug("outbox claimed batch", "count", len(rows))
	for _, row := range rows {
		d.publishRow(ctx, row)
	}
	return nil
}

// sampleLag updates outbox_lag_seconds. Best-effort — a failed sample logs
// and leaves the previous gauge value; alerting on the gauge being too old
// is a Prometheus-side concern.
func (d *Dispatcher) sampleLag(ctx context.Context) {
	if d.metrics == nil {
		return
	}
	lag, err := d.q.OutboxLagSeconds(ctx)
	if err != nil {
		d.logger.Warn("outbox lag sample failed", "err", err)
		return
	}
	d.metrics.OutboxLagSeconds.Set(lag)
}

func (d *Dispatcher) publishRow(ctx context.Context, row gen.Outbox) {
	// Decode headers JSONB → map[string]any.
	hdrs := map[string]any{}
	if len(row.Headers) > 0 {
		if err := json.Unmarshal(row.Headers, &hdrs); err != nil {
			d.logger.Warn("outbox row headers malformed; publishing without headers",
				"id", row.ID, "err", err)
			hdrs = map[string]any{}
		}
	}

	// Resume the API-side trace via the propagator if the row carries traceparent.
	carrier := propagationHeaders(hdrs)
	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)
	tr := otel.Tracer("notifyd/outbox")
	ctx, span := tr.Start(ctx, "outbox.dispatch")
	defer span.End()

	// Re-inject the (now dispatch-scoped) context back into the headers so the
	// worker that consumes this message continues the trace as a child of
	// outbox.dispatch — not the original API span. Without this, the published
	// traceparent still points at the API span and the worker's spans attach
	// one level too high (or orphan entirely if the worker doesn't extract).
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	// Priority comes off the smallint column; AMQP carries an uint8.
	priority := uint8(row.Priority)
	if priority > 9 {
		priority = 9
	}

	msg := queue.PublishMessage{
		RoutingKey: row.RoutingKey,
		Payload:    row.Payload,
		Headers:    hdrs,
		Priority:   priority,
	}

	if err := d.pub.Publish(ctx, msg); err != nil {
		d.markFailure(ctx, row, err)
		return
	}

	if err := d.q.MarkOutboxPublished(ctx, row.ID); err != nil {
		d.logger.Error("outbox mark published failed", "id", row.ID, "err", err)
		// Not fatal — the message will be re-published on the next claim cycle.
		// Worker-side idempotency layers protect against dupes (SETNX in-flight lock + CAS status).
		return
	}

	// Best-effort transition notifications.status pending → queued so the listing API reflects reality.
	notifID, perr := uuid.Parse(extractNotificationID(row.Payload))
	if perr == nil {
		if _, mqErr := d.q.MarkQueued(ctx, notifID); mqErr != nil {
			d.logger.Debug("mark queued non-fatal", "id", notifID, "err", mqErr)
		}
	}
}

func (d *Dispatcher) markFailure(ctx context.Context, row gen.Outbox, pubErr error) {
	// Broker-unavailable failures are infrastructure outages, not delivery
	// attempts. Counting them toward MaxAttempts would dead-letter a
	// notification that never even reached the broker — during a ~2.5s flap at
	// the 250ms tick, 10 failed publishes would terminate a perfectly healthy
	// message. Instead clear the claim and let the next tick retry, without
	// advancing attempt_count, until the broker returns.
	if errors.Is(pubErr, queue.ErrPublisherUnavailable) {
		d.logger.Warn("outbox publish deferred; broker unavailable (not counted)", "id", row.ID, "err", pubErr)
		errMsg := truncate(pubErr.Error(), 1024)
		if err := d.q.MarkOutboxUnpublishedRetry(ctx, gen.MarkOutboxUnpublishedRetryParams{
			ID:        row.ID,
			LastError: &errMsg,
		}); err != nil {
			d.logger.Error("mark outbox retry (broker unavailable) failed", "id", row.ID, "err", err)
		}
		return
	}

	attempts := row.AttemptCount + 1
	d.logger.Warn("outbox publish failed", "id", row.ID, "attempts", attempts, "err", pubErr)

	if int(attempts) >= d.cfg.MaxAttempts {
		// Terminal: move the underlying notification to dead_letter and stop retrying.
		notifID, perr := uuid.Parse(extractNotificationID(row.Payload))
		if perr == nil {
			errMsg := pubErr.Error()
			if _, err := d.q.MarkDeadLetter(ctx, gen.MarkDeadLetterParams{
				ID:        notifID,
				LastError: &errMsg,
			}); err != nil {
				d.logger.Error("mark dead_letter failed", "id", notifID, "err", err)
			}
			if _, err := d.q.InsertDeadLetter(ctx, gen.InsertDeadLetterParams{
				NotificationID: notifID,
				Reason:         truncate(pubErr.Error(), 500),
				Payload:        row.Payload,
			}); err != nil && !errors.Is(err, pgx.ErrNoRows) {
				d.logger.Error("insert dead_letter failed", "id", notifID, "err", err)
			}
		}
		// Mark the outbox row as published-with-zero-route so it's not retried.
		// Simpler than a dedicated `dead_outbox` table; the notification's status carries the failure.
		if err := d.q.MarkOutboxPublished(ctx, row.ID); err != nil {
			d.logger.Error("mark outbox terminated failed", "id", row.ID, "err", err)
		}
		return
	}

	// Retryable: increment attempt and clear claim so next cycle picks it up.
	errMsg := truncate(pubErr.Error(), 1024)
	if err := d.q.MarkOutboxFailed(ctx, gen.MarkOutboxFailedParams{
		ID:        row.ID,
		LastError: &errMsg,
	}); err != nil {
		d.logger.Error("mark outbox failed update", "id", row.ID, "err", err)
	}
}

// extractNotificationID pulls the notification_id field out of the JSONB payload
// without a full struct. Returns "" if not present.
func extractNotificationID(payload []byte) string {
	var env struct {
		NotificationID string `json:"notification_id"`
	}
	if err := json.Unmarshal(payload, &env); err != nil {
		return ""
	}
	return env.NotificationID
}

// propagationHeaders adapts the outbox headers map to OTel's TextMapCarrier.
type propagationHeaders map[string]any

var _ propagation.TextMapCarrier = (propagationHeaders)(nil)

func (m propagationHeaders) Get(key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
func (m propagationHeaders) Set(key, value string) { m[key] = value }
func (m propagationHeaders) Keys() []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
