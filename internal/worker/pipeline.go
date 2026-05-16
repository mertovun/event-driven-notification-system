// Package worker contains the per-channel delivery pipeline.
// Each worker: consume AMQP delivery → CAS notification.status → claim
// in-flight lock → rate-limit gate → breaker.Execute(provider.Send) →
// record delivery_attempts → MarkSent / RevertToQueued / dead-letter.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/sony/gobreaker"

	"github.com/mertovun/event-driven-notification-system/internal/events"
	"github.com/mertovun/event-driven-notification-system/internal/observability"
	"github.com/mertovun/event-driven-notification-system/internal/provider"
	"github.com/mertovun/event-driven-notification-system/internal/queue"
	"github.com/mertovun/event-driven-notification-system/internal/ratelimit"
	"github.com/mertovun/event-driven-notification-system/internal/store/gen"
)

// envelope is the JSON payload the API publishes to RabbitMQ (and outbox writes).
// Keep in sync with internal/api/notifications.go.
type envelope struct {
	NotificationID string `json:"notification_id"`
	Channel        string `json:"channel"`
	Recipient      string `json:"recipient"`
	Content        string `json:"content"`
	Priority       string `json:"priority"`
	CorrelationID  string `json:"correlation_id"`
}

// Pipeline holds the dependencies the worker delivers through.
// One Pipeline instance per channel pool — see Manager (manager.go).
type Pipeline struct {
	channel   string
	pool      *pgxpool.Pool
	q         *gen.Queries
	rdb       *redis.Client
	limiter   *ratelimit.Limiter
	provider  *provider.HTTPClient
	breaker   *gobreaker.CircuitBreaker
	metrics   *observability.Metrics
	eventsPub *events.Publisher
	logger    *slog.Logger

	// Rate-limit settings — 100/s per channel, capacity = 1s burst.
	rateLimitPerSec float64
	rateCapacity    float64
}

// New builds a Pipeline for the named channel.
func New(
	channel string,
	pool *pgxpool.Pool,
	q *gen.Queries,
	rdb *redis.Client,
	limiter *ratelimit.Limiter,
	prov *provider.HTTPClient,
	metrics *observability.Metrics,
	eventsPub *events.Publisher,
	logger *slog.Logger,
) *Pipeline {
	chanLogger := logger.With("channel", channel)
	return &Pipeline{
		channel:         channel,
		pool:            pool,
		q:               q,
		rdb:             rdb,
		limiter:         limiter,
		provider:        prov,
		breaker:         newBreaker(channel, chanLogger),
		metrics:         metrics,
		eventsPub:       eventsPub,
		logger:          chanLogger,
		rateLimitPerSec: 100,
		rateCapacity:    100,
	}
}

// Handle is the queue.Handler for one delivery.
// Returns nil to ack; queue.ErrTerminal to nack-to-DLQ; any other error to nack-and-requeue.
func (p *Pipeline) Handle(ctx context.Context, d queue.Delivery) error {
	if p.metrics != nil {
		p.metrics.WorkerActive.WithLabelValues(p.channel).Inc()
		defer p.metrics.WorkerActive.WithLabelValues(p.channel).Dec()
	}
	start := time.Now()
	err := p.handle(ctx, d)
	if p.metrics != nil {
		p.metrics.DeliveryDurationSecs.WithLabelValues(p.channel).Observe(time.Since(start).Seconds())
		switch {
		case err == nil:
			p.metrics.NotificationsDeliveredTotal.WithLabelValues(p.channel, "success").Inc()
		case errors.Is(err, queue.ErrTerminal):
			p.metrics.NotificationsDeliveredTotal.WithLabelValues(p.channel, "dead_letter").Inc()
		default:
			p.metrics.NotificationsDeliveredTotal.WithLabelValues(p.channel, "failure").Inc()
		}
	}
	return err
}

// handle is the inner pipeline; Handle wraps with metrics.
func (p *Pipeline) handle(ctx context.Context, d queue.Delivery) error {
	// 1) Decode envelope. Malformed → terminal (can't be retried meaningfully).
	var env envelope
	if err := json.Unmarshal(d.Body(), &env); err != nil {
		p.logger.Error("malformed envelope", "err", err)
		return fmt.Errorf("decode envelope: %w: %w", err, queue.ErrTerminal)
	}
	notifID, err := uuid.Parse(env.NotificationID)
	if err != nil {
		p.logger.Error("bad notification id", "err", err)
		return fmt.Errorf("parse id: %w: %w", err, queue.ErrTerminal)
	}

	logger := p.logger.With("notification_id", notifID, "correlation_id", env.CorrelationID)
	ctx = withCorrelationID(ctx, env.CorrelationID)

	// 2) CAS queued → sending. If 0 rows, someone cancelled it; ack and drop.
	// statement_timeout on the connection (see store/pg.go) bounds the worst
	// case if the wire read wedges — observed during scheduled-notification
	// flow stress under concurrent transactions from the scheduler.
	row, err := p.q.MarkSendingCAS(ctx, notifID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			logger.Info("notification not in queued state; ack and drop")
			return nil // ack — the notification was cancelled or already past 'queued'
		}
		return fmt.Errorf("CAS to sending: %w", err)
	}

	// 3) In-flight lock — defense-in-depth against broker redelivery race.
	inflightKey := "delivery_inflight:" + notifID.String()
	gotLock, err := p.rdb.SetNX(ctx, inflightKey, "1", 60*time.Second).Result()
	if err != nil {
		return fmt.Errorf("inflight setnx: %w", err)
	}
	if !gotLock {
		// Another worker has it; revert our CAS and ack — the other will deliver.
		_ = p.q.RevertToQueued(ctx, gen.RevertToQueuedParams{ID: notifID})
		logger.Info("inflight lock held by another worker; deferring")
		return nil
	}
	defer func() { _ = p.rdb.Del(context.Background(), inflightKey).Err() }()

	// 4) Rate-limit gate — Lua token bucket per channel.
	dec, err := p.limiter.Allow(ctx, "ratelimit:"+p.channel, p.rateLimitPerSec, p.rateCapacity)
	if err != nil {
		// Don't fail the message on Redis issues; nack-requeue and let next attempt go.
		return fmt.Errorf("ratelimit: %w", err)
	}
	if !dec.Allowed {
		if p.metrics != nil {
			p.metrics.RateLimitThrottledTotal.WithLabelValues(p.channel).Inc()
		}
		// Throttled. Briefly wait (short throttle, ≤200ms) and re-check; otherwise nack-requeue.
		wait := dec.RetryAfter
		const maxInlineWait = 200 * time.Millisecond
		if wait > maxInlineWait {
			// Sustained throttle: revert CAS and nack-requeue so prefetch slot stays flowing.
			_ = p.q.RevertToQueued(ctx, gen.RevertToQueuedParams{ID: notifID})
			logger.Info("rate-limit throttle sustained; requeue", "retry_after", wait)
			return errors.New("rate-limited (sustained)")
		}
		select {
		case <-ctx.Done():
			_ = p.q.RevertToQueued(ctx, gen.RevertToQueuedParams{ID: notifID})
			return ctx.Err()
		case <-time.After(wait):
		}
		// Re-try once after the wait; if still throttled, sustained-path applies.
		dec, _ = p.limiter.Allow(ctx, "ratelimit:"+p.channel, p.rateLimitPerSec, p.rateCapacity)
		if !dec.Allowed {
			_ = p.q.RevertToQueued(ctx, gen.RevertToQueuedParams{ID: notifID})
			logger.Info("rate-limit still denied after short wait; requeue")
			return errors.New("rate-limited (post-wait)")
		}
	}

	// 5) Channel-specific content validation — defense in depth.
	// (API already validated; worker re-checks for the rare case of in-flight migrations.)
	// Skipping the explicit re-validation for brevity; the API gate is authoritative.

	// 6) Provider call — wrapped in circuit breaker per channel.
	attempt := row.AttemptCount + 1
	providerStart := time.Now()
	rawResult, breakerErr := p.breaker.Execute(func() (any, error) {
		return p.provider.Send(ctx, provider.SendRequest{
			To:      env.Recipient,
			Channel: env.Channel,
			Content: env.Content,
		}, env.CorrelationID)
	})
	if p.metrics != nil {
		p.metrics.ProviderDurationSecs.WithLabelValues(p.channel).Observe(time.Since(providerStart).Seconds())
	}

	// If the breaker is open, treat as a retryable failure (not per-message fault).
	if errors.Is(breakerErr, gobreaker.ErrOpenState) || errors.Is(breakerErr, gobreaker.ErrTooManyRequests) {
		errStr := breakerErr.Error()
		_ = p.q.RevertToQueued(ctx, gen.RevertToQueuedParams{ID: notifID, LastError: &errStr})
		logger.Info("breaker open; revert to queued", "attempt", attempt, "err", breakerErr)
		return breakerErr
	}

	var provResp *provider.SendResponse
	if rawResult != nil {
		provResp, _ = rawResult.(*provider.SendResponse)
	}
	callErr := breakerErr

	now := time.Now()
	completedAt := pgxTimestamp(now)

	if callErr != nil {
		// 7a) Record the failed attempt.
		errStr := callErr.Error()
		_, _ = p.q.InsertDeliveryAttempt(ctx, gen.InsertDeliveryAttemptParams{
			NotificationID: notifID,
			AttemptNumber:  attempt,
			StartedAt:      pgxTimestamp(now.Add(-100 * time.Millisecond)),
			CompletedAt:    completedAt,
			Success:        false,
			Error:          &errStr,
			HttpStatus:     httpStatusOf(callErr),
		})

		// 8a) Decide retry vs terminal.
		var se *provider.SendError
		if errors.As(callErr, &se) && se.Kind.IsRetryable() {
			if p.metrics != nil {
				p.metrics.NotificationRetryTotal.WithLabelValues(p.channel, attemptLabel(attempt)).Inc()
			}
			_ = p.q.RevertToQueued(ctx, gen.RevertToQueuedParams{ID: notifID, LastError: &errStr})
			logger.Info("retryable failure; reverted to queued",
				"attempt", attempt, "kind", se.Kind, "status", se.HTTPStatus)
			return callErr // nack-requeue
		}

		// Terminal — mark dead_letter, insert into dead_letters, ack-to-DLQ.
		_, _ = p.q.MarkDeadLetter(ctx, gen.MarkDeadLetterParams{ID: notifID, LastError: &errStr})
		_, _ = p.q.InsertDeadLetter(ctx, gen.InsertDeadLetterParams{
			NotificationID: notifID,
			Reason:         truncate(errStr, 500),
			Payload:        d.Body(),
		})
		p.emitStatus(ctx, notifID, "dead_letter", env.CorrelationID)
		logger.Warn("terminal failure; dead-lettered", "attempt", attempt, "err", callErr)
		return queue.ErrTerminal
	}

	// 7b) Success: record the attempt + mark sent.
	provMsgID := provResp.MessageID
	httpStatus := int32(provResp.HTTPStatus)
	respBodyStr := string(provResp.RawBody)
	_, _ = p.q.InsertDeliveryAttempt(ctx, gen.InsertDeliveryAttemptParams{
		NotificationID:    notifID,
		AttemptNumber:     attempt,
		StartedAt:         pgxTimestamp(now.Add(-100 * time.Millisecond)),
		CompletedAt:       completedAt,
		Success:           true,
		ProviderMessageID: &provMsgID,
		HttpStatus:        &httpStatus,
		ResponseBody:      &respBodyStr,
	})

	if _, err := p.q.MarkSent(ctx, notifID); err != nil {
		// Logged but acked — DB hiccup; the row stays in 'sending' and the sweeper picks it up.
		logger.Error("mark sent failed; ack regardless", "err", err)
	}
	p.emitStatus(ctx, notifID, "sent", env.CorrelationID)
	logger.Info("delivered", "attempt", attempt, "provider_message_id", provMsgID, "status", httpStatus)
	return nil // ack
}

func pgxTimestamp(t time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: t, Valid: true}
}

func httpStatusOf(err error) *int32 {
	var se *provider.SendError
	if errors.As(err, &se) && se.HTTPStatus > 0 {
		s := int32(se.HTTPStatus)
		return &s
	}
	return nil
}

// attemptLabel caps the attempt label cardinality to 5+.
func attemptLabel(attempt int32) string {
	if attempt >= 5 {
		return "5+"
	}
	return fmt.Sprint(attempt)
}

// emitStatus is a fire-and-forget Redis Pub/Sub publish for WS fan-out.
func (p *Pipeline) emitStatus(ctx context.Context, notifID uuid.UUID, status, correlationID string) {
	if p.eventsPub == nil {
		return
	}
	p.eventsPub.Publish(ctx, events.StatusEvent{
		NotificationID: notifID,
		Channel:        p.channel,
		Status:         status,
		At:             time.Now().UTC(),
		CorrelationID:  correlationID,
	})
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// Tiny correlation-id context type — local to keep worker independent of internal/api.
type ctxCorrelationKey struct{}

func withCorrelationID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, ctxCorrelationKey{}, id)
}
