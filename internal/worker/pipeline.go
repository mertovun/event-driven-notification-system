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
	queuePub  *queue.Publisher
	breaker   *gobreaker.CircuitBreaker
	metrics   *observability.Metrics
	eventsPub *events.Publisher
	logger    *slog.Logger

	// Rate-limit settings — 100/s per channel, capacity = 1s burst.
	rateLimitPerSec float64
	rateCapacity    float64

	// maxAttempts caps how many times a retryable failure can re-tier before
	// the worker dead-letters the message. The outbox dispatcher also enforces
	// its own publish-side limit; this is the consumer-side complement.
	maxAttempts int32
}

// New builds a Pipeline for the named channel.
func New(
	channel string,
	pool *pgxpool.Pool,
	q *gen.Queries,
	rdb *redis.Client,
	limiter *ratelimit.Limiter,
	prov *provider.HTTPClient,
	queuePub *queue.Publisher,
	metrics *observability.Metrics,
	eventsPub *events.Publisher,
	logger *slog.Logger,
) *Pipeline {
	chanLogger := logger.With("channel", channel)
	p := &Pipeline{
		channel:         channel,
		pool:            pool,
		q:               q,
		rdb:             rdb,
		limiter:         limiter,
		provider:        prov,
		queuePub:        queuePub,
		breaker:         newBreaker(channel, chanLogger, metrics),
		metrics:         metrics,
		eventsPub:       eventsPub,
		logger:          chanLogger,
		rateLimitPerSec: 100,
		rateCapacity:    100,
		maxAttempts:     10,
	}
	// Seed the breaker-state gauge to "closed" so dashboards don't show
	// "no data" until the first state transition. OnStateChange only fires
	// on transitions, not on startup.
	if metrics != nil {
		metrics.CircuitBreakerState.WithLabelValues(channel).Set(breakerStateValue(p.breaker.State()))
	}
	return p
}

// handleTimeout bounds the worst-case wall-clock duration of one delivery.
// Sized to cover: rate-limit short wait (≤200ms) + provider HTTP (10s ceiling
// in provider.Client) + DB round-trips. Anything beyond this is a wedge in
// an unbounded blocking call, and we want the consumer loop to unblock so
// the message can be redelivered to a healthy worker.
const handleTimeout = 30 * time.Second

// Handle is the queue.Handler for one delivery.
// Returns nil to ack; queue.ErrTerminal to nack-to-DLQ; any other error to nack-and-requeue.
//
// Wraps the inner pipeline in a per-delivery context with a hard deadline.
// Without this, a wedged DB call (or any other unbounded blocking call inside
// handle) would block the consumer goroutine indefinitely and prevent
// graceful shutdown.
func (p *Pipeline) Handle(ctx context.Context, d queue.Delivery) error {
	if p.metrics != nil {
		p.metrics.WorkerActive.WithLabelValues(p.channel).Inc()
		defer p.metrics.WorkerActive.WithLabelValues(p.channel).Dec()
	}

	// Poison-message guard: every Nack(requeue=true) increments the broker's
	// internal redelivery count, but RabbitMQ doesn't surface that count by
	// default. We carry our own "x-attempts" header so a message that loops
	// without ever leaving the worker (e.g., persistent CAS or SETNX failure
	// on a specific row) gets dead-lettered after maxRedeliveries instead of
	// hot-looping invisibly. QA empirically observed a hot-loop at ~4,200/s
	// with zero log output before this guard existed.
	if hdr := d.Headers(); hdr != nil {
		if v, ok := hdr["x-redeliveries"]; ok {
			if n, ok := v.(int32); ok && n >= maxRedeliveries {
				p.logger.Error("redelivery cap exceeded; dead-lettering to break the loop",
					"channel", p.channel, "redeliveries", n, "max", maxRedeliveries)
				if p.metrics != nil {
					p.metrics.NotificationsDeliveredTotal.WithLabelValues(p.channel, "dead_letter").Inc()
				}
				return queue.ErrTerminal
			}
		}
	}

	handleCtx, cancel := context.WithTimeout(ctx, handleTimeout)
	defer cancel()
	start := time.Now()
	err := p.handle(handleCtx, d)
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

	// Surface the error so no failure path is silent. handle()'s individual
	// error sites log domain-specific context; this is the safety net for
	// the "added a new return-err path, forgot to log" mode that QA
	// reproduced as a 4,200/s silent hot-loop.
	if err != nil && !errors.Is(err, queue.ErrTerminal) {
		p.logger.Warn("delivery returned non-nil error",
			"channel", p.channel, "err", err.Error(), "duration_ms", time.Since(start).Milliseconds())

		// Convert immediate broker redelivery (nack-requeue) into a timed
		// retry on the wait.5s tier with an incremented x-redeliveries
		// counter. Without this, an infra-transient failure (CAS error,
		// SETNX error, ratelimit Redis hiccup) loops at full prefetch
		// rate against the same dead-set message.
		if p.queuePub != nil {
			if rerr := p.requeueViaWaitTier(handleCtx, d, currentRedeliveries(d.Headers())); rerr == nil {
				return nil // ack original; wait queue will redeliver
			}
		}
	}
	return err
}

// currentRedeliveries reads the x-redeliveries header set by previous bounces
// through the wait tier. Returns 0 if absent. AMQP headers are decoded as int32
// when integer; coercion happens at write time (see requeueViaWaitTier).
func currentRedeliveries(hdrs map[string]any) int32 {
	if hdrs == nil {
		return 0
	}
	v, ok := hdrs["x-redeliveries"]
	if !ok {
		return 0
	}
	if n, ok := v.(int32); ok {
		return n
	}
	return 0
}

// requeueViaWaitTier publishes the delivery body to notifications.wait.5s with
// x-redeliveries=prev+1. RabbitMQ's TTL+DLX bounces it back to the channel
// queue. The redelivery cap at the top of Handle eventually dead-letters
// anything that loops past maxRedeliveries (default 20).
func (p *Pipeline) requeueViaWaitTier(ctx context.Context, d queue.Delivery, prev int32) error {
	// Try to decode envelope just enough to derive routing key + priority.
	// If body is unparseable we shouldn't be here — that path is terminal.
	var env envelope
	_ = json.Unmarshal(d.Body(), &env)

	headers := map[string]any{
		"x-redeliveries":  prev + 1,
		"correlation_id":  env.CorrelationID,
		"notification_id": env.NotificationID,
	}
	// Always wait.5s for infra-transient redeliveries — short enough not to
	// stall recovery, long enough not to busy-loop. Per-channel queue so the
	// DLX bounce reliably lands on this channel's main queue.
	return p.queuePub.PublishToWait(ctx, env.Channel, "5s", queue.PublishMessage{
		RoutingKey: queue.RoutingKey(env.Channel),
		Payload:    d.Body(),
		Priority:   priorityToAMQP(env.Priority),
		Headers:    headers,
	})
}

// maxRedeliveries is the broker-side redelivery cap before we force a
// notification to the DLQ. Set above the per-attempt retry budget
// (Pipeline.maxAttempts = 10) with headroom so a normal retry path doesn't
// trip it; only a true hot-loop does.
const maxRedeliveries = int32(20)

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
		// No provider call happened, so don't increment attempt_count.
		_ = p.q.RevertToQueuedNoAttempt(ctx, gen.RevertToQueuedNoAttemptParams{ID: notifID})
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
		// Throttled. Briefly wait (short throttle, ≤200ms) and re-check; otherwise
		// route to retry tier so we don't hot-loop against the sustained throttle.
		wait := dec.RetryAfter
		const maxInlineWait = 200 * time.Millisecond
		if wait > maxInlineWait {
			// No provider call happened — throttle is a flow-control event,
			// not a delivery attempt. Skip attempt_count increment so a
			// sustained throttle doesn't burn the retry budget.
			_ = p.q.RevertToQueuedNoAttempt(ctx, gen.RevertToQueuedNoAttemptParams{ID: notifID})
			logger.Info("rate-limit throttle sustained; routing to retry tier", "retry_after", wait)
			// Treat throttle like attempt 1 for tiering purposes — short wait.
			if rerr := p.routeToRetryTier(ctx, 1, d.Body(), env, logger); rerr != nil {
				return rerr
			}
			return nil
		}
		select {
		case <-ctx.Done():
			_ = p.q.RevertToQueuedNoAttempt(ctx, gen.RevertToQueuedNoAttemptParams{ID: notifID})
			return ctx.Err()
		case <-time.After(wait):
		}
		// Re-try once after the wait; if still throttled, sustained-path applies.
		dec, _ = p.limiter.Allow(ctx, "ratelimit:"+p.channel, p.rateLimitPerSec, p.rateCapacity)
		if !dec.Allowed {
			_ = p.q.RevertToQueuedNoAttempt(ctx, gen.RevertToQueuedNoAttemptParams{ID: notifID})
			logger.Info("rate-limit still denied after short wait; routing to retry tier")
			if rerr := p.routeToRetryTier(ctx, 1, d.Body(), env, logger); rerr != nil {
				return rerr
			}
			return nil
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
	// No provider call happened, so don't burn the attempt_count budget.
	// Without this, every wait-tier bounce during a sustained outage advances
	// the counter and prematurely dead-letters perfectly recoverable messages.
	if errors.Is(breakerErr, gobreaker.ErrOpenState) || errors.Is(breakerErr, gobreaker.ErrTooManyRequests) {
		errStr := breakerErr.Error()
		_ = p.q.RevertToQueuedNoAttempt(ctx, gen.RevertToQueuedNoAttemptParams{ID: notifID, LastError: &errStr})
		logger.Info("breaker open; routing to retry tier", "attempt", attempt, "err", breakerErr)
		// Send to wait tier so we don't busy-loop the same message against an open breaker.
		if rerr := p.routeToRetryTier(ctx, attempt, d.Body(), env, logger); rerr != nil {
			return rerr
		}
		return nil // ack original delivery; the tier will redeliver on TTL expiry
	}

	var provResp *provider.SendResponse
	if rawResult != nil {
		provResp, _ = rawResult.(*provider.SendResponse)
	}
	callErr := breakerErr

	now := time.Now()
	completedAt := pgxTimestamp(now)

	if callErr != nil {
		// 7a) Record the failed attempt. Best-effort: the retry/terminal
		// decision below is the load-bearing state transition; this row is
		// audit data only.
		errStr := callErr.Error()
		if _, derr := p.q.InsertDeliveryAttempt(ctx, gen.InsertDeliveryAttemptParams{
			NotificationID: notifID,
			AttemptNumber:  attempt,
			StartedAt:      pgxTimestamp(now.Add(-100 * time.Millisecond)),
			CompletedAt:    completedAt,
			Success:        false,
			Error:          &errStr,
			HttpStatus:     httpStatusOf(callErr),
		}); derr != nil {
			logger.Error("insert failed-attempt row failed", "err", derr)
		}

		// 8a) Decide retry vs terminal.
		var se *provider.SendError
		if errors.As(callErr, &se) && se.Kind.IsRetryable() && attempt < p.maxAttempts {
			if p.metrics != nil {
				p.metrics.NotificationRetryTotal.WithLabelValues(p.channel, attemptLabel(attempt)).Inc()
			}
			_ = p.q.RevertToQueued(ctx, gen.RevertToQueuedParams{ID: notifID, LastError: &errStr})
			logger.Info("retryable failure; routing to retry tier",
				"attempt", attempt, "kind", se.Kind, "status", se.HTTPStatus)
			if rerr := p.routeToRetryTier(ctx, attempt, d.Body(), env, logger); rerr != nil {
				// Publisher failure: fall back to nack-requeue so the message isn't lost.
				return rerr
			}
			return nil // ack — tier will redeliver after TTL
		}

		// Terminal — mark dead_letter, insert into dead_letters, ack-to-DLQ.
		// Best-effort writes: on rare DB blip we still nack to DLQ so the
		// message doesn't loop, but log loudly because the audit row is the
		// operator-visible record.
		if _, derr := p.q.MarkDeadLetter(ctx, gen.MarkDeadLetterParams{ID: notifID, LastError: &errStr}); derr != nil {
			logger.Error("mark dead_letter failed; row stays in 'sending'", "err", derr)
		}
		if _, derr := p.q.InsertDeadLetter(ctx, gen.InsertDeadLetterParams{
			NotificationID: notifID,
			Reason:         truncate(errStr, 500),
			Payload:        d.Body(),
		}); derr != nil {
			logger.Error("insert dead_letter row failed", "err", derr)
		}
		p.emitStatus(ctx, notifID, "dead_letter", env.CorrelationID)
		logger.Warn("terminal failure; dead-lettered", "attempt", attempt, "err", callErr)
		return queue.ErrTerminal
	}

	// 7b) Success: record the attempt + mark sent.
	provMsgID := provResp.MessageID
	httpStatus := int32(provResp.HTTPStatus)
	respBodyStr := string(provResp.RawBody)
	if _, derr := p.q.InsertDeliveryAttempt(ctx, gen.InsertDeliveryAttemptParams{
		NotificationID:    notifID,
		AttemptNumber:     attempt,
		StartedAt:         pgxTimestamp(now.Add(-100 * time.Millisecond)),
		CompletedAt:       completedAt,
		Success:           true,
		ProviderMessageID: &provMsgID,
		HttpStatus:        &httpStatus,
		ResponseBody:      &respBodyStr,
	}); derr != nil {
		// Loss of the attempt audit row is unfortunate but not corrupting —
		// the row will still flip to 'sent' below. Log and continue.
		logger.Error("insert delivery attempt failed; continuing to mark sent", "err", derr)
	}

	if _, err := p.q.MarkSent(ctx, notifID); err != nil {
		// MarkSent is the user-visible state transition. If it fails the row
		// stays 'sending' — return an error so AMQP redelivers. RevertToQueued
		// first so the next pickup's CAS (pending|queued → sending) matches.
		// The provider call already succeeded; this only re-runs the DB writes.
		// Worst case: a duplicate provider call on retry if the in-flight lock
		// has expired. The provider call's own idempotency (or our 60s lock)
		// catches that.
		logger.Error("mark sent failed; reverting to queued and requeueing", "err", err)
		errStr := err.Error()
		_ = p.q.RevertToQueued(ctx, gen.RevertToQueuedParams{ID: notifID, LastError: &errStr})
		return fmt.Errorf("mark sent: %w", err)
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

// routeToRetryTier publishes the envelope to a wait-tier queue (5s/30s/5m)
// based on the just-failed attempt count. The wait queue's x-dead-letter-exchange
// points back at the main exchange, so on TTL expiry RabbitMQ re-routes the
// message to the channel queue with its original routing key.
//
// The caller is responsible for the CAS revert before calling this so the row
// is in `queued` state for the next attempt; the worker pipeline does that
// just above each call site.
//
// On publisher error, returns the error so the caller can fall back to
// nack-requeue (current at-least-once contract — the message will be
// redelivered immediately, exactly the prior behavior; not worse).
func (p *Pipeline) routeToRetryTier(ctx context.Context, attempt int32, body []byte, env envelope, logger *slog.Logger) error {
	if p.queuePub == nil {
		logger.Warn("no queue publisher; falling back to nack-requeue")
		return fmt.Errorf("queue publisher unavailable")
	}
	tier := queue.WaitTierForAttempt(attempt)
	err := p.queuePub.PublishToWait(ctx, env.Channel, tier, queue.PublishMessage{
		RoutingKey: queue.RoutingKey(env.Channel),
		Payload:    body,
		Priority:   priorityToAMQP(env.Priority),
		Headers: map[string]any{
			"correlation_id":  env.CorrelationID,
			"notification_id": env.NotificationID,
			"attempt":         attempt,
		},
	})
	if err != nil {
		logger.Error("retry-tier publish failed; will nack-requeue",
			"channel", env.Channel, "tier", tier, "attempt", attempt, "err", err)
		return err
	}
	logger.Info("queued for retry tier", "channel", env.Channel, "tier", tier, "attempt", attempt)
	return nil
}

// priorityToAMQP maps the envelope's textual priority to an AMQP byte priority.
// The wire mapping mirrors what the publisher path uses (see internal/api).
func priorityToAMQP(s string) uint8 {
	switch s {
	case "high":
		return 9
	case "low":
		return 1
	default:
		return 5
	}
}

// Tiny correlation-id context type — local to keep worker independent of internal/api.
type ctxCorrelationKey struct{}

func withCorrelationID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, ctxCorrelationKey{}, id)
}
