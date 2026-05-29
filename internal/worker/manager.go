package worker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"

	"github.com/mertovun/event-driven-notification-system/internal/events"
	"github.com/mertovun/event-driven-notification-system/internal/observability"
	"github.com/mertovun/event-driven-notification-system/internal/provider"
	"github.com/mertovun/event-driven-notification-system/internal/queue"
	"github.com/mertovun/event-driven-notification-system/internal/ratelimit"
	"github.com/mertovun/event-driven-notification-system/internal/store/gen"
)

// Manager owns N worker pools (one per channel). Each pool runs `count` goroutines
// each holding their own AMQP consumer with prefetch=1 so RabbitMQ
// priority queues are honoured per-message (a consumer with prefetch>1
// would have already pulled lower-priority work into its local buffer).
type Manager struct {
	amqpURL   string
	pool      *pgxpool.Pool
	q         *gen.Queries
	rdb       *redis.Client
	limiter   *ratelimit.Limiter
	prov      *provider.HTTPClient
	queuePub  *queue.Publisher
	metrics   *observability.Metrics
	eventsPub *events.Publisher
	logger    *slog.Logger
	counts    map[string]int // channel → worker count

	mu        sync.Mutex
	consumers []*queue.Consumer
}

// PoolSpec describes how many workers per channel.
type PoolSpec struct {
	SMSCount   int
	EmailCount int
	PushCount  int
}

// NewManager constructs a Manager. Consumers are dialed lazily in Run().
// queuePub is required: the worker publishes to retry-tier queues on retryable
// failures (TTL+DLX bounces back to main exchange), instead of nack-requeue
// which would busy-loop the same message at full prefetch rate.
func NewManager(
	amqpURL string,
	pool *pgxpool.Pool,
	q *gen.Queries,
	rdb *redis.Client,
	limiter *ratelimit.Limiter,
	prov *provider.HTTPClient,
	queuePub *queue.Publisher,
	metrics *observability.Metrics,
	eventsPub *events.Publisher,
	logger *slog.Logger,
	spec PoolSpec,
) *Manager {
	return &Manager{
		amqpURL:   amqpURL,
		pool:      pool,
		q:         q,
		rdb:       rdb,
		limiter:   limiter,
		prov:      prov,
		queuePub:  queuePub,
		metrics:   metrics,
		eventsPub: eventsPub,
		logger:    logger,
		counts: map[string]int{
			"sms":   spec.SMSCount,
			"email": spec.EmailCount,
			"push":  spec.PushCount,
		},
	}
}

// Run starts all worker goroutines. Blocks until ctx is cancelled or any
// consumer fails.
//
// Shutdown semantics: cancelling ctx stops new deliveries (consumer.Run
// returns when the broker confirms the consumer cancel) and the AMQP
// channel is torn down via the deferred Close. In-flight handler
// goroutines invoked from the for-select see a cancelled ctx and unwind
// — their cleanup (inflight-lock Del, RevertToQueuedNoAttempt) runs with
// context.Background so it survives, but the broker has not been acked
// for those messages, so AMQP will redeliver after the heartbeat timeout.
// That redelivery is caught downstream by the worker-side dedupe stack
// (MarkSendingCAS + SETNX inflight lock); no message is lost. Earlier
// docs claimed "in-flight handlers run to completion" — they don't, and
// the at-least-once contract is what carries the safety here.
//
// Dials ONE shared amqp.Connection for the whole manager and opens a
// channel per consumer off it. An earlier design dialed a fresh TCP
// connection per worker — 24 connections per replica at the default
// pool spec — which RabbitMQ accepts but is well past the broker's
// per-connection overhead sweet spot. One connection + N channels is
// the published guidance.
func (m *Manager) Run(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)

	cc, err := queue.NewConsumerConnection(gctx, m.amqpURL, m.logger)
	if err != nil {
		return fmt.Errorf("amqp consumer connection: %w", err)
	}
	defer func() { _ = cc.Close() }()

	channelQueues := map[string]string{
		"sms":   queue.QueueSMS,
		"email": queue.QueueEmail,
		"push":  queue.QueuePush,
	}

	for chName, qName := range channelQueues {
		count := m.counts[chName]
		if count <= 0 {
			continue
		}
		// One Pipeline per channel — shared across the N workers for that channel.
		pipe := New(chName, m.pool, m.q, m.rdb, m.limiter, m.prov, m.queuePub, m.metrics, m.eventsPub, m.logger)

		for i := 0; i < count; i++ {
			workerName := fmt.Sprintf("%s-%d", chName, i)
			qn := qName
			g.Go(func() error {
				// Park until the shared connection is live. Handles the
				// broker-down-at-startup case: without this, NewConsumer would
				// fail on a nil connection and crash the errgroup.
				if !cc.WaitReady(gctx) {
					return nil // ctx cancelled before broker ever came up
				}
				cons, err := queue.NewConsumer(gctx, cc, qn, 1, m.logger.With("worker", workerName))
				if err != nil {
					return fmt.Errorf("worker %s consumer: %w", workerName, err)
				}
				m.mu.Lock()
				m.consumers = append(m.consumers, cons)
				m.mu.Unlock()
				defer func() { _ = cons.Close() }()
				// RunForever (not Run) so a transient RabbitMQ outage pauses
				// and resumes this worker instead of returning an error that
				// fails the errgroup and crashes the whole process. The shared
				// ConsumerConnection auto-reconnects; the worker re-opens its
				// channel and resumes once the broker is back.
				return cons.RunForever(gctx, pipe.Handle)
			})
		}
		m.logger.Info("worker pool started", "channel", chName, "count", count)
	}

	return g.Wait()
}
