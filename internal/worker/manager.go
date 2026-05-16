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

// Run starts all worker goroutines. Blocks until ctx is cancelled or any consumer fails.
// Graceful shutdown: cancelling ctx stops consumers; in-flight handlers run to completion.
//
// Dials ONE shared amqp.Connection for the whole manager and opens a channel
// per consumer off it. Previously each consumer dialed its own TCP connection
// — 24 connections per replica at the default pool spec — which the Architecture
// and Go reviewers flagged. RabbitMQ's published guidance is one connection
// per process (or per role) with N channels.
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
				cons, err := queue.NewConsumer(gctx, cc, qn, 1, m.logger.With("worker", workerName))
				if err != nil {
					return fmt.Errorf("worker %s consumer: %w", workerName, err)
				}
				m.mu.Lock()
				m.consumers = append(m.consumers, cons)
				m.mu.Unlock()
				defer func() { _ = cons.Close() }()
				return cons.Run(gctx, pipe.Handle)
			})
		}
		m.logger.Info("worker pool started", "channel", chName, "count", count)
	}

	return g.Wait()
}
