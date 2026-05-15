package worker

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"

	"github.com/mertovun/event-driven-notification-system/internal/provider"
	"github.com/mertovun/event-driven-notification-system/internal/queue"
	"github.com/mertovun/event-driven-notification-system/internal/ratelimit"
	"github.com/mertovun/event-driven-notification-system/internal/store/gen"
)

// Manager owns N worker pools (one per channel). Each pool runs `count` goroutines
// each holding their own AMQP consumer with prefetch=1 (per docs/04 §3.1).
type Manager struct {
	amqpURL string
	pool    *pgxpool.Pool
	q       *gen.Queries
	rdb     *redis.Client
	limiter *ratelimit.Limiter
	prov    *provider.HTTPClient
	logger  *slog.Logger
	counts  map[string]int // channel → worker count

	consumers []*queue.Consumer
}

// PoolSpec describes how many workers per channel.
type PoolSpec struct {
	SMSCount   int
	EmailCount int
	PushCount  int
}

// NewManager constructs a Manager. Consumers are dialed lazily in Run().
func NewManager(
	amqpURL string,
	pool *pgxpool.Pool,
	q *gen.Queries,
	rdb *redis.Client,
	limiter *ratelimit.Limiter,
	prov *provider.HTTPClient,
	logger *slog.Logger,
	spec PoolSpec,
) *Manager {
	return &Manager{
		amqpURL: amqpURL,
		pool:    pool,
		q:       q,
		rdb:     rdb,
		limiter: limiter,
		prov:    prov,
		logger:  logger,
		counts: map[string]int{
			"sms":   spec.SMSCount,
			"email": spec.EmailCount,
			"push":  spec.PushCount,
		},
	}
}

// Run starts all worker goroutines. Blocks until ctx is cancelled or any consumer fails.
// Graceful shutdown: cancelling ctx stops consumers; in-flight handlers run to completion.
func (m *Manager) Run(ctx context.Context) error {
	g, gctx := errgroup.WithContext(ctx)

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
		pipe := New(chName, m.pool, m.q, m.rdb, m.limiter, m.prov, m.logger)

		for i := 0; i < count; i++ {
			workerName := fmt.Sprintf("%s-%d", chName, i)
			qn := qName
			g.Go(func() error {
				cons, err := queue.NewConsumer(gctx, m.amqpURL, qn, 1, m.logger.With("worker", workerName))
				if err != nil {
					return fmt.Errorf("worker %s consumer: %w", workerName, err)
				}
				m.consumers = append(m.consumers, cons) // best-effort tracking
				defer func() { _ = cons.Close() }()
				return cons.Run(gctx, pipe.Handle)
			})
		}
		m.logger.Info("worker pool started", "channel", chName, "count", count)
	}

	return g.Wait()
}
