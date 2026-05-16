package queue

import (
	"context"
	"log/slog"
	"time"

	"github.com/mertovun/event-driven-notification-system/internal/observability"
)

// DepthSampler polls broker-reported queue depths into the queue_depth gauge
// at a fixed interval. One sampler per process is enough; the gauge is labeled
// by queue + priority bucket so dashboards can break out per-channel.
//
// Sampling cadence is deliberately slow (default 5s) because each call is one
// AMQP round-trip on the shared publisher channel — under heavy publish load
// the mutex contention from a 100ms sampler would shape the publisher path.
type DepthSampler struct {
	pub      *Publisher
	metrics  *observability.Metrics
	logger   *slog.Logger
	interval time.Duration
}

// NewDepthSampler returns a sampler ready to Run. interval ≤ 0 → 5s default.
func NewDepthSampler(pub *Publisher, metrics *observability.Metrics, logger *slog.Logger, interval time.Duration) *DepthSampler {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &DepthSampler{
		pub:      pub,
		metrics:  metrics,
		logger:   logger,
		interval: interval,
	}
}

// Run polls per-channel queue depths until ctx is cancelled. Each tick samples
// the three main queues (sms/email/push) and the three dead-letter queues.
// Errors are logged at debug — broker hiccups are noisy and the gauge survives.
//
// Returns nil on graceful shutdown; never returns a non-nil error so the
// errgroup root isn't tripped by a sampler hiccup.
func (s *DepthSampler) Run(ctx context.Context) error {
	if s.pub == nil || s.metrics == nil {
		return nil
	}
	s.logger.Info("queue depth sampler started", "interval", s.interval)
	t := time.NewTicker(s.interval)
	defer t.Stop()

	queues := []struct{ name, priority string }{
		{QueueSMS, "main"},
		{QueueEmail, "main"},
		{QueuePush, "main"},
		{QueueSMSDLQ, "dlq"},
		{QueueEmailDLQ, "dlq"},
		{QueuePushDLQ, "dlq"},
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			for _, q := range queues {
				depth, err := s.pub.QueueDepth(q.name)
				if err != nil {
					s.logger.Debug("queue depth sample failed", "queue", q.name, "err", err)
					continue
				}
				s.metrics.QueueDepth.WithLabelValues(q.name, q.priority).Set(float64(depth))
			}
		}
	}
}
