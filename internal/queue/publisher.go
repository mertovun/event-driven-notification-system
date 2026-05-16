// Package queue wraps RabbitMQ: topology, publisher, and consumer adapters.
package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// PublishMessage is the small input contract for Publisher.Publish.
// payload, headers, routing_key, and priority match the columns on the outbox row.
type PublishMessage struct {
	RoutingKey string
	Payload    []byte
	Headers    map[string]any
	Priority   uint8
}

// Publisher owns the AMQP connection + a channel pool for publishing with confirms.
// One Publisher per process. Safe for concurrent use.
type Publisher struct {
	url    string
	logger *slog.Logger

	mu      sync.Mutex
	conn    *amqp.Connection
	channel *amqp.Channel // single confirm-enabled channel; serialized via mu

	closeOnce sync.Once
	stopped   chan struct{}
}

// NewPublisher dials AMQP, declares the topology, and arms publisher confirms.
// The caller must Close() on shutdown.
func NewPublisher(ctx context.Context, url string, logger *slog.Logger) (*Publisher, error) {
	p := &Publisher{
		url:     url,
		logger:  logger,
		stopped: make(chan struct{}),
	}
	if err := p.dial(ctx); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Publisher) dial(ctx context.Context) error {
	// Use DialConfig so we can pass a heartbeat appropriate for slow consumers.
	conn, err := amqp.DialConfig(p.url, amqp.Config{
		Heartbeat: 30 * time.Second,
		Locale:    "en_US",
	})
	if err != nil {
		return fmt.Errorf("amqp dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("amqp channel: %w", err)
	}
	if err := DeclareTopology(ch); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("declare topology: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("confirm.select: %w", err)
	}

	p.mu.Lock()
	p.conn = conn
	p.channel = ch
	p.mu.Unlock()

	// Async reconnect on NotifyClose.
	closeCh := make(chan *amqp.Error, 1)
	conn.NotifyClose(closeCh)
	go p.watchClose(ctx, closeCh)

	p.logger.Info("amqp publisher connected", "url-redacted", redactURL(p.url))
	return nil
}

func (p *Publisher) watchClose(ctx context.Context, closeCh chan *amqp.Error) {
	select {
	case <-p.stopped:
		return
	case <-ctx.Done():
		return
	case err, ok := <-closeCh:
		if !ok {
			return
		}
		p.logger.Warn("amqp connection closed; reconnecting", "err", err)
	}

	backoff := time.Second
	for {
		select {
		case <-p.stopped:
			return
		case <-ctx.Done():
			return
		default:
		}
		if err := p.dial(ctx); err == nil {
			p.logger.Info("amqp reconnected")
			return
		} else {
			p.logger.Warn("amqp reconnect failed", "err", err, "next-in", backoff)
		}
		time.Sleep(backoff)
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

// Publish publishes one message to ExchangeMain with the supplied routing key.
// Blocks until the broker confirms (or 5s timeout).
//
// Persistent (delivery_mode=2), mandatory=true so unroutable messages surface
// instead of vanishing silently.
func (p *Publisher) Publish(ctx context.Context, m PublishMessage) error {
	return p.publish(ctx, ExchangeMain, m.RoutingKey, true, m)
}

// PublishToWaitQueue publishes directly to one of the TTL retry-tier queues
// (notifications.wait.5s/30s/5m) via the default exchange. The retry queue's
// x-dead-letter-exchange routes the message back to ExchangeMain on TTL
// expiry — RabbitMQ preserves the original routing key in the x-death header
// and reuses it, so the message lands on the channel queue exactly as if it
// had been freshly published. The original routing key is also encoded in the
// headers under "x-original-routing-key" for handlers that want to inspect it.
//
// mandatory=false: the wait queue is the only valid destination and we want
// the publish to fail fast if topology is misconfigured rather than spin.
func (p *Publisher) PublishToWaitQueue(ctx context.Context, waitQueue string, m PublishMessage) error {
	// Tag the wait-bound publish so headers carry the original routing key —
	// useful for debugging, not load-bearing (the broker re-routes via x-death).
	if m.Headers == nil {
		m.Headers = make(map[string]any, 1)
	}
	if _, ok := m.Headers["x-original-routing-key"]; !ok {
		m.Headers["x-original-routing-key"] = m.RoutingKey
	}
	return p.publish(ctx, "", waitQueue, false, m)
}

func (p *Publisher) publish(ctx context.Context, exchange, routingKey string, mandatory bool, m PublishMessage) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.channel == nil {
		return errors.New("publisher: no channel")
	}

	publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	hdrs := amqp.Table{}
	for k, v := range m.Headers {
		hdrs[k] = v
	}

	conf, err := p.channel.PublishWithDeferredConfirmWithContext(
		publishCtx,
		exchange,
		routingKey,
		mandatory,
		false, // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         m.Payload,
			Headers:      hdrs,
			DeliveryMode: amqp.Persistent,
			Priority:     m.Priority,
			Timestamp:    time.Now().UTC(),
		},
	)
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	// Wait for ack/nack with the per-publish deadline.
	acked, err := conf.WaitContext(publishCtx)
	if err != nil {
		return fmt.Errorf("confirm wait: %w", err)
	}
	if !acked {
		return errors.New("publish: nacked by broker")
	}
	return nil
}

// Ping returns nil if the AMQP connection is alive and the channel is open.
// Lightweight: does not write to the broker.
func (p *Publisher) Ping(_ context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conn == nil || p.conn.IsClosed() {
		return errors.New("amqp connection closed")
	}
	if p.channel == nil {
		return errors.New("amqp channel nil")
	}
	return nil
}

// Close shuts down the channel and connection. Idempotent.
func (p *Publisher) Close() error {
	var err error
	p.closeOnce.Do(func() {
		close(p.stopped)
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.channel != nil {
			_ = p.channel.Close()
		}
		if p.conn != nil && !p.conn.IsClosed() {
			err = p.conn.Close()
		}
	})
	return err
}

// redactURL hides any embedded credentials before logging the AMQP URL.
func redactURL(u string) string {
	// amqp://user:pass@host:port/ → amqp://***@host:port/
	at := -1
	scheme := -1
	for i := 0; i < len(u); i++ {
		if u[i] == ':' && i+2 < len(u) && u[i+1] == '/' && u[i+2] == '/' {
			scheme = i + 3
		}
		if u[i] == '@' {
			at = i
			break
		}
	}
	if scheme < 0 || at < 0 || at <= scheme {
		return u
	}
	return u[:scheme] + "***" + u[at:]
}
