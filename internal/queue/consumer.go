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

// Delivery is the consumer-side handoff envelope. Wraps amqp.Delivery with
// helper accessors for headers and provides ack/nack abstractions so the worker
// pipeline does not need amqp091-go in its signature.
type Delivery struct {
	d amqp.Delivery
}

func (d Delivery) Body() []byte         { return d.d.Body }
func (d Delivery) Headers() amqp.Table  { return d.d.Headers }
func (d Delivery) RoutingKey() string   { return d.d.RoutingKey }
func (d Delivery) Priority() uint8      { return d.d.Priority }
func (d Delivery) DeliveryTag() uint64  { return d.d.DeliveryTag }
func (d Delivery) Timestamp() time.Time { return d.d.Timestamp }
func (d Delivery) Ack() error           { return d.d.Ack(false) }
func (d Delivery) NackRequeue() error   { return d.d.Nack(false, true) }
func (d Delivery) NackDLQ() error       { return d.d.Nack(false, false) }

// Handler processes one delivery. Return nil to ack; return an error to let
// the consumer decide ack/nack/republish based on policy.
type Handler func(ctx context.Context, d Delivery) error

// ErrTerminal — handler signal that the message should go to the DLQ, not retried.
var ErrTerminal = errors.New("terminal: route to DLQ")

// ConsumerConnection wraps one shared amqp.Connection that multiple Consumers
// share channels off. AMQP best practice is one TCP connection per process
// (or per role) and N lightweight channels off it. An earlier shape dialed
// a fresh connection per worker — at the default pool spec that's 24 TCP
// connections per replica, which the broker accepts but is the wrong
// resource shape to take to production.
type ConsumerConnection struct {
	url    string
	logger *slog.Logger

	mu   sync.Mutex
	conn *amqp.Connection

	closeOnce sync.Once
	stopped   chan struct{}
}

// NewConsumerConnection dials AMQP and declares topology on a throwaway
// channel. The shared connection is then handed out to NewConsumer via
// (*ConsumerConnection).NewConsumer.
func NewConsumerConnection(_ context.Context, url string, logger *slog.Logger) (*ConsumerConnection, error) {
	cc := &ConsumerConnection{
		url:     url,
		logger:  logger,
		stopped: make(chan struct{}),
	}
	if err := cc.dial(); err != nil {
		return nil, err
	}
	return cc, nil
}

func (cc *ConsumerConnection) dial() error {
	conn, err := amqp.DialConfig(cc.url, amqp.Config{Heartbeat: 30 * time.Second})
	if err != nil {
		return fmt.Errorf("amqp dial: %w", err)
	}

	// One-shot topology declaration on a throwaway channel — broker stores
	// topology globally so consumer channels just bind/consume.
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("amqp channel for topology: %w", err)
	}
	if err := DeclareTopology(ch); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("declare topology: %w", err)
	}
	_ = ch.Close()

	cc.mu.Lock()
	cc.conn = conn
	cc.mu.Unlock()

	cc.logger.Info("amqp consumer connection established", "url-redacted", redactURL(cc.url))
	return nil
}

// channel returns a fresh AMQP channel on the shared connection. Callers
// own the lifecycle (Close on shutdown).
func (cc *ConsumerConnection) channel() (*amqp.Channel, error) {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	if cc.conn == nil || cc.conn.IsClosed() {
		return nil, errors.New("consumer connection: closed (reconnecting?)")
	}
	return cc.conn.Channel()
}

// Close shuts down the shared connection. Idempotent.
func (cc *ConsumerConnection) Close() error {
	var err error
	cc.closeOnce.Do(func() {
		close(cc.stopped)
		cc.mu.Lock()
		defer cc.mu.Unlock()
		if cc.conn != nil && !cc.conn.IsClosed() {
			err = cc.conn.Close()
		}
	})
	return err
}

// Consumer wraps one AMQP channel + one queue and pumps deliveries to a Handler.
// One Consumer per (channel, replica). Prefetch=1 to make priority queues honour priority.
// Holds a channel on a shared ConsumerConnection — no per-Consumer TCP connection.
type Consumer struct {
	queueName string
	prefetch  int
	logger    *slog.Logger
	channel   *amqp.Channel
}

// NewConsumer opens a channel on the shared connection, applies prefetch,
// and returns a ready Consumer. The shared connection MUST outlive the
// Consumer.
func NewConsumer(_ context.Context, cc *ConsumerConnection, queueName string, prefetch int, logger *slog.Logger) (*Consumer, error) {
	ch, err := cc.channel()
	if err != nil {
		return nil, fmt.Errorf("acquire channel: %w", err)
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("basic.qos: %w", err)
	}
	logger.Info("amqp consumer ready", "queue", queueName, "prefetch", prefetch)
	return &Consumer{
		queueName: queueName, prefetch: prefetch, logger: logger,
		channel: ch,
	}, nil
}

// Run subscribes and invokes handler for each delivery until ctx is cancelled or the
// channel is closed by the broker. ack/nack policy:
//   - handler returns nil → Ack
//   - handler returns ErrTerminal → NackDLQ (no requeue)
//   - any other error → NackRequeue
func (c *Consumer) Run(ctx context.Context, h Handler) error {
	consumerTag := fmt.Sprintf("notifyd-%s", c.queueName)
	deliveries, err := c.channel.ConsumeWithContext(
		ctx, c.queueName, consumerTag,
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	if err != nil {
		return fmt.Errorf("basic.consume: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			// Stop receiving new deliveries; the broker will redeliver unacked messages.
			_ = c.channel.Cancel(consumerTag, false)
			return nil
		case d, ok := <-deliveries:
			if !ok {
				return errors.New("delivery channel closed")
			}
			wrapped := Delivery{d: d}
			if err := h(ctx, wrapped); err != nil {
				if errors.Is(err, ErrTerminal) {
					_ = wrapped.NackDLQ()
				} else {
					_ = wrapped.NackRequeue()
				}
				continue
			}
			_ = wrapped.Ack()
		}
	}
}

// Close shuts down only this consumer's channel. The shared connection
// belongs to the ConsumerConnection that constructed us.
func (c *Consumer) Close() error {
	if c.channel != nil {
		return c.channel.Close()
	}
	return nil
}
