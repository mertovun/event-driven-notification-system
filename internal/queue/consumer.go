package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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

// Consumer wraps one AMQP channel + one queue and pumps deliveries to a Handler.
// One Consumer per (channel, replica). Prefetch=1 to make priority queues honour priority.
type Consumer struct {
	url       string
	queueName string
	prefetch  int
	logger    *slog.Logger

	conn    *amqp.Connection
	channel *amqp.Channel
}

// NewConsumer dials AMQP, declares topology, and applies prefetch=N.
func NewConsumer(_ context.Context, url, queueName string, prefetch int, logger *slog.Logger) (*Consumer, error) {
	conn, err := amqp.DialConfig(url, amqp.Config{Heartbeat: 30 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("amqp dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("amqp channel: %w", err)
	}
	if err := DeclareTopology(ch); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("declare topology: %w", err)
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, fmt.Errorf("basic.qos: %w", err)
	}
	logger.Info("amqp consumer connected", "queue", queueName, "prefetch", prefetch)
	return &Consumer{
		url: url, queueName: queueName, prefetch: prefetch, logger: logger,
		conn: conn, channel: ch,
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

// ErrTerminal — handler signal that the message should go to the DLQ, not retried.
var ErrTerminal = errors.New("terminal: route to DLQ")

// Close shuts down the channel and connection.
func (c *Consumer) Close() error {
	if c.channel != nil {
		_ = c.channel.Close()
	}
	if c.conn != nil && !c.conn.IsClosed() {
		return c.conn.Close()
	}
	return nil
}
