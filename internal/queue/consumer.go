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

// errBrokerDropped is returned by Consumer.Run when the delivery channel closes
// because the broker connection dropped (as opposed to a clean ctx cancel). The
// Manager treats it as recoverable: wait for reconnect, re-open the channel,
// resume. Internal to the package.
var errBrokerDropped = errors.New("amqp delivery channel closed (broker drop)")

// ConsumerConnection wraps one shared amqp.Connection that multiple Consumers
// share channels off. AMQP best practice is one TCP connection per process
// (or per role) and N lightweight channels off it. An earlier shape dialed
// a fresh connection per worker — at the default pool spec that's 24 TCP
// connections per replica, which the broker accepts but is the wrong
// resource shape to take to production.
type ConsumerConnection struct {
	url    string
	logger *slog.Logger
	ctx    context.Context

	mu    sync.Mutex
	conn  *amqp.Connection
	ready chan struct{} // closed while a live connection exists; replaced on drop

	closeOnce sync.Once
	stopped   chan struct{}
}

// NewConsumerConnection dials AMQP and declares topology on a throwaway
// channel. The shared connection is then handed out to NewConsumer via
// (*ConsumerConnection).NewConsumer. The ctx governs reconnect attempts — when
// it is cancelled, the background reconnect loop stops re-dialing.
func NewConsumerConnection(ctx context.Context, url string, logger *slog.Logger) (*ConsumerConnection, error) {
	cc := &ConsumerConnection{
		url:     url,
		logger:  logger,
		ctx:     ctx,
		ready:   make(chan struct{}),
		stopped: make(chan struct{}),
	}
	if err := cc.dial(); err != nil {
		// Broker down at startup: don't fail. Start the reconnect loop and
		// return a usable connection whose consumers park on waitReady until
		// the broker comes up. Lets the process boot with RabbitMQ down.
		cc.logger.Warn("amqp consumer connection: broker unreachable at startup; will retry in background", "err", err)
		go cc.reconnectLoop()
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

	closeCh := make(chan *amqp.Error, 1)
	conn.NotifyClose(closeCh)

	cc.mu.Lock()
	cc.conn = conn
	// Signal readiness: close (or recreate-then-close) the ready gate so
	// waitReady unblocks for every worker currently parked on a broker drop.
	select {
	case <-cc.ready:
		// already closed from a prior successful dial — leave it closed
	default:
		close(cc.ready)
	}
	cc.mu.Unlock()

	go cc.watchClose(closeCh)

	cc.logger.Info("amqp consumer connection established", "url-redacted", redactURL(cc.url))
	return nil
}

// watchClose blocks until the broker connection drops, then re-dials with
// exponential backoff until success, ctx cancel, or Close(). Mirrors the
// publisher's reconnect loop so a broker blip no longer crashes the process.
func (cc *ConsumerConnection) watchClose(closeCh chan *amqp.Error) {
	select {
	case <-cc.stopped:
		return
	case <-cc.ctx.Done():
		return
	case err, ok := <-closeCh:
		if !ok {
			return
		}
		cc.logger.Warn("amqp consumer connection closed; reconnecting", "err", err)
	}

	// Arm a fresh ready gate so workers calling waitReady block until the
	// reconnect below succeeds.
	cc.mu.Lock()
	cc.ready = make(chan struct{})
	cc.conn = nil
	cc.mu.Unlock()

	cc.reconnectLoop()
}

// reconnectLoop re-dials with exponential backoff (1s→30s) until success,
// ctx cancel, or Close(). A successful dial arms a fresh watchClose for the
// new connection. Shared by the startup path (broker down at boot) and
// watchClose (broker dropped while running).
func (cc *ConsumerConnection) reconnectLoop() {
	backoff := time.Second
	for {
		select {
		case <-cc.stopped:
			return
		case <-cc.ctx.Done():
			return
		default:
		}
		if err := cc.dial(); err == nil {
			cc.logger.Info("amqp consumer reconnected")
			return // dial() started a new watchClose for the new connection
		} else {
			cc.logger.Warn("amqp consumer reconnect failed", "err", err, "next-in", backoff)
		}
		select {
		case <-cc.stopped:
			return
		case <-cc.ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

// WaitReady blocks until a live connection exists, the context is cancelled, or
// the connection is permanently closed. Returns true if ready, false if the
// caller should stop. Callers should invoke this before NewConsumer so that a
// broker that is down at startup parks the worker instead of failing.
func (cc *ConsumerConnection) WaitReady(ctx context.Context) bool {
	return cc.waitReady(ctx)
}

// waitReady blocks until a live connection exists, the context is cancelled, or
// the connection is permanently closed. Returns true if the connection is ready
// to use, false if the caller should stop (shutdown).
func (cc *ConsumerConnection) waitReady(ctx context.Context) bool {
	cc.mu.Lock()
	ready := cc.ready
	cc.mu.Unlock()
	select {
	case <-ready:
		return true
	case <-ctx.Done():
		return false
	case <-cc.stopped:
		return false
	}
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
	cc        *ConsumerConnection
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
		cc: cc, queueName: queueName, prefetch: prefetch, logger: logger,
		channel: ch,
	}, nil
}

// reopen acquires a fresh channel on the (reconnected) shared connection and
// re-applies prefetch. Called after a broker drop once the connection is back.
func (c *Consumer) reopen() error {
	ch, err := c.cc.channel()
	if err != nil {
		return fmt.Errorf("acquire channel: %w", err)
	}
	if err := ch.Qos(c.prefetch, 0, false); err != nil {
		_ = ch.Close()
		return fmt.Errorf("basic.qos: %w", err)
	}
	c.channel = ch
	return nil
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
		// If the broker connection died between channel acquisition and the
		// basic.consume call, the channel is already closed and this fails
		// with a 504. That's a broker drop, not a genuine failure — signal
		// the resume path to wait for reconnect rather than crashing.
		if ctx.Err() == nil && c.channel.IsClosed() {
			return errBrokerDropped
		}
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
				// Distinguish a clean shutdown (ctx already cancelled, handled
				// by the case above on the next loop) from a broker-side drop.
				// If ctx is still live, the broker connection died — signal the
				// caller to wait for reconnect and resume rather than treating
				// it as a fatal error.
				if ctx.Err() != nil {
					return nil
				}
				return errBrokerDropped
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

// RunForever runs the consumer across broker reconnects. It returns nil on a
// clean ctx cancel (shutdown). On a broker drop it waits for the shared
// connection to reconnect, re-opens its channel, and resumes consuming —
// turning a transient RabbitMQ outage into a recoverable pause instead of a
// fatal error that tears the process down. Unacked in-flight messages are
// redelivered by the broker after reconnect and deduped by the worker-side
// CAS + SETNX stack, so no message is lost.
func (c *Consumer) RunForever(ctx context.Context, h Handler) error {
	for {
		err := c.Run(ctx, h)
		switch {
		case err == nil:
			return nil // clean shutdown
		case errors.Is(err, errBrokerDropped):
			// Drop the dead channel, wait for the connection to come back.
			_ = c.channel.Close()
			c.logger.Warn("consumer paused; waiting for broker reconnect", "queue", c.queueName)
			if !c.cc.waitReady(ctx) {
				return nil // ctx cancelled or connection permanently closed
			}
			if rerr := c.reopen(); rerr != nil {
				// Connection came back but the channel re-open raced another
				// drop; loop and wait again rather than failing the process.
				c.logger.Warn("consumer channel re-open failed; retrying", "queue", c.queueName, "err", rerr)
				continue
			}
			c.logger.Info("consumer resumed after reconnect", "queue", c.queueName)
		default:
			return err // genuine, non-recoverable failure
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
