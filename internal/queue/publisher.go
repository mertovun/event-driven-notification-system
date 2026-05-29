// Package queue wraps RabbitMQ: topology, publisher, and consumer adapters.
package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ErrPublisherUnavailable signals that a publish failed because the broker
// connection is down (no channel, or a closed-connection error), not because
// the message itself was rejected. Callers should treat this as a transient
// infrastructure failure — retry without counting it against any per-message
// attempt budget — rather than a delivery failure that should dead-letter.
var ErrPublisherUnavailable = errors.New("publisher: broker unavailable")

// PublishMessage is the small input contract for Publisher.Publish.
// payload, headers, routing_key, and priority match the columns on the outbox row.
type PublishMessage struct {
	RoutingKey string
	Payload    []byte
	Headers    map[string]any
	Priority   uint8
}

// pubChannel wraps an AMQP channel with its own per-channel mutex. The
// publisher fans out across N of these so confirm RTTs run in parallel
// instead of serializing behind one process-wide lock — a single shared
// mutex around Publish+WaitContext caps throughput at ~1 / RTT (around
// 200/s at a 5ms broker RTT) regardless of how many goroutines call in.
type pubChannel struct {
	ch *amqp.Channel
	mu sync.Mutex
}

// DefaultChannelPoolSize is the number of confirm-enabled channels per
// publisher process. Sized so confirm RTTs don't stack up on any one
// channel under typical (1k-10k msg/s) load — past 8 the contention
// returns are diminishing because we share one TCP connection.
const DefaultChannelPoolSize = 8

// Publisher owns one AMQP connection + N confirm-enabled channels. One
// Publisher per process; safe for concurrent use.
type Publisher struct {
	url      string
	logger   *slog.Logger
	poolSize int

	// connMu guards conn + chans during dial/reconnect. Hot-path publishes
	// don't take this lock — they take the per-channel mu inside pubChannel.
	connMu sync.RWMutex
	conn   *amqp.Connection
	chans  []*pubChannel

	// roundRobin counter for channel selection. Unsigned wrap-around is
	// intentional; modulo poolSize handles the boundary.
	roundRobin atomic.Uint64

	closeOnce sync.Once
	stopped   chan struct{}
}

// NewPublisher dials AMQP, declares the topology, and arms publisher confirms
// on DefaultChannelPoolSize channels.
//
// If the broker is unreachable at startup, this does NOT fail — it logs a
// warning and starts a background reconnect loop, returning a usable Publisher
// whose hot-path publishes degrade gracefully (Publish returns "no channel")
// until the broker comes up. This lets the process boot and serve the API even
// when RabbitMQ is down at cold start.
func NewPublisher(ctx context.Context, url string, logger *slog.Logger) (*Publisher, error) {
	p := &Publisher{
		url:      url,
		logger:   logger,
		poolSize: DefaultChannelPoolSize,
		stopped:  make(chan struct{}),
	}
	if err := p.dial(ctx); err != nil {
		p.logger.Warn("amqp publisher: broker unreachable at startup; will retry in background", "err", err)
		go p.reconnectLoop(ctx)
	}
	return p, nil
}

// dial brings up a fresh connection + N confirm-enabled channels. Called
// once at construction and again from watchClose on broker-side disconnect.
func (p *Publisher) dial(ctx context.Context) error {
	conn, err := amqp.DialConfig(p.url, amqp.Config{
		Heartbeat: 30 * time.Second,
		Locale:    "en_US",
	})
	if err != nil {
		return fmt.Errorf("amqp dial: %w", err)
	}

	chans := make([]*pubChannel, 0, p.poolSize)
	for i := 0; i < p.poolSize; i++ {
		ch, err := conn.Channel()
		if err != nil {
			closeChans(chans)
			_ = conn.Close()
			return fmt.Errorf("amqp channel[%d]: %w", i, err)
		}
		// Declare topology on the first channel only; broker stores it
		// globally so subsequent channels just inherit. We still confirm
		// on every channel.
		if i == 0 {
			if err := DeclareTopology(ch); err != nil {
				_ = ch.Close()
				closeChans(chans)
				_ = conn.Close()
				return fmt.Errorf("declare topology: %w", err)
			}
		}
		if err := ch.Confirm(false); err != nil {
			_ = ch.Close()
			closeChans(chans)
			_ = conn.Close()
			return fmt.Errorf("confirm.select on channel[%d]: %w", i, err)
		}
		chans = append(chans, &pubChannel{ch: ch})
	}

	p.connMu.Lock()
	p.conn = conn
	p.chans = chans
	p.connMu.Unlock()

	closeCh := make(chan *amqp.Error, 1)
	conn.NotifyClose(closeCh)
	go p.watchClose(ctx, closeCh)

	p.logger.Info("amqp publisher connected", "url-redacted", redactURL(p.url), "channels", p.poolSize)
	return nil
}

func closeChans(chans []*pubChannel) {
	for _, c := range chans {
		if c != nil && c.ch != nil {
			_ = c.ch.Close()
		}
	}
}

// pickChannel returns the next channel in round-robin order. Returns nil
// when the pool is empty (between dial and a reconnect succeeding).
func (p *Publisher) pickChannel() *pubChannel {
	p.connMu.RLock()
	defer p.connMu.RUnlock()
	if len(p.chans) == 0 {
		return nil
	}
	idx := p.roundRobin.Add(1) % uint64(len(p.chans))
	return p.chans[idx]
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
	p.reconnectLoop(ctx)
}

// reconnectLoop re-dials with exponential backoff (1s→30s) until success,
// ctx cancel, or Close(). A successful dial arms a fresh watchClose for the
// new connection, so this returns once reconnected. Shared by the startup
// path (broker down at boot) and watchClose (broker dropped while running).
func (p *Publisher) reconnectLoop(ctx context.Context) {
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
		select {
		case <-p.stopped:
			return
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

// Publish publishes one message to ExchangeMain with the supplied routing key.
// Persistent (delivery_mode=2), mandatory=true. Confirm RTT is bounded by 5s.
func (p *Publisher) Publish(ctx context.Context, m PublishMessage) error {
	return p.publish(ctx, ExchangeMain, m.RoutingKey, true, m)
}

// PublishToWait routes a message into the per-channel wait queue for the
// given tier ("5s" / "30s" / "5m"). The publish goes through ExchangeMain
// with the wait-tier routing key (see WaitRoutingKey); the wait queue's
// x-dead-letter-routing-key bounces the message back to the channel queue
// on TTL expiry.
//
// Previously this published via the default exchange with routing key set
// to the wait queue name, which caused the broker-side DLX to preserve THAT
// key on bounce — so the bounced message had routing key
// `notifications.wait.5s` and ExchangeMain had no binding for it. Result:
// the wait-tier message bounced into the void.
func (p *Publisher) PublishToWait(ctx context.Context, channel, tier string, m PublishMessage) error {
	return p.publish(ctx, ExchangeMain, WaitRoutingKey(channel, tier), true, m)
}

func (p *Publisher) publish(ctx context.Context, exchange, routingKey string, mandatory bool, m PublishMessage) error {
	pc := p.pickChannel()
	if pc == nil {
		return fmt.Errorf("no channel (reconnecting?): %w", ErrPublisherUnavailable)
	}

	publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	hdrs := amqp.Table{}
	for k, v := range m.Headers {
		hdrs[k] = v
	}

	// Per-channel mutex: serializes Publish+WaitContext on this specific
	// channel only. Other channels in the pool run in parallel.
	pc.mu.Lock()
	defer pc.mu.Unlock()

	conf, err := pc.ch.PublishWithDeferredConfirmWithContext(
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
		// A closed connection/channel means the broker dropped mid-publish —
		// surface it as unavailable so callers retry without burning the
		// message's attempt budget.
		if errors.Is(err, amqp.ErrClosed) {
			return fmt.Errorf("publish on closed connection: %w", ErrPublisherUnavailable)
		}
		return fmt.Errorf("publish: %w", err)
	}

	acked, err := conf.WaitContext(publishCtx)
	if err != nil {
		if errors.Is(err, amqp.ErrClosed) {
			return fmt.Errorf("confirm on closed connection: %w", ErrPublisherUnavailable)
		}
		return fmt.Errorf("confirm wait: %w", err)
	}
	if !acked {
		return errors.New("publish: nacked by broker")
	}
	return nil
}

// QueueDepth returns the broker-reported message_count for a named queue.
// Uses QueueDeclarePassive on the first pool channel — read-only and cheap.
func (p *Publisher) QueueDepth(queueName string) (int, error) {
	p.connMu.RLock()
	chans := p.chans
	p.connMu.RUnlock()
	if len(chans) == 0 {
		return 0, errors.New("publisher: no channel")
	}
	pc := chans[0]
	pc.mu.Lock()
	defer pc.mu.Unlock()
	q, err := pc.ch.QueueDeclarePassive(queueName, true, false, false, false, amqp.Table{
		"x-max-priority":            maxPriority,
		"x-dead-letter-exchange":    ExchangeDLX,
		"x-dead-letter-routing-key": queueName[len("notifications."):] + ".dead",
	})
	if err != nil {
		return 0, fmt.Errorf("queue inspect %q: %w", queueName, err)
	}
	return q.Messages, nil
}

// Ping returns nil if the AMQP connection is alive and at least one channel
// is open. Lightweight: does not write to the broker.
func (p *Publisher) Ping(_ context.Context) error {
	p.connMu.RLock()
	defer p.connMu.RUnlock()
	if p.conn == nil || p.conn.IsClosed() {
		return errors.New("amqp connection closed")
	}
	if len(p.chans) == 0 {
		return errors.New("amqp channels empty")
	}
	return nil
}

// Close shuts down all channels and the connection. Idempotent.
func (p *Publisher) Close() error {
	var err error
	p.closeOnce.Do(func() {
		close(p.stopped)
		p.connMu.Lock()
		defer p.connMu.Unlock()
		closeChans(p.chans)
		p.chans = nil
		if p.conn != nil && !p.conn.IsClosed() {
			err = p.conn.Close()
		}
	})
	return err
}

// redactURL hides any embedded credentials before logging the AMQP URL.
func redactURL(u string) string {
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
