package queue

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Topology naming. One topic exchange + per-channel priority queues + DLX +
// per-channel retry tiers. The per-channel retry tier is needed because
// RabbitMQ's DLX bounce preserves the *message's* current routing key, not
// the *intended* destination's; a single shared wait queue can't dead-letter
// its messages back to three different channel queues without an
// x-dead-letter-routing-key, which is per-queue, not per-message.
const (
	ExchangeMain = "notifications.exchange"
	ExchangeDLX  = "notifications.dlx"

	// Per-channel main queues — priority-armed (x-max-priority=10), DLX-routed.
	QueueSMS   = "notifications.sms"
	QueueEmail = "notifications.email"
	QueuePush  = "notifications.push"

	// Dead-letter queues, fed from main on rejection or TTL expiry.
	QueueSMSDLQ   = "notifications.sms.dlq"
	QueueEmailDLQ = "notifications.email.dlq"
	QueuePushDLQ  = "notifications.push.dlq"

	maxPriority = int32(10)
)

// Channels enumerates the supported notification channels. Driving lists like
// this off a single source keeps topology declarations + consumer wiring aligned.
var Channels = []string{"sms", "email", "push"}

// RoutingKey returns the AMQP routing key for a channel.
func RoutingKey(channel string) string {
	return "notification." + channel
}

// WaitQueue returns the per-channel TTL retry queue name for a given tier
// (one of "5s", "30s", "5m") and channel ("sms" / "email" / "push").
// Format: notifications.<channel>.wait.<tier>.
func WaitQueue(channel, tier string) string {
	return "notifications." + channel + ".wait." + tier
}

// WaitTierForAttempt picks the tier suffix for the given attempt count
// (1-indexed; "attempt that just failed").
//
//	attempt 1 → 5s    (first retry — fast, network blip)
//	attempt 2 → 30s   (sustained 5xx, slower second try)
//	attempt 3+ → 5m   (longer cooldown, possibly degraded provider)
func WaitTierForAttempt(attempt int32) string {
	switch {
	case attempt <= 1:
		return "5s"
	case attempt == 2:
		return "30s"
	default:
		return "5m"
	}
}

// WaitQueueForAttempt is the convenience accessor used by the worker.
// Returns the full queue name for the given (channel, attempt) pair.
func WaitQueueForAttempt(channel string, attempt int32) string {
	return WaitQueue(channel, WaitTierForAttempt(attempt))
}

// DeclareTopology declares all exchanges, queues, and bindings idempotently.
// Safe to call on every consumer/publisher boot and on reconnect.
func DeclareTopology(ch *amqp.Channel) error {
	// Main topic exchange.
	if err := ch.ExchangeDeclare(
		ExchangeMain, "topic",
		true,  // durable
		false, // auto-delete
		false, // internal
		false, // no-wait
		nil,
	); err != nil {
		return fmt.Errorf("declare exchange %q: %w", ExchangeMain, err)
	}

	// DLX — direct, durable.
	if err := ch.ExchangeDeclare(
		ExchangeDLX, "direct",
		true, false, false, false, nil,
	); err != nil {
		return fmt.Errorf("declare exchange %q: %w", ExchangeDLX, err)
	}

	// Per-channel main queue + DLQ.
	channels := map[string]struct{ main, dlq string }{
		"sms":   {QueueSMS, QueueSMSDLQ},
		"email": {QueueEmail, QueueEmailDLQ},
		"push":  {QueuePush, QueuePushDLQ},
	}
	for chName, q := range channels {
		args := amqp.Table{
			"x-max-priority":            maxPriority,
			"x-dead-letter-exchange":    ExchangeDLX,
			"x-dead-letter-routing-key": chName + ".dead",
		}
		if _, err := ch.QueueDeclare(q.main, true, false, false, false, args); err != nil {
			return fmt.Errorf("declare %q: %w", q.main, err)
		}
		if err := ch.QueueBind(q.main, RoutingKey(chName), ExchangeMain, false, nil); err != nil {
			return fmt.Errorf("bind %q → %q: %w", q.main, ExchangeMain, err)
		}

		if _, err := ch.QueueDeclare(q.dlq, true, false, false, false, nil); err != nil {
			return fmt.Errorf("declare %q: %w", q.dlq, err)
		}
		if err := ch.QueueBind(q.dlq, chName+".dead", ExchangeDLX, false, nil); err != nil {
			return fmt.Errorf("bind %q → DLX: %w", q.dlq, err)
		}
	}

	// Per-channel × per-tier retry queues. Each declares an
	// x-dead-letter-routing-key pointing at its target channel queue so the
	// TTL-expiry bounce lands on the right consumer. Total: 3 channels × 3
	// tiers = 9 retry queues.
	tiers := []struct {
		name string
		ttl  int32
	}{
		{"5s", 5_000},
		{"30s", 30_000},
		{"5m", 5 * 60 * 1000},
	}
	for chName := range channels {
		for _, t := range tiers {
			qName := WaitQueue(chName, t.name)
			args := amqp.Table{
				"x-message-ttl":             t.ttl,
				"x-dead-letter-exchange":    ExchangeMain,
				"x-dead-letter-routing-key": RoutingKey(chName),
			}
			if _, err := ch.QueueDeclare(qName, true, false, false, false, args); err != nil {
				return fmt.Errorf("declare %q: %w", qName, err)
			}
			// Bind the wait queue to ExchangeMain too, on a routing key only
			// the worker uses for explicit wait-tier publishes. The worker
			// publishes via ExchangeMain with key notification.<channel>.wait.<tier>;
			// the message enters this queue, sits for TTL, then bounces via
			// DLX back to ExchangeMain with the per-queue
			// x-dead-letter-routing-key = notification.<channel>.
			waitKey := RoutingKey(chName) + ".wait." + t.name
			if err := ch.QueueBind(qName, waitKey, ExchangeMain, false, nil); err != nil {
				return fmt.Errorf("bind %q → %q (key %q): %w", qName, ExchangeMain, waitKey, err)
			}
		}
	}

	return nil
}

// WaitRoutingKey returns the routing key for publishing into a per-channel
// wait queue via ExchangeMain. Symmetrical to the binding declared in
// DeclareTopology.
func WaitRoutingKey(channel, tier string) string {
	return RoutingKey(channel) + ".wait." + tier
}
