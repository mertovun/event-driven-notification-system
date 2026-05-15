package queue

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Topology naming. See docs/03-queue-and-messaging.md §1.
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

	// Retry tiers — TTL'd queues whose DLX points back at the main exchange,
	// so a message dies of TTL and is re-routed to the channel queue with its
	// attempt counter incremented in the headers.
	QueueRetry5s  = "notifications.wait.5s"
	QueueRetry30s = "notifications.wait.30s"
	QueueRetry5m  = "notifications.wait.5m"

	maxPriority = int32(10)
)

// Channels enumerates the supported notification channels. Driving lists like
// this off a single source keeps topology declarations + consumer wiring aligned.
var Channels = []string{"sms", "email", "push"}

// RoutingKey returns the AMQP routing key for a channel.
func RoutingKey(channel string) string {
	return "notification." + channel
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

	// For each channel: main queue (priority + DLX), DLQ, and bind both.
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

	// Retry tiers — TTL queues whose DLX is the main exchange. On expiry the
	// broker re-routes the message via the embedded routing key.
	retries := []struct {
		name string
		ttl  int32
	}{
		{QueueRetry5s, 5_000},
		{QueueRetry30s, 30_000},
		{QueueRetry5m, 5 * 60 * 1000},
	}
	for _, r := range retries {
		args := amqp.Table{
			"x-message-ttl":          r.ttl,
			"x-dead-letter-exchange": ExchangeMain,
			// no x-dead-letter-routing-key: the original routing key (notification.<channel>)
			// survives via the published envelope and is reused on TTL expiry.
		}
		if _, err := ch.QueueDeclare(r.name, true, false, false, false, args); err != nil {
			return fmt.Errorf("declare %q: %w", r.name, err)
		}
	}

	return nil
}
