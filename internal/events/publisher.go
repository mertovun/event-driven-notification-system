// Package events publishes status change events to Redis Pub/Sub for WS fan-out.
// See docs/13 Part A §4.
package events

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Channel is the Redis Pub/Sub channel for status events.
const Channel = "events:notifications"

// StatusEvent is the JSON wire shape pushed to subscribers.
// Intentionally PII-light: no recipient, no content (see docs/13 Part A §7).
type StatusEvent struct {
	NotificationID uuid.UUID  `json:"notification_id"`
	BatchID        *uuid.UUID `json:"batch_id,omitempty"`
	Channel        string     `json:"channel"`
	Status         string     `json:"status"`
	At             time.Time  `json:"at"`
	CorrelationID  string     `json:"correlation_id"`
}

// Publisher is the small wrapper around redis.PUBLISH.
type Publisher struct {
	rdb    *redis.Client
	logger *slog.Logger
}

func NewPublisher(rdb *redis.Client, logger *slog.Logger) *Publisher {
	return &Publisher{rdb: rdb, logger: logger}
}

// Publish fans out a single status event. Errors are logged but never returned —
// pub/sub is best-effort; the canonical state is in Postgres.
func (p *Publisher) Publish(ctx context.Context, ev StatusEvent) {
	body, err := json.Marshal(ev)
	if err != nil {
		p.logger.Warn("events: marshal failed", "err", err)
		return
	}
	if err := p.rdb.Publish(ctx, Channel, body).Err(); err != nil {
		p.logger.Warn("events: publish failed", "err", err)
	}
}
