// Package events publishes status change events to Redis Pub/Sub for WS fan-out.
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
// Intentionally PII-light: no recipient, no content.
//
// CreatedBy carries the api_key_id that owns the notification. The
// publisher includes it in the Pub/Sub payload so every replica's WS
// hub can apply the per-subscriber owner gate locally without crossing
// back to auth context on every dispatch. Without this field, a key
// with `notifications:read` scope could subscribe and observe other
// tenants' status timelines: the volumes and correlation_ids leak
// business-intelligence even when the payload itself is PII-light.
//
// Admin-scope subscribers bypass the filter the same way they bypass
// the per-key row-ownership SQL gates on the HTTP read path
// (ADR-0019). Nil means "ownership unknown" (legacy rows pre-migration
// 0006); conservatively visible only to admin-scope subscribers.
type StatusEvent struct {
	NotificationID uuid.UUID  `json:"notification_id"`
	BatchID        *uuid.UUID `json:"batch_id,omitempty"`
	Channel        string     `json:"channel"`
	Status         string     `json:"status"`
	At             time.Time  `json:"at"`
	CorrelationID  string     `json:"correlation_id"`
	CreatedBy      *uuid.UUID `json:"created_by,omitempty"`
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
