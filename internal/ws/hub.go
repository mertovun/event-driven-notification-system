// Package ws hosts the WebSocket upgrade handler and per-replica fan-out hub.
package ws

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/redis/go-redis/v9"

	"github.com/mertovun/event-driven-notification-system/internal/events"
)

// subscriber is one live WebSocket client.
type subscriber struct {
	id     uint64
	filter Filter
	send   chan []byte // bounded; eviction policy in dispatch()
	close  func(reason string)
}

// Hub fans out events:notifications messages from Redis Pub/Sub to local subscribers.
// One Hub per replica; the Redis subscribe is shared across all WS connections.
type Hub struct {
	rdb    *redis.Client
	logger *slog.Logger
	nextID atomic.Uint64

	mu   sync.RWMutex
	subs map[uint64]*subscriber

	// sendBuffer governs back-pressure per connection.
	sendBuffer int
}

func NewHub(rdb *redis.Client, logger *slog.Logger) *Hub {
	return &Hub{
		rdb:        rdb,
		logger:     logger,
		subs:       make(map[uint64]*subscriber),
		sendBuffer: 256,
	}
}

// Run subscribes to Redis Pub/Sub and pumps events to local subs until ctx is done.
func (h *Hub) Run(ctx context.Context) error {
	ps := h.rdb.Subscribe(ctx, events.Channel)
	defer func() { _ = ps.Close() }()
	if _, err := ps.Receive(ctx); err != nil {
		return err
	}
	h.logger.Info("ws hub subscribed to redis pub/sub", "channel", events.Channel)
	ch := ps.Channel()
	for {
		select {
		case <-ctx.Done():
			h.logger.Info("ws hub stopping")
			h.broadcastClose("server shutdown")
			return nil
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			h.dispatch([]byte(msg.Payload))
		}
	}
}

// dispatch parses the payload + fans out to filtered subscribers.
// Drops connections with a full send buffer (slow consumers) — RFC 6455 close code 1008.
//
// Lock discipline: the slow-consumer eviction calls back into the handler's
// close path which eventually wants Hub.Unregister (a write lock). Calling
// sub.close while still holding the read lock would either deadlock
// (synchronous close → Unregister) or stack up goroutine waiters (async
// close → Unregister blocks behind the dispatch loop). We instead collect
// slow subs into a slice, drop the read lock, and close outside it.
func (h *Hub) dispatch(payload []byte) {
	var ev events.StatusEvent
	if err := json.Unmarshal(payload, &ev); err != nil {
		h.logger.Warn("ws hub: bad event payload", "err", err)
		return
	}

	var slow []*subscriber
	h.mu.RLock()
	for _, sub := range h.subs {
		if !sub.filter.Matches(ev.BatchID, ev.Channel) {
			continue
		}
		select {
		case sub.send <- payload:
		default:
			slow = append(slow, sub)
		}
	}
	h.mu.RUnlock()

	for _, sub := range slow {
		sub.close("slow_consumer")
	}
}

// Register adds a subscriber. Returns its id and the receive channel.
func (h *Hub) Register(filter Filter, closeFn func(string)) (uint64, <-chan []byte) {
	sub := &subscriber{
		id:     h.nextID.Add(1),
		filter: filter,
		send:   make(chan []byte, h.sendBuffer),
		close:  closeFn,
	}
	h.mu.Lock()
	h.subs[sub.id] = sub
	h.mu.Unlock()
	return sub.id, sub.send
}

// Unregister removes a subscriber and drains its channel.
func (h *Hub) Unregister(id uint64) {
	h.mu.Lock()
	delete(h.subs, id)
	h.mu.Unlock()
}

// broadcastClose tells every subscriber to close (called on hub shutdown).
func (h *Hub) broadcastClose(reason string) {
	h.mu.RLock()
	subs := make([]*subscriber, 0, len(h.subs))
	for _, s := range h.subs {
		subs = append(subs, s)
	}
	h.mu.RUnlock()
	for _, s := range subs {
		s.close(reason)
	}
}

// Count returns the live subscriber count (for metrics).
func (h *Hub) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.subs)
}
