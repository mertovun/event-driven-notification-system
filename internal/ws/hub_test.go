package ws

import (
	"encoding/json"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/mertovun/event-driven-notification-system/internal/events"
)

// silentLogger discards hub logs so test output stays clean.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// newTestHub builds a Hub without a Redis dependency. The Run() loop is not
// exercised; we drive dispatch() directly so the test stays hermetic.
func newTestHub(buf int) *Hub {
	return &Hub{
		logger:     silentLogger(),
		subs:       make(map[uint64]*subscriber),
		sendBuffer: buf,
	}
}

// TestHub_SlowConsumerEviction verifies that a subscriber whose send buffer
// fills up gets evicted, and the eviction call runs after the hub releases
// its read lock (so the close handler can call Unregister without deadlock).
func TestHub_SlowConsumerEviction(t *testing.T) {
	t.Parallel()
	h := newTestHub(1) // buffer of 1 — second event fills it

	var (
		closed atomic.Bool
		subID  uint64
	)
	id, _ := h.Register(Filter{AdminBypass: true}, func(_ string) {
		// Simulate the handler's close path: call Unregister, which
		// requires the write lock. If dispatch were still holding the
		// read lock, this would deadlock.
		h.Unregister(subID)
		closed.Store(true)
	})
	subID = id

	mustPayload := func(t *testing.T) []byte {
		ev := events.StatusEvent{
			NotificationID: uuid.New(),
			Channel:        "sms",
			Status:         "sent",
			At:             time.Now(),
		}
		b, err := json.Marshal(ev)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return b
	}

	// First payload — fits in the buffer (capacity 1).
	h.dispatch(mustPayload(t))
	if closed.Load() {
		t.Fatalf("subscriber evicted prematurely")
	}

	// Second payload — buffer is full, so dispatch should evict.
	h.dispatch(mustPayload(t))
	if !closed.Load() {
		t.Fatalf("expected slow consumer eviction after second dispatch")
	}
}

// TestHub_DispatchFiltersMismatches verifies a subscriber with a channel
// filter does not receive events from other channels.
func TestHub_DispatchFiltersMismatches(t *testing.T) {
	t.Parallel()
	h := newTestHub(8)

	f, err := ParseFilter("channel:email")
	if err != nil {
		t.Fatalf("parse filter: %v", err)
	}
	f.AdminBypass = true // bypass owner gate so the channel filter is the only one in play
	_, recv := h.Register(f, func(string) {})

	ev := events.StatusEvent{Channel: "sms", Status: "sent", At: time.Now()}
	b, _ := json.Marshal(ev)
	h.dispatch(b)

	select {
	case got := <-recv:
		t.Fatalf("subscriber received non-matching event: %s", got)
	case <-time.After(50 * time.Millisecond):
		// Pass — no event delivered.
	}
}

// TestHub_DispatchDeliversToMatchingSubscriber verifies the happy path.
func TestHub_DispatchDeliversToMatchingSubscriber(t *testing.T) {
	t.Parallel()
	h := newTestHub(8)

	f, _ := ParseFilter("channel:sms")
	f.AdminBypass = true // bypass owner gate; this test exercises the channel filter
	_, recv := h.Register(f, func(string) {})

	ev := events.StatusEvent{Channel: "sms", Status: "sent", At: time.Now()}
	b, _ := json.Marshal(ev)
	h.dispatch(b)

	select {
	case got := <-recv:
		if len(got) == 0 {
			t.Fatalf("empty payload")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("subscriber did not receive matching event")
	}
}
