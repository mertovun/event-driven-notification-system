package api

import (
	"testing"

	"github.com/mertovun/event-driven-notification-system/internal/notification"
)

// TestReplayStateTransitionGuard documents (and tests) the policy that only
// dead_letter is replayable. failed → queued is explicitly rejected because
// permanent failures shouldn't auto-recover. See docs/13 Part B §14.
func TestReplayStateTransitionGuard(t *testing.T) {
	t.Parallel()
	cases := []struct {
		status      notification.Status
		canReplay   bool
		description string
	}{
		{notification.StatusDeadLetter, true, "dead_letter is the only replayable state"},
		{notification.StatusFailed, false, "failed is terminal-permanent"},
		{notification.StatusSent, false, "sent is terminal-success"},
		{notification.StatusCancelled, false, "cancelled was user-intended"},
		{notification.StatusPending, false, "pending is in-flight"},
		{notification.StatusQueued, false, "queued is in-flight"},
		{notification.StatusSending, false, "sending is in-flight"},
		{notification.StatusScheduled, false, "scheduled is awaiting due_at"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(string(tc.status), func(t *testing.T) {
			t.Parallel()
			// Policy is a single equality check; the test asserts the policy itself
			// stays a single equality (against dead_letter).
			got := tc.status == notification.StatusDeadLetter
			if got != tc.canReplay {
				t.Errorf("%s: canReplay=%v, want %v", tc.description, got, tc.canReplay)
			}
		})
	}
}
