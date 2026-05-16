package queue

import "testing"

func TestWaitTierForAttempt(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		attempt int32
		want    string
	}{
		{"first retry hits 5s tier", 1, "5s"},
		{"attempt 0 (defensive) also 5s", 0, "5s"},
		{"second retry hits 30s tier", 2, "30s"},
		{"third retry hits 5m tier", 3, "5m"},
		{"high attempt count stays on 5m", 99, "5m"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := WaitTierForAttempt(tc.attempt)
			if got != tc.want {
				t.Errorf("WaitTierForAttempt(%d) = %q; want %q", tc.attempt, got, tc.want)
			}
		})
	}
}

func TestWaitQueueForAttempt(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		channel string
		attempt int32
		want    string
	}{
		{"sms attempt 1", "sms", 1, "notifications.sms.wait.5s"},
		{"email attempt 2", "email", 2, "notifications.email.wait.30s"},
		{"push attempt 5", "push", 5, "notifications.push.wait.5m"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := WaitQueueForAttempt(tc.channel, tc.attempt)
			if got != tc.want {
				t.Errorf("WaitQueueForAttempt(%q, %d) = %q; want %q", tc.channel, tc.attempt, got, tc.want)
			}
		})
	}
}

func TestWaitRoutingKey(t *testing.T) {
	t.Parallel()
	got := WaitRoutingKey("email", "30s")
	want := "notification.email.wait.30s"
	if got != want {
		t.Errorf("WaitRoutingKey = %q; want %q", got, want)
	}
}
