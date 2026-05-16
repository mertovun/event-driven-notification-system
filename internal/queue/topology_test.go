package queue

import "testing"

func TestWaitQueueForAttempt(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		attempt int32
		want    string
	}{
		{"first retry hits wait.5s", 1, QueueRetry5s},
		{"attempt 0 (defensive) also wait.5s", 0, QueueRetry5s},
		{"second retry hits wait.30s", 2, QueueRetry30s},
		{"third retry hits wait.5m", 3, QueueRetry5m},
		{"high attempt count stays on wait.5m", 99, QueueRetry5m},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := WaitQueueForAttempt(tc.attempt)
			if got != tc.want {
				t.Errorf("WaitQueueForAttempt(%d) = %q; want %q", tc.attempt, got, tc.want)
			}
		})
	}
}
