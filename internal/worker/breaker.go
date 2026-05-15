package worker

import (
	"log/slog"
	"time"

	"github.com/sony/gobreaker"
)

// newBreaker returns the per-channel breaker per docs/04 §5.
// State NOT persisted across restart — cheap to rewarm.
func newBreaker(channel string, logger *slog.Logger) *gobreaker.CircuitBreaker {
	return gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "provider:" + channel,
		MaxRequests: 5,                // half-open trial requests
		Interval:    60 * time.Second, // bucket window
		Timeout:     30 * time.Second, // how long open stays open
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			// 5 consecutive failures OR ≥50% failure rate over ≥20 requests.
			if counts.ConsecutiveFailures >= 5 {
				return true
			}
			if counts.Requests >= 20 {
				return float64(counts.TotalFailures)/float64(counts.Requests) > 0.5
			}
			return false
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			logger.Warn("circuit breaker state change",
				"breaker", name, "from", from.String(), "to", to.String())
		},
	})
}
