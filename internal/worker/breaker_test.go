package worker

import (
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/sony/gobreaker"
)

// silentLogger discards breaker state-change logs to keep test output clean.
func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// TestBreaker_TripsOnConsecutiveFailures verifies the fast-trip path:
// 5 consecutive failures must open the circuit immediately, no sample-size gate.
func TestBreaker_TripsOnConsecutiveFailures(t *testing.T) {
	t.Parallel()
	cb := newBreaker("sms", silentLogger())
	boom := errors.New("provider 500")

	for i := 0; i < 5; i++ {
		_, _ = cb.Execute(func() (any, error) { return nil, boom })
	}
	// 6th call: breaker should refuse with ErrOpenState.
	_, err := cb.Execute(func() (any, error) { return "ok", nil })
	if !errors.Is(err, gobreaker.ErrOpenState) {
		t.Fatalf("expected ErrOpenState after 5 consecutive failures, got %v", err)
	}
}

// TestBreaker_TripsOnFailureRate verifies the slow-trip path:
// ≥20 requests and >50% failure rate. The breaker's gate is strictly greater
// than 0.5, so exactly 50% does not trip — we use a 7-success / 14-failure
// pattern (21 reqs, 66% failure) with no 5-in-a-row to avoid the fast path.
func TestBreaker_TripsOnFailureRate(t *testing.T) {
	t.Parallel()
	cb := newBreaker("email", silentLogger())
	boom := errors.New("provider 503")

	// Pattern: SFFSFFSFFSFFSFFSFFSFF → 7 success, 14 failure, 21 reqs total.
	// Max consecutive failures = 2 (well under the 5-fast-trip threshold).
	for i := 0; i < 7; i++ {
		_, _ = cb.Execute(func() (any, error) { return "ok", nil })
		_, _ = cb.Execute(func() (any, error) { return nil, boom })
		_, _ = cb.Execute(func() (any, error) { return nil, boom })
	}
	// Next call should see open state.
	_, err := cb.Execute(func() (any, error) { return "ok", nil })
	if !errors.Is(err, gobreaker.ErrOpenState) {
		t.Fatalf("expected ErrOpenState after >50%% failure rate over ≥20 reqs, got %v", err)
	}
}

// TestBreaker_DoesNotTripBelowMinimumSampleSize verifies the 50% rule
// requires ≥20 requests — below that, only the consecutive-failures path
// triggers, and our test never hits 5 in a row.
func TestBreaker_DoesNotTripBelowMinimumSampleSize(t *testing.T) {
	t.Parallel()
	cb := newBreaker("push", silentLogger())
	boom := errors.New("provider 500")

	// 10 calls: 5 success, 5 failure, interleaved. Total < 20.
	for i := 0; i < 5; i++ {
		_, _ = cb.Execute(func() (any, error) { return "ok", nil })
		_, _ = cb.Execute(func() (any, error) { return nil, boom })
	}
	// 11th call: breaker should still be closed (or at most half-open after
	// timeout, but we haven't waited).
	if state := cb.State(); state == gobreaker.StateOpen {
		t.Fatalf("breaker tripped below minimum sample size; state=%v", state)
	}
}
