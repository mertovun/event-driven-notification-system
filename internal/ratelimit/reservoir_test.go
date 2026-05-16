//go:build integration

package ratelimit_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/mertovun/event-driven-notification-system/internal/ratelimit"
)

// TestReservoir_RefillsFromBucket exercises the happy path: first Take
// triggers a batch refill, subsequent Takes are local and free.
func TestReservoir_RefillsFromBucket(t *testing.T) {
	rdb, cleanup := spinUpRedis(t)
	defer cleanup()
	l := ratelimit.New(rdb)
	r := ratelimit.NewReservoir(l, ratelimit.ReservoirConfig{
		Key: "test:reservoir:happy", RatePerSec: 100, Capacity: 100, BatchSize: 20,
	})

	// First take refills 20 (consumes 1 now, 19 in local cache).
	d, err := r.Take(context.Background())
	require.NoError(t, err)
	require.True(t, d.Allowed)
	first := r.LastRTT()
	require.Greater(t, first, time.Duration(0))

	// Next 19 takes must be local — no Redis traffic. We can't directly
	// observe "no Redis call," so we assert LastRTT does not advance:
	// the reservoir only writes LastRTT on actual refill.
	for i := 0; i < 19; i++ {
		d, err := r.Take(context.Background())
		require.NoError(t, err, "take %d", i)
		require.True(t, d.Allowed, "take %d should be local-allowed", i)
	}
	require.Equal(t, first, r.LastRTT(), "LastRTT must not advance during local-cache takes")

	// 21st take exhausts the local cache and triggers a fresh refill RTT.
	d, err = r.Take(context.Background())
	require.NoError(t, err)
	require.True(t, d.Allowed)
	// LastRTT was updated again; the second refill is typically faster
	// because the Lua script is cached on Redis, so we only assert
	// that it was recorded as a non-zero duration, not that it grew.
	require.Greater(t, r.LastRTT(), time.Duration(0), "refill RTT must be recorded")
}

// TestReservoir_FallsBackToSingleAllow ensures we don't abandon backpressure
// semantics: when the global bucket can't yield BatchSize tokens but has at
// least one, the reservoir takes the one. (This shape matters when the
// bucket is partly drained by another replica and there isn't enough for a
// full batch refill.)
func TestReservoir_FallsBackToSingleAllow(t *testing.T) {
	rdb, cleanup := spinUpRedis(t)
	defer cleanup()
	l := ratelimit.New(rdb)

	// Pre-drain the bucket to below BatchSize. Capacity=10, batchSize=20:
	// after one Allow, ~9 tokens remain — too few for a 20-batch refill,
	// but enough for the single-token fallback.
	key := "test:reservoir:fallback"
	dec, err := l.AllowN(context.Background(), key, 100, 10, 5)
	require.NoError(t, err)
	require.True(t, dec.Allowed)
	// Bucket now holds ~5 tokens (10 - 5).

	r := ratelimit.NewReservoir(l, ratelimit.ReservoirConfig{
		Key: key, RatePerSec: 100, Capacity: 10, BatchSize: 20,
	})

	// First Take: 20-batch refill denied (only 5 in bucket), single
	// fallback succeeds.
	d, err := r.Take(context.Background())
	require.NoError(t, err)
	require.True(t, d.Allowed, "single-token fallback must succeed when bucket has at least 1")
}

// TestReservoir_DeniesWhenBucketEmpty ensures real exhaustion still produces
// a backpressure decision with a useful RetryAfter.
func TestReservoir_DeniesWhenBucketEmpty(t *testing.T) {
	rdb, cleanup := spinUpRedis(t)
	defer cleanup()
	l := ratelimit.New(rdb)

	// Drain the bucket completely.
	key := "test:reservoir:empty"
	dec, err := l.AllowN(context.Background(), key, 1, 1, 1)
	require.NoError(t, err)
	require.True(t, dec.Allowed)

	r := ratelimit.NewReservoir(l, ratelimit.ReservoirConfig{
		Key: key, RatePerSec: 1, Capacity: 1, BatchSize: 5,
	})

	d, err := r.Take(context.Background())
	require.NoError(t, err)
	require.False(t, d.Allowed, "Take must be denied when bucket is empty and no refill is possible")
	require.Greater(t, d.RetryAfter, time.Duration(0), "RetryAfter must be propagated from the fallback Allow")
}

// TestReservoir_RespectsGlobalCap is the load-bearing correctness test:
// no matter how many local Takes happen across N reservoirs, the *total*
// allowed across all of them cannot exceed the bucket's true rate over a
// time window. (The reservoir trades freshness for fewer RTTs; it must not
// trade correctness.)
func TestReservoir_RespectsGlobalCap(t *testing.T) {
	rdb, cleanup := spinUpRedis(t)
	defer cleanup()
	l := ratelimit.New(rdb)

	// Tight bucket: 5 tokens/sec, capacity 5, batchSize 3. Two reservoirs
	// (simulating 2 workers) compete on the same key.
	key := "test:reservoir:cap"
	cfg := ratelimit.ReservoirConfig{
		Key: key, RatePerSec: 5, Capacity: 5, BatchSize: 3,
	}
	r1 := ratelimit.NewReservoir(l, cfg)
	r2 := ratelimit.NewReservoir(l, cfg)

	// Drain both reservoirs as fast as possible for 200 ms.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	allowed := 0
	for ctx.Err() == nil {
		for _, r := range []*ratelimit.Reservoir{r1, r2} {
			d, err := r.Take(context.Background())
			if err != nil {
				continue
			}
			if d.Allowed {
				allowed++
			}
		}
	}
	// Over 200ms at 5/s with capacity=5, the bucket can yield at most:
	//   5 (initial capacity) + 5/s * 0.2s = 6 tokens.
	// Reservoir batching means we can briefly "over-yield" by holding
	// up-to-(batchSize-1) tokens locally on each side; with batchSize=3
	// and 2 reservoirs that's up to 4 extra. So the strict upper bound
	// across this window is 6 + 4 = 10.
	require.LessOrEqual(t, allowed, 10,
		"reservoir over-yield must stay within batchSize-1 per worker (got %d)", allowed)
}
