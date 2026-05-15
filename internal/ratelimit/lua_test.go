//go:build integration

package ratelimit_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/mertovun/event-driven-notification-system/internal/ratelimit"
)

// spinUpRedis returns a connected redis.Client backed by a fresh container.
func spinUpRedis(t *testing.T) (*redis.Client, func()) {
	t.Helper()
	ctx := context.Background()
	c, err := tcredis.Run(ctx, "redis:7-alpine")
	require.NoError(t, err)
	endpoint, err := c.ConnectionString(ctx)
	require.NoError(t, err)
	opts, err := redis.ParseURL(endpoint)
	require.NoError(t, err)
	cli := redis.NewClient(opts)
	return cli, func() {
		_ = cli.Close()
		_ = c.Terminate(ctx)
	}
}

func TestLimiter_AllowFromFullBucket(t *testing.T) {
	rdb, cleanup := spinUpRedis(t)
	defer cleanup()
	l := ratelimit.New(rdb)

	d, err := l.Allow(context.Background(), "test:sms", 100, 100)
	require.NoError(t, err)
	require.True(t, d.Allowed, "first token from full bucket must be allowed")
	require.InDelta(t, 99.0, d.TokensLeft, 0.5)
	require.Zero(t, d.RetryAfter)
}

func TestLimiter_DenyWhenEmpty(t *testing.T) {
	rdb, cleanup := spinUpRedis(t)
	defer cleanup()
	l := ratelimit.New(rdb)

	// Drain — capacity 2, rate 0.1/s (slow refill).
	for i := 0; i < 2; i++ {
		d, err := l.Allow(context.Background(), "test:slow", 0.1, 2)
		require.NoError(t, err)
		require.True(t, d.Allowed, "first %d takes must succeed", i+1)
	}
	d, err := l.Allow(context.Background(), "test:slow", 0.1, 2)
	require.NoError(t, err)
	require.False(t, d.Allowed, "3rd take from empty bucket must be denied")
	require.Greater(t, d.RetryAfter.Milliseconds(), int64(0), "must report a retry-after")
}

func TestLimiter_RefillOverTime(t *testing.T) {
	rdb, cleanup := spinUpRedis(t)
	defer cleanup()
	l := ratelimit.New(rdb)

	key := "test:refill"
	// Capacity 1 at 10/s. After draining, ~100ms should refill 1 token.
	d, err := l.Allow(context.Background(), key, 10, 1)
	require.NoError(t, err)
	require.True(t, d.Allowed)

	d, err = l.Allow(context.Background(), key, 10, 1)
	require.NoError(t, err)
	require.False(t, d.Allowed, "second consecutive call should be denied")

	time.Sleep(150 * time.Millisecond)
	d, err = l.Allow(context.Background(), key, 10, 1)
	require.NoError(t, err)
	require.True(t, d.Allowed, "after 150ms refill at 10/s a token should be available")
}

func TestLimiter_KeyTTLRefresh(t *testing.T) {
	rdb, cleanup := spinUpRedis(t)
	defer cleanup()
	l := ratelimit.New(rdb)

	key := "test:ttl"
	_, err := l.Allow(context.Background(), key, 100, 100)
	require.NoError(t, err)
	ttl, err := rdb.TTL(context.Background(), key).Result()
	require.NoError(t, err)
	require.Greater(t, ttl.Seconds(), 50.0, "TTL must be ~60s after Allow refreshes it")
}

func TestLimiter_InvalidArgs(t *testing.T) {
	rdb, cleanup := spinUpRedis(t)
	defer cleanup()
	l := ratelimit.New(rdb)

	_, err := l.Allow(context.Background(), "k", 0, 1)
	require.Error(t, err)
	_, err = l.Allow(context.Background(), "k", 1, 0)
	require.Error(t, err)
}
