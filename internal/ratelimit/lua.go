// Package ratelimit implements the per-channel token bucket via a Redis Lua script.
// Atomic across replicas; one shared bucket per channel.
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// luaTokenBucket — atomic token bucket.
//
// Storage: a single hash per key with fields {tokens, ts}.
//
//	tokens: float remaining at last update
//	ts:     epoch-ms of last update
//
// ARGV:
//
//	[1] now_ms        — caller's clock (epoch-ms) — caller authority avoids skew vs Redis time.
//	[2] rate_per_sec  — tokens added per second.
//	[3] capacity      — max bucket size.
//	[4] tokens_take   — tokens to consume (typically 1).
//	[5] ttl_seconds   — refresh key TTL so idle channels GC themselves.
//
// Returns: { allowed (0|1), retry_after_ms (int), tokens_left (float) }.
//
// Refill is linear: tokens += min(capacity, rate * elapsed_seconds).
const luaTokenBucket = `
local key       = KEYS[1]
local now       = tonumber(ARGV[1])
local rate      = tonumber(ARGV[2])
local capacity  = tonumber(ARGV[3])
local take      = tonumber(ARGV[4])
local ttl       = tonumber(ARGV[5])

local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])

if tokens == nil then
    tokens = capacity
    ts = now
end

local elapsed_ms = math.max(0, now - ts)
local refill = (elapsed_ms / 1000.0) * rate
tokens = math.min(capacity, tokens + refill)

local allowed = 0
local retry_after_ms = 0
if tokens >= take then
    tokens = tokens - take
    allowed = 1
else
    -- ms until enough tokens accumulate.
    local needed = take - tokens
    retry_after_ms = math.ceil((needed / rate) * 1000.0)
end

redis.call('HMSET', key, 'tokens', tokens, 'ts', now)
redis.call('EXPIRE', key, ttl)

return { allowed, retry_after_ms, tostring(tokens) }
`

// Limiter wraps a Redis client + the Lua script.
type Limiter struct {
	rdb    *redis.Client
	script *redis.Script
}

// New builds a Limiter. The script is hashed once; Redis caches it across replicas.
func New(rdb *redis.Client) *Limiter {
	return &Limiter{
		rdb:    rdb,
		script: redis.NewScript(luaTokenBucket),
	}
}

// Decision is the bucket script's return: whether the call is allowed and the
// suggested retry-after on denial.
type Decision struct {
	Allowed    bool
	RetryAfter time.Duration
	TokensLeft float64
}

// Allow attempts to take one token from the bucket keyed by `key`.
// Returns (allowed=true, retry=0) on success, (allowed=false, retry=N) on denial.
func (l *Limiter) Allow(ctx context.Context, key string, ratePerSec, capacity float64) (Decision, error) {
	return l.AllowN(ctx, key, ratePerSec, capacity, 1)
}

// AllowN consumes `take` tokens. Useful for batch endpoints to reserve N tokens at once.
func (l *Limiter) AllowN(ctx context.Context, key string, ratePerSec, capacity, take float64) (Decision, error) {
	if ratePerSec <= 0 || capacity <= 0 || take <= 0 {
		return Decision{}, errors.New("ratelimit: rate, capacity, take must be positive")
	}
	nowMs := time.Now().UnixMilli()
	// TTL: long enough that idle channels survive a quiet hour but eventually GC.
	const ttlSeconds = 60

	res, err := l.script.Run(ctx, l.rdb, []string{key},
		nowMs,
		ratePerSec,
		capacity,
		take,
		ttlSeconds,
	).Result()
	if err != nil {
		return Decision{}, fmt.Errorf("eval script: %w", err)
	}
	arr, ok := res.([]any)
	if !ok || len(arr) != 3 {
		return Decision{}, fmt.Errorf("unexpected script result: %v", res)
	}

	allowed, _ := arr[0].(int64)
	retryAfterMs, _ := arr[1].(int64)
	tokensLeftStr, _ := arr[2].(string)

	var tokensLeft float64
	_, _ = fmt.Sscanf(tokensLeftStr, "%f", &tokensLeft)

	return Decision{
		Allowed:    allowed == 1,
		RetryAfter: time.Duration(retryAfterMs) * time.Millisecond,
		TokensLeft: tokensLeft,
	}, nil
}
