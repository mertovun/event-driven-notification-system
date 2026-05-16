package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

// authCacheTTL is the lifetime of a successful argon2id verify in the cache.
// 60s is short enough that revocation propagates quickly without an explicit
// cache-bust; long enough that a steady-state caller hits the cache 99% of the time.
const authCacheTTL = 60 * time.Second

// cachedAuth is what we store in Redis keyed by a hash of the raw bearer.
// Mirrors authedKey but is JSON-serializable.
type cachedAuth struct {
	ID     string   `json:"id"`
	Name   string   `json:"name"`
	Scopes []string `json:"scopes"`
}

// authCacheKey derives the Redis key from the raw bearer token.
// Format: auth:v1:<sha256-of-raw-key>. The hash means a Redis dump cannot be
// replayed against the API — to authenticate you still need the original raw
// key, which must hash to the same Redis key AND verify against the stored
// argon2 hash on cache miss.
func authCacheKey(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return "auth:v1:" + hex.EncodeToString(sum[:16])
}

// lookupAuthCache returns the cached authedKey for the raw token, or (nil, nil) on miss.
func lookupAuthCache(ctx context.Context, rdb *redis.Client, raw string) (*authedKey, error) {
	if rdb == nil {
		return nil, nil
	}
	v, err := rdb.Get(ctx, authCacheKey(raw)).Bytes()
	if err != nil {
		// redis.Nil is a miss, not an error worth propagating.
		return nil, nil
	}
	var c cachedAuth
	if err := json.Unmarshal(v, &c); err != nil {
		// Corrupt cache entry; treat as miss. Self-heals on next verify.
		return nil, nil
	}
	return &authedKey{ID: c.ID, Name: c.Name, Scopes: c.Scopes}, nil
}

// storeAuthCache writes a verified key result to Redis with authCacheTTL.
// Best-effort: failures are logged elsewhere; the miss path still authenticates correctly.
func storeAuthCache(ctx context.Context, rdb *redis.Client, raw string, k authedKey) {
	if rdb == nil {
		return
	}
	body, err := json.Marshal(cachedAuth{ID: k.ID, Name: k.Name, Scopes: k.Scopes})
	if err != nil {
		return
	}
	_ = rdb.Set(ctx, authCacheKey(raw), body, authCacheTTL).Err()
}

// invalidateAuthCache removes an entry. Call this from the admin revoke endpoint
// so revocations propagate without waiting for TTL.
func invalidateAuthCache(ctx context.Context, rdb *redis.Client, raw string) {
	if rdb == nil {
		return
	}
	_ = rdb.Del(ctx, authCacheKey(raw)).Err()
}
