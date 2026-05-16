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

// invalidateAuthCache removes an entry by raw bearer. Useful when the caller
// has the raw key in hand (testing, dev seed rotation). Production revoke
// flow uses MarkKeyRevoked instead because admins know the api_key_id, not
// the raw bearer.
func invalidateAuthCache(ctx context.Context, rdb *redis.Client, raw string) {
	if rdb == nil {
		return
	}
	_ = rdb.Del(ctx, authCacheKey(raw)).Err()
}

// authRevokedKey is the per-id revocation marker. Set by MarkKeyRevoked,
// checked by the auth middleware after a cache hit. A short TTL keeps Redis
// from accumulating markers; it just needs to outlive the auth-cache TTL so
// every cached entry has been re-verified at least once since revocation.
func authRevokedKey(apiKeyID string) string {
	return "auth:revoked:" + apiKeyID
}

// authRevokedTTL — slightly longer than the auth cache TTL so any cached
// authedKey that was minted just before revocation also gets re-verified
// (and fails) before the marker disappears.
const authRevokedTTL = 5 * time.Minute

// authFailureWindow + authFailureLimit cap the per-prefix slow-path budget.
// Without these, an attacker could brute-force the prefix space at one
// argon2id verify (~50ms server work) per attempt — a CPU DoS more than a
// credential attack. Counter resets after authFailureWindow with no failed
// attempt.
//
// Cached hits and successful verifies do NOT count toward this budget; only
// failed slow-path verifies and unknown-prefix lookups do. The legitimate
// hot path is the cache, so a real client never trips this.
const (
	authFailureWindow = 60 * time.Second
	authFailureLimit  = 20
)

// authFailureKey is the Redis key tracking failed slow-path attempts for a
// bearer prefix. The prefix is non-secret (it's the public lookup
// discriminator), so storing it in plain in the key is fine.
func authFailureKey(prefix string) string {
	return "auth:fail:" + prefix
}

// noteAuthFailure increments the per-prefix counter and returns true when
// the limit has been exceeded (caller should 429 and skip the verify).
// Best-effort: Redis errors fail open so a Redis outage doesn't lock everyone
// out — the DB-side hashed compare still authenticates, just without the
// brute-force gate. Worst case: revert to pre-fix behavior.
func noteAuthFailure(ctx context.Context, rdb *redis.Client, prefix string) bool {
	if rdb == nil {
		return false
	}
	pipe := rdb.Pipeline()
	incr := pipe.Incr(ctx, authFailureKey(prefix))
	pipe.Expire(ctx, authFailureKey(prefix), authFailureWindow)
	if _, err := pipe.Exec(ctx); err != nil {
		return false
	}
	return incr.Val() > authFailureLimit
}

// isAuthFailureLocked checks the counter without incrementing. Used to
// 429 incoming requests once the budget is already exhausted, without
// running argon2.
func isAuthFailureLocked(ctx context.Context, rdb *redis.Client, prefix string) bool {
	if rdb == nil {
		return false
	}
	n, err := rdb.Get(ctx, authFailureKey(prefix)).Int64()
	if err != nil {
		return false
	}
	return n > authFailureLimit
}

// MarkKeyRevoked writes the per-id revocation marker. The admin revoke
// endpoint calls this after updating Postgres so the auth hot path stops
// honouring cached entries for this key immediately.
//
// Idempotent: safe to call twice. Returns nil if Redis is unavailable —
// the DB revoke still took effect, the only cost is the up-to-60s tail until
// the cache entry expires naturally.
func MarkKeyRevoked(ctx context.Context, rdb *redis.Client, apiKeyID string) error {
	if rdb == nil {
		return nil
	}
	return rdb.Set(ctx, authRevokedKey(apiKeyID), "1", authRevokedTTL).Err()
}

// isKeyRevoked checks whether the per-id revocation marker is set.
// Best-effort: on Redis error returns false (we'd rather honour a stale cache
// than 401 every request during a Redis outage; the DB is the source of truth
// and the cache will rotate out within authCacheTTL).
func isKeyRevoked(ctx context.Context, rdb *redis.Client, apiKeyID string) bool {
	if rdb == nil {
		return false
	}
	n, err := rdb.Exists(ctx, authRevokedKey(apiKeyID)).Result()
	if err != nil {
		return false
	}
	return n > 0
}
