# ADR-0021: Verified-key cache for the auth hot path

## Status

Accepted (2026-05-16)

## Context

[ADR-0011](0011-argon2id-over-bcrypt.md) chose argon2id over bcrypt for API
key storage, with parameters `time=2, memory=64MB, parallelism=1` calibrated
to ~50 ms per verify on the target hardware. That cost is deliberate — it
makes offline brute-force prohibitively expensive — but it dominates the
runtime profile.

Under sustained load, profiling (see [PERFORMANCE.md](../../PERFORMANCE.md))
showed **~96% of CPU time** spent inside `argon2.processBlockGeneric` and
`argon2.blamkaGeneric`. The pre-cache k6 baseline at 50 RPS measured
p99 = 116 ms and consumed ~2.5 cores of authentication work per second.
The application code itself was a rounding error.

The cost is paid **per request**. A long-lived integration that authenticates
once and reuses a TLS connection still pays the full 50 ms on every HTTP
request because the bearer header is verified each time. That is the wrong
shape: a verification result is safe to memoize for a short window.

## Decision

Add a Redis-backed verified-key cache in front of the argon2id verify in
`AuthMiddleware`. On a cache hit, authentication is a single Redis GET
(~0.3-1.6 ms). On a miss, the original argon2id verify runs and the result
is stored for 60 seconds. The cache key is `auth:v1:<sha256(raw_bearer)[:16]>`,
hashed so the raw bearer never lives in Redis as plaintext.

Cached value is the same authenticated-key struct already attached to the
request context (`ID`, `Name`, `Scopes`), serialized as JSON.

TTL: **60 seconds**, fixed. Long enough to remove argon2 from the hot path
for any realistic call pattern; short enough that a revoked key stops
working within one minute even without explicit invalidation.

The cache name is versioned (`auth:v1:`) so we can roll forward a change to
the cached schema without colliding with the old format.

## Consequences

**Positive (measured, not predicted):**

- p99 at 50 RPS dropped from 116 ms to 7.5 ms (~15×).
- Total CPU samples over 25 s dropped from 102.12 s to 1.78 s (~57×).
- argon2 no longer appears in the top 15 profile nodes.
- Demonstrated ceiling moved from ~50 RPS (CPU-bound) to ~1611 RPS
  (Postgres-pool-bound). The next bottleneck is `MaxConns=20` in pgxpool,
  exactly as predicted in the original Tier 3 recommendations.

**Negative:**

- **Revocation lag of up to 60 seconds.** A revoked key still authenticates
  until the cache entry expires. Mitigated for now by the short TTL; an
  explicit cache-bust on the admin revoke flow is a follow-up if zero-second
  revocation becomes a requirement.
- **Redis becomes a hot-path dependency for auth.** It already is for rate
  limiting and idempotency, so this does not change the failure surface —
  but if Redis is unavailable, the middleware falls back to argon2id verify
  (preserving correctness, losing the speedup).
- **Cache key hash narrows the keyspace.** `sha256[:16]` is 64 bits, giving
  ~2^32 birthday-collision pressure at our key population. With a few
  thousand keys this is fine; if we ever issued millions of API keys we
  would widen to `sha256[:24]` or 32.

**Security notes:**

- The cache key is `sha256(raw_bearer)` — an attacker who sees a Redis cache
  key cannot reverse it to the bearer.
- The cached value contains the key's `scopes`, which is the same data the
  middleware would otherwise look up from Postgres. No additional secrets
  enter the cache.
- A timing-safe comparison is unnecessary on the cache hit path — the cache
  key is itself the proof of knowledge of the bearer (you need the full
  bearer to compute the key).

## Alternatives considered

- **Per-process in-memory cache (LRU).** Faster than Redis (no network hop)
  but does not survive process restart and does not share state across
  replicas. A multi-replica deployment would pay argon2 N× on a restart
  storm. Redis was already in the stack, so we use it.
- **Lower argon2 parameters instead of caching.** Cuts the per-verify cost
  but does not eliminate it, and weakens the security posture documented in
  [ADR-0011](0011-argon2id-over-bcrypt.md). The cache leaves the security
  parameters untouched while removing the runtime cost.
- **JWT or session tokens.** Larger change. Would still need a verification
  step (signature check) on every request, and the API contract is bearer
  tokens that map to long-lived integration credentials. Out of scope.

## References

- [PERFORMANCE.md](../../PERFORMANCE.md) Phase 1 & Phase 2 measurements
- [ADR-0011: argon2id over bcrypt](0011-argon2id-over-bcrypt.md)
- [`internal/api/auth_cache.go`](../../internal/api/auth_cache.go)
- [`internal/api/auth_cache_test.go`](../../internal/api/auth_cache_test.go)
