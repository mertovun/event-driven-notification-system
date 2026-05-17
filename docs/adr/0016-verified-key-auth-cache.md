# ADR-0016: Verified-key cache for the auth hot path

## Status

Accepted (2026-05-16). Note: the cache is correct *given* argon2id; the
prior question — whether argon2id was the right baseline for a 100 RPS
notification demo, vs. bcrypt-cost-10 (which has comparable verify cost
but is GPU-friendly under credential theft) — is the more interesting
one. The cache exists because we chose argon2id; pick bcrypt-cost-10 at
100 RPS and the cache + the per-prefix brute-force gate disappear.

## Context

[ADR-0009](0009-argon2id-over-bcrypt.md)'s argon2id is deliberately ~50 ms
per verify. Profiling under 50 RPS (PERFORMANCE.md Phase 1) showed **~96%
of CPU** in `argon2.processBlockGeneric`; the rest of the app was a
rounding error. The cost is paid per request — a long-lived integration
that authenticates once and reuses a TLS connection still pays the full
50 ms on every header.

## Decision

Redis-backed verified-key cache in front of argon2id in `AuthMiddleware`.
On hit: one Redis GET (~0.3-1.6 ms). On miss: argon2id verify, cache the
result for 60 s. Cache key: `auth:v1:<sha256(raw_bearer)[:16]>` — hashed
so the bearer never lives in Redis as plaintext. Versioned (`v1:`) so
we can roll forward the schema without colliding.

Cached value: `{ID, Name, Scopes}` — the same struct the middleware
attaches to the request context.

## Consequences

- **Measured (PERFORMANCE.md Phase 2):** p99 at 50 RPS dropped 116 ms →
  7.5 ms (~15×); CPU samples 102 s → 1.78 s (~57×); argon2 fell off the
  top-15 profile.
- **Revocation lag up to 60 s** — addressed by [ADR-0020](0020-key-revocation-cache-bust.md)
  per-id marker.
- **Redis becomes hot-path for auth.** Already true for rate-limit +
  idempotency. On Redis outage the middleware falls back to argon2id
  (correctness preserved, speedup lost).
- **`sha256[:16]` = 64-bit cache key.** Fine at our key population; widen
  to `[:24]` past millions of keys.
- **No timing-safe compare needed on hit** — possessing the bearer is
  the proof; no oracle on Redis-side string comparison.

## Alternatives considered

- **In-memory LRU** — faster, but doesn't survive restart or share across
  replicas; cold-start storm would pay argon2 N×.
- **Lower argon2 params** — cuts cost without eliminating it; weakens
  the security posture deliberately set by ADR-0009.
- **JWT / session tokens** — larger contract change; signature check
  still per-request.

## References

- [`internal/api/auth_cache.go`](../../internal/api/auth_cache.go)
- [ADR-0009](0009-argon2id-over-bcrypt.md), [ADR-0020](0020-key-revocation-cache-bust.md)
- [PERFORMANCE.md](../../PERFORMANCE.md) Phases 1 & 2
