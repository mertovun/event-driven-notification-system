# ADR-0020: Admin key revocation endpoint + verified-key cache invalidation

## Status

Accepted (2026-05-16)

## Context

[ADR-0016](0016-verified-key-auth-cache.md)'s 60s cache TTL meant revoking a
leaked key took **up to 60s** to propagate. The DB-level `RevokeAPIKey` SQL
existed but had no HTTP endpoint, so revocation was a Postgres console
operation. The `invalidateAuthCache` helper existed but had no caller —
and couldn't help anyway, because the cache key is `sha256(raw_bearer)`
and admins know the api_key_id, not the raw bearer.

## Decision

Add `POST /v1/admin/api-keys/{id}/revoke` (admin scope). Flow:

1. `UPDATE api_keys SET revoked_at = now()` via existing `RevokeAPIKey`.
2. `SET auth:revoked:<api_key_id> 1 EX 300` — per-id marker, 5 min
   (longer than cache TTL so any extant cache entry is forced through
   slow path at least once).
3. `admin_audit` row with `action=key_revoke`, `actor_id`, target.

Auth middleware on cache hit: extra `EXISTS auth:revoked:<id>` check. If
set, delete the cache entry and fall through to the slow path — which
queries Postgres, which excludes revoked rows, which returns 401.

## Consequences

- **Revocation tail drops to effectively zero** (next request after the
  marker is written fails).
- **One extra Redis op per cached auth.** At 1771 r/s (PERFORMANCE.md
  Phase 4) that's ~3500 ops/s including the existing auth GET — well
  inside single-instance Redis budget.
- **Self-revoke is allowed** and is the recommended rotation flow
  (admin creates successor, tests, revokes self).
- **Marker TTL 5 min, key UUID is a PK** — id reuse is structurally
  impossible so the marker doesn't accidentally block a future key.
- **Redis-down degrades gracefully**: DB revoke still works, cache lags
  up to 60s as before. No worse than pre-ADR.

## Alternatives considered

- **Scan + DEL `auth:v1:*`** — O(N) per revoke or new reverse index;
  more moving parts than the per-id marker.
- **Epoch-bumped cache values** — strong consistency, doubles cache reads
  on every hit. Per-id marker pays for itself only on revoke.
- **Pub/Sub fanout** — real-time but adds per-replica subscriber +
  failure modes when subscriber drops. Pull-based marker is stateless.

## References

- [`internal/api/auth_cache.go`](../../internal/api/auth_cache.go), [`internal/api/admin.go`](../../internal/api/admin.go)
- Migration [0007](../../internal/store/migrations/0007_audit_key_revoke.up.sql) — `actor_id` column
- [ADR-0016](0016-verified-key-auth-cache.md), [ADR-0019](0019-per-key-row-ownership.md)
