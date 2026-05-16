# ADR-0025: Admin key revocation endpoint and verified-key cache invalidation

## Status

Accepted (2026-05-16)

## Context

[ADR-0021](0021-verified-key-auth-cache.md) added the verified-key cache to
avoid running argon2id on every authenticated request. The cache TTL is 60s,
which means revoking a leaked key took **up to 60 seconds** to propagate
through the cache — a real window for an attacker to keep authenticating
with a known-leaked bearer.

The cache invalidation function (`invalidateAuthCache`) existed in the
codebase but had no caller. The DB-level `RevokeAPIKey` SQL also existed
but was reachable only via direct Postgres access — admins couldn't revoke
through the API surface, which made incident response uglier than it needs
to be.

Worse: the admin would need the *raw bearer* to call
`invalidateAuthCache` (the cache key is `sha256(raw_bearer)`). An admin
knows the api_key_id (UUID), not the raw bearer they're trying to revoke —
the raw bearer is by construction not stored anywhere in the system after
the key is hashed. So a different invalidation strategy is required.

## Decision

Add `POST /v1/admin/api-keys/{id}/revoke` (admin scope required).

Implementation flow:

1. `UPDATE api_keys SET revoked_at = now() WHERE id = $1` via the existing
   `RevokeAPIKey` query.
2. Write a Redis revocation marker:
   `SET auth:revoked:<api_key_id> 1 EX 300` (5 minutes — longer than the
   60s auth cache TTL so any cached entry for this key is forced through
   the slow path at least once, where it will 401 from Postgres).
3. Insert an `admin_audit` row with `action=key_revoke`, the caller's
   `actor_id`, and the target key id.

Auth middleware update: on cache hit, do one extra `EXISTS auth:revoked:<id>`
check. If present, treat the cache entry as a miss and `DEL` it. The slow
path then queries Postgres, which already excludes revoked rows, and the
request gets 401.

Cost: one extra Redis op per cached auth (cheap; still much faster than
argon2id verify). The marker is per-id, not per-token, so the cardinality
is bounded by the number of currently-active keys.

## Consequences

**Positive:**

- Revocation tail drops from up to 60s to effectively zero (next request
  after the marker is written re-checks Postgres and fails).
- The revocation primitive is now reachable from the API — admins don't
  need DB access for incident response.
- Audit attribution is correct via the new `actor_id` column (also in
  this migration; previously the audit log only stored the free-text key
  *name*, which was non-unique and rename-fragile — security review #L162).

**Negative:**

- Adds one Redis op per cached auth. On a 1771 r/s workload (see
  PERFORMANCE.md Phase 4) that's ~3500 ops/s including the auth GET +
  this EXISTS — well within Redis' single-instance budget.
- Self-revoke is allowed and is the recommended rotation flow (admin
  creates successor key, tests it, revokes self). If the admin self-revokes
  while another admin request is in flight on the same key, the in-flight
  request completes (cache hit before the marker was written); subsequent
  requests fail. Acceptable for a rotation flow.
- The Redis marker has a 5-minute TTL. If the same key id is re-issued
  later (impossible in this system — api_keys.id is a primary key — but
  pathologically: if you `DELETE` the row and re-`INSERT` with the same
  UUID), the marker would block the new key for up to 5 minutes. We do
  not support key id reuse; this is a non-issue.

**Operational note:**

- The Redis revocation marker is best-effort. If Redis is unavailable,
  revocation still works (DB query catches the row), but the cache hit
  path will keep honouring the cached entry until natural expiry (≤60s).
  This is the worst-case latency, and it matches what the system did
  before this ADR.

## Alternatives considered

- **Scan and DEL the `auth:v1:*` keyspace.** Has to either scan all keys
  (O(N) on every revoke) or build a reverse index keyed by api_key_id.
  Either way, more moving parts than the per-id marker.
- **Make the auth cache value carry an epoch and check epoch on every hit.**
  Strong consistency but doubles the cache reads. We chose per-id marker
  + cache hit + (rare) marker check because the steady state is
  "no recent revocations" — the marker is absent and the EXISTS is
  one Redis round-trip.
- **Push-based invalidation via Pub/Sub.** A `auth:revoked` channel that
  per-replica caches subscribe to. Real-time but adds a subscriber per
  replica and complicates the failure mode (subscriber drops → cache goes
  stale). The per-id marker is pull-based and stateless.

## References

- [`internal/api/auth_cache.go`](../../internal/api/auth_cache.go) — `MarkKeyRevoked`, `isKeyRevoked`
- [`internal/api/admin.go`](../../internal/api/admin.go) — `revokeAPIKey` handler
- [`internal/api/router.go`](../../internal/api/router.go) — route mount
- [`internal/store/migrations/0007_audit_key_revoke.up.sql`](../../internal/store/migrations/0007_audit_key_revoke.up.sql) — schema follow-ups
- [ADR-0021](0021-verified-key-auth-cache.md) — verified-key cache
- [ADR-0024](0024-per-key-row-ownership.md) — per-key row ownership
