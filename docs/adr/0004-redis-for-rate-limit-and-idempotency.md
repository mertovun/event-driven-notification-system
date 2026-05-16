# ADR-0004: Redis for rate-limit, idempotency, and pub/sub — not as a queue

## Status

Accepted (2026-05-16)

## Context

The notification system needs three pieces of cache-shaped state that live outside
Postgres' transactional store:

1. **Per-channel rate limiter** — 100 msg/s, enforced across all API replicas.
2. **Idempotency-Key cache** — 24h TTL keyed by client-supplied header, storing the
   canonical response body so replays are byte-identical.
3. **Pub/Sub for status events** — workers fan out delivery transitions to the
   WebSocket hub on each replica.

All three are read-heavy, write-cheap, and tolerate sub-second eventual consistency.
None of them are the system of record — Postgres is. We use `redis/go-redis/v9`
against Redis 7.

## Decision

Use a single Redis cluster for all three concerns, with a clear rule: **Redis is
state, not a queue.** RabbitMQ owns the work queue (see ADR-0003).

- **Rate limit** (`internal/ratelimit/lua.go`): token bucket implemented as an
  atomic Lua script. One round-trip per check (~0.5ms). The script reads current
  tokens + last-refill timestamp, computes the refill, decrements, and writes back
  in a single server-side execution. One Redis key per channel, shared across
  replicas. Bucket key auto-expires after a long idle window — no GC needed.
- **Idempotency** (`internal/idempotency/idempotency.go`): `SET NX EX 86400` on
  `idem:<key>` with a canonicalized request body hash + stored response payload as
  the value. On `NX` failure we `GET` the existing entry, verify the body hash
  matches (mismatch → 409), and replay the response with header
  `Idempotency-Replayed: true`.
- **Pub/Sub** (`internal/events/publisher.go`): workers `PUBLISH events:notifications <json>`
  on status transitions (queued → sent → failed). Each WS hub replica
  `SUBSCRIBE`s once and locally filters by user/notification id, fanning out to
  connections it owns.

## Consequences

- Hot-path latency stays tight: idempotency check is one Redis GET on a hit (vs.
  three Postgres round-trips with the INSERT/catch-23505/SELECT pattern). Every
  POST goes through this code path.
- The Lua script is the canonical compare-and-set primitive in Redis. We pin it
  by SHA via `EVALSHA` with `EVAL` fallback on `NOSCRIPT`. Atomicity is
  guaranteed by Redis' single-threaded execution model.
- **Pub/Sub is at-most-once.** If a worker publishes and a hub replica is briefly
  disconnected, that event is lost. This is acceptable because the WS event is a
  hint ("re-fetch this notification"), not the source of truth — clients can
  always re-query Postgres for canonical state. We do not use Pub/Sub for
  delivery-critical signals.
- Redis becomes a hard dependency for the API write path. If Redis is down, we
  fail closed on rate-limit and idempotency (return 503). The trade is worth it:
  losing rate-limit enforcement is worse than losing availability for the window.
- TTL strategy keeps memory bounded without explicit eviction logic: idempotency
  entries expire after 24h, rate-limit buckets after their idle window. No
  background cleanup job required.

## Alternatives Considered

- **Postgres for rate-limit** (`UPDATE ... RETURNING tokens`): correct, but every
  check is a transaction + row update. Under burst load this serializes on the
  per-channel row. Redis Lua is faster and contention-free.
- **Postgres for idempotency** via `UNIQUE` constraint and the catch-23505
  pattern: three round-trips on a duplicate. Doable, but wasteful for what is
  effectively a cache lookup.
- **In-memory rate-limit** (per-process token bucket): silently broken with more
  than one replica — each grants its own 100 msg/s budget, so total throughput
  scales with replica count and violates the contract.
- **Postgres LISTEN/NOTIFY for WS fan-out**: couples WebSocket scaling to
  Postgres connection limits. Pub/Sub scales independently of the DB.
- **Server-Sent Events instead of a WS hub**: viable, but the brief calls out
  WebSocket updates explicitly, and WS keeps the door open for bidirectional
  client signals later (e.g., read receipts).
- **Redis as the work queue**: rejected in ADR-0003. Redis Streams or
  `LPUSH/BRPOP` lacks the acknowledgement, requeue, and DLQ semantics RabbitMQ
  gives us for free.
