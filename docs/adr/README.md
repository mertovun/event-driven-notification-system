# Architecture Decision Records

Load-bearing decisions for the notification system. Each ADR captures
**Context**, **Decision**, **Consequences**, and **Alternatives** in
roughly that order, kept brief. ADRs are immutable once accepted — if a
decision changes, write a new ADR that marks the old one *Superseded*.

## Index

### Foundations
- [ADR-0001](0001-go-and-single-binary.md) — One Go binary, modes selected by `--mode={api,worker,all}`
- [ADR-0002](0002-postgres-with-sqlc.md) — PostgreSQL with `sqlc`, not an ORM
- [ADR-0003](0003-rabbitmq-over-redis-streams.md) — RabbitMQ over Redis Streams / Kafka / Postgres-as-queue
- [ADR-0004](0004-redis-for-rate-limit-and-idempotency.md) — Redis for rate-limit, idempotency, and pub/sub — not as a queue
- [ADR-0007](0007-app-runs-migrations.md) — App runs migrations on boot

### API contract
- [ADR-0005](0005-cursor-pagination.md) — Cursor pagination, never offset
- [ADR-0006](0006-idempotency-key-header.md) — `Idempotency-Key` header with body-hash canonicalization
- [ADR-0011](0011-api-key-auth-not-oauth.md) — API key auth with scopes, not OAuth/OIDC
- [ADR-0013](0013-rfc7807-problem-details.md) — RFC 7807 Problem Details for HTTP errors

### Domain & flow
- [ADR-0008](0008-transactional-outbox.md) — Transactional outbox, not direct publish
- [ADR-0010](0010-template-render-at-create-time.md) — Templates render at create-time, not delivery-time
- [ADR-0014](0014-dlq-replay-reset-in-place.md) — DLQ replay resets the row in place
- [ADR-0015](0015-amqp-prefetch-one.md) — AMQP `prefetch=1`; scale by worker count
- [ADR-0018](0018-retry-tier-publish-path.md) — Retry-tier publish path (TTL+DLX bounce)

### Security & realtime
- [ADR-0009](0009-argon2id-over-bcrypt.md) — argon2id over bcrypt for API key hashing
- [ADR-0012](0012-websocket-over-sse.md) — WebSocket over Server-Sent Events for status push
- [ADR-0016](0016-verified-key-auth-cache.md) — Verified-key cache in Redis on the auth hot path
- [ADR-0017](0017-circuit-breaker-thresholds.md) — Circuit breaker thresholds
- [ADR-0019](0019-per-key-row-ownership.md) — Per-key row ownership and admin scope as the bypass
- [ADR-0020](0020-key-revocation-cache-bust.md) — Admin revocation endpoint + cache invalidation
- [ADR-0021](0021-audit-hash-chain.md) — Audit log hash chain + content-integrity verifier

### Performance
- [ADR-0022](0022-rate-limit-reservoir-and-throttle-loop.md) — Per-worker token reservoir + bounded inline retry on throttle

## Decisions deliberately left out of ADRs

Some choices are uncontroversial enough that an ADR is debt rather than
documentation. Captured here as one-liners so a reader still sees the
choice:

- **distroless + nonroot + read-only rootfs + cap_drop ALL** — runtime container hardening.
- **OpenTelemetry on by default**, with a no-op fallback when no collector is reachable.
- **webhook.site as the provider** — specified by the brief.
- **Chi router** over the Go 1.22 stdlib mux — chi's middleware-as-handlers API was the deciding factor.
- **`coder/websocket`** over `gorilla/websocket` — newer, smaller, comparable feature set.
