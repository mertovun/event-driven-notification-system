# Architecture Decision Records

This directory holds the load-bearing decisions for the notification system.
Each ADR follows the conventional structure: **Context** (what forced the
choice), **Decision** (stated as fact), **Consequences** (what we got and what
we paid for it), and **Alternatives Considered** (what we looked at and why we
didn't pick it).

ADRs are immutable once accepted. If a decision needs to be revisited, write a
new ADR that marks the old one as *Superseded* and explains the change.

## Index

### Foundations

| # | Decision |
|---|---|
| [0001](0001-go-and-single-binary.md) | One Go binary, modes selected by `--mode={api,worker,all}` |
| [0002](0002-postgres-with-sqlc.md) | PostgreSQL with `sqlc`, not an ORM |
| [0003](0003-rabbitmq-over-redis-streams.md) | RabbitMQ over Redis Streams / Kafka / Postgres-as-queue |
| [0004](0004-redis-for-rate-limit-and-idempotency.md) | Redis for rate-limit, idempotency, and pub/sub — not as a queue |
| [0007](0007-distroless-runtime.md) | distroless + nonroot runtime container |
| [0008](0008-app-runs-migrations.md) | App runs migrations on boot |
| [0010](0010-tracing-on-by-default.md) | OpenTelemetry tracing on by default |

### API contract

| # | Decision |
|---|---|
| [0005](0005-cursor-pagination.md) | Cursor pagination, never offset |
| [0006](0006-idempotency-key-header.md) | `Idempotency-Key` request header with body-hash canonicalization |
| [0013](0013-api-key-auth-not-oauth.md) | API key auth with scopes, not OAuth/OIDC |
| [0016](0016-chi-router.md) | Chi router over net/http stdlib patterns |
| [0017](0017-rfc7807-problem-details.md) | RFC 7807 Problem Details for HTTP errors |

### Domain & flow

| # | Decision |
|---|---|
| [0009](0009-transactional-outbox.md) | Transactional outbox, not direct publish |
| [0012](0012-template-render-at-create-time.md) | Templates render at create-time, not delivery-time |
| [0015](0015-webhook-site-as-provider-stub.md) | webhook.site as the only provider; no real SMS/Email/Push integrations |
| [0019](0019-dlq-replay-reset-in-place.md) | DLQ replay resets the row in place |
| [0020](0020-amqp-prefetch-one.md) | AMQP `prefetch=1`; scale by worker count |

### Security & realtime

| # | Decision |
|---|---|
| [0011](0011-argon2id-over-bcrypt.md) | argon2id over bcrypt for API key hashing |
| [0014](0014-websocket-over-sse.md) | WebSocket over Server-Sent Events for status push |
| [0018](0018-coder-websocket-library.md) | `coder/websocket` over `gorilla/websocket` |
| [0021](0021-verified-key-auth-cache.md) | Verified-key cache in Redis for the auth hot path |
| [0022](0022-circuit-breaker-thresholds.md) | Circuit breaker thresholds (per-channel, 5/50/20) |
| [0023](0023-retry-tier-publish-path.md) | Retry-tier publish path (TTL+DLX bounce, not nack-requeue) |
| [0024](0024-per-key-row-ownership.md) | Per-key row ownership and admin scope as the bypass |
| [0025](0025-key-revocation-cache-bust.md) | Admin key revocation endpoint and verified-key cache invalidation |
