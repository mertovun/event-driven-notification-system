# Architecture Decision Records

This directory holds the load-bearing decisions for the notification system.
Each ADR follows the conventional structure: **Context** (what forced the
choice), **Decision** (stated as fact), **Consequences** (what we got and what
we paid for it), and **Alternatives Considered** (what we looked at and why we
didn't pick it).

ADRs are immutable once accepted. If a decision needs to be revisited, write a
new ADR that marks the old one as *Superseded* and explains the change.

## Index

| # | Decision |
|---|---|
| [0001](0001-go-and-single-binary.md) | One Go binary, modes selected by `--mode={api,worker,all}` |
| [0002](0002-postgres-with-sqlc.md) | PostgreSQL with `sqlc`, not an ORM |
| [0003](0003-rabbitmq-over-redis-streams.md) | RabbitMQ over Redis Streams / Kafka / Postgres-as-queue |
| [0004](0004-redis-for-rate-limit-and-idempotency.md) | Redis for rate-limit, idempotency, and pub/sub — not as a queue |
| [0005](0005-cursor-pagination.md) | Cursor pagination, never offset |
| [0006](0006-idempotency-key-header.md) | `Idempotency-Key` request header with body-hash canonicalization |
| [0007](0007-distroless-runtime.md) | distroless + nonroot runtime container |
| [0008](0008-app-runs-migrations.md) | App runs migrations on boot |
| [0009](0009-transactional-outbox.md) | Transactional outbox, not direct publish |
| [0010](0010-tracing-on-by-default.md) | OpenTelemetry tracing on by default |
