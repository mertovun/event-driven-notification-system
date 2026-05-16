# ADR-0018: Retry-tier publish path (TTL+DLX bounce, not nack-requeue)

## Status

Accepted (2026-05-16). Supersedes the implicit retry assumption in
[ADR-0003](0003-rabbitmq-over-redis-streams.md).

## Context

The `notifications.wait.5s/30s/5m` queues declared in
[`topology.go`](../../internal/queue/topology.go) (TTL + DLX back to the
main exchange, no `x-dead-letter-routing-key` so the original routing key
survives) existed before this ADR — **but nothing was publishing to them**.
The worker's retryable-failure path called `Nack(false, true)`: instant
requeue, no delay, no attempt counter. Under provider degradation the
worker hot-looped the same poison message at full prefetch rate.

## Decision

On retryable failure (provider 5xx / 429 / 408 / network) and on
breaker-open, the worker:

1. `RevertToQueued` so the row's CAS state matches "ready for redelivery."
2. Publishes the envelope to `notifications.wait.<tier>` (default exchange,
   queue-as-routing-key). The wait queue's TTL+DLX preserves the original
   routing key on the bounce.
3. **Acks** the original AMQP delivery — the wait queue is now the
   in-flight buffer; the prefetch slot is freed.

Tier selection: `attempt 1 → wait.5s`, `attempt 2 → wait.30s`,
`attempt 3+ → wait.5m`. `maxAttempts = 10` on the worker side and the same
cap in the outbox dispatcher both fire, because each side knows different
things about a delivery's history.

On publish-side failure (Redis unreachable, broker dead) the worker falls
back to nack-requeue — pre-fix behavior, now only as a degraded fallback.

## Consequences

- **Hot-loop fixed.** Pre-fix: ~1000 nack/s observed under provider 503.
  Post-fix: prefetch slot freed, workers immediately pull other messages
  while the failing one marinates.
- **Out-of-order delivery.** A new message can deliver before an older
  one that's waiting. Already true under priority queues + prefetch=1; not
  a new property.
- **2× AMQP messages per retry** (one to wait, one bounced back). Small
  bytes; counts toward broker I/O budget.
- **Breaker-open / wait.5s timing is loose.** Breaker `Timeout=30s` vs
  wait.5s means ~6 bounces during one open window. Deliberately not
  aligned — see ADR-0017 (shorter Timeout = more half-open false-positives).
  Mitigated by not incrementing `attempt_count` on breaker-open reverts
  (worker uses `RevertToQueuedNoAttempt`).

## Alternatives considered

- **App-level sleep before nack-requeue** — pins a prefetch slot for the
  whole wait. 12% throughput hit per worker at our 5-min tier. Rejected.
- **Sleep+nack in a side-goroutine** — loses message durability if the
  process crashes during the wait.
- **Header-driven exponential backoff with a custom router exchange** —
  more flexibility, doubles topology surface. Three tiers cover 95% of
  useful backoff curves.

## References

- [`internal/queue/topology.go`](../../internal/queue/topology.go) — wait queues + `WaitQueueForAttempt`
- [`internal/queue/publisher.go`](../../internal/queue/publisher.go) — `PublishToWaitQueue`
- [`internal/worker/pipeline.go`](../../internal/worker/pipeline.go) — `routeToRetryTier`
- [ADR-0003](0003-rabbitmq-over-redis-streams.md), [ADR-0017](0017-circuit-breaker-thresholds.md)
