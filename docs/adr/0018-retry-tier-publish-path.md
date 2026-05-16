# ADR-0018: Retry-tier publish path (per-channel TTL+DLX wait queues)

## Status

Accepted (2026-05-16). Supersedes the implicit retry assumption in
[ADR-0003](0003-rabbitmq-over-redis-streams.md). Revised when re-validating
the retry path post-scheduler-fix surfaced a routing bug (see below).

## Context

The original topology had three shared `notifications.wait.{5s,30s,5m}`
queues with TTL+DLX-to-ExchangeMain and **no** `x-dead-letter-routing-key`,
relying on "the broker preserves the original routing key on bounce." That
*nearly* worked, except: a message published into a wait queue via the
default exchange (queue-as-routing-key) carries the wait queue's name as
its current routing key. On TTL expiry the broker re-published with that
same routing key, which no main-queue binding matched. The message
vanished. The bug wasn't visible until we re-validated end-to-end after
fixing the scheduler-wedge CHECK constraint.

Before the fix to the retry path itself, the worker's
retryable-failure handler called `Nack(false, true)` — instant requeue,
no delay, no attempt counter — so under provider degradation the worker
hot-looped the same poison message at full prefetch.

## Decision

**Per-channel × per-tier wait queues.** 3 channels × 3 tiers = 9
queues: `notifications.<channel>.wait.<tier>`. Each declared with:

- `x-message-ttl` = tier duration (5s / 30s / 5m)
- `x-dead-letter-exchange` = ExchangeMain
- `x-dead-letter-routing-key` = `notification.<channel>` (the main queue's key)

Each wait queue is bound to ExchangeMain on its own key
`notification.<channel>.wait.<tier>` so the worker can publish to it.

Retry-failure flow:

1. `RevertToQueuedNoAttempt` (breaker / throttle) or `RevertToQueued`
   (provider fault) on the notifications row.
2. Worker `Publish`es to ExchangeMain with routing key
   `notification.<channel>.wait.<tier>`. Message lands in the per-channel
   wait queue.
3. Worker `Ack`s the original AMQP delivery — prefetch slot freed.
4. TTL expires, broker uses the wait queue's `x-dead-letter-routing-key`
   to re-route via ExchangeMain with key `notification.<channel>`. The
   main channel queue receives it.

Tier selection: `attempt 1 → 5s`, `attempt 2 → 30s`, `attempt 3+ → 5m`.
`maxAttempts = 10` on the worker side and the same cap in the outbox
dispatcher both fire, because each side knows different things about a
delivery's history.

On publish-side failure (Redis unreachable, broker dead) the worker falls
back to nack-requeue — degraded fallback only.

## Consequences

- **Hot-loop fixed.** Pre-fix: ~1000 nack/s observed under provider 503.
- **Bounce path verified end-to-end.** A 503 retry shows attempt_count
  incrementing across 1 → 2 → 3 with the message visibly hopping through
  `notifications.sms.wait.5s` → `wait.30s` → `wait.5m`.
- **Topology grew 3 → 9 queues.** Acceptable — RabbitMQ handles thousands
  of queues comfortably; the operational shape is unchanged (still one
  exchange + one DLX).
- **Out-of-order delivery.** A new message can deliver before an older
  one waiting. Already true under priority queues + prefetch=1.
- **2× AMQP messages per retry.** Small bytes; counts toward broker I/O.
- **Breaker-open / wait.5s timing is loose.** Breaker `Timeout=30s` vs
  wait.5s means ~6 bounces during one open window. Mitigated by not
  incrementing `attempt_count` on breaker-open reverts (worker uses
  `RevertToQueuedNoAttempt`).

## Alternatives considered

- **Shared wait queues + a router consumer** that reads
  `x-original-routing-key` from headers and re-publishes. Adds a goroutine,
  introduces a single point of contention. Rejected.
- **App-level sleep before nack-requeue.** Pins a prefetch slot for the
  whole wait. 12% throughput hit per worker at the 5-min tier.
- **Sleep+nack in a side-goroutine.** Loses durability if the side
  goroutine's process crashes during the wait.
- **Header-driven exponential backoff with a custom router exchange.**
  More flexibility, doubles topology surface; the 9-queue per-channel set
  is the minimal correct topology.

## References

- [`internal/queue/topology.go`](../../internal/queue/topology.go) — `WaitQueue`, `WaitTierForAttempt`, `WaitRoutingKey`
- [`internal/queue/publisher.go`](../../internal/queue/publisher.go) — `PublishToWait`
- [`internal/worker/pipeline.go`](../../internal/worker/pipeline.go) — `routeToRetryTier`, `requeueViaWaitTier`
- [ADR-0003](0003-rabbitmq-over-redis-streams.md), [ADR-0017](0017-circuit-breaker-thresholds.md)
