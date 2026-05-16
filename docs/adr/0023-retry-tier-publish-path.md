# ADR-0023: Retry-tier publish path (TTL+DLX bounce, not nack-requeue)

## Status

Accepted (2026-05-16). Supersedes the implicit retry assumption in
[ADR-0003](0003-rabbitmq-over-redis-streams.md).

## Context

[ADR-0003](0003-rabbitmq-over-redis-streams.md) chose RabbitMQ over Redis
Streams partly *because* AMQP TTL+DLX cleanly express "wait 5s/30s/5m and
re-route." The topology was declared accordingly in
[`internal/queue/topology.go`](../../internal/queue/topology.go):

- `notifications.wait.5s` — TTL=5s, DLX=ExchangeMain
- `notifications.wait.30s` — TTL=30s, DLX=ExchangeMain
- `notifications.wait.5m` — TTL=5m, DLX=ExchangeMain

These queues had no `x-dead-letter-routing-key` so the *original routing key*
survives the DLX bounce — exactly what we want for "delay this message, then
redeliver it to its channel queue."

The problem caught in the assessment review: **nothing was publishing to
these queues.** The worker pipeline's retryable-failure path called
`d.Nack(false, true)` — instant requeue to the head of the same main queue,
no delay, no attempt counter. Under provider degradation the worker would
hot-loop the same poison message against the open breaker, burning Postgres
+ Redis + AMQP round-trips for zero forward progress.

## Decision

On retryable failure (provider 5xx / 429 / 408 / network), and on
breaker-open, the worker:

1. `RevertToQueued` so the row CAS state is consistent with redelivery.
2. **Publishes the same envelope** to `notifications.wait.<tier>` via the
   default exchange (queue-as-routing-key), with the original routing key
   set in the publish — RabbitMQ's TTL+DLX preserves it on the bounce.
3. **Acks the original AMQP delivery.** The wait queue is now the
   in-flight buffer; the original prefetch slot is freed.

Tier selection ([`internal/queue/topology.go:WaitQueueForAttempt`](../../internal/queue/topology.go)):

```
attempt 1 → wait.5s    (network blip)
attempt 2 → wait.30s   (sustained 5xx)
attempt 3+ → wait.5m   (degraded provider, longer cooldown)
```

`maxAttempts = 10` on the worker side ([`internal/worker/pipeline.go`](../../internal/worker/pipeline.go))
caps the loop. The outbox dispatcher also caps at 10 — both gates exist
because each side knows different things about a given delivery's history.

On publish-side failure (Redis unreachable, broker dead): the worker falls
back to nack-requeue. Pre-fix behavior, but now only as a degraded fallback.

## Consequences

**Positive — load test in fault-injection scenario (provider returning 503):**

- Pre-fix: workers hot-loop the same message N times against the open
  breaker, blowing through `attempt_count` rapidly. Real CPU spent on
  RabbitMQ ack/nack + Postgres CAS write + Redis SETNX/Del — zero
  forward progress, ~1000 nack/s observed under load.
- Post-fix: workers publish to wait queue, ack the original, prefetch slot
  freed. Workers immediately pull the next message — which might be a
  different recipient, different content, that the provider would accept.
  Throughput on the *good* messages is preserved while the failing
  message marinates in the wait queue.

**Positive — breaker recovery:**

The breaker-open path now publishes to wait.5s instead of nack-requeue.
The breaker is open for `Timeout=30s` ([ADR-0022](0022-circuit-breaker-thresholds.md)),
so wait.5s is shorter than the breaker open duration. The message bounces
back from wait.5s, the breaker is *still* open, it re-publishes to wait.5s
again. After 6 bounces it's exhausted the 30s open window and the breaker
transitions to half-open. *Yes, the math doesn't perfectly align* — the
trade-off was made deliberately (see "Why not align breaker timeout with
wait.5s exactly" below).

**Negative — out-of-order delivery:**

Within a channel, messages now interleave: a fresh new message can deliver
before an older message that's waiting in `wait.30s`. This was already
true in the prior design (priority queues + prefetch=1 cause inversion
under load), so the change does not make things worse. It is a real
inversion for "guaranteed in-order delivery" use cases, which this system
explicitly does not promise (single-shot notifications, not a sequence).

**Negative — increased AMQP traffic:**

Each retry now produces 2 AMQP messages (one to wait, one bounced back).
Previously a retry was a single nack-and-redeliver — no new message. The
overhead is small (~bytes-per-retry) but counts toward the broker's I/O
budget. Acceptable for the throughput envelope this system targets.

## Alternatives considered

- **Application-level sleep before nack-requeue.** Simple, but blocks the
  worker goroutine for the full sleep duration. A 5-minute wait pin-occupies
  a prefetch slot for 5 minutes — at our pool size of 8/channel that's a 12%
  throughput hit per worker. The TTL+DLX approach has zero in-process cost
  for the wait period.
- **Sleep+nack in a side-goroutine.** Recovers the prefetch slot but loses
  the durability of the message — if the side-goroutine's process crashes,
  the message is lost. The TTL+DLX approach persists the message at the
  broker.
- **Header-driven exponential backoff with per-attempt routing key.** More
  flexibility (any delay, not just 5s/30s/5m) but doubles the topology size
  and requires a custom router exchange. Three tiers cover 95% of useful
  backoff curves.
- **Aligning breaker `Timeout` with `wait.5s`.** Would require dropping
  breaker timeout from 30s to 5s, which means the half-open trial runs
  much sooner and is more likely to false-positive. We accept some loop
  inefficiency to preserve breaker stability.

## References

- [`internal/queue/topology.go`](../../internal/queue/topology.go) — wait queues + `WaitQueueForAttempt`
- [`internal/queue/publisher.go`](../../internal/queue/publisher.go) — `PublishToWaitQueue` helper
- [`internal/worker/pipeline.go`](../../internal/worker/pipeline.go) — `routeToRetryTier`
- [ADR-0003](0003-rabbitmq-over-redis-streams.md) — RabbitMQ topology choice
- [ADR-0022](0022-circuit-breaker-thresholds.md) — breaker thresholds
