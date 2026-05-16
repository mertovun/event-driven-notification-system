# ADR-0020: AMQP prefetch=1 per consumer; scale by worker count

## Status

Accepted (2026-05-16)

## Context

The per-channel queues `notifications.sms`, `notifications.email`, and
`notifications.push` are declared with `x-max-priority=10` (ADR-0003). The
intent is OTP-style traffic preempting marketing-style traffic during a
backlog: `high` should be delivered ahead of `normal` and `low` even when the
queue is already deep.

RabbitMQ enforces priority **at delivery time** — when a message moves from
the queue into a consumer's local buffer. Once pushed, the message sits in
that consumer's FIFO buffer until acked. If the broker has already pushed ten
low-priority messages to a worker, a high-priority message arriving moments
later queues behind them — not on the broker, but inside the consumer
process. Priority silently degrades to "priority among the first message
pulled, then FIFO for the rest." This is the prefetch trap, the single most
common gotcha in the RabbitMQ priority-queue documentation. Without
addressing it, `x-max-priority=10` is decorative.

## Decision

Every consumer issues **`ch.Qos(1, 0, false)`** (AMQP `basic.qos`,
`prefetch_count=1`, `prefetch_size=0`, `global=false`) before `ch.Consume`.
Each goroutine in the worker pool holds at most one unacknowledged delivery
at a time. Concurrency is provided by goroutine count per channel —
`WORKER_COUNT_SMS`, `WORKER_COUNT_EMAIL`, `WORKER_COUNT_PUSH` (default 8 each)
— not by buffered prefetch.

Files: `internal/queue/consumer.go` (the `ch.Qos(prefetch, 0, false)` call);
`internal/worker/manager.go` (the per-channel pool that spawns N goroutines,
each with its own consumer tag against the same channel). `global=false` is
deliberate — AMQP's spec uses "global" to mean "shared across all consumers
on the channel," which is misleading prose; `amqp091-go` gets the semantics
right but we pass it explicitly rather than rely on the zero value.

### Worker-count math

The rate-limit ceiling is 100 msg/s per channel (ADR-0004). Per-message
end-to-end latency is ~150ms (network + Redis token-bucket check + Postgres
state update + provider HTTP call). At `prefetch=1`, one goroutine sustains
`1 / 0.150s ≈ 6.6 msg/s`. With 8 workers per replica per channel:
`8 × 6.6 ≈ 50 msg/s`. Two replicas hit `100 msg/s`, matching the rate-limit
ceiling exactly. This is the desired outcome — the rate limiter, not the
worker pool, is the binding constraint. The rate limit is the explicit
contract; throughput should not be an emergent property of prefetch tuning.

## Consequences

- **Priority works as advertised.** A high-priority message arriving while N
  workers each hold one low-priority message waits at most one low-priority
  message — not an N-deep buffer.
- **Per-message broker round-trip on the hot path.** With `prefetch=1` the
  broker cannot pipeline the next delivery while the consumer works the
  current one. Adds ~1ms per message vs ~0.05ms amortized for a buffered
  prefetch. At 100 msg/s/channel this is ~100ms/s of extra network —
  negligible. At 10k msg/s it would matter.
- **Scaling is by worker count and replica count.** Both are visible in
  config and k8s manifests; no hidden knob.

## Alternatives Considered

- **`prefetch=10` (or higher) per consumer** — the conventional default.
  Higher per-consumer throughput; broker can pipeline. Breaks priority: a
  worker holding ten normals will not see a high arriving at "position 11"
  of its buffer — the buffer is FIFO once pulled. We cannot have priority
  queues as advertised and `prefetch>1` simultaneously.
- **`prefetch=0` (unlimited).** Worst case of the above: the broker shoves
  the queue at the first consumer; priority is disabled and one slow worker
  holds the backlog.
- **One queue per (channel × priority), `prefetch>1` per queue, round-robin
  in the consumer.** Triples queue count (9 main queues instead of 3),
  forces tripling the retry tiers `wait.5s` / `wait.30s` / `wait.5m`
  (ADR-0003), and moves priority enforcement into application code. Cost is
  real, the benefit is throughput we do not need.
