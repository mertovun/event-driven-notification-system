# ADR-0022: Per-worker token reservoir + bounded inline retry on throttle

## Status

Accepted (2026-05-17).

## Context

The brief requires 100 msg/s/channel delivered. The Lua bucket
([ADR-0004](0004-redis-for-rate-limit-and-idempotency.md)) is configured
for 100/s/channel — but a single shared bucket key + 8 workers per
channel means **every `Allow()` is a Redis round-trip on a hot key**.
On local Docker the achieved rate was ~30 msg/s/channel against the
configured 100. The bucket arithmetic is correct; the deployment shape
(8 contenders on one key) is what walks observed throughput down.

A second, distinct bottleneck shows up on bursts. The original throttle
handling waited once (≤200ms) and routed to `wait.5s` on the second
denial. Under bursty injection 561 of 1061 handler invocations were
wait-tier bounces for a 500-message test — each one a 5-second round
trip through AMQP for what should have been a 10ms bucket refill. The
wait-tier ladder ([ADR-0018](0018-retry-tier-publish-path.md)) is
designed for provider outages, not rate-limit-bucket contention.

## Decision

Two complementary fixes for two distinct bottlenecks on the same hot path.

### Per-worker token reservoir

`internal/ratelimit/reservoir.go`. One `Reservoir` per `Pipeline`
(one Pipeline per channel; shared across that channel's N worker
goroutines). The worker calls `reservoir.Take(ctx)` instead of
`limiter.Allow(...)`:

- On the happy path, Take returns a token from a process-local counter
  under a sub-microsecond mutex.
- When the local counter hits zero, the reservoir runs one
  `AllowN(BatchSize=20)` call against the shared Lua bucket. If allowed,
  it credits `BatchSize-1=19` tokens locally and consumes one.
- If the batch refill is denied (the global bucket can't yield 20),
  the reservoir falls back to a single-token `Allow` so `RetryAfter`
  backpressure is preserved when the bucket is genuinely exhausted.

Effect: per-channel Redis traffic drops by ~20× (one RTT per 20 deliveries
on the happy path). The shared Lua bucket remains authoritative for the
cross-replica global cap. The reservoir is a freshness optimization, not
an isolation primitive.

### Bounded inline retry on throttle

`Pipeline.handle` (around the `reservoir.Take` call):

- Sub-second `RetryAfter` (≤ 1s): bucket-refill contention. Loop on
  `reservoir.Take`, sleeping the suggested `RetryAfter` between
  attempts, up to a 5-second total inline budget.
- Super-second `RetryAfter` (> 1s): sustained provider-side rate
  limit. Route to `wait.5s` retry tier via
  `RevertToQueuedNoAttempt` (so flow-control doesn't burn the attempt
  budget — see [ADR-0017](0017-circuit-breaker-thresholds.md)) and
  ack the original delivery to free the prefetch slot.

The discriminator is the `RetryAfter` value, which the bucket itself
returns. Small means local contention; large means the provider's own
policy is denying us.

## Alternatives considered

- **Partitioned bucket keys** (`ratelimit:sms:<shard>` with the global
  cap split N ways). Rejected: changes the semantics. An idle worker
  in shard A would still throttle a busy worker in shard A; the
  channel's full 100/s budget wouldn't be available to whoever needs
  it. Wrong shape for the brief.
- **Single bucket-keeper goroutine** per channel that owns the Lua
  bucket and hands out tokens via a Go channel. Cleaner semantics (no
  reservoir-local burst window), but introduces a coordinator with
  its own lifecycle and a single point of failure. Reservoir is the
  smaller change that meets the brief.
- **Server-side `redis.TIME`** instead of caller `time.Now()` in the
  Lua. Would fix the related clock-skew vulnerability but doubles the
  Redis RTT (server-side TIME is a separate call). The reservoir
  removes the hot RTT path; clock-skew is documented as a known
  failure mode in [ADR-0004](0004-redis-for-rate-limit-and-idempotency.md).

For the throttle loop, the alternatives were:

- **Always inline-loop until allowed**, no fallback to wait-tier. Rejected:
  a sustained provider-side throttle ties up a worker indefinitely with
  no admission control.
- **Always wait-tier on first denial**, no inline retry. The original
  shape. Rejected: turns a 10ms bucket-refill wait into a 5-second
  AMQP round-trip.
- **Cap inline retries by count, not time**. Rejected: time is the
  natural budget (the worker has a 30s `handleTimeout` parent context);
  a count-based cap is less aligned with the per-message latency target.

## Consequences

- **Throughput meets the brief.** Five consecutive runs of
  `internal/worker/throughput_test.go` deliver 500 messages in ~4 s
  (123-125 msg/s/channel sustained). Test SLO is set at 100. The
  `calls.Load() == N` correctness assertion holds (the provider was
  called exactly once per notification).
- **Bounded over-rate burst window.** The reservoir can credit up to
  `BatchSize-1 = 19` tokens locally per worker. Hard upper bound at
  burst:
  ```
  capacity + (workers × (BatchSize-1)) + (rate × clock_skew_seconds)
  ```
  With capacity=100, BatchSize=20, 8 workers, 1s NTP skew: ~252 in a
  one-second burst after a quiet period. Bucket capacity absorbs this
  within ~1s of refill. Acceptable for any provider that tolerates
  small overshoots before returning 429; for a hard-rejecting provider,
  drop BatchSize to 1 (effectively disables the reservoir's batching)
  and accept the lower achieved rate.
- **Reservoir loss-on-shutdown direction is under-delivery, not over.**
  If a worker pod is killed mid-batch, up to `BatchSize-1` tokens
  vanish (the shared bucket decremented them; no delivery happened).
  At 8 workers × 19 tokens × 3 channels = 456 tokens worst case = ~5s
  of throughput lost on a full pool restart. Acceptable for deploy
  cadence; would be unacceptable for a low-RPS audit workload, but
  this is the high-RPS notification path.
- **Cross-replica coordination unchanged.** The shared Lua bucket is
  still the authoritative cap. Two replicas × 8 workers = 16 contenders,
  and each contender now pays one RTT per 20 deliveries instead of one
  per delivery. The global cap is still enforced at the bucket.
- **Wait-tier reserved for provider outages.** The 5s/30s/5m ladder is
  no longer the primary response to short bucket-refill contention.
  Sustained provider-side throttle (> 1s RetryAfter) still routes
  through it, which is the original design intent of
  [ADR-0018](0018-retry-tier-publish-path.md).
- **`Pipeline.handle` retains a 5-second worst-case inline budget on
  throttle.** That's within the `handleTimeout=30s` parent context,
  and the throttle loop respects `ctx.Done()` so graceful shutdown is
  not blocked.
