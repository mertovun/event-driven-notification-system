# ADR-0017: Circuit breaker thresholds (per-channel, 5/50/20)

## Status

Accepted (2026-05-16)

## Context

The worker pipeline wraps every provider call in a per-channel circuit
breaker (`gobreaker`). The breaker exists to *amplify* the rate-limit signal
into a hard pause when the provider is degraded: instead of paying rate-limit
+ network round-trip for every doomed request, we open the circuit and let
the wait-tier queues (see [ADR-0003](0003-rabbitmq-over-redis-streams.md))
absorb the in-flight work until the provider recovers.

The original [ADR-0009](0009-argon2id-over-bcrypt.md) review flagged that the
specific thresholds chosen — 5 consecutive failures OR ≥50% over 20 requests,
30-second open state, 5 half-open trial requests — were undocumented. This
ADR records the reasoning.

## Decision

Single breaker per channel (sms / email / push). Shared across the workers
for that channel, not per-replica-per-worker.

Settings ([`internal/worker/breaker.go`](../../internal/worker/breaker.go)):

| Knob | Value | Reasoning |
|---|---|---|
| `MaxRequests` (half-open trial size) | 5 | Provider 5xx blips on a single failed request shouldn't flip closed. 5 successes is short enough to recover quickly when the provider is back. |
| `Interval` (bucket window) | 60s | Matches the order of magnitude of provider SLO p99. Long enough that transient spikes don't trip; short enough that stale failures don't keep the breaker open after recovery. |
| `Timeout` (open duration) | 30s | The wait.30s retry tier delivers cohorts at 30s; a breaker opened at the same moment retries each batch once they arrive. Aligning the timeline keeps the wait-queue ↔ breaker handshake monotone. |
| `ReadyToTrip` rule | ≥5 consecutive failures OR ≥50% over ≥20 reqs | Two trip triggers: a fast path for sustained outage (5 in a row), a slow path for degraded-but-not-out (50% over a meaningful sample size). |
| Minimum sample size | 20 | Below 20 the 50%-error rule is noisy; the consecutive-failures path catches everything else. |

## Consequences

**Positive:**

- Predictable behavior under provider degradation: the breaker opens, the
  worker pipeline reverts to `queued`, the retry-tier wiring (ADR-0018)
  publishes to `wait.5s/30s/5m`. Without the breaker, the rate-limit gate
  would pay full network cost per failed request.
- Per-channel isolation: an SMS provider outage doesn't take down email
  delivery.
- State **not** persisted across process restart. Cheap to rewarm — typical
  recovery scenario is a deploy where the new instance learns the provider
  state in the first 60s.

**Negative:**

- One bad endpoint per channel breaks the channel for **every customer**
  using that channel. A real multi-tenant deploy would need per-tenant or
  per-region breakers; this is acceptable for the single-tenant scope
  documented in [ADR-0011](0011-api-key-auth-not-oauth.md).
- The 30s open duration means the wait.5s queue is effectively ineffective
  while a breaker is open — the open-state path skips the wait queue at all
  (we'd just hit the breaker again). The wait.30s and wait.5m tiers do
  shield us; a 5s wait alone would still bounce off the open breaker.

**Negative — observability:**

- The `notifications_circuit_breaker_state` metric is declared but not
  wired (security review #L91). Belongs in a follow-up: hook the
  `OnStateChange` callback to set the gauge.

## Alternatives considered

- **Threshold tuned for higher MaxRequests** (e.g. 20) for half-open: noisier
  signal during half-open recovery, more risk of flapping. 5 is the
  gobreaker default and matches our minimum sample size for the 50% rule.
- **Per-recipient or per-region breaker**: correct shape for a multi-tenant
  SaaS. Out of scope for the single-tenant model; called out in the README's
  "What's not built" list.
- **Breaker state in Redis**: would persist across restarts and across
  replicas. Adds a Redis dependency to the hot path and a new failure mode
  (Redis down → breaker state lost), which trades one footgun for another.
  Process-local state plus a 60s recovery interval handles the common cases.

## References

- [`internal/worker/breaker.go`](../../internal/worker/breaker.go)
- [ADR-0003](0003-rabbitmq-over-redis-streams.md) — retry-tier topology
- [ADR-0018](0018-retry-tier-publish-path.md) — retry-tier publish path
- Sony's gobreaker docs: https://pkg.go.dev/github.com/sony/gobreaker
