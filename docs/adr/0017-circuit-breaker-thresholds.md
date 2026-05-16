# ADR-0017: Circuit breaker thresholds

## Status

Accepted (2026-05-16)

## Context

The worker wraps every provider call in a per-channel `gobreaker`. The
breaker amplifies the rate-limit signal into a hard pause when the
provider is degraded — instead of paying rate-limit + network RTT for
every doomed request, we open the circuit and let the wait-tier queues
([ADR-0018](0018-retry-tier-publish-path.md)) absorb in-flight work until
the provider recovers.

## Decision

One breaker per channel (sms / email / push), shared across the workers.
Settings in [`internal/worker/breaker.go`](../../internal/worker/breaker.go):

| Knob | Value | Reasoning |
|---|---|---|
| `MaxRequests` (half-open) | 5 | Short enough to recover quickly; long enough that a single 5xx blip won't re-trip. |
| `Interval` (bucket) | 60s | Matches provider SLO p99 order-of-magnitude. |
| `Timeout` (open duration) | 30s | Aligns with `wait.30s` retry tier so a cohort retries when the breaker re-opens. |
| `ReadyToTrip` | 5 consecutive **OR** ≥50% over ≥20 reqs | Fast path for outage, slow path for degradation. |
| Min sample size | 20 | Below 20 the 50% rule is noisy; consecutive-failures path covers. |

State **not** persisted across restart — cheap to rewarm in ~60 s.

`OnStateChange` writes the `circuit_breaker_state` Prometheus gauge
(0=closed / 1=open / 2=half-open) so oncall has a signal beyond logs.

## Consequences

- **Per-channel isolation**: SMS provider outage doesn't take down email.
- **One bad endpoint breaks the channel for everyone** using it. Real
  multi-tenant deploys need per-tenant / per-region breakers; out of
  scope here (single-tenant per [ADR-0011](0011-api-key-auth-not-oauth.md)).
- **Open-state vs wait.5s timing is loose** — see [ADR-0018](0018-retry-tier-publish-path.md);
  mitigated by not incrementing `attempt_count` on breaker-open reverts.

## Alternatives considered

- **Higher `MaxRequests`** (e.g. 20) — noisier half-open recovery, more
  flapping risk.
- **Per-tenant / per-region breaker** — correct for SaaS, premature here.
- **Breaker state in Redis** — survives restart, but adds hot-path Redis
  dependency and "Redis down → breaker state lost" failure mode.

## References

- [`internal/worker/breaker.go`](../../internal/worker/breaker.go)
- [ADR-0003](0003-rabbitmq-over-redis-streams.md), [ADR-0018](0018-retry-tier-publish-path.md)
- Sony gobreaker: <https://pkg.go.dev/github.com/sony/gobreaker>
