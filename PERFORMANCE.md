# Performance Analysis

This document captures bottleneck analysis from `pprof` profiling under
sustained k6 load on a single-replica stack. **The system is CPU-bound on
authentication.** Every other path is a rounding error at the load levels we
measured. The same data also shows that the rate limit, queue depth, and worker
pool sizing all work as designed.

The raw profiles are committed in [`loadtest/profiles/`](loadtest/profiles/)
and reproducible via:

```bash
PPROF_ENABLED=true make up
make load-baseline &        # background 30s @ 50 RPS
sleep 3 && make profile-cpu # 25s CPU profile during steady state
```

## Test environment

| | |
|---|---|
| Hardware | Apple M-series, macOS, Docker Desktop |
| Stack | docker-compose: app + postgres + rabbitmq + redis, all single-replica |
| Workload | k6 baseline: 50 RPS × 30s, POST /v1/notifications, raw content (no template) |
| Build | Go 1.26.3, distroless final, `-ldflags="-s -w"` |
| Profile capture | `/debug/pprof/profile?seconds=25` mid-run |

k6 result for the profiled run:
- **1500 requests, 0 failed, 49.86 r/s**
- **p99 = 116ms, p95 = 99ms, p90 = 80ms**

## Headline finding

**~96% of CPU time is spent in argon2id key verification.**

```
flat  flat%   sum%        cum   cum%
60.68s 59.42% 59.42%   97.72s 95.69%  argon2.processBlockGeneric
36.98s 36.21% 95.63%   37.00s 36.23%  argon2.blamkaGeneric
```

This is **not a bug**. It is the design from
[ADR-0011](docs/adr/0011-argon2id-over-bcrypt.md): argon2id is memory-hard
with `time=2, memory=64MB, parallelism=1`, tuned to consume ~50ms per verify.
The profile confirms this is exactly what is happening, and at 50 RPS we burn
~50ms × 50 = 2.5 CPU-seconds per wall-clock second, or **~2.5 cores' worth
of authentication work per second**.

Everything else combined is ~4% of CPU.

## Why this is the right finding

Authentication cost was a deliberate decision, and it dominates the profile by
~25× the next-largest contributor. Conclusions:

1. **The microservice glue is essentially free.** Chi router (`Mux.ServeHTTP`,
   middleware chain), OTel HTTP middleware (`otelhttp.serveHTTP`), JSON
   encoding/decoding, slog handlers, metrics middleware — combined they
   account for ~2.2 seconds of the 102 sample-seconds, i.e. ~2% of CPU. None
   of these is a bottleneck.
2. **Per-route handlers are sub-1%.** The notification create path
   (validation, idempotency cache lookup, two SQL INSERTs in a TX) does not
   appear in the top 30 nodes. The database round-trips are I/O-bound and
   counted as waits, not CPU.
3. **The rate-limit Lua script is sub-0.5%.** The token bucket round-trips to
   Redis cost essentially nothing measurable at 50 RPS.

## Heap and allocations

In-use heap during steady state:

```
Type: inuse_space
flat  flat%   cum%  cum
256MB 98.08%  98.08% 256MB  argon2.initBlocks
2.5MB  0.96%  99.04%   2.5MB runtime.mallocgc
```

All-time allocation count:

```
Type: alloc_space
flat        flat%   cum
145024MB  99.88%  145024MB  argon2.initBlocks
```

Reading these:
- Each `argon2.IDKey` call allocates a fresh 64 MB block array (the memory
  parameter). The GC reclaims it after every call.
- During the 30s test, 1500 verifications × 64 MB ≈ **96 GB allocated and
  freed**. The pprof reports 145 GB because internally argon2 also allocates
  some smaller working buffers.
- The **in-use** number (256 MB) means ~4 verifications were in flight at the
  moment we snapshotted the heap. That tracks at 50 RPS × 80 ms ≈ 4 inflight.

The GC handles this volume without strain — the heap profile shows no GC
pressure outside the argon2 allocations. But it is wasteful: every verify
re-initializes 64 MB it could share.

## Goroutine state

Snapshot taken under the same load: **170 goroutines total**, broken down as:

| State | Count | Source |
|---|---|---|
| select (parked) | 46+42 | net/http server, worker consumers waiting for AMQP deliveries |
| IO wait | 44 | poll readers (HTTP requests, AMQP connections, etc.) |
| chan receive | 17+8 | dispatcher waiters, hub fan-out |
| runnable | 5 | in-flight work |
| sync.WaitGroup.Wait | 4+2 | errgroup roots (main + worker manager) |
| running | 1 | the goroutine that captured the dump |

**No goroutine leak.** Every long-lived goroutine traces back to an errgroup
or a consumer with a clearly bounded exit condition (`<-ctx.Done()`). 24 of
the goroutines are the worker pool (8 per channel × 3 channels). The argon2
verify work happens on the HTTP server's net/http worker goroutines, not on
dedicated auth threads.

## Recommendations

Tiered by how much work fixes them and how much improvement to expect. None
are blockers for the assessment.

### Tier 1 — actually useful at moderate scale

**Add a verified-key cache.** A 60-second Redis SETEX on
`auth:verified:<key-prefix>:<key-suffix-hash>` short-circuits argon2id on the
hot path. The argon2 verify still runs once every 60s per active key; in
between, auth is a single Redis GET (~0.3ms vs ~50ms). Expected impact at
50 RPS with one active key: CPU drops from ~96% argon2 to ~0.1%. Trade-off:
revocation has a 60s tail (revoked keys still authenticate until cache TTL
expires). Mitigate with cache-bust on key revocation.

**Estimate**: 30-line change, ~2 hours including tests.

### Tier 2 — worth doing before production

**Tune argon2 parameters under measurement.** The current parameters
(`t=2, m=64MB, p=1`) are OWASP-recommended baselines. Production should
benchmark against the target hardware and CPU budget. Lowering `memory` to
32 MB roughly halves the cost; lowering `time` from 2 to 1 also halves it.
Either changes the security posture and needs review.

**Enable block and mutex profiling in a debug build.** Block / mutex profiles
in this run were near-empty (199 bytes each) because runtime profiling is
disabled by default. A debug image with
`runtime.SetBlockProfileRate(N)` and `runtime.SetMutexProfileFraction(M)`
would surface any pgxpool acquire contention, AMQP channel mutex contention,
WS hub broadcast contention, etc. — none of which show up at 50 RPS but
would matter when the auth bottleneck is removed.

### Tier 3 — only if numbers demand it

**Connection pooling for pgxpool defaults.** `MaxConns=20` is conservative
for a CPU-bound binary; if we removed the argon2 bottleneck and scaled
throughput up, the pool could become the next limit. The keyset query for
list endpoints is the slowest single query (cursor predicate on a composite
index) and warrants `EXPLAIN ANALYZE` on a million-row table.

**Sticky JSON buffers (`sync.Pool`) for the worker pipeline.** Every message
goes through a marshal + unmarshal cycle. At 50 RPS the cost is invisible
(~0%) but if we ever drove this binary at 5000 RPS the marshalling allocation
churn would matter.

**HTTP/2 PUSH or shared connections to webhook.site.** The provider client
already keeps `MaxIdleConnsPerHost=50`; once a real provider replaces
webhook.site, profiling against the new endpoint should validate the keep-alive
behaviour.

## What we did NOT measure

- **Saturation load.** We tested at 50 RPS (well within the breaker / rate-limit
  / capacity bounds). The actual ceiling of the single replica at the current
  argon2 cost is in the 80-100 RPS range (one core, ~50ms per request). A
  4-core machine would hit ~300-400 RPS before auth saturates.
- **Burst behaviour.** k6 priority scenario tested 150 RPS for 20s and showed
  the API back-pressures gracefully (p95 = 1.21s, no errors) but we did not
  characterize the failure mode beyond it.
- **Memory pressure under sustained burst.** With 64 MB allocated per verify
  and many concurrent requests, peak heap can spike. We did not test for OOM
  thresholds.

## Reproduction

1. Edit `.env`: set `PPROF_ENABLED=true`.
2. `make up`.
3. Confirm pprof is reachable: `curl http://localhost:8090/debug/pprof/`.
4. In one terminal: `make load-baseline` (runs k6 for 30s).
5. After 3 seconds, in another terminal: `make profile-cpu`.
6. Open the profile:
   - Text view: `go tool pprof -top loadtest/profiles/cpu-baseline.pprof`.
   - Interactive browser flamegraph: requires Graphviz (`brew install graphviz`),
     then `go tool pprof -http=:6060 loadtest/profiles/cpu-baseline.pprof`.

Profiles captured during the analysis run are committed to
[`loadtest/profiles/`](loadtest/profiles/) for reference. Total size: ~244 KB.

| File | Contents |
|---|---|
| `cpu-baseline.pprof` | 25s CPU profile under 50 RPS |
| `heap-loaded.pprof` | In-use heap snapshot under load |
| `allocs-loaded.pprof` | All-time allocations |
| `goroutine-loaded.txt` | Goroutine dump (text) |
| `block-loaded.pprof`, `mutex-loaded.pprof` | Empty (profiling not enabled in this build) |
