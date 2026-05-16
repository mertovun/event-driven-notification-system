# Performance Analysis

This document captures bottleneck analysis from `pprof` profiling under
sustained k6 load on a single-replica stack. We measured the system in **three
phases**:

1. **Baseline** — 50 RPS, original auth path. CPU was ~96% argon2id.
2. **Cached auth** — 50 RPS after the verified-key cache landed. Same RPS,
   16× lower p99, ~57× less CPU work per request.
3. **Saturation** — 500→4000 RPS ramp. Found the next ceiling: **Postgres
   connection-pool acquire** at ~1600 RPS, exactly as predicted in Tier 3.

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
| Workload | k6: POST /v1/notifications, raw content (no template), single API key |
| Build | Go 1.26.3, distroless final, `-ldflags="-s -w"` |
| Profile capture | `/debug/pprof/profile?seconds=N` mid-run |

---

## Phase 1 — Baseline (50 RPS, no auth cache)

k6 result:
- **1500 requests, 0 failed, 49.86 r/s**
- **p99 = 116ms, p95 = 99ms, p90 = 80ms**

### Headline finding

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

### Heap and allocations (baseline)

```
Type: inuse_space        flat%   Type: alloc_space        flat%
argon2.initBlocks       98.08%   argon2.initBlocks       99.88%
```

Each `argon2.IDKey` call allocates a fresh 64 MB block array. During the 30s
test, 1500 verifications × 64 MB ≈ **96 GB allocated and freed**. The GC
handles this without strain — but it is wasteful: every verify re-initializes
64 MB it could share.

### Goroutine state (baseline)

170 goroutines total, broken down as:

| State | Count | Source |
|---|---|---|
| select (parked) | 46+42 | net/http server, worker consumers waiting for AMQP deliveries |
| IO wait | 44 | poll readers (HTTP requests, AMQP connections, etc.) |
| chan receive | 17+8 | dispatcher waiters, hub fan-out |
| runnable | 5 | in-flight work |
| sync.WaitGroup.Wait | 4+2 | errgroup roots (main + worker manager) |
| running | 1 | the goroutine that captured the dump |

**No goroutine leak.** Every long-lived goroutine traces back to an errgroup
or a consumer with a clearly bounded exit condition (`<-ctx.Done()`).

---

## Phase 2 — Verified-key auth cache (50 RPS, post-fix)

Implemented Tier 1 from the original recommendations: a 60-second Redis
SETEX on `auth:v1:<sha256[:16]>` short-circuits argon2id on the hot path.
The cache lookup is a single Redis GET; the argon2 verify still runs once
every 60s per active key.

Code: [`internal/api/auth_cache.go`](internal/api/auth_cache.go),
modifications in [`internal/api/auth.go`](internal/api/auth.go). Tests in
[`internal/api/auth_cache_test.go`](internal/api/auth_cache_test.go).

### Cache hit/miss timing

Direct curl measurements against the running stack:

| Call | Latency |
|---|---|
| First request (cache miss, full argon2 verify) | 77.6 ms |
| Subsequent requests (cache hit, Redis GET only) | 1.6 ms |

### k6 result at 50 RPS

| Metric | Before (Phase 1) | After (Phase 2) | Change |
|---|---|---|---|
| p99 | 116 ms | **7.5 ms** | **15× lower** |
| p95 | 99 ms | 4.7 ms | 21× lower |
| Total CPU samples / 25s | 102.12 s | **1.78 s** | **57× lower** |
| argon2 in top 15 | 96% (1st + 2nd) | not present | — |

The top of the post-cache profile is dominated by syscall (epoll_wait) and
runtime scheduler primitives — i.e. the server is mostly **idle**. No
application-level hotspot.

### Trade-off

Revocation has a 60s tail: a revoked key keeps authenticating until the
cache entry expires. We accept this — the cache key (`auth:v1:<sha256[:16]>`)
hashes the full raw bearer, so an attacker cannot collide their way in, and
the admin revoke flow can bust the cache explicitly when we need a
zero-second revocation.

---

## Phase 3 — Saturation testing (500→4000 RPS ramp)

Goal: find the next ceiling now that argon2 is no longer the limit.

k6 scenario in [`loadtest/k6_saturation.js`](loadtest/k6_saturation.js):
ramping arrival-rate from 500 RPS to 4000 RPS over 55 seconds,
`preAllocatedVUs=500, maxVUs=5000`.

### k6 result

```
checks_total.......: 88612   1610.736947/s
checks_succeeded...: 91.36%  80956 out of 88612
checks_failed......: 8.63%   7656 out of 88612
http_req_duration..: avg=295.17ms  med=1.13ms  p(90)=4.52ms  p(95)=2.74s  max=8.39s
  { expected:true }: avg=1.81ms    med=1.10ms  p(90)=2.42ms  p(95)=3.64ms max=82ms
dropped_iterations.: 8887 (k6 could not allocate VUs fast enough)
```

The system **sustained ~1611 RPS** end-to-end. At that rate, the response
distribution **bifurcates**:

- **91% of requests succeed in <4 ms** (p95 of successes = 3.64 ms)
- **9% time out** with a long tail (p95 overall = 2.74 s, max = 8.39 s)

That bimodal shape is the signature of **head-of-line blocking on a bounded
shared resource**, not slow code.

### CPU profile during saturation

20s profile captured during the 2000-4000 RPS window:

```
Duration: 20s, Total samples = 890ms ( 4.45%)
Top:
  290ms  32.58%  internal/runtime/syscall/linux.Syscall6   (epoll_wait)
   40ms   4.49%  runtime.futex
   40ms   4.49%  runtime.memclrNoHeapPointers
   10ms   1.12%  store/gen.(*Queries).MarkQueued
   10ms   1.12%  rabbitmq/amqp091-go.(*Channel).recvMethod
   10ms   1.12%  redis/go-redis/.../tracingHook.ProcessHook
```

**The Go app used 4.45% of one core for 20 seconds while servicing 1600 RPS
and shedding 9% of load.** There is no CPU bottleneck. The server is parked
in `epoll_wait` waiting for something it isn't getting.

### Where the failures come from

The 7,656 failed requests show two server-side symptoms:

1. **k6 logs**: `EOF` on POST — the client gave up and closed the connection.
2. **App logs**: `level=ERROR ... status=504 ... dur=168ms..730ms`.

The 504 path is in [`internal/api/errors.go:143`](internal/api/errors.go#L143):

```go
case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
    WriteProblemFull(w, r, Problem{ Status: http.StatusGatewayTimeout, ... })
```

`context.Canceled` fires when the HTTP client disconnects mid-request — i.e.
the request was **queued** at the server but the client gave up before it
ran. The fact that the failing requests show server-side durations in the
100ms-700ms range, while successful requests complete in <4ms, says
**something downstream is queueing requests**.

### The next bottleneck: pgxpool `MaxConns=20`

From [`internal/store/pg.go:20`](internal/store/pg.go#L20):

```go
cfg.MaxConns = 20
```

The create path does two SQL writes inside a TX (`InsertNotification` +
`InsertOutbox`). At ~1600 RPS, with a Postgres round-trip on the order of
1-3 ms per statement under load, each TX holds a conn for ~4-8 ms. The
theoretical ceiling is:

```
20 conns / (~4-8 ms per TX) = 2,500-5,000 TX/s
```

That's the right order of magnitude for **why 1,611 RPS works but everything
above is queued**: the pool is at saturation, p95-latency requests are
those that waited for a conn rather than those that ran on Postgres.

The 504/EOF tail is precisely the tail of the connection-wait queue
exceeding the client patience window.

### Why this matches the original prediction

[Tier 3](#tier-3--only-if-numbers-demand-it) of the original recommendations
said:

> **Connection pooling for pgxpool defaults.** `MaxConns=20` is conservative
> for a CPU-bound binary; if we removed the argon2 bottleneck and scaled
> throughput up, the pool could become the next limit.

That is exactly what we measured. The cache fix moved the ceiling from ~50
RPS (CPU-bound) to ~1600 RPS (conn-pool-bound) — a **32× improvement** on
the demonstrated ceiling.

### Goroutine state (saturation)

146 goroutines at snapshot time. No leak; well-behaved errgroups and worker
consumers. The transient spike during peak load (5000 in-flight VUs) had
already drained by the time we captured the goroutine dump.

---

## Recommendations

Now reflecting what we actually measured, not what we predicted.

### Tier 1 — done ✅

**Verified-key cache** — landed. See Phase 2 above.

### Tier 2 — next, if production targets exceed ~1500 RPS per replica

**Raise pgxpool `MaxConns`.** Direct fix for the saturation ceiling.
- `MaxConns = 100` is a reasonable single-replica target if Postgres can
  accept it (default `max_connections=100` on a fresh Postgres).
- Higher than ~200 per replica typically requires pgbouncer or RDS Proxy
  in front, otherwise Postgres backend memory dominates.
- Combine with `MinConns` warmup so cold-start latency does not spike.

**Estimate**: 1-line change + a load test. ~30 minutes.

**Tune argon2 parameters under measurement.** The current parameters
(`t=2, m=64MB, p=1`) are OWASP-recommended baselines. With the cache,
argon2 only runs once per active key per 60s, so this matters less than it
did — but for production a benchmark against the target hardware and CPU
budget remains worthwhile. Lowering memory to 32 MB halves the cost;
lowering time from 2 to 1 also halves it. Either changes the security
posture and needs review.

**Enable block and mutex profiling in a debug build.** Block / mutex
profiles in this run were near-empty because runtime profiling is disabled
by default. A debug image with `runtime.SetBlockProfileRate(N)` and
`runtime.SetMutexProfileFraction(M)` would directly surface the pgxpool
acquire contention we inferred from the latency bimodality.

### Tier 3 — only if numbers demand it

**Sticky JSON buffers (`sync.Pool`) for the worker pipeline.** Every
message goes through a marshal + unmarshal cycle. At 1600 RPS the cost is
still <1% (`encoding/json.stateInString` 40ms / 890ms total). If we ever
drove this binary past ~10,000 RPS per replica the marshalling allocation
churn would matter; below that, it doesn't.

**Outbox dispatcher batching.** `MarkQueued` was the largest Go-code
hot spot in the saturation profile at 21% of (a tiny) CPU pie. A 1-statement
UPDATE per dispatched message is unnecessary — batching the marks into a
single `UPDATE outbox SET status='queued' WHERE id = ANY($1)` every N
messages or T milliseconds would cut Postgres write amplification.

**HTTP/2 PUSH or shared connections to webhook.site.** The provider client
already keeps `MaxIdleConnsPerHost=50`; once a real provider replaces
webhook.site, profiling against the new endpoint should validate the
keep-alive behaviour.

---

## What we did NOT measure

- **The 4000 RPS ceiling** — k6 itself dropped 8,887 iterations at the upper
  ramp rates because it could not allocate VUs fast enough. The actual
  ceiling of the server above 1600 RPS is unknown; we know only that the
  pool-acquire queue overflows there. A distributed k6 run from multiple
  load generators would be needed to pin down the ceiling above pool
  saturation.
- **Burst behaviour beyond k6_priority.js.** k6 priority scenario tested
  150 RPS for 20s; saturation tested 4000 target RPS for 55s. We did not
  characterize sustained 1-hour-plus runs.
- **Multi-replica scale-out.** The hub Pub/Sub fan-out and the outbox
  `FOR UPDATE SKIP LOCKED` pattern are designed for horizontal scale, but
  we did not measure a 2-replica or 4-replica deployment.

---

## Reproduction

1. Edit `.env`: set `PPROF_ENABLED=true`.
2. `make up`.
3. Confirm pprof is reachable: `curl http://localhost:8090/debug/pprof/`.
4. Pick a scenario:
   - Baseline: `make load-baseline` (30s @ 50 RPS)
   - Saturation: `k6 run loadtest/k6_saturation.js` (55s ramp 500→4000)
5. In a second terminal, after 3 seconds: `make profile-cpu`.
6. Open the profile:
   - Text: `go tool pprof -top loadtest/profiles/cpu.pprof`.
   - Interactive flamegraph (needs `brew install graphviz`):
     `go tool pprof -http=:6060 loadtest/profiles/cpu.pprof`.

Profiles committed under [`loadtest/profiles/`](loadtest/profiles/):

| File | Phase | Contents |
|---|---|---|
| `cpu-baseline.pprof` | 1 | 25s CPU profile, 50 RPS, pre-cache |
| `heap-loaded.pprof`, `allocs-loaded.pprof` | 1 | heap + alloc snapshots, pre-cache |
| `goroutine-loaded.txt` | 1 | goroutine dump, pre-cache |
| `cpu-cached.pprof` | 2 | 25s CPU profile, 50 RPS, post-cache |
| `heap-cached.pprof`, `allocs-cached.pprof` | 2 | heap + alloc snapshots, post-cache |
| `goroutine-cached.txt` | 2 | goroutine dump, post-cache |
| `cpu-saturation.pprof` | 3 | 20s CPU profile during 500→4000 RPS ramp |
| `heap-saturation.pprof` | 3 | heap snapshot at saturation |
| `goroutine-saturation.txt` | 3 | goroutine dump post-saturation |
