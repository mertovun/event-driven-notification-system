# Known Issues

## Scheduled-notification worker hang

**Symptom.** When a notification scheduled via `scheduled_at` becomes due, the
scheduler transitions it correctly (`scheduled → pending → queued`) and writes
a fresh `outbox` row. The outbox dispatcher publishes the AMQP message
successfully. The worker consumes the message but then hangs indefinitely
inside `MarkSendingCAS` — the very first DB query of the delivery pipeline.

**Reproduction.**
1. `make up` on a clean stack.
2. `POST /v1/notifications` with `scheduled_at` ~3s in the future.
3. After ~3s the notification reaches `queued`, then never advances.
4. `rabbitmqctl list_queues` shows `1 messages_unacknowledged`.
5. SIGQUIT goroutine dump shows worker stuck at:
   `worker/pipeline.go:131 → store/gen/notifications.sql.go:318 →
    pgxpool.Pool.QueryRow → pgx/v5/conn.Query → pgconn.peekMessage →
    pgproto3.chunkReader.Next → io.ReadAtLeast → bgreader.Read →
    cond.Wait()`.
6. `pg_stat_activity` shows the matching backend in `wait_event=ClientRead`
   (Postgres is waiting for the Go client to read the response).
7. **Non-scheduled notifications published right after the stuck one are
   delivered fine by other workers in the pool** — the bug is isolated to
   whichever worker happened to claim the scheduled message.

**What we ruled out.**

| Hypothesis | Outcome |
|---|---|
| OTel tracing back-pressure | Reproduces with `OTEL_SDK_DISABLED=true` |
| `otelpgx` interfering with pgx | Reproduces with the tracer commented out |
| pgxpool exhaustion | `MaxConns=20` is well above all callers; goroutine has a connection |
| Postgres row-level lock contention | `pg_locks` shows no waits, `pg_stat_activity` shows no other holders |
| Worker decode / context-propagation bug | The same notification id can be CAS-updated manually from `psql` instantly |
| `statement_timeout=5000ms` on the connection | PG side actually completed and went idle (`wait_event=ClientRead`). Timeout never fires because there's no in-progress statement to abort. |
| **Separate pgxpool for scheduler vs workers** | **Reproduces. Pool contention is NOT the root cause.** |

## Diagnosed: pgx bgreader state-corruption

The deeper goroutine dump analysis revealed:

```
Foreground stuck at:
  bgreader.go:100 → cond.Wait()   ← parked here, status==Running

Background reader goroutine:
  (does not exist)                ← zero `bgRead` frames in dump
```

This is a **broken invariant** inside `pgx/v5/pgconn/internal/bgreader`: the
foreground caller sees `status == StatusRunning` (so it takes the
`cond.Wait()` path) but **no `bgRead` goroutine is actually running**.
Result: the foreground waits forever for a signal that nobody can send.

The suspected race lives in `pgconn.enterPotentialWriteReadDeadlock` +
`exitPotentialWriteReadDeadlock` interacting with `BGReader.Start` /
`BGReader.Stop`. Specifically, the `case StatusStopping: r.status =
StatusRunning` branch in `Start()` flips status back to Running **without
spawning a fresh `bgRead` goroutine**, relying on the existing one. If the
existing goroutine exits cleanly through a separate path (e.g. clean
EOF/timeout), the result is `Running` status with no goroutine.

This is reproducible against pgx v5.9.2 and is bug-class — not configuration
or schema. The fix likely lives in upstream pgx.

**Likely root cause.** A `pgx` v5 bgreader/cond.Wait() interaction triggered by
*some* specific combination of:
- The scheduler holding a brief transaction that inserts an outbox row, and
- The outbox dispatcher claiming that row almost immediately on its next 250ms tick, and
- A worker consuming the AMQP message and reaching for a pgxpool connection
  whose background reader is in a stale state.

This needs a focused pgx-internals investigation. Time-boxed out of the
assessment build; documented here so it's not silently shipped.

**Workarounds available today.**
- *For testing / demo*: Non-scheduled notifications work end-to-end at the
  full 100 msg/s rate-limit ceiling. The scheduler dispatcher fires correctly
  (proven by the status transition + scheduler log line). The break is
  downstream in the worker.
- *For production-ish use*: Set a shorter `statement_timeout` (e.g., 2s) and a
  worker-side context deadline; the message will fail-and-requeue rather than
  block a worker forever. The retry tier will pick it up on the second pass
  (which seems to succeed in our observations, though we haven't fully
  characterized this).

**Files involved.**
- [`internal/worker/pipeline.go`](internal/worker/pipeline.go) — `MarkSendingCAS` call site
- [`internal/scheduler/dispatcher.go`](internal/scheduler/dispatcher.go) — the dispatcher transition
- [`internal/store/pg.go`](internal/store/pg.go) — pool config

**Next steps if revisiting.**
1. ✅ ~~Try a separate pool for scheduler vs workers~~ — tested, no effect.
2. Add `pgx.LogLevelTrace` logging on a fresh small repro.
3. Try `pool.Reset()` after each scheduler dispatch to invalidate stale conns.
4. Try `MaxConns=2, MinConns=0` plus `MaxConnLifetime=1s` — force connection
   recycling. If that fixes it, confirms stale-conn theory.
5. **Reduce to minimal repro** outside the assessment codebase — two goroutines,
   one pool, one TX-then-release pattern, one single-statement pattern. Submit
   upstream to https://github.com/jackc/pgx/issues with the goroutine dump.
