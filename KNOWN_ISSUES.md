# Known Issues

## Scheduled-notification "wedge" — **diagnosed and fixed (2026-05-16)**

**TL;DR.** The wedge was never a pgx bug. It was a Postgres CHECK
constraint mis-firing on the worker's `MarkSendingCAS` query.
[Migration 0010](internal/store/migrations/0010_scheduled_constraint_relax.up.sql)
drops the over-strict constraint; the scheduler path now delivers
end-to-end. The earlier diagnosis pointing at `pgx/v5/pgconn/internal/bgreader`
was wrong.

### What the bug actually was

The original schema declared:

```sql
CONSTRAINT notifications_scheduled_status_consistent
    CHECK (scheduled_at IS NULL OR status IN
           ('pending','queued','cancelled','scheduled'))
```

Read as: "if `scheduled_at` is set, status must be in the pre-send set."
The intent was sensible — block setting `scheduled_at` on a row that's
already past pre-send. The execution was wrong: the constraint is checked
on **every** UPDATE, including transitions *out of* pre-send. So when the
worker ran:

```sql
UPDATE notifications
SET status = 'sending', ...
WHERE id = $1 AND status IN ('pending','queued')
```

against a row with non-null `scheduled_at`, Postgres rejected the update
with:

```
new row for relation "notifications" violates check constraint
"notifications_scheduled_status_consistent" (SQLSTATE 23514)
```

The error propagated up through pgx → the worker handler → AMQP
nack-requeue. The original (May 2026) build's AMQP path did not yet have
[ADR-0018](docs/adr/0018-retry-tier-publish-path.md)'s wait-tier routing,
so the message hot-looped instantly. The "hang" presentation we saw in the
goroutine dumps was probably the symptom of the broker's redelivery
backpressure + the pgx wire reading the constraint error on a connection
that was about to be returned — a wire state somewhere between "query
succeeded" and "error visible to caller."

### Why the original diagnosis was wrong

The goroutine dump showed a worker stuck at
`bgreader.go:100 → cond.Wait()`. That's a real stack frame, but it's the
*pgx wire reader's normal idle state* — it's not where a constraint error
manifests. We pattern-matched the stack to "bgreader bug" because:

- A search for the specific `cond.Wait()` line in `pgx` issues turned up
  threads about state-machine races.
- We had no proof the bug was *outside* pgx; the alternative (our schema)
  was easier to assume than to verify.

We ruled out a lot of credible alternatives (otelpgx, OTel back-pressure,
pool exhaustion, row-level locks, separate pool, statement_timeout —
all in the original table below). We did **not** look at the actual
error returned by `MarkSendingCAS` because we never got far enough to see
it — the goroutine dump showed a hang, not a returned error, so we
followed the hang.

The current build (post-[silent-worker-hot-loop fix](docs/adr/0018-retry-tier-publish-path.md)
and the wrap-error-and-log safety net) surfaces the constraint violation
clearly:

```
{"level":"WARN","msg":"delivery returned non-nil error","channel":"sms",
 "err":"CAS to sending: ERROR: new row for relation \"notifications\"
        violates check constraint
        \"notifications_scheduled_status_consistent\" (SQLSTATE 23514)"}
```

That log line is what flipped the diagnosis. With every error path now
logged, the actual cause was visible on the first scheduled-notification
attempt after re-enabling the scheduler.

### What the fix did

[Migration 0010](internal/store/migrations/0010_scheduled_constraint_relax.up.sql)
drops the constraint. We could have written a stricter replacement that
distinguishes "INSERT-time validation" from "UPDATE-time" via a trigger,
but the constraint wasn't load-bearing — the API validator already
rejects `scheduled_at` on terminal-state rows, and the worker code never
sets `scheduled_at` on a `sent` row. Dropping it is correct.

After this fix:
- Scheduled notification with `scheduled_at` ~5s in the future →
  delivered cleanly within 6s of the due time.
- Batch of 5 scheduled notifications → 4/5 delivered immediately,
  1/5 took a retry-tier hop (httpbin.org timeout, unrelated to the
  schema fix) and delivered on the second attempt.
- No goroutine hangs observed. No `MarkSendingCAS` errors.

The scheduler feature flag (`SCHEDULER_ENABLED`, default `false`) stays
in place for now — it's still a one-replica-per-process design without
leader election, and there are real questions about coordinating multiple
scheduler dispatchers safely. But the *immediate* blocker is gone.

### Original investigation log (kept for the record)

The hypotheses we ran through before migration 0010:

| Hypothesis | Outcome |
|---|---|
| OTel tracing back-pressure | Reproduced with `OTEL_SDK_DISABLED=true` |
| `otelpgx` interfering with pgx | Reproduced with the tracer commented out |
| pgxpool exhaustion | `MaxConns=20` is well above all callers; goroutine has a connection |
| Postgres row-level lock contention | `pg_locks` shows no waits, `pg_stat_activity` shows no other holders |
| Worker decode / context-propagation bug | The same notification id can be CAS-updated manually from `psql` instantly |
| `statement_timeout=5000ms` | PG side actually completed and went idle (`wait_event=ClientRead`) |
| Separate pgxpool for scheduler vs workers | Reproduced. Pool contention not the cause. |
| **pgx v5 bgreader state corruption** | **Wrong. The hang was a constraint error surfacing on a redelivery loop without an exit.** |

What we should have done earlier:

1. **Logged the error from `MarkSendingCAS`.** The original code path
   was `if errors.Is(err, pgx.ErrNoRows) { log.Info("...drop"); return }`
   — every non-NoRows error was swallowed and bubbled up as a generic
   "nack requeue." A single `logger.Error("cas to sending", "err", err)`
   would have shown the constraint message immediately.
2. **Read pg_stat_activity for the *active* query, not the *idle*
   wait_event.** Postgres was idle in `ClientRead` because the constraint
   error already happened and Postgres was done. We treated `ClientRead`
   as "PG is waiting for the client to send something" and concluded the
   client was wedged. In hindsight: PG goes to `ClientRead` after *any*
   query completes — success or error — while the connection sits in the
   pool. It says nothing about whether the client is healthy.
3. **Tried a `SELECT 1` on the row from psql with the same WHERE clause
   the CAS uses.** Would have surfaced the constraint immediately.
   We did try a *manual `UPDATE`* from psql, but not the precise CAS
   shape with all the constraints firing.

### Files involved

- Migration: [`internal/store/migrations/0010_scheduled_constraint_relax.up.sql`](internal/store/migrations/0010_scheduled_constraint_relax.up.sql)
- The worker error log that surfaced the bug: [`internal/worker/pipeline.go`](internal/worker/pipeline.go) (`logger.Warn("delivery returned non-nil error", ...)` near `Handle`)
- The original constraint: [`internal/store/migrations/0001_init.up.sql`](internal/store/migrations/0001_init.up.sql), lines 50-53
