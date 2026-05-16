# ADR-0009: Transactional outbox, not direct publish

## Status

Accepted (2026-05-16)

## Context

`POST /v1/notifications` must atomically achieve two effects in two independent systems: persist a row in Postgres and enqueue an AMQP message that a worker will consume. Postgres and RabbitMQ share no transactional boundary, so any naive ordering exposes a window where one side commits and the other does not. The two failure modes are symmetric and both unacceptable: a Postgres row with `status=pending` that no worker will ever see, or an AMQP message referencing a notification that does not exist. Neither is recoverable without either dropping work or shipping duplicates, and a notification API cannot do either silently.

We also want trace continuity. A request that enters the HTTP handler should produce a single trace that spans the async hop into the worker, otherwise on-call has to stitch logs by hand during an incident.

## Decision

The API uses a **transactional outbox**. The `POST` handler opens a Postgres transaction, INSERTs the `notifications` row, INSERTs a sibling `outbox` row carrying the full AMQP envelope (`payload`, `headers`, `routing_key`, `priority`), then COMMITs. The handler **never** talks to RabbitMQ. Either both rows land or neither does; there is no third state.

A separate **outbox dispatcher** goroutine (mounted in worker mode) drains the table. It polls every 250ms with:

```sql
SELECT ... FROM outbox
WHERE published_at IS NULL
  AND (claimed_at IS NULL OR claimed_at < now() - interval '60s')
ORDER BY created_at LIMIT 50
FOR UPDATE SKIP LOCKED
```

It claims rows by stamping `claimed_at, claimed_by`, publishes each one with `PublishWithDeferredConfirm`, and on broker ack sets `published_at = now()`. After 10 failed attempts the dispatcher transitions the parent notification to `status='dead_letter'` and inserts a row in `dead_letters`. Implementation: `internal/outbox/dispatcher.go`, schema in `0002_outbox.up.sql`, queries in `internal/store/queries/outbox.sql`.

We inject the W3C `traceparent` into `outbox.headers` at INSERT time. The dispatcher extracts it and starts `outbox.dispatch` as a child span, so a single trace spans the API handler, the Postgres commit, and the AMQP publish.

## Consequences

- Atomicity is guaranteed by Postgres alone. The cross-system race is gone.
- Recovery latency for crashed publishes is ~250ms (next poll tick), not the minutes a status-sweeper would take.
- Traces stitch across the async boundary without bespoke correlation IDs.
- Extra cost: one additional `INSERT` per notification on the hot path.
- Publish latency is sub-second (poll tick + AMQP confirm) rather than the sub-millisecond of a direct publish. For a notification system this is well under any user-visible threshold.
- The `outbox` table needs retention. We keep published rows for 24h for debuggability and run a daily prune; the table never grows unbounded.
- **Duplicate on recovery:** if the broker confirms a publish but the dispatcher crashes before `UPDATE outbox SET published_at = ...`, the next claim cycle republishes the row. This is by design - at-least-once is the only honest semantic across a crash boundary. The worker-side idempotency layers (Redis SETNX inflight lock plus DB CAS on `status`) absorb the duplicate.

## Alternatives Considered

- **Direct publish from the API handler (commit Postgres, then publish):** the obvious approach and the one we rejected. A crash in the millisecond between `COMMIT` and `basic.publish` leaves a pending row that no worker will ever see. Silent data loss is worse than the latency cost of the outbox.
- **Direct publish plus a pending-row sweeper:** moves the recovery window from "never" to "five minutes" but not to "250ms," and now there are two publish paths to reason about. We keep a sweeper concept, but it covers a different race (worker-side hang after claim), not the API-side publish race.
- **Debezium / Postgres logical decoding (CDC):** the principled upgrade. It eliminates the polling tick and pushes changes at write latency. It also requires a replication slot, a dedicated role, slot-lag monitoring, and a Debezium operator - operational surface we will not justify for this assessment. Because CDC reads the same `outbox` table, the migration is a swap of the dispatcher implementation; the API contract does not change.
- **Two-phase commit across Postgres and RabbitMQ:** `amqp091-go` does not implement XA, and XA across heterogeneous systems is an operational sinkhole even when the libraries do support it. Not viable.
