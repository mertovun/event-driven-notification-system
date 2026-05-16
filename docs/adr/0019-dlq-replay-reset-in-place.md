# ADR-0019: DLQ replay resets the row in place; no clone-with-replay_of

## Status

Accepted (2026-05-16)

## Context

`POST /v1/admin/dead-letters/{id}/replay` takes a dead-lettered notification and pushes it back through the pipeline. There are two coherent models for what "replay" means at the row level, and they have to be chosen once because every downstream consumer — status checks, reporting queries, the dashboard — depends on which one we picked.

The first is **reset-in-place**: UPDATE the existing row to `status='pending'`, clear `attempt_count` and `last_error`, rotate the `correlation_id`, write a fresh outbox row, and let the normal dispatcher (ADR-0009) republish. The notification keeps its id.

The second is **clone with `replay_of` link**: INSERT a new notification with a new id, set `replay_of = original_id`, write outbox for the new id, leave the original sitting in `dead_letter` forever as a tombstone.

Both work. They differ on the client contract, on reporting, and on how much rope we hand operators who run bulk replays.

## Decision

**Reset-in-place.** The replay handler in `internal/api/admin.go` opens a transaction and:

1. Verifies the current state is `dead_letter`. Only this status is eligible — `failed` is rejected with 409. Permanent failures (bad recipient, provider validation rejection) must not auto-recover; `dead_letter` means "retries exhausted on a transient-class failure" and is the only honest replay target.
2. ```sql
   UPDATE notifications
      SET status='pending', attempt_count=0, last_error=NULL, sent_at=NULL,
          correlation_id=$2, updated_at=now()
    WHERE id=$1 AND status='dead_letter'
   ```
   The `AND status='dead_letter'` guard is the CAS — if a concurrent operator already replayed, the UPDATE affects zero rows and the handler returns 409.
3. `DELETE FROM dead_letters WHERE notification_id=$1` — the row is live again, so the DLQ index no longer points at it.
4. `INSERT INTO outbox (notification_id, payload, headers, routing_key, priority) VALUES (...)` — the dispatcher picks it up on the next 250ms tick and publishes via the same path as any new notification. No bespoke replay route.
5. `INSERT INTO admin_audit (actor, action, target_id, details) VALUES ($actor, 'dlq_replay', $1, jsonb_build_object('prior_correlation_id', $old, 'prior_attempt_count', $n))` — the audit log is the system-of-record for who replayed what, when, and what the row looked like before.

The state transitions aren't sqlc-generated queries; the admin path uses `tx.Exec` directly because the transition is admin-only and the SQL is short enough to keep in the handler beside its preconditions.

## Consequences

- **Stable id across replay.** A client polling `GET /v1/notifications/{id}` sees the same id move from `dead_letter` back to `pending` and (hopefully) to `sent`. With clone, the client would have to follow `replay_of` chains to find the live row.
- **No duplicate counting in reports.** `SELECT count(*) FROM notifications WHERE status='dead_letter'` is the truth without a `replay_of IS NULL` filter. A "5 failures today" dashboard panel doesn't double when an operator hits replay.
- **Audit log is the only history dimension we add.** Every attempt — pre- and post-replay — is already in `delivery_attempts`. The operator action goes in `admin_audit`. We don't need a third tracking table.
- **Honest tradeoff: the original `correlation_id` is lost from the row.** We rotate it so the new trace doesn't inherit the failed correlation chain, which would poison tracing dashboards. The prior value is captured in `admin_audit.details`, so a reviewer reconstructing "which trace failed before this replay?" reads the audit log, not the notification row.
- **Honest tradeoff: `attempt_count` resets along with `last_error`.** A reviewer asking "did this ever succeed before being DLQ'd?" reads `delivery_attempts.started_at` ordered ascending; the row-level counter only reflects the current life. We accept this because `delivery_attempts` already carries the full per-attempt record.
- **Replay is idempotent under the CAS guard.** Two concurrent replay calls race on the UPDATE; the loser sees zero affected rows and returns 409. We don't need a separate lock.

## Alternatives Considered

- **Clone with `replay_of`.** Preserves the original row verbatim, which is appealing for "immutable history" instincts. Rejected because every reporting query, every status check, and every dashboard would need to know about the chain. The history we actually want is already in `delivery_attempts` plus `admin_audit` — adding a `replay_of` linked list on `notifications` duplicates that information at the cost of making the primary table harder to query.
- **Allow `failed → pending` too.** Tempting for operator convenience ("just replay everything that didn't send"). Rejected because `failed` carries permanent-failure semantics by design — a malformed recipient address won't become well-formed on retry, and surfacing it as a replay-eligible state invites the operator to mash the button on a class of failures that needs a different fix (correct the input, re-create the notification). The status taxonomy only works if the boundaries are enforced.
- **Republish the existing outbox row instead of inserting a new one.** The old outbox row is `published_at IS NOT NULL` (that's how we got to `dead_letter` in the first place — the dispatcher gave up after N publishes). Resetting `published_at` on an existing row mixes the original publish history with the replay's, and the dispatcher's `WHERE published_at IS NULL` query becomes ambiguous. A fresh outbox row keeps each publish attempt as its own auditable record and matches how ADR-0009 reasons about the table.
