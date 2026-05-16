-- name: InsertScheduled :one
INSERT INTO scheduled_notifications (id, due_at)
VALUES (@id, @due_at)
RETURNING *;

-- name: ClaimDueScheduled :many
-- Scheduler poller: claim due rows whose claim is either fresh OR has aged
-- out (claim_ttl exceeded — original dispatcher crashed mid-transaction
-- before deleting the row). Without TTL reclamation a crashed scheduler
-- would permanently strand its claimed rows. Mirrors the outbox dispatcher's
-- claim-TTL pattern (see outbox.sql).
WITH claimed AS (
    SELECT id FROM scheduled_notifications
    WHERE due_at <= now()
      AND (
            claimed_at IS NULL
         OR claimed_at < now() - (@claim_ttl::int || ' seconds')::interval
          )
    ORDER BY due_at
    LIMIT @batch_size::int
    FOR UPDATE SKIP LOCKED
)
UPDATE scheduled_notifications
SET claimed_at = now(), claimed_by = @claimed_by
WHERE id IN (SELECT id FROM claimed)
RETURNING *;

-- name: MarkScheduledPending :one
-- Scheduler transition: scheduled → pending. CAS-style so a row that was
-- cancelled in the meantime (or already transitioned) is left untouched
-- and the caller can detect the no-op via 0 rows.
UPDATE notifications
SET status = 'pending', updated_at = now()
WHERE id = $1 AND status = 'scheduled'
RETURNING *;

-- name: DeleteScheduled :exec
DELETE FROM scheduled_notifications WHERE id = $1;
