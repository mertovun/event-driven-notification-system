-- name: InsertScheduled :one
INSERT INTO scheduled_notifications (id, due_at)
VALUES (@id, @due_at)
RETURNING *;

-- name: ClaimDueScheduled :many
-- Scheduler poller: claim due, unclaimed rows.
WITH claimed AS (
    SELECT id FROM scheduled_notifications
    WHERE due_at <= now() AND claimed_at IS NULL
    ORDER BY due_at
    LIMIT @batch_size::int
    FOR UPDATE SKIP LOCKED
)
UPDATE scheduled_notifications
SET claimed_at = now(), claimed_by = @claimed_by
WHERE id IN (SELECT id FROM claimed)
RETURNING *;

-- name: DeleteScheduled :exec
DELETE FROM scheduled_notifications WHERE id = $1;
