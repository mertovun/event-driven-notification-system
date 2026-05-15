-- name: InsertOutbox :one
INSERT INTO outbox (
    notification_id, routing_key, payload, headers, priority
) VALUES (
    @notification_id, @routing_key, @payload, @headers, @priority
)
RETURNING *;

-- name: ClaimOutboxBatch :many
-- Atomic claim with FOR UPDATE SKIP LOCKED. Reclaims rows whose previous
-- claim has expired (claimed dispatcher died). Limit governs batch size.
WITH claimed AS (
    SELECT id FROM outbox
    WHERE published_at IS NULL
      AND (claimed_at IS NULL OR claimed_at < now() - make_interval(secs => @claim_ttl_seconds::int))
    ORDER BY created_at
    LIMIT @batch_size::int
    FOR UPDATE SKIP LOCKED
)
UPDATE outbox SET claimed_at = now(), claimed_by = @claimed_by
WHERE id IN (SELECT id FROM claimed)
RETURNING *;

-- name: MarkOutboxPublished :exec
UPDATE outbox SET published_at = now()
WHERE id = $1;

-- name: MarkOutboxFailed :exec
UPDATE outbox
SET attempt_count = attempt_count + 1, last_error = $2, claimed_at = NULL, claimed_by = NULL
WHERE id = $1;

-- name: DeletePublishedOutboxBefore :exec
DELETE FROM outbox WHERE published_at IS NOT NULL AND published_at < $1;

-- name: OutboxLagSeconds :one
SELECT COALESCE(EXTRACT(EPOCH FROM (now() - MIN(created_at))), 0)::double precision AS lag_seconds
FROM outbox
WHERE published_at IS NULL;
