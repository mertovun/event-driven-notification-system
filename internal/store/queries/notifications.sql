-- name: InsertNotification :one
INSERT INTO notifications (
    id, batch_id, channel, recipient, content, priority, status,
    idempotency_key, scheduled_at, correlation_id,
    template_id, template_version
) VALUES (
    @id, @batch_id, @channel, @recipient, @content, @priority, @status,
    @idempotency_key, @scheduled_at, @correlation_id,
    @template_id, @template_version
)
RETURNING *;

-- name: GetNotificationByID :one
SELECT * FROM notifications WHERE id = $1;

-- name: GetNotificationsByBatchID :many
SELECT * FROM notifications
WHERE batch_id = $1
ORDER BY created_at DESC, id DESC;

-- name: CancelPendingOrQueued :one
-- CAS-style: only transitions pending or queued → cancelled.
-- Returns the updated row; 0 rows means someone else already moved it.
UPDATE notifications
SET status = 'cancelled', updated_at = now()
WHERE id = $1 AND status IN ('pending', 'queued', 'scheduled')
RETURNING *;

-- name: MarkQueued :one
-- Outbox dispatcher transition: pending → queued after publish confirm.
UPDATE notifications
SET status = 'queued', updated_at = now()
WHERE id = $1 AND status = 'pending'
RETURNING *;

-- name: MarkSendingCAS :one
-- Worker pickup: queued → sending. Returns 0 rows if losing the race (cancelled meanwhile).
UPDATE notifications
SET status = 'sending', updated_at = now()
WHERE id = $1 AND status = 'queued'
RETURNING *;

-- name: MarkSent :one
UPDATE notifications
SET status = 'sent', sent_at = now(), updated_at = now()
WHERE id = $1 AND status = 'sending'
RETURNING *;

-- name: MarkFailed :one
UPDATE notifications
SET status = 'failed', last_error = $2, updated_at = now()
WHERE id = $1 AND status = 'sending'
RETURNING *;

-- name: MarkDeadLetter :one
UPDATE notifications
SET status = 'dead_letter', last_error = $2, updated_at = now()
WHERE id = $1
RETURNING *;

-- name: RevertToQueued :exec
-- Worker retry path: sending → queued (next attempt re-publishes).
UPDATE notifications
SET status = 'queued', attempt_count = attempt_count + 1,
    last_error = $2, updated_at = now()
WHERE id = $1 AND status = 'sending';

-- name: IncrementAttempt :exec
UPDATE notifications
SET attempt_count = attempt_count + 1, updated_at = now()
WHERE id = $1;
