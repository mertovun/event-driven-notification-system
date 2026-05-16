-- name: InsertNotification :one
INSERT INTO notifications (
    id, batch_id, channel, recipient, content, priority, status,
    idempotency_key, scheduled_at, correlation_id,
    template_id, template_version, created_by
) VALUES (
    @id, @batch_id, @channel, @recipient, @content, @priority, @status,
    @idempotency_key, @scheduled_at, @correlation_id,
    @template_id, @template_version, @created_by
)
RETURNING *;

-- name: GetNotificationByID :one
SELECT * FROM notifications WHERE id = $1;

-- name: GetNotificationByIDScoped :one
-- Owner-scoped fetch — caller (handler) supplies the api_key id from auth context.
-- A NULL created_by row (pre-migration) is hidden from non-admin callers; the
-- handler short-circuits to admin scope when AuthedKey has admin scope, and
-- bypasses this query entirely.
SELECT * FROM notifications
WHERE id = $1 AND created_by = $2;

-- name: GetNotificationsByBatchID :many
SELECT * FROM notifications
WHERE batch_id = $1
ORDER BY created_at DESC, id DESC;

-- name: GetNotificationsByBatchIDScoped :many
SELECT * FROM notifications
WHERE batch_id = $1 AND created_by = $2
ORDER BY created_at DESC, id DESC;

-- name: CancelPendingOrQueued :one
-- CAS-style: only transitions pending or queued → cancelled.
-- Returns the updated row; 0 rows means someone else already moved it OR the
-- caller doesn't own it. Owner check is on (id, created_by) so a non-admin
-- can only cancel their own rows.
UPDATE notifications
SET status = 'cancelled', updated_at = now()
WHERE id = $1 AND status IN ('pending', 'queued', 'scheduled')
RETURNING *;

-- name: CancelPendingOrQueuedScoped :one
UPDATE notifications
SET status = 'cancelled', updated_at = now()
WHERE id = $1
  AND created_by = $2
  AND status IN ('pending', 'queued', 'scheduled')
RETURNING *;

-- name: MarkQueued :one
-- Outbox dispatcher transition: pending → queued after publish confirm.
UPDATE notifications
SET status = 'queued', updated_at = now()
WHERE id = $1 AND status = 'pending'
RETURNING *;

-- name: MarkSendingCAS :one
-- Worker pickup: pending OR queued → sending.
-- We accept both because the AMQP consumer can race the dispatcher's MarkQueued —
-- the worker may receive a delivery while the row is still 'pending'.
-- Returns 0 rows only if the row was already cancelled / past 'sending'.
UPDATE notifications
SET status = 'sending', updated_at = now()
WHERE id = $1 AND status IN ('pending', 'queued')
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

-- name: SweepStuckSending :many
-- Recovery path for rows orphaned by a crashed worker. The flow is:
--   MarkSendingCAS (pending|queued → sending) → worker crashes → row stays
--   sending forever → MarkSendingCAS rejects the redelivery (not in pending|
--   queued) → ack-and-drop → row stranded.
--
-- This sweeper reclaims rows that have been 'sending' longer than
-- stuck_seconds and returns them so the caller can write a fresh outbox row
-- (the original outbox row was already marked published when the worker
-- picked it up, so without a new row the dispatcher would never re-publish).
-- attempt_count increments so the maxAttempts cap still terminates a
-- poison message.
UPDATE notifications
SET status = 'queued',
    attempt_count = attempt_count + 1,
    last_error = COALESCE(last_error, '') || ' [sweep: stuck in sending]',
    updated_at = now()
WHERE status = 'sending'
  AND updated_at < now() - make_interval(secs => @stuck_seconds::int)
RETURNING *;

-- name: IncrementAttempt :exec
UPDATE notifications
SET attempt_count = attempt_count + 1, updated_at = now()
WHERE id = $1;
