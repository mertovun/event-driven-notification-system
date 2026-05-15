-- name: InsertDeadLetter :one
INSERT INTO dead_letters (notification_id, reason, payload)
VALUES (@notification_id, @reason, @payload)
ON CONFLICT (notification_id) DO NOTHING
RETURNING *;

-- name: GetDeadLetterByNotification :one
SELECT * FROM dead_letters WHERE notification_id = $1;

-- name: ListDeadLetters :many
SELECT * FROM dead_letters
WHERE (@channel_filter::text = '' OR notification_id IN (
    SELECT id FROM notifications WHERE channel = @channel_filter
  ))
ORDER BY dlq_at DESC
LIMIT @page_limit::int;

-- name: DeleteDeadLetter :exec
DELETE FROM dead_letters WHERE notification_id = $1;
