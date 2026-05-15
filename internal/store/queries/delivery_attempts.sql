-- name: InsertDeliveryAttempt :one
INSERT INTO delivery_attempts (
    notification_id, attempt_number, started_at, completed_at, success,
    provider_message_id, http_status, error, response_body
) VALUES (
    @notification_id, @attempt_number, @started_at, @completed_at, @success,
    @provider_message_id, @http_status, @error, @response_body
)
RETURNING *;

-- name: GetDeliveryAttemptsByNotification :many
SELECT * FROM delivery_attempts
WHERE notification_id = $1
ORDER BY attempt_number DESC;
