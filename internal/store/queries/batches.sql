-- name: InsertBatch :one
INSERT INTO batches (
    id, total_count, accepted_count, rejected_count, idempotency_key, status
) VALUES (
    @id, @total_count, @accepted_count, @rejected_count, @idempotency_key, @status
)
RETURNING *;

-- name: GetBatchByID :one
SELECT * FROM batches WHERE id = $1;
