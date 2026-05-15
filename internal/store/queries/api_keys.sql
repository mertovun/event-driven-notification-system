-- name: GetActiveAPIKeyByHash :one
SELECT * FROM api_keys
WHERE hashed_key = $1 AND revoked_at IS NULL;

-- name: InsertAPIKey :one
INSERT INTO api_keys (name, hashed_key, scopes)
VALUES (@name, @hashed_key, @scopes)
RETURNING *;

-- name: RevokeAPIKey :exec
UPDATE api_keys SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL;
