-- name: GetActiveAPIKeyByHash :one
SELECT * FROM api_keys
WHERE hashed_key = $1 AND revoked_at IS NULL;

-- name: ListActiveAPIKeysByPrefix :many
-- Auth fast-path: candidates whose stored prefix matches the request prefix.
-- The middleware then runs argon2.Verify on each candidate (typically 1).
SELECT * FROM api_keys
WHERE key_prefix = $1 AND revoked_at IS NULL;

-- name: InsertAPIKey :one
INSERT INTO api_keys (name, hashed_key, key_prefix, scopes)
VALUES (@name, @hashed_key, @key_prefix, @scopes)
RETURNING *;

-- name: UpsertAPIKey :one
-- Idempotent seed for the dev key: insert if absent, update if present.
INSERT INTO api_keys (name, hashed_key, key_prefix, scopes)
VALUES (@name, @hashed_key, @key_prefix, @scopes)
ON CONFLICT (hashed_key) DO UPDATE SET
    name = EXCLUDED.name,
    key_prefix = EXCLUDED.key_prefix,
    scopes = EXCLUDED.scopes,
    revoked_at = NULL
RETURNING *;

-- name: RevokeAPIKey :exec
UPDATE api_keys SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL;

-- name: HasDevSeedRow :one
-- Audit query: returns true when an active 'dev-seed' row exists. Used by
-- the startup guard (ADR-0013) to warn when a production deploy is running
-- against a database that still carries the dev key — even if DEV_API_KEY
-- is unset, the row remains valid until revoked.
SELECT EXISTS (
  SELECT 1 FROM api_keys
  WHERE name = 'dev-seed' AND revoked_at IS NULL
) AS present;
