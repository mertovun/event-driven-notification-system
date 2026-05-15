-- name: InsertTemplate :one
INSERT INTO templates (name, channel, version, body, required_vars)
VALUES (@name, @channel, @version, @body, @required_vars)
RETURNING *;

-- name: GetTemplateByID :one
SELECT * FROM templates WHERE id = $1;

-- name: GetActiveTemplateByNameChannel :one
SELECT * FROM templates
WHERE name = $1 AND channel = $2 AND deprecated_at IS NULL
ORDER BY version DESC
LIMIT 1;

-- name: BumpTemplateVersion :one
UPDATE templates
SET body = $2, required_vars = $3, version = version + 1, updated_at = now()
WHERE id = $1 AND deprecated_at IS NULL
RETURNING *;

-- name: DeprecateTemplate :exec
UPDATE templates SET deprecated_at = now() WHERE id = $1 AND deprecated_at IS NULL;

-- name: ListActiveTemplates :many
SELECT * FROM templates
WHERE deprecated_at IS NULL
ORDER BY created_at DESC, id DESC
LIMIT @page_limit::int;
