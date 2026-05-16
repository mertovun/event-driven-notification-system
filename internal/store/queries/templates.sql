-- name: InsertTemplate :one
INSERT INTO templates (name, channel, version, body, required_vars, created_by)
VALUES (@name, @channel, @version, @body, @required_vars, @created_by)
RETURNING *;

-- name: GetTemplateByID :one
SELECT * FROM templates WHERE id = $1;

-- name: GetActiveTemplateByNameChannel :one
SELECT * FROM templates
WHERE name = $1 AND channel = $2 AND deprecated_at IS NULL
ORDER BY version DESC
LIMIT 1;

-- name: BumpTemplateVersion :one
-- Unscoped variant — admin-only path.
UPDATE templates
SET body = $2, required_vars = $3, version = version + 1, updated_at = now()
WHERE id = $1 AND deprecated_at IS NULL
RETURNING *;

-- name: BumpTemplateVersionScoped :one
-- Owner-scoped variant: rejects mutations on templates the caller doesn't own.
-- Returns 0 rows on either "not found" or "not owned"; handler maps to 404.
UPDATE templates
SET body = $2, required_vars = $3, version = version + 1, updated_at = now()
WHERE id = $1 AND created_by = $4 AND deprecated_at IS NULL
RETURNING *;

-- name: DeprecateTemplate :exec
-- Unscoped — admin-only path.
UPDATE templates SET deprecated_at = now() WHERE id = $1 AND deprecated_at IS NULL;

-- name: DeprecateTemplateScoped :execrows
-- Owner-scoped variant. Returns 0 affected on not-found / not-owned.
UPDATE templates SET deprecated_at = now()
WHERE id = $1 AND created_by = $2 AND deprecated_at IS NULL;

-- name: ListActiveTemplates :many
SELECT * FROM templates
WHERE deprecated_at IS NULL
ORDER BY created_at DESC, id DESC
LIMIT @page_limit::int;

-- name: ListActiveTemplatesScoped :many
-- Non-admin callers see only their own templates.
SELECT * FROM templates
WHERE deprecated_at IS NULL AND created_by = @created_by
ORDER BY created_at DESC, id DESC
LIMIT @page_limit::int;
