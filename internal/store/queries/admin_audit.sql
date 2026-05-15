-- name: InsertAuditEntry :one
INSERT INTO admin_audit (actor, action, target_id, details)
VALUES (@actor, @action, @target_id, @details)
RETURNING *;

-- name: ListRecentAuditEntries :many
SELECT * FROM admin_audit
ORDER BY at DESC
LIMIT @page_limit::int;
