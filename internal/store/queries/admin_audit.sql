-- name: InsertAuditEntry :one
INSERT INTO admin_audit (actor, actor_id, action, target_id, details)
VALUES (@actor, @actor_id, @action, @target_id, @details)
RETURNING *;

-- name: ListRecentAuditEntries :many
SELECT * FROM admin_audit
ORDER BY at DESC
LIMIT @page_limit::int;

-- name: VerifyAuditChain :one
-- Tamper-evidence check: returns the count of broken links — rows whose
-- prev_hash doesn't equal the previous row's row_hash. Zero means the chain
-- is intact. Any non-zero result is evidence that a row was edited or
-- deleted after the fact. Operators page on this.
WITH chain AS (
  SELECT id, row_hash, prev_hash,
         LAG(row_hash) OVER (ORDER BY id) AS expected_prev
  FROM admin_audit
)
SELECT COUNT(*)::bigint AS broken_links
FROM chain
WHERE id > (SELECT MIN(id) FROM admin_audit)
  AND prev_hash IS DISTINCT FROM expected_prev;
