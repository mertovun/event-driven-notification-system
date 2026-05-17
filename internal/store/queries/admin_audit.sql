-- name: InsertAuditEntry :one
INSERT INTO admin_audit (actor, actor_id, action, target_id, details)
VALUES (@actor, @actor_id, @action, @target_id, @details)
RETURNING *;

-- name: ListRecentAuditEntries :many
SELECT * FROM admin_audit
ORDER BY at DESC
LIMIT @page_limit::int;

-- name: VerifyAuditChain :one
-- Tamper-evidence check. Returns the count of broken links — rows that
-- fail either of two checks:
--   1) Linkage: prev_hash must equal the previous row's row_hash.
--   2) Content: row_hash must equal sha256(prev_hash || canonical(row)),
--      where canonical() comes from the shared SQL function so trigger
--      and verifier compute the same bytes. Migration 0012 added this;
--      previously the verifier only checked (1) and an attacker who
--      edited a column in place without touching row_hash passed.
-- Zero means the chain is intact. Any non-zero result is evidence that a
-- row was edited, deleted, or had its hash columns desynchronized from
-- its content after the fact. Operators page on this.
WITH chain AS (
  SELECT
    a.id,
    a.row_hash,
    a.prev_hash,
    LAG(a.row_hash) OVER (ORDER BY a.id) AS expected_prev,
    -- Recompute the expected row_hash from canonical(row) || prev_hash.
    -- COALESCE on prev_hash handles the genesis row (first row's
    -- prev_hash is genesis zero-bytes; we treat NULL the same way for
    -- defense). The cast to bytea + sha256 mirrors the trigger.
    digest(
      COALESCE(a.prev_hash, decode('00000000000000000000000000000000', 'hex'))
        || admin_audit_canonical(
             a.id, a.actor, a.actor_id, a.action,
             a.target_id, a.details, a.at
           )::bytea,
      'sha256'
    ) AS expected_row_hash
  FROM admin_audit a
)
SELECT COUNT(*)::bigint AS broken_links
FROM chain
WHERE (id > (SELECT MIN(id) FROM admin_audit)
       AND prev_hash IS DISTINCT FROM expected_prev)
   OR row_hash IS DISTINCT FROM expected_row_hash;
