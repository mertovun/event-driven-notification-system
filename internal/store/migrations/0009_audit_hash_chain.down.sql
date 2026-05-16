DROP TRIGGER IF EXISTS admin_audit_chain_hash_trigger ON admin_audit;
DROP FUNCTION IF EXISTS admin_audit_chain_hash();
ALTER TABLE admin_audit DROP COLUMN IF EXISTS row_hash;
ALTER TABLE admin_audit DROP COLUMN IF EXISTS prev_hash;
-- Leave pgcrypto extension alone — other code may rely on it.
