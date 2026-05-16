-- Tamper-evident audit log via a hash chain. The app role can still UPDATE
-- or DELETE rows (we don't role-separate in this Postgres-only deployment),
-- but any after-the-fact mutation is detectable: each row's hash includes
-- the previous row's hash, so editing row N requires recomputing every hash
-- from N onward — and the verifier knows the expected chain head.
--
-- Compute model:
--   row_hash = sha256(prev_hash || canonical(row_content))
--   canonical(row) = bytes of (id || actor || COALESCE(actor_id::text,'') ||
--                              action || COALESCE(target_id,'') || details::text ||
--                              at::text)
-- where 'prev_hash' is the previous row's row_hash (zeroes for the first row).
--
-- A BEFORE INSERT trigger reads the last committed row's hash, computes the
-- new row's hash, and writes both into the new row. We use pgcrypto's
-- digest() for sha256.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

ALTER TABLE admin_audit
    ADD COLUMN prev_hash bytea NULL,
    ADD COLUMN row_hash  bytea NULL;

-- Trigger function: reads last row's hash, computes this row's hash, writes both.
CREATE OR REPLACE FUNCTION admin_audit_chain_hash() RETURNS trigger AS $$
DECLARE
    last_hash bytea;
    canonical text;
BEGIN
    SELECT row_hash INTO last_hash
    FROM admin_audit
    WHERE id < NEW.id
    ORDER BY id DESC
    LIMIT 1;
    IF last_hash IS NULL THEN
        last_hash := decode('00000000000000000000000000000000', 'hex'); -- genesis
    END IF;

    canonical := COALESCE(NEW.id::text, '') ||
                 '|' || COALESCE(NEW.actor, '') ||
                 '|' || COALESCE(NEW.actor_id::text, '') ||
                 '|' || COALESCE(NEW.action, '') ||
                 '|' || COALESCE(NEW.target_id, '') ||
                 '|' || COALESCE(NEW.details::text, '{}') ||
                 '|' || COALESCE(NEW.at::text, '');

    NEW.prev_hash := last_hash;
    NEW.row_hash  := digest(last_hash || canonical::bytea, 'sha256');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER admin_audit_chain_hash_trigger
    BEFORE INSERT ON admin_audit
    FOR EACH ROW
    EXECUTE FUNCTION admin_audit_chain_hash();
