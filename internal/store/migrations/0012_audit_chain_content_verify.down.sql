-- Restore the migration 0011 trigger body (without the shared canonical
-- function call) and drop the function. Reverts the content-integrity
-- verification path back to linkage-only.

CREATE OR REPLACE FUNCTION admin_audit_chain_hash() RETURNS trigger AS $$
DECLARE
    last_hash bytea;
    canonical text;
BEGIN
    PERFORM pg_advisory_xact_lock('admin_audit'::regclass::oid::bigint);

    SELECT row_hash INTO last_hash
    FROM admin_audit
    WHERE id < NEW.id
    ORDER BY id DESC
    LIMIT 1;
    IF last_hash IS NULL THEN
        last_hash := decode('00000000000000000000000000000000', 'hex');
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

DROP FUNCTION IF EXISTS admin_audit_canonical(uuid, text, uuid, text, text, jsonb, timestamptz);
