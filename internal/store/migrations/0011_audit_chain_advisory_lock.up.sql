-- Serialize concurrent admin_audit INSERTs at the trigger so the hash chain
-- can't fork. The original BEFORE INSERT trigger (migration 0009) reads the
-- previous row's row_hash with a plain SELECT; two concurrent inserts in two
-- TXs both observe the same last_hash and both compute their row_hash chained
-- off it. The second row's prev_hash matches the first row's *prev_hash*,
-- not the first row's *row_hash* — a permanent broken link, no actual
-- tampering. VerifyAuditChain then flags it on the next run.
--
-- Fix: take a transaction-scoped advisory lock keyed to admin_audit's regclass
-- OID. The lock is released automatically at COMMIT or ROLLBACK; concurrent
-- inserters serialize cleanly through the trigger. Admin volume is low (one
-- insert per admin action) so contention is negligible — and the lock only
-- covers the SELECT-then-INSERT critical section, not the surrounding TX.
--
-- Why advisory lock and not SERIALIZABLE / SELECT ... FOR UPDATE:
--   - SERIALIZABLE forces every admin-touching TX to opt in, leaks isolation
--     concerns up into handlers, and risks 40001 retries on unrelated writes
--     in the same TX.
--   - SELECT ... FOR UPDATE needs a row to lock — there's no "tail" row to
--     point at when the table is empty, and even with one, every admin action
--     would block on it serially, which is what advisory_xact_lock does
--     without the locator gymnastics.
--   - pg_advisory_xact_lock(<table-keyed bigint>) is the textbook fix for
--     "serialize a hot path that doesn't map cleanly to a row".

CREATE OR REPLACE FUNCTION admin_audit_chain_hash() RETURNS trigger AS $$
DECLARE
    last_hash bytea;
    canonical text;
BEGIN
    -- Hold a per-table advisory lock for the rest of this TX. Released at
    -- COMMIT/ROLLBACK. Concurrent INSERTs serialize here.
    PERFORM pg_advisory_xact_lock('admin_audit'::regclass::oid::bigint);

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
