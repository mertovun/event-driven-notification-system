-- Migration 0012 — make the audit verifier check content integrity, not
-- just linkage.
--
-- The verifier shipped in migration 0009 + 0011 only checks that each
-- row's prev_hash equals the previous row's row_hash. It never recomputes
-- row_hash from the row's contents. An attacker who edits the `details`
-- (or any other) column in place without touching row_hash passes
-- verification. That's *half* a tamper-evidence story: the chain proves
-- nobody deleted a row or reordered them, but not that any individual
-- row's contents are intact.
--
-- Fix has two parts:
--   1) Extract the canonical-string computation into a shared SQL
--      function. Trigger and verifier must agree byte-for-byte on the
--      string they hash; sharing the function makes that a compile-time
--      property of the schema, not a comment.
--   2) Rewrite the VerifyAuditChain query to recompute every row's hash
--      from canonical(row) || prev_hash and compare against the stored
--      row_hash. Now both linkage *and* content tampering produce
--      broken_links > 0.
--
-- Cost: VerifyAuditChain becomes O(n) digests instead of O(n) compares.
-- The verifier ticks every 5 minutes (internal/audit/verifier.go), and
-- admin_audit growth rate is bounded by admin-action frequency (low). On
-- a million-row table the recomputation is ~1 s of CPU — acceptable for
-- a tamper-evidence check that previously gave false confidence.
--
-- Threat model unchanged from migration 0011's "tamper-evident, not
-- tamper-proof": an attacker who can write to admin_audit and recompute
-- digests (DBA-level access, app role with UPDATE) can still rewrite the
-- chain end-to-end. Real tamper-proof needs HMAC-with-app-secret, a
-- separate append-only DB role, or external attestation. This migration
-- closes the "attacker forgot to update row_hash" oversight only.

CREATE OR REPLACE FUNCTION admin_audit_canonical(
    a_id        uuid,
    a_actor     text,
    a_actor_id  uuid,
    a_action    text,
    a_target_id text,
    a_details   jsonb,
    a_at        timestamptz
) RETURNS text AS $$
    SELECT COALESCE(a_id::text, '') ||
           '|' || COALESCE(a_actor, '') ||
           '|' || COALESCE(a_actor_id::text, '') ||
           '|' || COALESCE(a_action, '') ||
           '|' || COALESCE(a_target_id, '') ||
           '|' || COALESCE(a_details::text, '{}') ||
           '|' || COALESCE(a_at::text, '');
$$ LANGUAGE sql IMMUTABLE;

-- Rewrite the trigger to use the shared canonical function. Behavior
-- unchanged; the call out keeps trigger and verifier in lockstep.
CREATE OR REPLACE FUNCTION admin_audit_chain_hash() RETURNS trigger AS $$
DECLARE
    last_hash bytea;
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

    NEW.prev_hash := last_hash;
    NEW.row_hash  := digest(
        last_hash || admin_audit_canonical(
            NEW.id, NEW.actor, NEW.actor_id, NEW.action,
            NEW.target_id, NEW.details, NEW.at
        )::bytea,
        'sha256'
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
