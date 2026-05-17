-- Restore the broken uuid signature (matches migration 0012 as originally
-- shipped). Note: rolling this back reintroduces the bug — admin audit
-- inserts and VerifyAuditChain will 500 again.

DROP FUNCTION IF EXISTS admin_audit_canonical(bigint, text, uuid, text, text, jsonb, timestamptz);

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
