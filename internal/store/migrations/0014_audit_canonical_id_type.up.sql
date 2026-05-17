-- Migration 0014 — fix the admin_audit_canonical function's a_id argument
-- type. Migration 0012 declared it as `a_id uuid` (mirroring actor_id),
-- but admin_audit.id is bigserial — so the trigger's call
-- `admin_audit_canonical(NEW.id, ...)` failed with SQLSTATE 42883
-- ("function ... does not exist") at every INSERT.
--
-- Symptom: every admin action (dlq_replay, dlq_purge, key_revoke) and
-- every GET /v1/admin/audit/verify call returned 500 with
--   "function admin_audit_canonical(bigint, text, uuid, text, text,
--    jsonb, timestamp with time zone) does not exist"
-- Pre-0012 audit rows were unaffected because the trigger inlined the
-- canonical string then; the regression is in the shared-function
-- factoring 0012 introduced.
--
-- We drop and recreate the function with the correct bigint signature.
-- DROP is necessary because Postgres CREATE OR REPLACE FUNCTION won't
-- change parameter types.

DROP FUNCTION IF EXISTS admin_audit_canonical(uuid, text, uuid, text, text, jsonb, timestamptz);

CREATE OR REPLACE FUNCTION admin_audit_canonical(
    a_id        bigint,
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
