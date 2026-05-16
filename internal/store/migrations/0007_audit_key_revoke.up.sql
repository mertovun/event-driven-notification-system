-- Audit-log + revoke endpoint follow-ups from review:
--   1. `actor` (text) is the API-key NAME, which is non-unique and free-text.
--      Two keys can share a name; renames erase the link. Add an `actor_id`
--      uuid column that references api_keys.id so audit attribution is stable
--      even after key renames. We keep `actor` as the display name.
--   2. Extend the action check to allow `key_revoke` so the new admin
--      revocation endpoint can write an audit row.
--   3. Index actor_id for incident-response queries ("everything this key did").

ALTER TABLE admin_audit
    ADD COLUMN actor_id uuid NULL REFERENCES api_keys(id) ON DELETE SET NULL;

ALTER TABLE admin_audit
    DROP CONSTRAINT IF EXISTS admin_audit_action_check;

ALTER TABLE admin_audit
    ADD CONSTRAINT admin_audit_action_check CHECK (action IN (
        'dlq_replay',
        'dlq_replay_bulk',
        'dlq_replay_dryrun',
        'dlq_purge',
        'key_revoke'
    ));

CREATE INDEX admin_audit_actor_id_at_idx
    ON admin_audit (actor_id, at DESC)
    WHERE actor_id IS NOT NULL;
