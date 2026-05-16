DROP INDEX IF EXISTS admin_audit_actor_id_at_idx;
ALTER TABLE admin_audit DROP COLUMN IF EXISTS actor_id;

ALTER TABLE admin_audit DROP CONSTRAINT IF EXISTS admin_audit_action_check;
ALTER TABLE admin_audit
    ADD CONSTRAINT admin_audit_action_check CHECK (action IN (
        'dlq_replay',
        'dlq_replay_bulk',
        'dlq_replay_dryrun',
        'dlq_purge'
    ));
