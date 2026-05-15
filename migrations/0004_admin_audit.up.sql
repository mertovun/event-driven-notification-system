-- Admin audit log. See docs/13-websocket-and-admin.md Part B §11.

CREATE TABLE admin_audit (
    id          bigserial   PRIMARY KEY,
    actor       text        NOT NULL CHECK (length(actor) BETWEEN 1 AND 100),
    action      text        NOT NULL CHECK (action IN (
                                'dlq_replay',
                                'dlq_replay_bulk',
                                'dlq_replay_dryrun',
                                'dlq_purge'
                              )),
    target_id   text        NULL,
    details     jsonb       NOT NULL DEFAULT '{}'::jsonb,
    at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX admin_audit_at_idx       ON admin_audit (at DESC);
CREATE INDEX admin_audit_actor_at_idx ON admin_audit (actor, at DESC);
CREATE INDEX admin_audit_action_idx   ON admin_audit (action, at DESC);
