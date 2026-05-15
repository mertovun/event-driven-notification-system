-- Template system. See docs/12-template-system.md.

CREATE TABLE templates (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    name            text        NOT NULL CHECK (length(name) BETWEEN 1 AND 100),
    channel         text        NOT NULL CHECK (channel IN ('sms','email','push')),
    version         int         NOT NULL DEFAULT 1 CHECK (version >= 1),
    body            text        NOT NULL CHECK (length(body) BETWEEN 1 AND 32768),
    required_vars   text[]      NOT NULL DEFAULT '{}'::text[],
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    deprecated_at   timestamptz,

    UNIQUE (name, channel, version)
);

-- Active templates by (name, channel) — operator's mental handle.
CREATE INDEX templates_name_channel_active_idx
    ON templates (name, channel)
    WHERE deprecated_at IS NULL;

-- Add template_id / template_version columns to notifications for audit/replay.
ALTER TABLE notifications
    ADD COLUMN template_id      uuid        REFERENCES templates(id) ON DELETE SET NULL,
    ADD COLUMN template_version int         CHECK (template_version IS NULL OR template_version >= 1);

-- Both columns present together, or both null. Enforces "templated request → both stamped".
ALTER TABLE notifications
    ADD CONSTRAINT notifications_template_pair_consistent
        CHECK ((template_id IS NULL) = (template_version IS NULL));
