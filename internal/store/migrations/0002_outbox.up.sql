-- Transactional outbox. See docs/11-transactional-outbox.md.

CREATE TABLE outbox (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    notification_id uuid        NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
    routing_key     text        NOT NULL,
    payload         jsonb       NOT NULL,
    headers         jsonb       NOT NULL DEFAULT '{}'::jsonb,
    priority        smallint    NOT NULL CHECK (priority IN (1, 5, 9)),
    created_at      timestamptz NOT NULL DEFAULT now(),
    claimed_at      timestamptz,
    claimed_by      text,
    published_at    timestamptz,
    attempt_count   int         NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_error      text,

    -- claimed_at/claimed_by go together: both set or both null.
    CONSTRAINT outbox_claim_consistent
        CHECK ((claimed_at IS NULL) = (claimed_by IS NULL))
);

-- Hot path: dispatcher polls unpublished rows oldest-first.
CREATE INDEX outbox_unpublished_idx
    ON outbox (created_at)
    WHERE published_at IS NULL;

-- Stuck-claim recovery for rows a dead dispatcher left half-processed.
CREATE INDEX outbox_stuck_claim_idx
    ON outbox (claimed_at)
    WHERE published_at IS NULL
      AND claimed_at IS NOT NULL;
