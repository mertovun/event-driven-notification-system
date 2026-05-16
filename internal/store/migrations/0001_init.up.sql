-- Initial schema: notifications + batches + delivery_attempts + dead_letters
-- + api_keys + scheduled_notifications, with indexes for the API's hot paths.

-- ---- Extensions ------------------------------------------------------------

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ---- batches ---------------------------------------------------------------

CREATE TABLE batches (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    total_count     int         NOT NULL CHECK (total_count >= 0),
    accepted_count  int         NOT NULL DEFAULT 0 CHECK (accepted_count >= 0),
    rejected_count  int         NOT NULL DEFAULT 0 CHECK (rejected_count >= 0),
    idempotency_key text        NULL,
    status          text        NOT NULL DEFAULT 'accepted'
                                CHECK (status IN ('accepted','partial','rejected','completed')),
    created_at      timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT batches_counts_consistent
        CHECK (accepted_count + rejected_count <= total_count)
);

-- ---- notifications ---------------------------------------------------------

CREATE TABLE notifications (
    id               uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id         uuid        NULL REFERENCES batches(id) ON DELETE SET NULL,
    channel          text        NOT NULL CHECK (channel IN ('sms','email','push')),
    recipient        text        NOT NULL CHECK (length(recipient) BETWEEN 1 AND 320),
    content          text        NOT NULL CHECK (length(content) BETWEEN 1 AND 10000),
    priority         smallint    NOT NULL DEFAULT 5 CHECK (priority IN (1, 5, 9)),
    status           text        NOT NULL DEFAULT 'pending'
                                  CHECK (status IN (
                                      'pending','queued','sending',
                                      'sent','failed','cancelled','dead_letter','scheduled'
                                  )),
    idempotency_key  text        NULL,
    attempt_count    int         NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_error       text        NULL,
    scheduled_at     timestamptz NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    sent_at          timestamptz NULL,
    correlation_id   text        NOT NULL,

    -- sent_at exists iff status='sent'
    CONSTRAINT notifications_sent_at_only_when_sent
        CHECK ((status = 'sent') = (sent_at IS NOT NULL)),
    -- scheduled_at only meaningful pre-send
    CONSTRAINT notifications_scheduled_status_consistent
        CHECK (scheduled_at IS NULL OR status IN ('pending','queued','cancelled','scheduled'))
);

-- Partial unique on idempotency_key (audit + DR fallback if Redis is lost).
CREATE UNIQUE INDEX notifications_idempotency_key_uniq
    ON notifications (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Listing by status, newest first (admin dashboard, ops queries).
CREATE INDEX notifications_status_created_at_idx
    ON notifications (status, created_at DESC);

-- Batch detail page: list all notifications in a batch.
CREATE INDEX notifications_batch_id_created_at_idx
    ON notifications (batch_id, created_at DESC)
    WHERE batch_id IS NOT NULL;

-- Keyset pagination cursor on (created_at DESC, id DESC).
CREATE INDEX notifications_created_at_id_idx
    ON notifications (created_at DESC, id DESC);

-- Channel + status filter combination for the filtered list endpoint.
CREATE INDEX notifications_channel_status_created_at_idx
    ON notifications (channel, status, created_at DESC);

-- Correlation-id lookup (debugging by request id).
CREATE INDEX notifications_correlation_id_idx
    ON notifications (correlation_id);

-- ---- delivery_attempts -----------------------------------------------------

CREATE TABLE delivery_attempts (
    id                  bigserial   PRIMARY KEY,
    notification_id     uuid        NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
    attempt_number      int         NOT NULL CHECK (attempt_number >= 1),
    started_at          timestamptz NOT NULL DEFAULT now(),
    completed_at        timestamptz NULL,
    success             bool        NOT NULL DEFAULT false,
    provider_message_id text        NULL,
    http_status         int         NULL CHECK (http_status IS NULL OR http_status BETWEEN 100 AND 599),
    error               text        NULL,
    response_body       text        NULL,

    CONSTRAINT delivery_attempts_unique_per_notification
        UNIQUE (notification_id, attempt_number),
    CONSTRAINT delivery_attempts_completion_consistent
        CHECK ((completed_at IS NULL) = (success = false AND error IS NULL AND http_status IS NULL))
);

-- Backward index scans on the UNIQUE (notification_id, attempt_number) index
-- handle ORDER BY attempt_number DESC at no extra cost — no separate index needed.

-- ---- dead_letters ----------------------------------------------------------

CREATE TABLE dead_letters (
    id              bigserial   PRIMARY KEY,
    notification_id uuid        NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
    reason          text        NOT NULL CHECK (length(reason) BETWEEN 1 AND 500),
    payload         jsonb       NOT NULL,
    dlq_at          timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT dead_letters_one_per_notification UNIQUE (notification_id)
);

-- Recent dead letters, newest first (admin DLQ listing).
CREATE INDEX dead_letters_dlq_at_idx
    ON dead_letters (dlq_at DESC);

-- ---- api_keys --------------------------------------------------------------

CREATE TABLE api_keys (
    id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    name        text        NOT NULL CHECK (length(name) BETWEEN 1 AND 100),
    hashed_key  text        NOT NULL UNIQUE,
    scopes      text[]      NOT NULL DEFAULT '{}'::text[],
    created_at  timestamptz NOT NULL DEFAULT now(),
    revoked_at  timestamptz NULL,

    CONSTRAINT api_keys_revoked_after_created
        CHECK (revoked_at IS NULL OR revoked_at >= created_at)
);

-- Active-keys lookup partial index for the auth hot path.
CREATE INDEX api_keys_active_idx
    ON api_keys (hashed_key)
    WHERE revoked_at IS NULL;

-- ---- scheduled_notifications -----------------------------------------------

CREATE TABLE scheduled_notifications (
    id          uuid        PRIMARY KEY
                            REFERENCES notifications(id) ON DELETE CASCADE,
    due_at      timestamptz NOT NULL,
    claimed_at  timestamptz NULL,
    claimed_by  text        NULL,

    CONSTRAINT scheduled_claim_consistent
        CHECK ((claimed_at IS NULL) = (claimed_by IS NULL))
);

-- Due-unclaimed partial index for the scheduler poller's hot path.
CREATE INDEX scheduled_notifications_due_unclaimed_idx
    ON scheduled_notifications (due_at)
    WHERE claimed_at IS NULL;
