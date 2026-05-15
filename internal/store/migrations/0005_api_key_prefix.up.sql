-- Add a non-secret prefix to api_keys for fast candidate lookup before argon2 verify.
-- See docs/05-security-and-networking.md §1.

ALTER TABLE api_keys
    ADD COLUMN key_prefix text NOT NULL DEFAULT '' CHECK (length(key_prefix) BETWEEN 0 AND 16);

-- Active-keys-by-prefix lookup. Auth middleware uses this to narrow candidates
-- before running argon2 verify (which is intentionally slow).
CREATE INDEX api_keys_prefix_active_idx
    ON api_keys (key_prefix)
    WHERE revoked_at IS NULL;
