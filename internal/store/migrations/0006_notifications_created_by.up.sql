-- Per-key ownership scoping. Without this, any API key with `notifications:read`
-- can list and fetch every other key's notifications (PII leak waiting to
-- happen the moment a second tenant key exists). The reads in
-- internal/api/notifications.go are gated by this column from this migration on.
--
-- Backfill strategy for existing rows: NULL. The read handlers treat NULL as
-- "legacy / unscoped" and surface it only to admin-scope keys. New writes are
-- always populated.
--
-- Notes:
--   * api_keys.id is the right key (UUID, immutable). name is free-text and
--     not unique — see ADR-0021 / audit-log review for the same critique.
--   * SET NULL on delete: if a key is hard-deleted, the rows survive as
--     "ownerless" rather than cascading the data loss. Operators should
--     prefer revoke (revoked_at) over delete.

ALTER TABLE notifications
    ADD COLUMN created_by uuid NULL REFERENCES api_keys(id) ON DELETE SET NULL;

ALTER TABLE batches
    ADD COLUMN created_by uuid NULL REFERENCES api_keys(id) ON DELETE SET NULL;

-- Index supports list-by-owner queries on notifications. Composite with
-- created_at for the same keyset pagination shape as the unscoped index.
CREATE INDEX notifications_created_by_created_at_idx
    ON notifications (created_by, created_at DESC, id DESC)
    WHERE created_by IS NOT NULL;
