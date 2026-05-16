-- Per-key template ownership. Without this, any API key with the write
-- scope can PUT/DELETE any other key's templates — a stored-template-
-- injection vector across keys. Mirrors the ownership model on
-- notifications (migration 0006). Admin scope bypasses the gate.

ALTER TABLE templates
    ADD COLUMN created_by uuid NULL REFERENCES api_keys(id) ON DELETE SET NULL;

-- Index supports owner-scoped reads (admin uses the unscoped path).
CREATE INDEX templates_created_by_idx
    ON templates (created_by)
    WHERE created_by IS NOT NULL;
