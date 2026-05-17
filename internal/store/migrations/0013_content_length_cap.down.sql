-- Restore the original 10000-char ceiling. Note: if any rows have been
-- inserted with content longer than 10000 chars since migration 0013
-- went up, this rollback fails — manually truncate or fork the offending
-- rows before reverting.

ALTER TABLE notifications
    DROP CONSTRAINT IF EXISTS notifications_content_check;

ALTER TABLE notifications
    ADD CONSTRAINT notifications_content_check
    CHECK (length(content) BETWEEN 1 AND 10000);
