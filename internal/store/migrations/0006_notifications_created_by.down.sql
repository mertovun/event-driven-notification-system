DROP INDEX IF EXISTS notifications_created_by_created_at_idx;
ALTER TABLE batches DROP COLUMN IF EXISTS created_by;
ALTER TABLE notifications DROP COLUMN IF EXISTS created_by;
