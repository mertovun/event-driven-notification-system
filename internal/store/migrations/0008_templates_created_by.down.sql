DROP INDEX IF EXISTS templates_created_by_idx;
ALTER TABLE templates DROP COLUMN IF EXISTS created_by;
