ALTER TABLE notifications
    DROP CONSTRAINT IF EXISTS notifications_template_pair_consistent,
    DROP COLUMN IF EXISTS template_version,
    DROP COLUMN IF EXISTS template_id;

DROP TABLE IF EXISTS templates;
