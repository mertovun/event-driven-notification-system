DROP INDEX IF EXISTS api_keys_prefix_active_idx;
ALTER TABLE api_keys DROP COLUMN IF EXISTS key_prefix;
