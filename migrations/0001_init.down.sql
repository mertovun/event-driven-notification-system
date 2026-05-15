-- Reverse of 0001_init.up.sql. Drop in reverse dependency order.

DROP TABLE IF EXISTS scheduled_notifications;
DROP TABLE IF EXISTS api_keys;
DROP TABLE IF EXISTS dead_letters;
DROP TABLE IF EXISTS delivery_attempts;
DROP TABLE IF EXISTS notifications;
DROP TABLE IF EXISTS batches;

-- Extensions: leave pgcrypto in place. Other migrations or future tables may rely on it.
