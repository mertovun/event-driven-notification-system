-- The original CHECK was too strict:
--
--   CHECK (scheduled_at IS NULL OR status IN
--          ('pending','queued','cancelled','scheduled'))
--
-- It prohibited rows from ever reaching 'sending', 'sent', 'failed', or
-- 'dead_letter' if scheduled_at was non-NULL. The intent was "scheduled_at
-- only applies pre-send," but rows that were scheduled should keep their
-- scheduled_at for audit/history after delivery — otherwise we lose the
-- "when was this supposed to go out?" data.
--
-- More importantly, the constraint fired on every UPDATE that transitioned
-- a scheduled row past pre-send. When the worker tried MarkSendingCAS
-- (queued → sending) on a scheduled-then-due row, the constraint raised
--   "violates check constraint notifications_scheduled_status_consistent"
-- and the error propagated up the worker pipeline. Without the safety-net
-- error log at the boundary (added in the silent-hot-loop fix), this
-- surfaced as an apparent worker hang.
--
-- Drop the constraint. The API validator already rejects scheduled_at on
-- terminal-state rows at create time; the constraint wasn't load-bearing.

ALTER TABLE notifications
    DROP CONSTRAINT IF EXISTS notifications_scheduled_status_consistent;
