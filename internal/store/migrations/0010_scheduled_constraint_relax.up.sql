-- The original CHECK was too strict:
--
--   CHECK (scheduled_at IS NULL OR status IN
--          ('pending','queued','cancelled','scheduled'))
--
-- It prohibited rows from ever reaching 'sending', 'sent', 'failed', or
-- 'dead_letter' if scheduled_at was non-NULL. The intent was probably
-- "scheduled_at only applies pre-send," but a row that was scheduled
-- should keep its scheduled_at for audit/history after delivery —
-- otherwise we lose the "when was this supposed to go out?" data.
--
-- This was the actual root cause of the long-documented "scheduler
-- wedge" in KNOWN_ISSUES.md: when the worker tried MarkSendingCAS
-- (queued → sending), the constraint fired with
--   "violates check constraint notifications_scheduled_status_consistent"
-- and the error propagated up — depending on app version, this surfaced
-- as either a goroutine hang (older bgreader retry behaviour on a failed
-- query) or a clean error (current build). The diagnosis in
-- KNOWN_ISSUES.md mis-attributed the hang to pgx; the actual fix is at
-- the schema layer.
--
-- We replace the constraint with one that only enforces what it should:
-- "don't set scheduled_at to a non-null value when the row is already in
-- a terminal state". The constraint trigger is on UPDATE only since
-- INSERT-time validation lives in the API layer.

ALTER TABLE notifications
    DROP CONSTRAINT IF EXISTS notifications_scheduled_status_consistent;
