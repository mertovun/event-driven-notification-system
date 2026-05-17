-- Migration 0013 — align the notifications.content CHECK constraint with
-- the API-layer per-channel validation caps.
--
-- The original CHECK from migration 0001 limited content to 1..10000 chars
-- — conservative when first written, but the per-channel validation in
-- internal/api/validate.go was later widened: SMS=1000 chars,
-- push=4 KiB, email=256 KiB. A legitimate 64 KB email body now passes
-- the API gate and 23514s at INSERT, surfacing as an unmapped 500 to the
-- caller (and a stack-trace log line). Either the validation was wrong or
-- the CHECK was; the validation matches the actual product surface so
-- the CHECK gets the update.
--
-- Cap shape: 262144 (= 256 KiB exactly) matches emailMaxBytes. Channel-
-- specific tighter caps stay in the application layer where they can be
-- channel-aware; the DB constraint is the global safety net.
--
-- length() in Postgres on a text column counts codepoints, not bytes.
-- That's fine here because the API validation also uses len([]byte) which
-- gives a byte count; we'd reject before reaching the DB for the
-- pathological cases where the two diverge significantly. Operators can
-- audit "is anyone trying to push past this" via the validation rejection
-- counter rather than the constraint violation log.

ALTER TABLE notifications
    DROP CONSTRAINT IF EXISTS notifications_content_check;

ALTER TABLE notifications
    ADD CONSTRAINT notifications_content_check
    CHECK (length(content) BETWEEN 1 AND 262144);
