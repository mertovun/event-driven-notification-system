-- Restore the original (over-strict) constraint. This breaks the
-- scheduled-notification delivery path again — included for completeness,
-- not for use.
ALTER TABLE notifications
    ADD CONSTRAINT notifications_scheduled_status_consistent
        CHECK (scheduled_at IS NULL OR status IN
               ('pending','queued','cancelled','scheduled'));
