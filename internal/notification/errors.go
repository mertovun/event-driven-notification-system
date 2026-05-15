package notification

import "errors"

// Sentinel errors for the notification bounded context.
// Adapters at the HTTP and queue boundaries translate these into status codes and DLQ decisions.
var (
	// ErrNotFound — no notification with the given id.
	ErrNotFound = errors.New("notification: not found")

	// ErrIdempotencyConflict — the same Idempotency-Key was reused with a different body.
	ErrIdempotencyConflict = errors.New("notification: idempotency key conflict")

	// ErrIdempotencyInFlight — a request with this key is still being processed.
	ErrIdempotencyInFlight = errors.New("notification: idempotency request in flight")

	// ErrInvalidState — the requested transition is not allowed from the current status.
	ErrInvalidState = errors.New("notification: invalid state transition")

	// ErrValidation — input failed validation (recipient format, content length, etc.).
	// Adapters wrap this with field-level detail.
	ErrValidation = errors.New("notification: validation failed")

	// ErrUnknownChannel — channel is not one of sms/email/push.
	ErrUnknownChannel = errors.New("notification: unknown channel")

	// ErrTemplateNotFound — referenced template id does not exist or is deprecated.
	ErrTemplateNotFound = errors.New("notification: template not found")

	// ErrTemplateRender — template render failed (missing var, parse error, length exceeded).
	ErrTemplateRender = errors.New("notification: template render failed")

	// ErrRateLimited — the caller exceeded the per-API-key rate limit.
	ErrRateLimited = errors.New("notification: rate limited")
)
