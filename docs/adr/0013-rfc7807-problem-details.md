# ADR-0013: RFC 7807 Problem Details for HTTP error responses

## Status

Accepted (2026-05-16)

## Context

Every error response from the API needs a consistent shape: machine-readable enough for clients to branch on without parsing English, human-readable enough for an on-call operator to debug from a support ticket, and predictable enough that we can centralize the error-to-HTTP mapping in one place instead of letting handlers each invent their own envelope. We have validation failures, idempotency conflicts (ADR-0006), rate-limit rejections (ADR-0004), template-render errors (ADR-0010), and the usual not-found / invalid-state / timeout family. Without a single contract these accumulate as ad-hoc JSON shapes, each handler picking its own field names, and clients end up writing a switch over response bodies that breaks the next time someone adds an endpoint.

We need one envelope, one mapper, and one rule for handlers: return errors, do not write status codes.

## Decision

Adopt **RFC 7807 Problem Details** (`application/problem+json`) as the error contract for every non-2xx response.

Body shape:

```json
{
  "type": "/problems/idempotency-conflict",
  "title": "Idempotency-Key Conflict",
  "status": 409,
  "detail": "Idempotency-Key reused with a different request body",
  "instance": "/requests/019e2c84-32fd-7066-80b2-1fc822aa13e5",
  "errors": [{"field": "recipient", "message": "must be E.164"}]
}
```

Extension members (`instance`, `errors`) are flattened into the top-level object via a custom `MarshalJSON` on the problem struct rather than nested under a sub-object - the RFC permits this and clients find it more ergonomic. `instance` is set from the per-request correlation-id (`/requests/<id>`) so support tickets can quote it and ops can grep logs by the same value.

A single centralized mapper, `internal/api/errors.go` `WriteErrorAsProblem(w, r, err)`, owns the entire error-to-HTTP translation:

- `ErrNotFound` / `ErrTemplateNotFound` -> 404 `/problems/not-found`
- `ErrIdempotencyConflict` -> 409 `/problems/idempotency-conflict`
- `ErrIdempotencyInFlight` -> 409 `/problems/idempotency-in-flight` + `Retry-After: 1`
- `ErrInvalidState` -> 409 `/problems/invalid-state`
- `ErrValidation` / `ErrUnknownChannel` / `ErrTemplateRender` -> 400 `/problems/validation` (with `errors[]` populated)
- `ErrRateLimited` -> 429 `/problems/rate-limited` + `Retry-After`
- `context.DeadlineExceeded` / `context.Canceled` -> 504 `/problems/timeout`
- default -> 500 `about:blank` with no `detail` (the raw error string is logged, never serialized to the client)

The mapper uses `errors.Is` / `errors.As` at the boundary, so handlers and services can wrap freely with `fmt.Errorf("get notification: %w", err)` and the mapper still sees the sentinel underneath. Handlers always return errors *to* the mapper - they never call `w.WriteHeader` for an error path directly.

## Consequences

- One contract, one place to change it. Adding a new error class is one sentinel + one mapper case; no handler edits.
- Clients can generically recognize `application/problem+json` - OpenAPI generators, Go's `connect-go`, Java's Spring, and most TypeScript HTTP clients have native support. We are not asking integrators to learn a bespoke envelope.
- The `errors[]` extension carries per-field validation issues; the batch create endpoint uses the same shape to return per-item failures, so the validation contract is uniform across single and batch.
- Honest tradeoff: a Problem body is a few dozen bytes larger than a minimal `{"error":"..."}` envelope. At our error rates (small relative to 2xx traffic) this is irrelevant; we accept it for the contract benefits.
- The default 500 case deliberately omits `detail`. Internal error strings leak schema names, file paths, and library versions - we log them with the correlation-id and return only the id to the client.

## Alternatives Considered

- **Custom JSON envelope (`{"error": "...", "code": 1234}`):** works fine in isolation but every client SDK has to learn it. RFC 7807 has been a standard since 2016 and the tooling has caught up; choosing the standard is free.
- **HTTP status code only, body opaque:** under-specifies. A 400 could be a missing field, a type mismatch, or a business-rule violation. Clients cannot branch without parsing prose, which is the failure mode we are trying to avoid.
- **gRPC-style status codes inside the body:** not applicable to a pure HTTP API and reinvents the wheel that the HTTP status line already turns.
- **Top-level `errors[]` array as the root JSON value:** less ergonomic - clients see "is this an object or an array?" and have to sniff. A single envelope with an `errors[]` member inside is unambiguous and keeps the validation case shaped like every other case.
