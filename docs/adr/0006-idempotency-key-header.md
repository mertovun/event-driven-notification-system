# ADR-0006: Idempotency-Key request header with body-hash canonicalization

## Status

Accepted (2026-05-16)

## Context

Notification creation must be idempotent under retry. Network partitions, client-side retry loops, and the at-least-once delivery semantics of every layer in the stack (load balancer, RabbitMQ consumers, mobile SDKs) all make duplicate `POST /v1/notifications` requests inevitable. Without a server-enforced contract, a client that retries after a timeout has no way to know whether the original request succeeded, and we either send the notification twice or force the client to reconcile via a follow-up `GET`. Both are unacceptable for a notification system where duplicates are user-visible.

We need a contract that lets clients retry safely, that does not conflate API concerns with payload, and that the storage layer can enforce cheaply on the hot path.

## Decision

We accept an **`Idempotency-Key` request header** on `POST /v1/notifications` and `POST /v1/notifications/batch`. The key is an opaque client-supplied string (UUID recommended, max 255 bytes).

- **Storage:** Redis is the primary store, `idem:<key>` -> `{body_sha256, status_code, response_body}`, 24h TTL. Postgres carries an `idempotency_key` column on the `notifications` row with a partial unique index `WHERE idempotency_key IS NOT NULL` as the audit/DR fallback.
- **Body canonicalization:** SHA-256 of canonical JSON, computed as re-marshal -> recursively sort map keys -> sha256. This ensures `{"a":1,"b":2}` and `{"b":2,"a":1}` collide as intended; the client must not be punished for map ordering it did not author.
- **Replay (same key + same body hash):** return the stored canonical response verbatim with header `Idempotency-Replayed: true`. The original status code is reproduced exactly (including 201).
- **Conflict (same key + different body hash):** 409 with `application/problem+json` type `/problems/idempotency-conflict`. The diff between bodies is **not** logged - leaking it would defeat the security property below.
- **In-flight (first request still processing):** 409 with `Retry-After: 1`. The Redis in-flight marker uses a separate short TTL (10s) so a crashed handler does not wedge the key.
- **TTL:** 24 hours, matching Stripe's de-facto industry standard. Long enough to cover human-scale retries (laptop sleep, mobile background-fetch), short enough to bound Redis memory at our request volume.

Implementation lives in `internal/idempotency/idempotency.go` and is wired into the handler in `internal/api/notifications.go` as middleware that wraps the create endpoints only.

## Consequences

- Clients get a clean retry contract: same key + same body = same response, deterministically, for 24h.
- Hot-path duplicate detection is a single Redis `GET`. Postgres `UNIQUE` would require attempt-INSERT, catch `23505`, re-SELECT - three round-trips on the duplicate path.
- Replay returns the **original** status code and response body, which means the `Date` header and any timestamp inside the body reflect the first request, not the retry. This may surprise clients that diff responses; we document it in the API reference.
- The Postgres unique index gives us a recovery path if Redis is lost: we can rebuild idempotency state from the database for any notification still within its retention window.
- Security boundary: within the TTL window, `Idempotency-Key` acts as a replay-attack boundary. If an attacker captures a key and replays the request with different content, they get a 409, not a successful injection. The key is bound to the body hash, not just the key alone.

## Alternatives Considered

- **Idempotency as a body field (`"idempotency_key": "..."` in the JSON):** mixes API contract with payload, complicates schema evolution, and diverges from Stripe and GitHub which both use the header. Matching the convention reduces friction for client library authors.
- **Postgres `UNIQUE` violation as the primary mechanism:** correct but slow on the duplicate path (three round-trips) and couples idempotency latency to database health. We keep it as DR fallback only.
- **No idempotency, deduplicate at delivery time only:** breaks for clients that retry on network errors before the request reaches the queue. They would see two notifications dispatched for one logical request. The API surface must enforce the guarantee; pushing it to the worker is too late.
- **Server-generated idempotency tokens (return a token on first call, require it on retries):** removes client agency and requires a successful first response - which is exactly what the client did not get if they are retrying. The client must control the key.
