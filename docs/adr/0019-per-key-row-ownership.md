# ADR-0019: Per-key row ownership and admin scope as the bypass

## Status

Accepted (2026-05-16)

## Context

The original notification schema had no `created_by` column. Any API key
with `notifications:read` could `GET /v1/notifications/{id}` for any UUID
and read recipient + content. The system is single-tenant by the
[ADR-0011](0011-api-key-auth-not-oauth.md) threat model, but the API
**surface** is shaped like a SaaS: write a notification, scope read access
to that notification, deny everyone else. The data model was a bug waiting
for a second customer key to be issued.

The hiring assessment review flagged this as the single biggest security
gap: "I'm a customer of yours. I have a valid API key. What stops me from
reading every other customer's recipient phone numbers and message bodies?"
— nothing, until this ADR.

## Decision

Add `notifications.created_by` (uuid, nullable, references `api_keys(id)`
with `ON DELETE SET NULL`). Same for `batches.created_by` since batches
own their items. New writes always stamp the column with the authenticated
key's UUID; existing rows (pre-migration) get `NULL` and are visible only
to admin-scope callers.

Read paths gate on `created_by`:

- `GET /v1/notifications/{id}` → 404 if not owned by caller
- `GET /v1/notifications` → `WHERE created_by = $caller`
- `GET /v1/notifications/batch/{id}` → returns batch metadata + only the
  caller's items in that batch
- `DELETE /v1/notifications/{id}` (cancel) → 404 if not owned

The **admin scope** is the bypass. A key with the `admin` scope sees every
row and can cancel any notification — useful for support, incident triage,
and the dev seed key. Operators should issue admin scopes sparingly.

**Probing prevention:** non-admin callers always get 404 on cross-owner
access, never 403. 403 would be an existence oracle (the attacker could
enumerate UUIDs and learn which exist).

## Consequences

**Positive:**

- Issuing a second customer key is no longer dangerous. The read paths
  isolate the two customers' notifications by `created_by`.
- The audit log (admin_audit) already records the actor; now matches the
  ownership column.
- Backfill story is trivial: NULL on existing rows, visible only to admins.
  When a real second-customer migration happens, the operator can backfill
  with a one-line UPDATE keyed off some external mapping table.

**Negative:**

- Three new sqlc queries (`GetNotificationByIDScoped`,
  `GetNotificationsByBatchIDScoped`, `CancelPendingOrQueuedScoped`)
  duplicate the unscoped ones. squirrel was considered for a dynamic
  predicate but the duplication is cheap and the sqlc queries are easier
  to review.
- Cancel ambiguity: non-admin who tries to cancel a notification in
  `sent` state used to get 409 (invalid state). Now gets 404 — they
  can't distinguish "not yours" from "wrong state." Documented in the
  handler comment as deliberate.
- The list endpoint's keyset pagination predicate now combines with the
  `created_by` filter. The `notifications_created_by_created_at_idx`
  partial index covers this.

## Alternatives considered

- **Tenant_id column instead of created_by.** Right shape for a real
  multi-tenant deploy. We chose `created_by` (= api_key_id) because we're
  *not* yet multi-tenant; introducing a tenant abstraction without a real
  notion of tenant ownership would be premature. The migration path is
  clear: add `tenants` table, add `tenant_id` to `api_keys`, switch
  predicates from `created_by` to `tenant_id`.
- **Row-level security (RLS) in Postgres.** Strong invariant guarantee but
  pgxpool's connection sharing makes RLS contexts fiddly (the `SET
  app.tenant_id = ...` doesn't survive pool checkout). Application-side
  filtering is simpler and the test suite proves it covers all read paths.
- **Drop the unscoped sqlc queries entirely.** The admin scope wouldn't
  have a bypass for cross-tenant reads. Keeping them lets admins do
  support without DB access, which is the most important admin workflow.

## References

- [`internal/store/migrations/0006_notifications_created_by.up.sql`](../../internal/store/migrations/0006_notifications_created_by.up.sql)
- [`internal/store/queries/notifications.sql`](../../internal/store/queries/notifications.sql) — `*Scoped` queries
- [`internal/api/notifications_batch.go`](../../internal/api/notifications_batch.go) — handler-side admin-vs-owner gating
- [ADR-0011](0011-api-key-auth-not-oauth.md) — API key auth, scopes
- [ADR-0020](0020-key-revocation-cache-bust.md) — revoking those keys
