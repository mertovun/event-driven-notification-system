# ADR-0019: Per-key row ownership; admin scope as the bypass

## Status

Accepted (2026-05-16)

## Context

The original notification schema had no owner column. Any key with
`notifications:read` could `GET /v1/notifications/{id}` for any UUID and
read recipient + content. The system is single-tenant by the
[ADR-0011](0011-api-key-auth-not-oauth.md) threat model, but the API
surface is shaped like a SaaS. The data model was a bug waiting for a
second customer key to be issued.

## Decision

`notifications.created_by uuid REFERENCES api_keys(id) ON DELETE SET NULL`
(same for `batches`, `templates`). New writes stamp the authenticated
key's UUID; existing rows get NULL and are admin-only.

Reads gate on the column:

- `GET /v1/notifications/{id}` → 404 if not owned by caller
- `GET /v1/notifications` → `WHERE created_by = $caller`
- `GET /v1/notifications/batch/{id}` → caller's items only
- `DELETE /v1/notifications/{id}` → 404 if not owned

`admin` scope bypasses ownership for support / incident triage.

**404 on cross-owner access, never 403** — 403 would be an existence
oracle. Cancel of an already-`sent` row by a non-admin owner becomes
ambiguous between "not yours" and "wrong state"; both surface as 404.
Documented in the handler comment as deliberate.

## Consequences

- A second customer key is no longer dangerous; read paths isolate by
  `created_by`.
- Three `*Scoped` sqlc queries duplicate the unscoped ones — cheap; sqlc
  queries are easier to review than dynamic squirrel predicates.
- Backfill is `NULL` on existing rows; a real tenant migration is a
  one-line UPDATE keyed off an external mapping.

## Alternatives considered

- **`tenant_id` instead of `created_by`** — right shape for true
  multi-tenant, but premature without a tenant concept. Migration path
  clear: add `tenants` table, swap predicates.
- **Postgres RLS** — strong invariant, but pgxpool's connection sharing
  makes `SET app.tenant_id` brittle across pool checkout.
- **Drop unscoped queries** — would leave admins with no support path
  short of DB access. Kept.

## References

- Migration [0006](../../internal/store/migrations/0006_notifications_created_by.up.sql) — `created_by` on `notifications` and `batches`
- Migration [0008](../../internal/store/migrations/0008_templates_created_by.up.sql) — same on `templates`
- [`internal/store/queries/notifications.sql`](../../internal/store/queries/notifications.sql) — `*Scoped` queries
- [ADR-0011](0011-api-key-auth-not-oauth.md), [ADR-0020](0020-key-revocation-cache-bust.md)
