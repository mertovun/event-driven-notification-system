# ADR-0002: PostgreSQL with sqlc, not an ORM

## Status

Accepted (2026-05-16)

## Context

The notification system persists a small, well-known set of tables — `notifications`,
`batches`, `delivery_attempts`, `dead_letters`, `outbox`, `scheduled_notifications`,
`templates`, `api_keys`, `admin_audit` — and runs a closed set of queries against them.
We need to pick the Go-to-Postgres layer: an ORM, a query builder, a hand-rolled driver
wrapper, or a code generator.

The workload is OLTP-shaped: short transactions, high write volume on `notifications`
and `delivery_attempts`, frequent point reads keyed by ID, and one list endpoint with
dynamic filters. The schema evolves through reviewed migrations, not at runtime.

## Decision

- **PostgreSQL 16** is the system of record.
- **`pgx/v5`** is the driver — not `database/sql + lib/pq`. We want the native protocol,
  batch queries, `COPY`, and `pgx.Tracer` for OpenTelemetry.
- **`sqlc`** generates type-safe Go from hand-written SQL in
  `internal/store/queries/*.sql`. Output lands in `internal/store/gen/` and is
  **committed** (not generated at build time) so CI, IDEs, and reviewers see the same code.
- **`squirrel`** is permitted in exactly one place: the dynamic-filter list endpoint
  where the WHERE clause is composed from optional query parameters.
- **No ORM.** No GORM, no ent, no bun.
- **Migrations:** `golang-migrate` reading `internal/store/migrations/*.sql`, embedded
  via `go:embed` so the binary is self-contained.
- **Config:** `sqlc.yaml` at the repo root pins the engine to `postgresql`, points at
  the `pgx/v5` driver, and enables `emit_prepared_queries: true`.

## Consequences

**Positive.**
- Compile-time guarantees: change a column in a migration, regenerate, and any Go
  call site that references the dropped/renamed field fails to build. No runtime
  surprises.
- All SQL is greppable. `rg "WHERE status = "` finds every relevant query — impossible
  with tag-based ORMs.
- Parameterized queries are enforced by construction; SQL injection is not a category
  of bug we can write.
- `pgx` batch queries and `COPY` are first-class — sqlc emits `:batchexec` /
  `:batchmany` for our fan-out paths (e.g. inserting a batch of `delivery_attempts`).
- `otelpgx` plugs into the `pgx.Tracer` hook with zero changes to call sites, so every
  query is a span.

**Negative.**
- Two artifacts to keep in sync: the `.sql` file and the regenerated Go. A pre-commit
  hook runs `sqlc generate` and fails if the diff is non-empty.
- sqlc cannot express dynamic WHERE clauses cleanly. The list endpoint
  (`GET /v1/notifications?status=&tenant=&since=`) uses `squirrel` instead — documented
  inline at the call site so it stays the only exception.
- Onboarding cost: contributors must learn sqlc's annotation conventions (`:one`,
  `:many`, `:exec`, `:batchmany`).

## Alternatives Considered

- **GORM.** Struct-tag magic, hidden N+1s, runtime reflection on every query. Tag-based
  query construction makes the actual SQL non-obvious — you cannot grep for the
  predicate you are about to change. Rejected.
- **ent.** Type-safe, but schema-first: the schema lives in a separate Go DSL with its
  own codegen pipeline, heavier than sqlc and mismatched with our SQL-first migration
  workflow. Rejected.
- **`database/sql` + `sqlx`.** Removes some boilerplate but still does runtime type
  mapping via reflection; column renames surface as runtime errors in production, not
  compile errors in CI. Rejected.
- **Raw `pgx.QueryRow` / `pgx.Query` everywhere.** Correct and fast, but ~80% of the
  code is mechanical row-scanning. sqlc eliminates that boilerplate while leaving
  pgx fully accessible for the ~20% (dynamic filters via squirrel, ad-hoc admin
  queries) where generation does not fit.
