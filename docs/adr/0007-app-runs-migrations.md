# ADR-0007: App runs migrations on boot

## Status

Accepted (2026-05-16)

## Context

The service owns its Postgres schema and needs that schema applied before it accepts traffic. Schema application can live in several places in the deployment pipeline: a sidecar/init container, a separate one-shot `migrate` service in docker-compose, a manual operator step, or inside the app process itself. Each placement implies a different operational model and a different failure mode when the deployed binary and the live schema diverge.

We run a small stack (single binary, Postgres, RabbitMQ, Redis) on docker-compose, without Kubernetes. Drift between "what is deployed" and "what is migrated" is the most common operational incident pattern we want to eliminate.

## Decision

**The app process runs `golang-migrate` programmatically during boot, before opening its HTTP listener.**

- Migration files live in `internal/store/migrations/*.up.sql` and `*.down.sql`.
- They are embedded via `go:embed` so the image is self-contained — no host bind-mounts, no separate migrations image.
- `internal/store/migrations.go` builds an `iofs` source and calls `migrate.NewWithSourceInstance(iofs, src, databaseURL)` then `m.Up()`. `migrate.ErrNoChange` is treated as success.
- A `--skip-migrate` flag exists in `main.go` for emergencies (e.g., split-brain recovery where schema has been applied out-of-band and a re-attempt would be noisy).
- Boot logs are explicit: `migrations: applied from=N to=M` or `migrations: nothing to apply version=M`.

Boot lifecycle, fail-fast on any step:

1. Parse flags / load config.
2. Run migrations (unless `--skip-migrate`).
3. Open the DB pool.
4. Seed the dev API key (dev profile only).
5. Wire dependencies (publisher, consumer, rate limiter, idempotency store).
6. Start the HTTP listener.

`golang-migrate` takes a Postgres advisory lock (`pg_advisory_lock`) on its `schema_migrations` table before applying. If N replicas boot simultaneously, exactly one acquires the lock and applies; the others block briefly, then observe `ErrNoChange` and proceed. The race is safe by construction.

## Consequences

**Positive.**

- Single source of truth: the binary that serves requests is the binary that owns the schema it expects. Version skew between "deployed app" and "applied schema" is structurally impossible.
- No extra service to operate, no init-container manifest, no separate image to build and tag.
- Local development is identical to production: `go run ./cmd/server` brings up a working schema.
- Self-contained image: `docker run` works against any reachable Postgres without mounting SQL files.

**Negative / accepted.**

- A slow migration blocks boot. A five-minute index build would delay readiness. We accept this: at our scale every migration is sub-second, and a production-grade rollout would use blue-green or canary anyway — the new replicas would migrate while the old replicas still serve.
- An app crash mid-migration leaves `schema_migrations.dirty = true`. Recovery is manual (`force` to a known version, then re-run). This is the standard `golang-migrate` failure mode; we accept it in exchange for the simpler topology.
- Rolling deploys briefly run the new binary against a not-yet-migrated DB on the losing replicas — but they block on the advisory lock until the winner finishes, so they never observe an inconsistent schema.

## Alternatives Considered

- **Dedicated `migrate` one-shot service in docker-compose** with `depends_on: service_completed_successfully`. Pros: strict ordering, no race. Cons: extra service, sequencing complexity, divergent local vs. prod startup. Rejected because the advisory lock already makes the race safe, and the operational tax is real.
- **Kubernetes init container.** The k8s-native version of the above; same tradeoffs. Out of scope — we do not run k8s.
- **Manual `make migrate-up` before deploy.** Fine for development, dangerous in production: the most common production incident in services that do this is "we deployed the app but forgot the migration." Rejected.

## Migration discipline

- Every `*.up.sql` has a matching `*.down.sql`. Down migrations are exercised in CI integration tests (`up → down → up` on a scratch DB).
- A merged migration is immutable. Schema corrections ship as a new migration, never as an edit to a previous one.
- Migrations are reviewed with the same rigor as application code; destructive changes (`DROP`, `ALTER … DROP COLUMN`) require an explicit checklist in the PR.
