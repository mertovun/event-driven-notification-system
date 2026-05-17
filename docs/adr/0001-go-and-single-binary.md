# ADR-0001: One Go binary, modes selected by flag

## Status

Accepted (2026-05-16)

## Context

The notification system has six distinct runtime concerns: an HTTP API, an outbox dispatcher (background poller), a scheduler dispatcher (1Hz Postgres claim), worker pools per channel, a WebSocket hub, and a stuck-row sweeper (5-min reclaim of crashed-mid-send notifications). They share configuration, a Postgres pool, AMQP wiring, and observability. Language is fixed (Go); the open question is deployment topology — one binary or several. At submission scope, deploy cadence and ownership boundaries do not diverge between these concerns.

## Decision

We ship a single Go binary at `cmd/notifyd/main.go`. A `--mode` flag selects which subsystems the process activates:

- `--mode=api` — HTTP server + WebSocket hub
- `--mode=worker` — outbox dispatcher + scheduler + worker pools + stuck-row sweeper
- `--mode=all` (default) — everything in one process

Scaling is by replica count of the same image. `docker compose up` runs one `--mode=all` replica. Production would run, e.g., 2–3 replicas with `--mode=api` plus 2–3 replicas with `--mode=worker`. The composition root lives entirely in `main()`: constructors take their dependencies as arguments, no package uses `init()` for work, and wiring is dependency-injected so each mode boots only what it needs.

## Consequences

- Positive: one build, one image, one set of CI steps, one config schema, one observability bootstrap.
- Positive: local dev is `go run ./cmd/notifyd` — the full system in one process, no orchestration.
- Positive: horizontal scale is per-role via `--mode`, without code changes.
- Positive: splitting later is mechanical because the composition root is already isolated to `main()`.
- Negative: one process means one OOM kills every subsystem on that replica.
- Negative: a CVE or panic on (e.g.) the WebSocket path can take down the API path in the same process.
- Negative: resource limits are coarse — we cannot cap CPU on workers independently of the API within a single replica (only across replicas, via `--mode`).
- Negative: a single `go.mod` couples upgrade cadence across subsystems.

## Alternatives Considered

**Microservices (separate `api/`, `worker/`, `scheduler/`, `ws/` binaries).** Premature for the scope. Forces a service mesh, RPCs between services that already share the same Postgres and AMQP, separate CI pipelines, and independent release coordination. The trigger for splitting is divergent deploy cadence or ownership — neither applies here. We can split when that changes.

**Two binaries (api + worker).** The historically common split and defensible. Rejected because it duplicates shared config loading, DB pool setup, and observability bootstrap across two `main()` files. Once `--mode` exists, the two-binary layout is one refactor away: add `cmd/notifyd-api/main.go` and `cmd/notifyd-worker/main.go` that each import `internal/...` selectively and call the same constructors. We are not paying that complexity until it buys something.

**Function-as-a-service / serverless.** Wrong shape. We hold open AMQP consumer channels, maintain goroutine worker pools, run a 1Hz scheduler claim loop, and keep WebSocket connections alive. None of that fits a request-scoped, short-lived execution model.

**Single binary with no `--mode` (always run everything).** Rejected because it eliminates the ability to scale roles independently. Adding `--mode` costs a flag and a switch in `main()`; removing it later would cost a refactor.
