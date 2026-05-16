# Event-Driven Notification System

A Go service that accepts notification requests over HTTP and delivers them across SMS / Email / Push channels through asynchronous workers, with per-channel rate limiting, priority queues, idempotency, retries, dead-lettering, scheduled delivery, real-time status push, and full observability.

Single binary. One `docker compose up` and the entire stack is running.

---

## Quickstart

```bash
git clone https://github.com/mertovun/event-driven-notification-system
cd event-driven-notification-system
cp .env.example .env
# Edit .env if you want — defaults work for local dev.
make up
```

Wait ~5 seconds for the stack to boot, then:

```bash
# Create a notification
curl -X POST http://localhost:8090/v1/notifications \
  -H "Authorization: Bearer dev1234-do-not-use-in-prod-xxxxxxxxxx" \
  -H "Content-Type: application/json" \
  -d '{"channel":"sms","recipient":"+905551234567","content":"hello","priority":"high"}'

# Browse the API docs
open http://localhost:8090/docs

# Watch metrics
curl http://localhost:8090/metrics | grep notifications_
```

`make test` runs the unit suite. `make test-integration` runs the integration suite (spins up real Postgres/Redis/RabbitMQ in containers).

---

## Architecture

```
                                                    +-------------+
   client ── HTTP ──→ [API: Chi]  ──TX──→ Postgres ─┤notifications│
                                                    +─────────────┤
                       │                            │   outbox    ├──┐
                       │ (Redis: idempotency cache)  +─────────────+  │
                       ▼                                              │
                   Redis ─── Pub/Sub ─── WS hub ─── client (status push)
                                                                      │
                                       ┌──────────────────────────────┘
                                       │ (1Hz claim)
                                       ▼
                              [Outbox dispatcher] ─── confirms ───→ RabbitMQ
                                                        │              │
                                       (priority queues per channel)   │
                                          + retry tiers (5s/30s/5m) + DLX
                                                        │              │
                              [Workers] ←── prefetch=1 ─┘              │
                                  │                                    │
                                  │ token bucket (Redis Lua, 100/s)    │
                                  │ circuit breaker (per channel)      │
                                  │                                    │
                                  ▼                                    │
                              [Provider: webhook.site]                 │
                                                                       │
                              [Scheduler dispatcher] (1Hz) ────────────┘
                                  scheduled_notifications.due_at <= now()

   Cross-cutting: slog JSON (PII-redacting), Prometheus /metrics, OpenTelemetry OTLP/gRPC
```

**Key decisions, in one line each** — see [`docs/adr/`](docs/adr/) for the full rationale of each:

- **Single Go binary** with `--mode=api|worker|all` (default `all`). Scale by replica count. → [ADR-0001](docs/adr/0001-go-and-single-binary.md)
- **PostgreSQL + sqlc**, no ORM. Parameterized SQL enforced at compile time. → [ADR-0002](docs/adr/0002-postgres-with-sqlc.md)
- **RabbitMQ over Redis Streams** for priority queues + DLQ-and-replay. Three retry tiers with TTL. → [ADR-0003](docs/adr/0003-rabbitmq-over-redis-streams.md)
- **Redis** for rate-limit (token bucket via atomic Lua), idempotency cache, and Pub/Sub status events. Not used as a queue. → [ADR-0004](docs/adr/0004-redis-for-rate-limit-and-idempotency.md)
- **Cursor pagination** on `(created_at DESC, id DESC)`. Offset never used. → [ADR-0005](docs/adr/0005-cursor-pagination.md)
- **Idempotency-Key** request header with 24h TTL, body-hash canonicalization, 409 on conflict, `Idempotency-Replayed: true` on success. → [ADR-0006](docs/adr/0006-idempotency-key-header.md)
- **distroless + nonroot + read-only rootfs + cap_drop ALL** runtime container. → [ADR-0007](docs/adr/0007-distroless-runtime.md)
- **App runs migrations on boot.** Atomic with deploy. → [ADR-0008](docs/adr/0008-app-runs-migrations.md)
- **Transactional outbox** is primary; a separate dispatcher claims rows with `FOR UPDATE SKIP LOCKED` and publishes with broker confirms. The API never touches AMQP on the request path. → [ADR-0009](docs/adr/0009-transactional-outbox.md)
- **OpenTelemetry on by default** with trace context propagated through the outbox `headers` JSONB column. → [ADR-0010](docs/adr/0010-tracing-on-by-default.md)
- **argon2id** API keys with prefix-narrowed lookup. Scopes: `notifications:read|write|admin`.

---

## API

All authenticated endpoints require `Authorization: Bearer <api_key>`. Errors are RFC 7807 problem documents (`application/problem+json`).

### Notifications

```bash
# Create
POST /v1/notifications
# Headers: Idempotency-Key (optional), X-Request-Id (optional)
# Body: { channel, recipient, content, priority?, scheduled_at? }
#   OR: { channel, recipient, template_id, variables, priority?, scheduled_at? }

# Batch (up to 1000)
POST /v1/notifications/batch
# Body: { items: [...] } — partial success returns 207 with per-item results

# Get / list
GET  /v1/notifications/{id}
GET  /v1/notifications/batch/{batchId}
GET  /v1/notifications?status=&channel=&batch_id=&created_after=&created_before=&cursor=&limit=

# Cancel (pending/queued/scheduled only)
DELETE /v1/notifications/{id}
```

### Templates

```bash
POST   /v1/templates           # body: { name, channel, body }
GET    /v1/templates           # list, hides deprecated
GET    /v1/templates/{id}      # incl. deprecated (for audit)
PUT    /v1/templates/{id}      # body: { body } — bumps version
DELETE /v1/templates/{id}      # soft-deprecate
```

Substitution happens **at create time**, not at delivery time. The rendered content is stored on the notification row. This means retries produce identical wire bytes, and template edits never retroactively alter history.

### Admin (scope: `admin`)

```bash
GET    /v1/admin/dead-letters
GET    /v1/admin/dead-letters/{id}
POST   /v1/admin/dead-letters/{id}/replay   # resets to pending, fresh outbox row, audit log
DELETE /v1/admin/dead-letters/{id}          # purge with audit log
```

### Real-time

```bash
GET /v1/ws/notifications?filter=channel:sms,batch_id:<uuid>
# WebSocket connection; receives JSON status events
# { notification_id, channel, status, at, correlation_id }
```

PII (recipient, content) is **never** sent over the WebSocket. Heartbeat: 30s ping, 10s pong deadline. Per-connection 256-message buffer with slow-consumer eviction.

### Operational

```bash
GET /livez               # process liveness
GET /readyz              # dependency reachability (postgres + redis + rabbitmq), JSON breakdown
GET /version             # build metadata
GET /metrics             # Prometheus
GET /openapi.yaml        # OpenAPI 3.1 spec
GET /docs                # Swagger UI
```

Full spec: [`internal/api/openapi.yaml`](internal/api/openapi.yaml).

---

## Tradeoffs and Limitations

**What's not built, and how I'd build it.**

- **OAuth/OIDC.** Argon2id API keys instead. Upgrade path: JWKS-fetching middleware in front of the existing auth middleware.
- **Multi-tenancy.** Single tenant. Multi-tenant needs `tenant_id` on every table + `WHERE tenant_id=$1` on every query (and `validator` already supports tag-based gates).
- **Real provider clients** (Twilio/SES/FCM). webhook.site stub only. The `provider` package interface is consumer-defined — adding a real client is one struct + a constructor.
- **WebSocket fan-out across replicas.** Each replica subscribes to Redis Pub/Sub independently, so per-key connection caps are per-replica. A fleet-wide cap would need a shared registry (Redis).
- **DLQ-drain consumer.** Workers write to `dead_letters` Postgres table directly on terminal failure; there's no consumer draining the RabbitMQ DLQ queues into Postgres. Documented because if a message goes to the DLQ via DLX (TTL or explicit nack), it sits there until manually inspected.
- **i18n templates.** Single locale. Would need `(name, channel, locale, version)` uniqueness.

### Known issue

[`KNOWN_ISSUES.md`](KNOWN_ISSUES.md) documents one reproducible bug: scheduled notifications can hang a worker on its first DB call after the scheduler dispatcher fires. Root-caused to a pgx v5 internal state corruption (bgreader `Running` status with no goroutine). Pool isolation experiment didn't help. All other features work end-to-end.

---

## Operational notes

### Scaling workers

`WORKER_COUNT_SMS`, `WORKER_COUNT_EMAIL`, `WORKER_COUNT_PUSH` env vars (default 8 each). Each worker holds one AMQP consumer at `prefetch=1` so priority queues are honoured per-message. Default config delivers ~50 msg/s per channel per replica.

### Rate limiting

100 msg/s per channel, atomic Redis token bucket via Lua. Multi-replica correct. Worker on throttle: sleeps up to 200ms in-place; if still throttled, nacks to retry tier so the AMQP prefetch slot stays flowing.

### Retry policy

- **Retryable** (network, 5xx, 408, 429, `context.DeadlineExceeded`): nack to next retry tier (`wait.5s` → `wait.30s` → `wait.5m`), TTL'd via RabbitMQ, DLX back to main queue.
- **Terminal** (4xx other than 408/429, validation errors, attempts ≥ 10): dead-letter table + AMQP DLQ.
- Circuit breaker per channel: opens on 5 consecutive failures or ≥50% failure rate over 20 requests. Open-state messages go to retry tier (not DLQ — breaker-open is not a per-message fault).

### Where logs go

stdout (JSON, structured via `log/slog`). Compose captures them; `make logs` tails them. Every log line carries `correlation_id`. PII fields (recipient, email, content, api_key) are masked at the slog handler layer.

### Where metrics go

`/metrics` Prometheus endpoint on the same HTTP port. Add a scrape config and you're good. 14 application metrics + Go runtime metrics. Full list in [`internal/observability/metrics.go`](internal/observability/metrics.go).

### Where traces go

OTLP/gRPC to `$OTEL_EXPORTER_OTLP_ENDPOINT` (default `otel-collector:4317`). Falls back to no-op if no collector. Disable entirely with `OTEL_SDK_DISABLED=true`. Sample rate via `OTEL_SAMPLE_RATIO` (default 0.1).

---

## Development setup

Requirements:
- **Go 1.26+** (matches Dockerfile)
- **Docker + Docker Compose**
- **k6** (load testing) — `brew install k6`
- **sqlc** (regenerating queries) — `brew install sqlc`
- **govulncheck** — `go install golang.org/x/vuln/cmd/govulncheck@latest`

Install the pre-commit hook (one-time):
```bash
make install-hooks
```
The hook runs `gofmt -l + go vet + go build` on every commit.

Common commands:
```bash
make build           # go build ./...
make test            # unit tests with race detector
make test-integration # integration tests (testcontainers)
make lint            # golangci-lint
make vuln            # govulncheck
make generate        # sqlc generate
make up / down       # docker compose
make logs            # tail app logs
make load-test       # run all 3 k6 scenarios
```

---

## Testing

| Layer | What | How |
|---|---|---|
| **Unit** | Pure logic — error mapping, idempotency canonicalization, cursor codec, filter parser, validators, SSRF deny-zone, breaker thresholds | `make test` (race detector ON) |
| **Integration** | Real DB / AMQP / Redis via testcontainers — migrations, CAS races, Lua script, outbox dispatch end-to-end, worker pipeline against mock provider | `make test-integration` |
| **E2E** | Full docker-compose stack — API contract, WS push, admin DLQ replay | curl + python WS client. See `make up` quickstart curl examples. |
| **Load** | k6 scenarios — baseline 50 RPS, priority burst 150 RPS, idempotency replay | `make load-test` |

Latest measured numbers on a clean stack (single replica, macOS arm64, Docker Desktop):

| Scenario | Total reqs | Failed | Throughput | p95 | p99 | Notes |
|---|---|---|---|---|---|---|
| Baseline 50 RPS × 30s | 1,501 | 0 | 49.9 r/s | 99ms | **134ms** | well within 500ms threshold |
| Priority burst 150 RPS × 20s | 2,174 | 0 | 106.5 r/s | 1.21s | — | API back-pressures gracefully under burst; zero errors |
| Idempotency 20 RPS × 15s | 301 | 0 | 19.9 r/s | 95ms | 101ms | replay path under load |

`make load-test` runs all three.

---

## Repository layout

```
.
├── cmd/notifyd/             # main package; --mode=api|worker|all
├── internal/
│   ├── api/                 # Chi router, middleware, RFC 7807 errors, auth,
│   │                        # OpenAPI/Swagger, all resource handlers
│   ├── notification/        # domain sentinels + types (Channel, Priority, Status)
│   ├── store/               # pgx pool, embedded migrations, sqlc-generated queries
│   │   ├── migrations/      # versioned SQL
│   │   ├── queries/         # *.sql for sqlc
│   │   └── gen/             # generated (committed)
│   ├── idempotency/         # Redis-backed Idempotency-Key cache
│   ├── ratelimit/           # token bucket Lua script
│   ├── provider/            # outbound HTTPS client, SSRF guard, retry classifier
│   ├── queue/               # RabbitMQ topology, publisher, consumer
│   ├── outbox/              # transactional outbox dispatcher
│   ├── scheduler/           # scheduled notifications dispatcher
│   ├── worker/              # delivery pipeline, breaker, pool manager
│   ├── ws/                  # WebSocket hub + handler + filter parser
│   ├── events/              # Redis Pub/Sub status event publisher
│   ├── template/            # text/template parser + renderer
│   ├── observability/       # metrics, PII-redacting slog, OTel SDK init
│   ├── config/              # envconfig
│   └── platform/            # shared utilities
├── docs/adr/                # architecture decision records
├── loadtest/                # k6 scenarios
├── scripts/                 # pre-commit hook source
├── deploy/                  # docker-compose.yml
├── Dockerfile               # multi-stage, distroless final
├── Makefile                 # make help to list targets
├── sqlc.yaml
├── KNOWN_ISSUES.md
└── README.md
```

---

## CI

GitHub Actions runs on every push and PR:
- `lint` — golangci-lint
- `test-unit` — `go test -race`
- `test-integration` — testcontainers
- `vuln` — govulncheck
- `build` — multi-stage docker image (no push)

Dependabot watches `gomod`, `docker`, and `github-actions` weekly.

---

## License

MIT — see [LICENSE](LICENSE).
