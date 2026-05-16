# ADR-0010: OpenTelemetry tracing on by default

## Status

Accepted (2026-05-16)

## Context

A single notification traverses four to five process boundaries between the inbound HTTP request and the outbound provider call:

- HTTP API to Postgres transaction (insert notification + outbox row).
- Outbox dispatcher to Postgres (claim batch) to RabbitMQ (publish with confirm).
- Worker AMQP consume to Postgres CAS to Redis (rate-limit + inflight check) to external HTTP provider.

When a single notification takes four seconds, "why" is not answerable from any one component's logs. You have to correlate timestamps across the API, the dispatcher, RabbitMQ, the worker, and the provider call — three of those hops are async, so wall-clock correlation is the only thread. That is the bug class distributed tracing exists for.

## Decision

**OpenTelemetry tracing is initialized at startup and is on by default.**

- `internal/observability/otel.go` wires the SDK with an OTLP gRPC exporter pointed at `$OTEL_EXPORTER_OTLP_ENDPOINT` (default `otel-collector:4317`).
- Sampler: `ParentBased(TraceIDRatioBased(ratio))` where `ratio` reads from `OTEL_SAMPLE_RATIO` (default `0.1`). A parent decision propagated via `traceparent` is always honored, so a sampled API request stays sampled through the outbox.
- Propagator: W3C `traceparent` + W3C Baggage, composed via `propagation.NewCompositeTextMapPropagator`.
- Resource attributes: `service.name`, `service.version`, `service.instance.id` — populated from `runtime/ldflags` build metadata (`-X main.version=…`) and the pod / container hostname.
- **Failure mode.** If the collector is unreachable, exporter init logs a warning and the SDK installs a no-op tracer provider. The app boots normally; tracing is best-effort, never load-bearing.
- **Kill switch.** `OTEL_SDK_DISABLED=true` disables the SDK entirely, for operators who need to rule out tracing during incident triage.

Instrumentation points:

- HTTP server: `otelhttp.NewHandler` with a skip-list for `/livez`, `/readyz`, `/metrics`, `/version`, `/openapi.yaml`, `/docs` — health and discovery routes do not produce useful spans and would dominate the trace volume.
- HTTP client (worker to provider): the hardened outbound client is wrapped with `otelhttp.NewTransport` at the call sites that need it.
- Postgres: `otelpgx.NewTracer()` attached to the `pgx` pool config — every query is a span.
- Redis: `redisotel.InstrumentTracing(rdb)` on the shared client.
- RabbitMQ: manual spans wrap `Publish` (dispatcher) and `Consume` (worker). On publish, the current span context is injected into a `propagation.MapCarrier` and written into the `outbox.headers` JSONB column. On consume, the worker extracts that carrier from the AMQP message headers and starts a child span — **the async hop does not break the trace**.

## Consequences

**Positive.**

- One end-to-end trace covers API ingest, outbox publish, AMQP delivery, worker processing, and the provider HTTP call. The four-second-latency question reduces to reading a flame graph.
- OTLP is the open standard. The same exporter targets Jaeger, Tempo, Honeycomb, or DataDog without code changes — just an endpoint and credentials.
- Two real outcomes from on-by-default tracing during this build:
  - A scheduled-notification hang was localized in minutes — the trace showed time accumulating inside `MarkSendingCAS` in the worker, specifically the pgx round-trip, not the provider call as initially suspected.
  - When the OTel collector was unreachable in a dev environment, the SDK logged `traces export: exporter export timeout: …` as warnings and the app kept serving. No crash, no startup failure, no operator action required.

**Negative / accepted.**

- Tracing is not free. Each span allocates a struct, copies attributes, and queues a batch. At 10% sampling and our throughput, measured CPU overhead is sub-1%. We accept it.
- One more boot dependency (the collector) and one more failure surface (exporter health). Mitigated by the no-op fallback above.
- Trace context lives in `outbox.headers` JSONB, which is one more column to keep in sync if the outbox schema changes. The serialization is W3C-standard, not bespoke.

## Alternatives Considered

- **Tracing off by default, opt-in via env var.** Rejected. The most expensive bug to investigate is the one that wasn't being traced when it happened. You cannot retroactively trace yesterday's incident. Leaving tracing on costs sub-1% CPU and pays for itself the first time production misbehaves.
- **Sidecar / auto-instrumentation agent** (à la Java's `-javaagent` or Python's `opentelemetry-instrument`). Not viable in Go: there is no runtime agent model. Instrumentation is in-code, which is what we are already doing.
- **DataDog or another proprietary APM SDK.** Rejected for vendor lock-in. OTLP gives us the same data with a swappable backend. If we later want DataDog, we point the exporter at their OTLP endpoint and change nothing else.
