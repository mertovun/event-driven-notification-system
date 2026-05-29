# Fixes — broker-outage resilience & cross-boundary tracing

This document records two hardening passes made on the `demo-prep` branch. Neither
adds a feature; both make the existing event-driven pipeline behave correctly
under infrastructure failure and observable end-to-end. File references point at
the changed code.

---

## Theme 1 — Broker-outage resilience

### Problem

A RabbitMQ outage — whether the broker was down at process startup or dropped
the connection mid-run — would **crash the process** and, worse, could
**dead-letter perfectly healthy notifications** that had never even reached the
broker. Three distinct failure modes:

1. **Broker down at startup.** `NewPublisher` / `NewConsumerConnection` returned
   an error on a failed dial, so the process refused to boot if RabbitMQ wasn't
   up yet.
2. **Broker drops while running.** A consumer's delivery channel closing was
   treated as a fatal error, which propagated up the errgroup and tore down the
   whole process.
3. **Failures miscounted as delivery attempts.** A publish that failed because
   the broker was unreachable incremented `attempt_count`. A ~2.5s broker flap
   at the 250ms dispatcher tick would burn ~10 attempts and dead-letter a
   message that was never actually delivered. Likewise, the worker's
   redelivery cap would dead-letter a message that only churned because the
   circuit breaker was open (provider never called → message isn't poison).

The root cause across all three: the system did not distinguish **"the
infrastructure is down"** from **"this message is bad."** Only genuinely poison
messages should dead-letter; infrastructure failures should retry until the
infrastructure returns.

### Fix

**Boot resilience — publisher and consumer survive a broker that's down at startup.**

- [internal/queue/publisher.go](internal/queue/publisher.go) — `NewPublisher`
  no longer fails when the dial fails at startup. It logs a warning and starts a
  background `reconnectLoop`, returning a usable `Publisher` whose hot-path
  publishes degrade gracefully (return "no channel") until the broker comes up.
- [internal/queue/consumer.go](internal/queue/consumer.go) — `NewConsumerConnection`
  likewise starts a background `reconnectLoop` instead of failing. Consumers
  park on `WaitReady` until a live connection exists.
- [internal/worker/manager.go](internal/worker/manager.go) — each worker calls
  `cc.WaitReady(gctx)` before `NewConsumer`, so a broker-down-at-startup parks
  the worker instead of crashing the errgroup on a nil connection.

**Runtime resilience — a broker drop pauses and resumes instead of crashing.**

- [internal/queue/consumer.go](internal/queue/consumer.go):
  - `ConsumerConnection` gains a `ready` gate (closed while a live connection
    exists, re-armed on drop), a `watchClose` goroutine that detects the drop,
    and a `reconnectLoop` with exponential backoff (1s→30s), shared by the
    startup and runtime paths.
  - A broker-side channel close is now reported with the internal sentinel
    `errBrokerDropped` rather than a fatal error, distinguished from a clean
    ctx-cancel shutdown.
  - New `Consumer.RunForever` wraps `Run`: on `errBrokerDropped` it closes the
    dead channel, `waitReady`s for reconnect, `reopen`s a fresh channel
    (re-applying prefetch), and resumes consuming. A clean ctx-cancel returns
    `nil`; only a genuine non-recoverable error propagates.
- [internal/worker/manager.go](internal/worker/manager.go) — workers call
  `cons.RunForever` instead of `cons.Run`, so a transient outage is a
  recoverable pause, not a process kill.

> No message is lost across a reconnect: unacked in-flight deliveries are
> redelivered by the broker and deduped by the worker-side CAS + Redis SETNX
> stack.

**Don't dead-letter healthy messages during an outage.**

- [internal/queue/publisher.go](internal/queue/publisher.go) — new sentinel
  `ErrPublisherUnavailable` distinguishes "broker down" from "message rejected."
  It's returned when there is no channel, or when a publish / confirm fails with
  `amqp.ErrClosed` (broker dropped mid-publish).
- [internal/store/queries/outbox.sql](internal/store/queries/outbox.sql) +
  [internal/store/gen/outbox.sql.go](internal/store/gen/outbox.sql.go) (sqlc
  regenerated) — new query `MarkOutboxUnpublishedRetry` releases the claim and
  records the error **without advancing `attempt_count`**.
- [internal/outbox/dispatcher.go](internal/outbox/dispatcher.go) — `markFailure`
  checks `errors.Is(pubErr, queue.ErrPublisherUnavailable)`; when true it uses
  `MarkOutboxUnpublishedRetry` so the next dispatcher tick simply reclaims and
  retries, without ever advancing toward `MaxAttempts`.
- [internal/worker/pipeline.go](internal/worker/pipeline.go) — when the
  redelivery cap is hit but the circuit breaker is **open**, the message is
  requeued (nack-requeue) rather than dead-lettered: an open breaker means the
  provider was never called, so the message is not poison.

### Outcome

- The process boots and serves the API with RabbitMQ down.
- A broker blip pauses workers and resumes them automatically; no crash.
- Notifications are dead-lettered only when genuinely poison — never because the
  broker was transiently unavailable or the breaker was open.

---

## Theme 2 — Distributed tracing across process boundaries

### Problem

The pipeline crosses two asynchronous boundaries — the API writes a row to the
Postgres **outbox**, and the dispatcher publishes that row as an **AMQP
message**. The trace context propagated to the broker still carried the original
**API span** as its parent. As a result the worker's spans either attached one
level too high (under the API span instead of the dispatch span) or orphaned
entirely if the worker never extracted the context. There was no single trace
spanning the full lifecycle.

### Fix

- [internal/outbox/dispatcher.go](internal/outbox/dispatcher.go) — after opening
  the `outbox.dispatch` span, the dispatcher **re-injects** the now
  dispatch-scoped context back into the AMQP headers
  (`otel.GetTextMapPropagator().Inject`). The published `traceparent` now points
  at `outbox.dispatch`, not the API span.
- [internal/worker/pipeline.go](internal/worker/pipeline.go):
  - A new `amqpHeaderCarrier` type adapts AMQP headers (`amqp.Table`, a
    `map[string]any`) to OTel's `propagation.TextMapCarrier` interface.
  - `Handle` **extracts** the traceparent from the headers and starts a
    `worker.deliver` span as a child of the dispatch span.

### Outcome

A single distributed trace now spans **API → `outbox.dispatch` → `worker.deliver`**
across two process boundaries (the Postgres outbox row, then the AMQP message
headers). Supporting tooling (added separately on this branch): a local-only
Jaeger all-in-one container in [deploy/docker-compose.yml](deploy/docker-compose.yml)
exposes the trace UI at `http://localhost:16686`.
