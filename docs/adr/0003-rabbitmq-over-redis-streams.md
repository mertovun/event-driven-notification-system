# ADR-0003: RabbitMQ over Redis Streams / Kafka / Postgres-as-queue

## Status

Accepted (2026-05-16)

## Context

The API publishes notifications via a transactional outbox; per-channel worker
pools consume and deliver to the external provider. The transport between them
must satisfy:

- **Per-message ack semantics** — at-least-once delivery, individual acks so a
  crashed worker's in-flight messages are redelivered, not silently advanced.
- **Priority queues** — `high` should preempt `normal` and `low` under load
  (e.g., OTP traffic during a backlog).
- **Dead-lettering** — terminal failures (4xx from the provider, exhausted
  retries) move to a DLQ for inspection/replay rather than being dropped.
- **Retry tiers** — exponential-ish backoff implemented as `wait.5s` /
  `wait.30s` / `wait.5m` queues using the TTL + DLX trick.
- **Per-channel queues** — SMS, Email, Push must have independent throughput
  control; a stuck provider on one channel must not starve the others.
- **Mature operations story** — management UI, documented failure modes,
  battle-tested HA.

## Decision

Use **RabbitMQ 3.13** via `rabbitmq/amqp091-go`. The topology declared in
`internal/queue/topology.go`:

- 1 topic exchange: `notifications.exchange`
- 3 per-channel queues with `x-max-priority=10` and DLX args:
  `notifications.sms`, `notifications.email`, `notifications.push`
  (routing keys `notify.sms`, `notify.email`, `notify.push`)
- 3 DLQs fed via direct exchange `notifications.dlx`:
  `notifications.sms.dlq`, `notifications.email.dlq`, `notifications.push.dlq`
- 3 retry tiers (TTL + DLX back to the main exchange):
  `notifications.wait.5s`, `notifications.wait.30s`, `notifications.wait.5m`
- Publisher confirms enabled; `mandatory=true`; `DeliveryMode=2` (persistent).
  The outbox dispatcher uses `PublishWithDeferredConfirm` and batches confirms
  before marking outbox rows as `dispatched`.
- Consumer `prefetch=1` per channel — see Consequences below.

## Consequences

- **Priority works per-message only with `prefetch=1`.** This is the canonical
  RabbitMQ gotcha: with a larger prefetch, the broker hands a worker a batch
  ordered at delivery time; a higher-priority message arriving mid-batch waits
  behind already-prefetched low-priority messages. We accept the throughput
  cost for correct preemption.
- **Throughput is scaled by worker count, not prefetch.** Default is 8
  workers/channel/replica. At our target of 100 msg/s/channel (the provider
  rate limit), ~6 msg/s/worker × 8 workers = ~50 msg/s/replica/channel; two
  replicas hit the 100 msg/s ceiling. Headroom comes from horizontal scale.
- **Retry/DLQ replay is trivial** via the management UI shovel or a CLI move,
  which we get for free.
- **Operational surface added:** another stateful dependency to run in
  docker-compose and (eventually) cluster in prod. Mitigated by RabbitMQ's
  maturity and our team's prior familiarity.

## Alternatives Considered

- **Redis Streams + consumer groups.** Plausible, but priority queues require
  hand-rolling (N streams + a multiplexing reader honouring priority);
  DLQ-and-replay is hand-rolled; durability defaults are weaker (AOF tuning).
  We'd reinvent half of RabbitMQ inside the worker.
- **Kafka.** Wrong shape for this workload. Kafka is an append-only partitioned
  log; we need per-message ack, immediate redelivery on consumer crash, and
  priority preemption — all of which fight the partition/offset model.
  Per-priority partitions break consumer-group load-balancing.
- **NATS JetStream.** Excellent product with per-message ack and DLQ support,
  but priority still requires workarounds (subject-per-priority + client-side
  selection), and team-unfamiliarity is a real tax inside a 12-hour budget.
- **Postgres-as-queue (`SELECT ... FOR UPDATE SKIP LOCKED`).** We *do* use this
  pattern — for the outbox dispatcher itself (small table, low throughput,
  already in the transactional boundary). But for the high-rate fan-out to
  per-channel workers, RabbitMQ's per-consumer prefetch + priority queues +
  TTL/DLX retry tiers give us native support for behaviours we'd otherwise
  reimplement in SQL with worse semantics (priority via `ORDER BY` doesn't
  preempt in-flight work; retries become a cron job; DLQ becomes another
  table; visibility timeouts become a heartbeat column).
