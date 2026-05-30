# Demo — all paths, one runbook

Every command, output, and log line below was captured from a live run on this stack. Where the system's real behavior differs from intuition (or from the older per-demo docs), it's flagged with **Reality:**.

Paths covered:
1. [Setup](#0-setup)
2. [Happy path + distributed trace](#1-happy-path--distributed-trace)
3. [Idempotency: first / replay / shuffle / conflict](#2-idempotency)
4. [Circuit breaker: trip + recovery](#3-circuit-breaker)
5. [Outbox: atomic dual-write, dispatch, broker-outage resilience](#4-outbox)
6. [DLQ replay + audit chain (with tamper detection)](#5-dlq-replay--audit-chain)
7. [Load test + pprof](#6-load-test--pprof)
8. [Templates (bonus)](#7-templates-bonus-feature)
9. [Scheduled notifications (bonus)](#8-scheduled-notifications-bonus-feature)
10. [WebSocket status updates (bonus)](#9-websocket-status-updates-bonus-feature)

---

## 0. Setup

```sh
make up && sleep 5

# load env + the names the commands below use
set -a && source .env && set +a
export API_KEY=$DEV_API_KEY PORT=${APP_HOST_PORT:-8090}

# confirm healthy
curl -sS http://localhost:$PORT/readyz | jq -c
```

Expected:
```json
{"checks":{"postgres":"ok","rabbitmq":"ok","redis":"ok"},"ok":true}
```

**Reality — env vars.** `export API_KEY=$DEV_API_KEY` only works if `.env` is sourced into *your* shell first (it's normally loaded into the container, not your terminal). The `set -a && source .env && set +a` line handles that; a plain `export API_KEY=$DEV_API_KEY` on a fresh terminal silently sets an empty key and every request returns 401.

**Reality — provider.** `WEBHOOK_URL` points at a webhook.site UUID. Those URLs expire — a stale one makes every delivery a 404 → terminal dead-letter. If deliveries fail with `status=404`, grab a fresh URL from https://webhook.site, put it in `.env`, and `make up`.

Tail logs in a second terminal: `docker compose -f deploy/docker-compose.yml logs -f app`

---

## 1. Happy path + distributed trace

### Send one notification

```sh
KEY=$(uuidgen)
curl -sS -X POST http://localhost:$PORT/v1/notifications \
  -H "Authorization: Bearer $API_KEY" \
  -H "Idempotency-Key: $KEY" \
  -H "Content-Type: application/json" \
  -d '{"channel":"sms","recipient":"+905551234567","content":"hello","priority":"normal"}' | jq
```

Expected (`201 Created`):
```json
{
  "id": "019e6e2b-f63c-7de1-9386-7727e995fb98",
  "batch_id": null,
  "channel": "sms",
  "recipient": "+905551234567",
  "content": "hello",
  "priority": "normal",
  "status": "pending",
  "attempt_count": 0,
  "created_at": "2026-05-28T10:40:35.388974Z",
  "updated_at": "2026-05-28T10:40:35.388974Z",
  "sent_at": null,
  "correlation_id": "019e6e2b-f5e0-7e9e-8475-30c618e48531"
}
```

**Reality — `priority` is a string** (`"high"`/`"normal"`/`"low"`), not a number. `"priority": 5` returns 400 `cannot unmarshal number into ... notification.Priority`. Omit it to default to `normal`.

**Reality — initial status is `pending`**, not `queued`. It transitions `pending → queued → sending → sent`; the API returns before the dispatcher runs.

### Confirm it was delivered

```sh
ID=<id-from-above>
curl -sS "http://localhost:$PORT/v1/notifications/$ID" \
  -H "Authorization: Bearer $API_KEY" | jq '{status, attempt_count, sent_at}'
```

Expected (within ~1s):
```json
{"status":"sent","attempt_count":0,"sent_at":"2026-05-28T10:40:35.858261Z"}
```

**Reality — `attempt_count` is 0 on first-try success.** It only increments on retries.

### The logs (only two lines)

```
{"level":"INFO","msg":"http request","method":"POST","path":"/v1/notifications","status":201,"bytes":328,"dur":102792625}
{"level":"INFO","msg":"delivered","channel":"sms","notification_id":"...","correlation_id":"...","attempt":1,"provider_message_id":"","status":200}
```

**Reality — the hot path is silent.** Only the HTTP request and the final `delivered` log. The dispatcher claim, AMQP publish, consume, and CAS transitions emit **no logs** (they'd flood at production rates). `dur` is nanoseconds (`102792625` ≈ 103ms; first request is slow from cold auth-cache + warmup). To *see* the intermediate states, query Postgres:

```sh
docker compose -f deploy/docker-compose.yml exec -T postgres \
  psql -U notif -d notif -c \
  "SELECT status, attempt_count, sent_at FROM notifications WHERE id='$ID';"
```

### The distributed trace

Open http://localhost:16686, service **`notifyd`** → Find Traces → open the most recent.

Expected span tree (one trace across three execution contexts):
```
notifyd-http                 (the POST)
└─ outbox.dispatch           (dispatcher — separate loop, resumed via traceparent in the outbox row)
   └─ worker.deliver         (worker — separate goroutine, resumed via traceparent in the AMQP headers)
      ├─ query BEGIN / INSERT / COMMIT, set, evalsha, get   (CAS, inflight lock, reservoir, provider call)
```

Verify from the API (no UI):
```sh
curl -sS "http://localhost:16686/api/traces?service=notifyd&limit=5" \
  | jq -r '.data[] | select([.spans[].operationName] | any(.=="worker.deliver")) | .traceID' | head -1
```
A trace ID prints → the full chain is linked.

**Reality — sampling + flush.** `OTEL_SAMPLE_RATIO=1.0` in `.env` traces every request (drop to `0.1` for load tests). Spans batch with a 2s flush, so wait ~3s before refreshing Jaeger. If service `notifyd` isn't listed, the exporter isn't connecting — check `OTEL_EXPORTER_OTLP_ENDPOINT=jaeger:4317` and `docker compose ps jaeger`.

---

## 2. Idempotency

One key, four requests.

```sh
KEY=$(uuidgen); echo "key: $KEY"
```

### a) First — 201, no replay header

```sh
curl -sS -X POST http://localhost:$PORT/v1/notifications \
  -H "Authorization: Bearer $API_KEY" -H "Idempotency-Key: $KEY" \
  -H "Content-Type: application/json" \
  -d '{"channel":"sms","recipient":"+905551234567","content":"hello"}' \
  -D - -o /dev/null | grep -iE "HTTP/|Idempotency-Replayed"
```
```
HTTP/1.1 201 Created
```

### b) Replay — same body → 201 + `Idempotency-Replayed: true`, same id

```sh
curl -sS -X POST http://localhost:$PORT/v1/notifications \
  -H "Authorization: Bearer $API_KEY" -H "Idempotency-Key: $KEY" \
  -H "Content-Type: application/json" \
  -d '{"channel":"sms","recipient":"+905551234567","content":"hello"}' \
  -D - -o /dev/null | grep -iE "HTTP/|Idempotency-Replayed"
```
```
HTTP/1.1 201 Created
Idempotency-Replayed: true
```

### c) Shuffle — same fields, reordered → still replayed (canonical hash)

```sh
curl -sS -X POST http://localhost:$PORT/v1/notifications \
  -H "Authorization: Bearer $API_KEY" -H "Idempotency-Key: $KEY" \
  -H "Content-Type: application/json" \
  -d '{"content":"hello","recipient":"+905551234567","channel":"sms"}' \
  -D - -o /dev/null | grep -iE "HTTP/|Idempotency-Replayed"
```
```
HTTP/1.1 201 Created
Idempotency-Replayed: true
```

### d) Conflict — same key, different body → 409

```sh
curl -sS -X POST http://localhost:$PORT/v1/notifications \
  -H "Authorization: Bearer $API_KEY" -H "Idempotency-Key: $KEY" \
  -H "Content-Type: application/json" \
  -d '{"channel":"sms","recipient":"+905551234567","content":"DIFFERENT"}' | jq
```
```json
{
  "detail": "Idempotency-Key reused with a different request body",
  "instance": "/requests/019e6e2c-4e73-7a38-a9da-130bd17a8220",
  "status": 409,
  "title": "Idempotency-Key Conflict",
  "type": "/problems/idempotency-conflict"
}
```

Steps b and c return the **same notification id** as a; the conflict is `application/problem+json` (RFC 7807), `instance` = correlation id.

---

## 3. Circuit breaker

Trip the per-channel breaker by pointing the provider at an address that times out, then restore.

### Baseline

```sh
cp .env .env.good
curl -sS http://localhost:$PORT/metrics | grep 'circuit_breaker_state{channel="sms"}'
```
```
circuit_breaker_state{channel="sms"} 0
```
(0 = closed, 1 = open, 2 = half-open)

### Break the provider

Edit `.env`: `WEBHOOK_URL=http://192.0.2.1:9`, then recreate the app:
```sh
docker compose -f deploy/docker-compose.yml up -d --force-recreate app && sleep 5
```

**Reality — use `192.0.2.1` (RFC 5737 TEST-NET), NOT `127.0.0.1`.** Loopback/private IPs are rejected by the SSRF guard as a *terminal* `ssrf-blocked` error → immediate dead-letter, which doesn't trip the breaker cleanly. `192.0.2.1` is non-routable but passes the SSRF check, producing a real ~2s dial timeout (retryable) — the failure type the breaker is designed for.

### Induce failures

```sh
for i in $(seq 1 8); do
  curl -sS -X POST http://localhost:$PORT/v1/notifications \
    -H "Authorization: Bearer $API_KEY" -H "Idempotency-Key: $(uuidgen)" \
    -H "Content-Type: application/json" \
    -d "{\"channel\":\"sms\",\"recipient\":\"+905551234567\",\"content\":\"burst-$i\"}" \
    -o /dev/null -w "%{http_code} "; done; echo
```
All `201` — the API accepts; failures happen async in the worker.

Poll the breaker (each dial times out ~2s, so ~5 consecutive failures ≈ 10s):
```sh
for n in $(seq 1 15); do
  st=$(curl -sS http://localhost:$PORT/metrics | grep 'circuit_breaker_state{channel="sms"}' | awk '{print $2}')
  echo "breaker=$st"; [ "$st" = "1" ] && break; sleep 2; done
```
```
breaker=1
```

Logs show the trip:
```
{"level":"INFO","msg":"retryable failure; routing to retry tier","channel":"sms","attempt":1,"kind":2,"status":0}
{"level":"WARN","msg":"circuit breaker state change","channel":"sms","breaker":"provider:sms","from":"closed","to":"open"}
{"level":"INFO","msg":"breaker open; routing to retry tier","channel":"sms","attempt":1,"err":"circuit breaker is open"}
```
Once open, requests don't hit the provider — they route to the wait-tier without burning attempt count.

### Recover

```sh
cp .env.good .env
docker compose -f deploy/docker-compose.yml up -d --force-recreate app && sleep 5
```

Wait for the wait-tier (5s/30s) to redeliver the backlog, then:
```sh
docker compose -f deploy/docker-compose.yml exec -T postgres psql -U notif -d notif \
  -c "SELECT status, count(*) FROM notifications WHERE content LIKE 'burst-%' GROUP BY status;"
```
```
   status   | count
------------+-------
 sent       |     8
```
All burst notifications eventually deliver — the breaker shielded the provider, the wait-tier kept the messages alive.

**Reality — you can't watch `open → half-open → closed` live with the `.env` swap.** Breaker state is in-memory and not persisted, so recreating the app to restore the provider **resets the breaker to closed**. What you demonstrate is: trip → metric=1 → restore → backlog drains. To observe the half-open trial transition you'd need to flip the URL *without* restarting, which the app doesn't support (config is load-once). The transition is real (it fires automatically after the 30s open window if the provider is still down — visible as `open → half-open → open` in logs during the outage), just not after a restart.

Clean up: `rm -f .env.good`

---

## 4. Outbox

The outbox's real value here is **atomic dual-write** (notification + outbox row in one transaction) and **asynchronous dispatch**, not surviving a broker outage.

### Atomic write + dispatch

```sh
ID=$(curl -sS -X POST http://localhost:$PORT/v1/notifications \
  -H "Authorization: Bearer $API_KEY" -H "Idempotency-Key: $(uuidgen)" \
  -H "Content-Type: application/json" \
  -d '{"channel":"sms","recipient":"+905551234567","content":"outbox-atomic"}' | jq -r .id)

# the outbox row was written in the SAME transaction, not yet published:
docker compose -f deploy/docker-compose.yml exec -T postgres psql -U notif -d notif -c \
  "SELECT routing_key, (published_at IS NOT NULL) AS published FROM outbox WHERE notification_id='$ID';"
```
```
   routing_key    | published
------------------+-----------
 notification.sms | f
```

~1s later the dispatcher publishes it and the worker delivers:
```sh
docker compose -f deploy/docker-compose.yml exec -T postgres psql -U notif -d notif -c \
  "SELECT (published_at IS NOT NULL) AS published FROM outbox WHERE notification_id='$ID';
   SELECT status FROM notifications WHERE id='$ID';"
```
```
 published     status
-----------    --------
 t             sent
```

### Survives a RabbitMQ outage (running app keeps serving)

Stop the broker while the app is running — the app stays up, the API keeps accepting writes, and consumers pause and resume automatically when the broker returns.

```sh
docker compose -f deploy/docker-compose.yml stop rabbitmq

# app stays alive — livez=200 throughout:
for n in $(seq 1 6); do curl -sS -o /dev/null -w "livez=%{http_code} " http://localhost:$PORT/livez; sleep 1; done; echo

# API still accepts writes (the outbox pattern's promise):
curl -sS -X POST http://localhost:$PORT/v1/notifications \
  -H "Authorization: Bearer $API_KEY" -H "Idempotency-Key: $(uuidgen)" \
  -H "Content-Type: application/json" \
  -d '{"channel":"sms","recipient":"+905551234567","content":"during-outage"}' \
  -o /dev/null -w "POST=%{http_code}\n"

# readyz reflects the outage (orchestrator drains LB) while the process lives:
curl -sS http://localhost:$PORT/readyz | jq -c
```
```
livez=200 livez=200 livez=200 livez=200 livez=200 livez=200
POST=201
{"checks":{"postgres":"ok","rabbitmq":"unreachable: amqp connection closed","redis":"ok"},"ok":false}
```

Logs show the workers pausing (no `fatal`):
```
{"level":"WARN","msg":"consumer paused; waiting for broker reconnect","worker":"sms-7","queue":"notifications.sms"}
{"level":"WARN","msg":"amqp consumer reconnect failed","err":"... connection refused","next-in":2000000000}
```

Restart the broker — consumers resume and `readyz` flips back to `ok:true`, all without the process restarting:
```sh
docker compose -f deploy/docker-compose.yml start rabbitmq
# within ~10-15s:
#   {"level":"INFO","msg":"amqp consumer reconnected"}
#   {"level":"INFO","msg":"consumer resumed after reconnect","queue":"notifications.sms"}
curl -sS http://localhost:$PORT/readyz | jq -c     # → "ok":true
```

**How it works.** The shared `ConsumerConnection` registers a `NotifyClose` watcher that re-dials with exponential backoff (1s→30s), mirroring the publisher's reconnect ([consumer.go](../../internal/queue/consumer.go)). When a worker's delivery channel closes due to a broker drop (not a ctx cancel), `Consumer.Run` returns a recoverable sentinel; `Consumer.RunForever` waits for the connection to come back, re-opens the channel, and resumes. Unacked in-flight messages are redelivered by the broker after reconnect and deduped by the worker-side CAS + SETNX stack — no message lost.

### Boots even if RabbitMQ is down at startup

Cold start with the broker down — the app still boots and serves the API; the publisher and consumer connection retry in the background and connect when the broker appears.

```sh
docker compose -f deploy/docker-compose.yml stop rabbitmq
docker compose -f deploy/docker-compose.yml up -d --force-recreate app
sleep 4
curl -sS -o /dev/null -w "livez=%{http_code}\n" http://localhost:$PORT/livez   # → livez=200

# API accepts writes; they accumulate in the outbox:
for i in 1 2 3; do curl -sS -X POST http://localhost:$PORT/v1/notifications \
  -H "Authorization: Bearer $API_KEY" -H "Idempotency-Key: $(uuidgen)" \
  -H "Content-Type: application/json" \
  -d "{\"channel\":\"sms\",\"recipient\":\"+905551234567\",\"content\":\"coldstart-$i\"}" \
  -o /dev/null -w "%{http_code} "; done; echo
docker compose -f deploy/docker-compose.yml exec -T postgres psql -U notif -d notif -t \
  -c "SELECT count(*) FROM outbox WHERE published_at IS NULL;"   # → 3 (backlog)

# bring the broker up — the backlog drains, nothing dead-lettered:
docker compose -f deploy/docker-compose.yml start rabbitmq
```

Logs at cold start show the deferred connection, not a fatal:
```
{"level":"WARN","msg":"amqp publisher: broker unreachable at startup; will retry in background","err":"..."}
{"level":"WARN","msg":"amqp consumer connection: broker unreachable at startup; will retry in background","err":"..."}
```

**How it works.** `NewPublisher` / `NewConsumerConnection` no longer fatal on the initial dial — on failure they log a warning and start the same background reconnect loop used for mid-run drops. Workers call `ConsumerConnection.WaitReady` before opening a channel, so they park until the broker is up instead of crashing. The outbox dispatcher's publishes fail gracefully (`ErrPublisherUnavailable`) and are retried without burning attempt budget (next point).

### Broker-unavailable publishes don't dead-letter

A publish that fails because the broker is down is an *infrastructure* failure, not a delivery attempt — it must not consume a notification's retry budget. When the broker is unreachable, `Publisher.Publish` returns `ErrPublisherUnavailable`; the outbox dispatcher records it via `MarkOutboxUnpublishedRetry` (clears the claim, sets `last_error`, but does **not** increment `attempt_count`), so the row is retried indefinitely until the broker returns instead of dead-lettering after the 10-attempt cap. On the worker side, the poison-message redelivery cap is skipped while the channel's breaker is open (infra outage ≠ poison message). Net result: a multi-second broker flap during the 250ms dispatch tick no longer dead-letters healthy notifications.

---

## 5. DLQ replay + audit chain

Needs a notification in `dead_letter`. Easiest source: the terminal failures from a broken-provider burst (the breaker section, or send a burst against `http://127.0.0.1:9` which dead-letters immediately as `ssrf-blocked`). Then:

### List the DLQ

```sh
curl -sS "http://localhost:$PORT/v1/admin/dead-letters" \
  -H "Authorization: Bearer $API_KEY" | jq '{count:(.items|length), first:.items[0]}'
```
```json
{
  "count": 10,
  "first": {
    "id": 10,
    "notification_id": "019e6e30-7b46-7657-a4af-71691942beb0",
    "reason": "send (ssrf-blocked, status=0): Post \"http://127.0.0.1:9\": ...",
    "dlq_at": "2026-05-28T10:47:02.006151Z",
    "payload": { "channel":"sms","content":"burst-8","recipient":"+905551234567", ... }
  }
}
```
**Reality — DLQ `id` is a bigint** (`10`), not a UUID. The list is under `items[]`.

### Replay one

```sh
NID=<notification_id-from-the-list>
curl -sS -X POST "http://localhost:$PORT/v1/admin/dead-letters/$NID/replay" \
  -H "Authorization: Bearer $API_KEY" | jq
```
```json
{"notification_id":"019e6e30-7b46-7657-a4af-71691942beb0","new_status":"pending"}
```

Within ~1s (provider must be healthy) it re-delivers:
```sh
curl -sS "http://localhost:$PORT/v1/notifications/$NID" \
  -H "Authorization: Bearer $API_KEY" | jq '{status, attempt_count, sent_at}'
```
```json
{"status":"sent","attempt_count":0,"sent_at":"2026-05-28T10:52:20.07234Z"}
```
Same notification id (reset in place), failure history preserved in `delivery_attempts`.

### The audit chain recorded it

```sh
curl -sS "http://localhost:$PORT/v1/admin/audit/verify" \
  -H "Authorization: Bearer $API_KEY" | jq
```
```json
{"broken_links":0,"intact":true}
```

Inspect the chain — the replay is the newest row, its `prev_hash` equals the prior row's `row_hash`:
```sh
docker compose -f deploy/docker-compose.yml exec -T postgres psql -U notif -d notif -c \
  "SELECT id, actor, action, target_id,
          substr(encode(prev_hash,'hex'),1,12) AS prev12,
          substr(encode(row_hash,'hex'),1,12)  AS row12
     FROM admin_audit ORDER BY id DESC LIMIT 3;"
```
```
 id |  actor   |   action   | target_id  |    prev12    |    row12
----+----------+------------+------------+--------------+--------------
  2 | dev-seed | dlq_replay | 019e6e30-… | b96b6658a414 | 026e01a5e358
  1 | dev-seed | key_revoke | 11111111-2222-… | 000000000000 | b96b6658a414
```
Row 2's `prev12` == row 1's `row12` → chain linked. Row 1's `prev_hash` is genesis zeros.

### Prove tamper detection

```sh
# corrupt a row's content in place (does NOT recompute the hash)
ORIG=$(docker compose -f deploy/docker-compose.yml exec -T postgres psql -U notif -d notif -t -A \
  -c "SELECT target_id FROM admin_audit WHERE id=1;")
docker compose -f deploy/docker-compose.yml exec -T postgres psql -U notif -d notif -c \
  "UPDATE admin_audit SET target_id='99999999-9999-9999-9999-999999999999' WHERE id=1;"

curl -sS "http://localhost:$PORT/v1/admin/audit/verify" -H "Authorization: Bearer $API_KEY" | jq
#   {"broken_links":1,"intact":false}

# restore
docker compose -f deploy/docker-compose.yml exec -T postgres psql -U notif -d notif -c \
  "UPDATE admin_audit SET target_id='$ORIG' WHERE id=1;"
curl -sS "http://localhost:$PORT/v1/admin/audit/verify" -H "Authorization: Bearer $API_KEY" | jq
#   {"broken_links":0,"intact":true}
```

**Reality — tamper a column that's free of CHECK constraints.** `action` has a CHECK constraint (an arbitrary value is rejected before it can corrupt the chain), so use `target_id` (a uuid in the canonical hash). The content check (migration 0012) recomputes `row_hash` from the row's canonical bytes, so even an in-place edit that doesn't touch the hash columns is caught.

**Reality — restore the *exact* seeded value, or the chain stays broken.** Row 1's seeded `target_id` is `11111111-2222-3333-4444-555555555555` (not all-1s — the truncated `11111111-…` above is the start of that value). The `ORIG=$(... SELECT target_id ...)` capture above handles this automatically; only matters if you restore by hand. A wrong restore leaves `verify` reporting `intact:false`.

---

## 6. Load test + pprof

Requires `k6` and `PPROF_ENABLED=true`.

```sh
sed -i '' 's/^PPROF_ENABLED=false/PPROF_ENABLED=true/' .env   # macOS sed
docker compose -f deploy/docker-compose.yml up -d --force-recreate app && sleep 5
curl -sS -o /dev/null -w "pprof=%{http_code}\n" http://localhost:$PORT/debug/pprof/   # → 200
```

### Run the baseline + capture a CPU profile

```sh
# capture a 30s CPU profile a few seconds into the run
( sleep 4 && make profile-cpu & )
make load-baseline
```

Real k6 result (50 req/s × 30s, dev key, local stack):
```
  checks_total.......: 1501   50.03/s
  checks_succeeded...: 100.00% 1501/1501
  http_req_duration..: avg=2.02ms med=1.48ms p(90)=2.02ms p(95)=2.32ms max=125.55ms
  http_req_failed....: 0.00%  0/1501
  ✓ 'p(99)<500' p(99)=3.38ms
```
**Reality — p99 ≈ 3.4ms, not the ~150ms** older docs claimed. The create path is cheap; the dominant cost is downstream (provider call), off the request path.

### Inspect the profile

```sh
go tool pprof -top -nodecount=10 loadtest/profiles/cpu.pprof
```
```
      flat  flat%        cum   cum%
     1.29s 20.16%      1.29s 20.16%  internal/runtime/syscall/linux.Syscall6   (network I/O)
     0.53s  8.28%      0.53s  8.28%  runtime.futex                             (goroutine contention)
     0.52s  8.12%      0.67s 10.47%  golang.org/x/crypto/argon2.processBlockGeneric
     0.15s  2.34%      0.37s  5.78%  runtime.pcvalue
     ...
     0.07s  1.09%      0.23s  3.59%  go.opentelemetry.io/otel/.../tracetransform.span
```
Or open the flame graph: `go tool pprof -http=:6060 loadtest/profiles/cpu.pprof`

**Reality — argon2 DOES appear (~13% cum).** Older notes said the auth cache makes argon2 invisible under load; in this run it sits near the top. With 100% trace sampling the OTel exporter also shows up. Top of profile: syscalls (network), then futex, then argon2.

### Restore

```sh
sed -i '' 's/^PPROF_ENABLED=true/PPROF_ENABLED=false/' .env
docker compose -f deploy/docker-compose.yml up -d --force-recreate app
```

---

## 7. Templates (bonus feature)

Templates are stored with `{{ .Var }}` placeholders (Go `text/template`); required vars are auto-extracted on create, and content is rendered **at create time** ([ADR-0010](../../docs/adr/0010-template-render-at-create-time.md)) so retries reproduce identical bytes and later template edits don't rewrite already-sent notifications.

### Create a template

```sh
TPL=$(curl -sS -X POST http://localhost:$PORT/v1/templates \
  -H "Authorization: Bearer $API_KEY" -H "Content-Type: application/json" \
  -d '{"name":"welcome","channel":"email","body":"Hello {{ .Name }}, your code is {{ .Code }}."}')
echo "$TPL" | jq
TPL_ID=$(echo "$TPL" | jq -r .id)
```
```json
{
  "id": "91d7a749-fbcc-4a63-a9ab-afd7d13be465",
  "name": "welcome",
  "channel": "email",
  "version": 1,
  "body": "Hello {{ .Name }}, your code is {{ .Code }}.",
  "required_vars": ["Code", "Name"],
  "created_at": "2026-05-28T11:39:23.129955Z",
  "updated_at": "2026-05-28T11:39:23.129955Z"
}
```
`required_vars` are parsed out of the body automatically; `version` starts at 1.

### Send via the template

```sh
curl -sS -X POST http://localhost:$PORT/v1/notifications \
  -H "Authorization: Bearer $API_KEY" -H "Idempotency-Key: $(uuidgen)" \
  -H "Content-Type: application/json" \
  -d "{\"channel\":\"email\",\"recipient\":\"user@example.com\",\"template_id\":\"$TPL_ID\",\"variables\":{\"Name\":\"Mert\",\"Code\":\"482913\"}}" \
  | jq '{id, content, template_id, template_version, status}'
```
```json
{
  "id": "019e6e61-ef9e-769a-9f34-bf688fa654da",
  "content": "Hello Mert, your code is 482913.",
  "template_id": "91d7a749-fbcc-4a63-a9ab-afd7d13be465",
  "template_version": 1,
  "status": "pending"
}
```
Content is rendered server-side; `template_version` is stamped on the notification so you always know which version produced it.

### Missing variable → 400

```sh
curl -sS -X POST http://localhost:$PORT/v1/notifications \
  -H "Authorization: Bearer $API_KEY" -H "Idempotency-Key: $(uuidgen)" \
  -H "Content-Type: application/json" \
  -d "{\"channel\":\"email\",\"recipient\":\"user@example.com\",\"template_id\":\"$TPL_ID\",\"variables\":{\"Name\":\"Mert\"}}" | jq
```
```json
{
  "detail": "one or more fields failed validation",
  "errors": [{"field": "variables.Code", "message": "missing required variable"}],
  "status": 400,
  "title": "Validation Failed",
  "type": "/problems/validation"
}
```

---

## 8. Scheduled notifications (bonus feature)

A notification with a future `scheduled_at` is held in `scheduled` status (and a `scheduled_notifications` due-time row) until the scheduler poller promotes it at due time. Requires `SCHEDULER_ENABLED=true` (the default).

### Create with a future `scheduled_at`

```sh
# 8 seconds out (macOS date; on Linux use: date -u -d '+8 seconds' +%Y-%m-%dT%H:%M:%SZ)
DUE=$(date -u -v+8S +%Y-%m-%dT%H:%M:%SZ)
RESP=$(curl -sS -X POST http://localhost:$PORT/v1/notifications \
  -H "Authorization: Bearer $API_KEY" -H "Idempotency-Key: $(uuidgen)" \
  -H "Content-Type: application/json" \
  -d "{\"channel\":\"email\",\"recipient\":\"user@example.com\",\"content\":\"scheduled hello\",\"scheduled_at\":\"$DUE\"}")
echo "$RESP" | jq '{id, status, scheduled_at}'
SID=$(echo "$RESP" | jq -r .id)
```
```json
{"id":"019e6e62-6293-784d-aff6-e977e6ee7f38","status":"scheduled","scheduled_at":"2026-05-28T11:40:10Z"}
```

Immediately, the row is `scheduled` and there's a due-time row:
```sh
docker compose -f deploy/docker-compose.yml exec -T postgres psql -U notif -d notif -c \
  "SELECT status FROM notifications WHERE id='$SID';
   SELECT due_at FROM scheduled_notifications WHERE id='$SID';"
```
```
  status   |         due_at
-----------+------------------------
 scheduled | 2026-05-28 11:40:10+00
```

### At due time the scheduler promotes it

Poll past the due time:
```sh
for n in $(seq 1 8); do sleep 2
  docker compose -f deploy/docker-compose.yml exec -T postgres psql -U notif -d notif -t -A -c \
    "SELECT status FROM notifications WHERE id='$SID';"
done
```
The status goes `scheduled → pending → queued → sent`, and the `scheduled_notifications` row is deleted once promoted. The scheduler logs exactly at due time:
```
{"level":"INFO","msg":"scheduler dispatched","id":"019e6e62-6293-784d-aff6-e977e6ee7f38"}
```

**Reality — rejected when the scheduler is off.** If `SCHEDULER_ENABLED=false`, a `scheduled_at` request is rejected at create with a 400 (`scheduler is disabled on this deployment`), rather than silently stranding the row in `scheduled` forever.

---

## 9. WebSocket status updates (bonus feature)

The worker publishes a status event to Redis Pub/Sub (`events:notifications`) on terminal outcomes (`sent`, `dead_letter`); the WS hub fans them out to connected subscribers, applying a per-key owner filter ([ADR-0019](../../docs/adr/0019-per-key-row-ownership.md)) — non-admin keys only see their own notifications' events.

### Connect and observe events

`websocat` 1.14.1 has an invocation bug here (`os error 22`); a tiny Python client is reliable:

```sh
cat > /tmp/ws_client.py <<'PY'
import asyncio, os, websockets
API_KEY=os.environ["API_KEY"]; PORT=os.environ["PORT"]
async def main():
    uri=f"ws://localhost:{PORT}/v1/ws/notifications"
    async with websockets.connect(uri, additional_headers={"Authorization": f"Bearer {API_KEY}"}) as ws:
        print("CONNECTED", flush=True)
        try:
            while True:
                print("EVENT", await asyncio.wait_for(ws.recv(), timeout=15), flush=True)
        except asyncio.TimeoutError:
            print("DONE", flush=True)
asyncio.run(main())
PY
python3 /tmp/ws_client.py &        # needs: pip install websockets
sleep 2

# send a couple notifications while the socket is open
for i in 1 2; do curl -sS -X POST http://localhost:$PORT/v1/notifications \
  -H "Authorization: Bearer $API_KEY" -H "Idempotency-Key: $(uuidgen)" \
  -H "Content-Type: application/json" \
  -d '{"channel":"push","recipient":"device-token-abcdefghij123456","content":"ws-test"}' \
  -o /dev/null -w "%{http_code} "; done; echo
```

The client prints a JSON event per delivery:
```
CONNECTED
EVENT {"notification_id":"019e6e6f-4d05-...","channel":"push","status":"sent","at":"2026-05-28T11:54:08.689Z","correlation_id":"019e6e6f-4d04-...","created_by":"4212ff01-..."}
EVENT {"notification_id":"019e6e6f-4cf3-...","channel":"push","status":"sent",...}
```

The server logs the accepted subscription with its resolved filter:
```
{"level":"INFO","msg":"ws connection accepted","sub_id":1,"filter":{"OwnerID":"4212ff01-...","AdminBypass":true}}
```

**Optional filters** via `?filter=` (AND-composed): `channel:sms`, `batch_id:<uuid>`. Example: `ws://localhost:$PORT/v1/ws/notifications?filter=channel:push`.

**Reality — events fire on terminal states only.** You get `sent` and `dead_letter` events, not the intermediate `queued`/`sending` transitions. The owner filter means a non-admin key sees only its own events; the dev key has admin scope (`AdminBypass:true`) so it sees all.

---

## Summary of corrections vs. the old per-demo docs

| Claim in old docs | Verified reality |
|---|---|
| `"priority": 5` | Must be a string: `"normal"` |
| Initial status `queued` | `pending` (then queued→sending→sent) |
| Per-step worker logs (`"provider call"`, `"delivery succeeded"`) | Don't exist; only `"delivered"` on success |
| `"outbox dispatcher tick"` / `"amqp publish"` logs | Don't exist; hot path is silent |
| `readyz` key `amqp` | `rabbitmq` |
| `notifications_retry_total` metric | Doesn't exist |
| Breaker recovery visible after `.env` swap | Restart resets the in-memory breaker |
| Broken provider `127.0.0.1` | SSRF-blocked terminal error; use `192.0.2.1` for a clean timeout |
| API survives RabbitMQ outage; backlog drains | **Now true** — fixed: running app stays up serving 201, consumer auto-reconnects + resumes; cold start with broker down also boots; broker-unavailable publishes no longer dead-letter (was: crash-loop). Fix added `ConsumerConnection`/publisher background reconnect, `RunForever`, `WaitReady`, `ErrPublisherUnavailable` + `MarkOutboxUnpublishedRetry` |
| DLQ `id` is a UUID | bigint |
| Tamper `action` column | CHECK-constrained; tamper `target_id` |
| Load p99 ~150ms; argon2 invisible | p99 ~3.4ms; argon2 ~13% of CPU |
| webhook.site as the provider | Free tier hard rate-limits (429) under demo traffic; use a stable always-200 endpoint (e.g. `https://postman-echo.com/post`) for repeatable demos |
| Bonus features (template/scheduled/websocket) undocumented | Now covered as sections 7–9, verified hands-on |
| push `recipient` any string | Must be a device token 16–512 chars |
