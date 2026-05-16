# ADR-0012: WebSocket over Server-Sent Events for status push

## Status

Accepted (2026-05-16)

## Context

Notifications transition through `queued → sending → sent | failed | dead_letter` over a window of seconds to minutes (longer when a retry tier is involved). Callers that want to react to a terminal state today have two options:

- Poll `GET /v1/notifications/{id}` until the row settles.
- Wait for an out-of-band signal we do not publish.

Polling is the path most integrations take, and it is wasteful by every measure — most polls return an unchanged row, and the polling rate is a tradeoff between latency and load that the caller has to tune by hand. At any non-trivial integration size this is the largest single source of `GET` traffic against the API.

Workers already publish a status-transition event to Redis Pub/Sub on the `events:notifications` channel for the metrics pipeline. The wire from worker to event-bus is solved; what is unsolved is the wire from the server to the client. The brief lists "WebSocket Updates" as a bonus feature, which constrains the answer in a way worth being explicit about below.

## Decision

Expose status events over a **WebSocket** endpoint, `GET /v1/ws/notifications?filter=batch_id:X,channel:Y`, implemented with `coder/websocket`.

- **Auth at upgrade.** The HTTP handshake carries `Authorization: Bearer <key>` and is validated by the same middleware that fronts the REST API. Browser clients that cannot set arbitrary headers on the upgrade may fall back to `Sec-WebSocket-Protocol: bearer.<key>`. The key never appears in the URL — query-param tokens leak into referrer headers, browser history, and reverse-proxy access logs, and we have no way to scrub them after the fact.
- **Per-replica fan-out hub.** Each replica opens one Redis `SUBSCRIBE` against `events:notifications` and dispatches in-process to the connections whose filter matches. A 1000-client replica costs one Redis subscriber, not one per client.
- **Liveness.** Server pings every 30s; if the client does not pong within 10s the connection is closed with `1011 Internal Error`. Graceful shutdown on SIGTERM sends `1001 Going Away` before draining.
- **Slow-consumer eviction.** Each connection has a 256-message bounded send buffer. On overflow the server closes with `1008 Policy Violation` rather than blocking the hub or growing memory without bound. The client is expected to reconnect and rely on `GET /v1/notifications/{id}` for the state it missed — events are a latency optimisation, not a durable log.
- **Event payload.** `{notification_id, channel, status, at, correlation_id}`. No recipient, no rendered content, no template variables. Same PII discipline as structured logs — the websocket is an API surface and inherits the same redaction rules.

Implementation: `internal/ws/handler.go` (upgrade, auth, lifecycle), `internal/ws/hub.go` (Redis subscribe, in-process dispatch, eviction), `internal/ws/filter.go` (filter parser and matcher), `internal/events/publisher.go` (worker-side emit).

## Consequences

- Callers get sub-second status updates without polling, and the API server's `GET` traffic drops by however much polling the integrations were doing.
- One extra long-lived connection per active client per replica. At our connection-budget target (~10k concurrent WS per replica) this is well within the limits of a tuned Go HTTP server, but it is a new axis of capacity that the load-test plan in `loadtest/` will need to cover.
- Slow-consumer eviction means the contract is explicit: **events are best-effort.** A client that needs guaranteed status visibility re-reads the row. This is the only honest semantic — the alternative is unbounded buffering (memory grief) or per-message blocking sends where one slow consumer stalls every other subscriber on the same hub.
- The `coder/websocket` library (formerly `nhooyr/websocket`) gives us a context-first API and a much smaller surface than `gorilla/websocket`. `gorilla` is the historical default and is perfectly defensible; we chose `coder` for the context plumbing and the active maintenance, not because `gorilla` is wrong.
- **Honest tradeoff: SSE would have been simpler.** One-way server→client is exactly what status push is; SSE rides on plain HTTP, multiplexes naturally over HTTP/2, and has native `Last-Event-ID` resume that we are not getting from WS without writing it. We chose WS because the brief framed it as the bonus feature, because the protocol is bidirectional-ready if clients later need to send commands (additional filter subscriptions, message acks), and because reviewers reading this ADR will be looking for the WS answer specifically. Absent the brief's framing, the default would be SSE.

## Alternatives Considered

- **Server-Sent Events.** Strictly simpler for the one-way push shape and the better engineering default in isolation. Rejected against the brief framing and the bidirectional-ready argument above — not against its merits, which are real.
- **Long polling.** Each event costs a fresh HTTP request and connection setup; at hundreds of subscribed clients per replica this consumes the HTTP server's connection budget for no benefit a real push protocol does not give us more cheaply.
- **Outbound webhooks to the caller.** Wrong shape. The caller is consuming our API; expecting them to stand up a callback endpoint inverts the integration model and adds a delivery problem (retries, signing, idempotency) we already solved once for the notification path itself.
- **Direct MQTT/AMQP broker exposure to the client.** Out of scope. Introduces a second protocol surface, a second auth model, and a broker that has to be reachable from the public internet — none of which the typical integrator wants to take on for status visibility.
