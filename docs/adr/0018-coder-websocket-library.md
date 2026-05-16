# ADR-0018: coder/websocket over gorilla/websocket

## Status

Accepted (2026-05-16)

## Context

ADR-0014 settles the wire protocol for status push as WebSocket; this ADR settles the library underneath it. The two well-established Go options are `github.com/gorilla/websocket` and `github.com/coder/websocket` (formerly `github.com/nhooyr/websocket`). Both implement RFC 6455 + RFC 7692 (permessage-deflate). Both are production-grade. The choice is real and small enough that we owe it an explicit record rather than burying it under "we used a library."

Two implementation files are in scope: `internal/ws/handler.go` (the HTTP upgrade handler + per-connection read/write pumps) and `internal/ws/hub.go` (subscriber registry + Redis Pub/Sub fan-out, per ADR-0014). The library shows up in both, and the API shape leaks into the goroutine model we write around it.

## Decision

**`github.com/coder/websocket` v1.8.x.**

The concrete usage patterns the rest of the code is built around:

- **Upgrade.**
  `cws.Accept(w, r, &cws.AcceptOptions{Subprotocols: []string{"bearer"}, CompressionMode: cws.CompressionDisabled})`.
  We disable permessage-deflate to keep CPU off the hot path; payloads are small JSON objects and compression buys nothing against the framing overhead.
- **Writes are bounded by context, not by deadlines.**
  `ctx, cancel := context.WithTimeout(ctx, WriteTimeout); defer cancel(); conn.Write(ctx, cws.MessageText, body)`.
  There is no `SetWriteDeadline` to remember; the timeout is local to the call and the surrounding goroutine's cancellation cascades through it.
- **Heartbeats.**
  `conn.Ping(ctx)` every 30s with `PongDeadline=10s` enforced via a `context.WithTimeout` around the ping.
  A failed ping returns an error and the read pump exits — no parallel deadline plumbing.
- **Explicit close codes.**
  `conn.Close(cws.StatusPolicyViolation, "slow_consumer")` on send-buffer overflow;
  `cws.StatusGoingAway` on SIGTERM;
  `cws.StatusInternalError` on unrecoverable read errors.
  The reason string is short and machine-readable for client-side reconnect logic.
- **Browser auth fallback.**
  Browser `new WebSocket(url)` cannot set `Authorization` headers — a real and well-known browser limitation, not a library problem.
  We accept `Sec-WebSocket-Protocol: bearer.<token>` as a fallback per ADR-0014; `cws.AcceptOptions.Subprotocols` makes the negotiation one line.

## Consequences

- **Context-first API.**
  Every blocking call takes `context.Context`.
  Cancellation flows naturally from the connection's lifecycle context down through reads, writes, and pings, which is the same plumbing we use everywhere else in the codebase (`pgx/v5`, `redis/go-redis/v9`, the worker pool).
  Gorilla's `SetReadDeadline` / `SetWriteDeadline` model is workable but requires manual deadline juggling and a second source of truth for "is this connection still alive."
- **Smaller surface area.**
  Roughly half the public methods of gorilla.
  Easier to audit, easier to mock at the test boundary, fewer footguns we have to write a wrapper to hide.
- **Standards-strict defaults.**
  Closer to RFC 6455 + RFC 7692 by default; gorilla has historically been more permissive about non-spec input (notably around close-frame handling and continuation framing), which is fine until it isn't.
- **Honest tradeoff: smaller community.**
  Fewer Stack Overflow hits, fewer "how do I do X with websockets in Go" tutorials pointing at our library.
  The trade we accept for the API quality.
  The library is small enough and well-documented enough that the godoc has answered every question we have asked of it.
- **Maintenance.**
  `coder/websocket` is actively maintained and used by `coder/code-server` among others.
  `gorilla/websocket` is also actively maintained today under the Gorilla collective, but had a ~2-year maintenance pause before that handover.
  The pause is in the past; we mention it as part of why the broader Go ecosystem has been drifting toward newer context-aware libraries (`redis/go-redis/v9`, `pgx/v5`, `coder/websocket`) rather than as a live concern.

## Alternatives Considered

- **`gorilla/websocket`.** Mature, the historical default, and perfectly defensible — most Go WebSocket tutorials still point here. Rejected on API ergonomics, not correctness: deadline-based blocking calls vs. context-based, larger surface area, and a defaults posture that is slightly looser than we want. If `coder/websocket` did not exist, gorilla would be the obvious pick and this ADR would be one paragraph.
- **`golang.org/x/net/websocket`.** The standard-library-adjacent option, and the wrong one. Explicitly discouraged by its own godoc in favour of third-party libraries; lacks permessage-deflate, lacks the close-code ergonomics we rely on, and has not seen meaningful API work in years.
- **Hand-rolled WebSocket framing on `net/http.Hijacker`.** Considered for the duration of one paragraph. Reimplementing RFC 6455 framing, masking, fragmentation, and close-handshake state machines is not engineering we should be doing on a notification system; the two libraries above have absorbed years of edge-case fixes we would otherwise rediscover under production load.
