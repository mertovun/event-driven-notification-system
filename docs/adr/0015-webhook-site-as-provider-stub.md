# ADR-0015: webhook.site as the only provider; no real SMS/Email/Push integrations

## Status

Accepted (2026-05-16)

## Context

The assessment brief names `webhook.site` as the external delivery target for every channel: the worker is to `POST https://webhook.site/{uuid}` with `{to, channel, content}` and treat the `202 Accepted` response as successful submission to the provider. We do not integrate with Twilio (SMS), SES/SendGrid (Email), or FCM/APNs (Push). The question this ADR answers is not "should we integrate real providers" — the brief settles that — but "where in the code is the seam, and what does the provider call actually exercise, given that the endpoint is a stub?"

We want the provider call to be a real HTTP call against a real (if simulated) endpoint. Replacing it with an in-process no-op would hide the failure modes that matter most in production: connection management, timeout layering, retry classification, and SSRF posture on outbound calls.

## Decision

**Single configurable outbound endpoint, `WEBHOOK_URL`, pointing at any URL that accepts `POST application/json {to, channel, content}` and returns `202 Accepted {messageId, status, timestamp}`.**

The endpoint is treated as the provider, not as a test double — the worker has no code path that bypasses it.

- `internal/provider/httpclient.go` owns one hardened `*http.Client` — never `http.DefaultClient`. Timeouts are explicit at every layer: `Client.Timeout=10s`, `Transport.ResponseHeaderTimeout=5s`, `Transport.TLSHandshakeTimeout=5s`, `Dialer.Timeout=2s`. No layer relies on the next to bound it.
- SSRF guard via `(*net.Dialer).Control`: the resolved IP is rejected if it falls in loopback, link-local, RFC 1918, CGNAT (100.64.0.0/10), IPv6 unique-local, or the cloud metadata ranges (169.254.169.254, fd00:ec2::254). Production webhook URLs resolve to public addresses; anything else is refused at dial time.
- Error classification kept in one place: 2xx is success; 5xx / 408 / 429 / context-deadline / `net.Error.Timeout()` are **retryable**; 4xx-other is **terminal**. The worker retry loop reads only this classification, not the raw error.
- The interface lives consumer-side: `worker.ProviderClient` is declared in `internal/worker/`, and `*provider.HTTPClient` happens to satisfy it. The provider package does not import the worker package and does not know an interface exists. This is the Go-idiomatic placement and is what makes the swap-in path below mechanical.

For local development and load testing: webhook.site, or `httpbin.org/post` when webhook.site is rate-limiting. For integration tests: `httptest.Server` with the SSRF guard temporarily disabled via a test-only constructor, so we can assert against `127.0.0.1` listeners.

## Consequences

- The provider call is genuinely exercised on every notification: TLS handshake, header timeout, body read, status classification. The bugs we care about in this layer surface against webhook.site exactly as they would against a real provider.
- webhook.site does not simulate provider failure modes — it returns 200/202 for everything. Our retry classifier, backoff calculator, and DLQ handoff are therefore exercised against `httptest.Server` in integration tests (deterministic 429, 503, slow-body, connection-reset cases), not against the live stub. Production-readiness of the retry path is asserted in tests; webhook.site only proves the happy path end-to-end.
- Plugging in a real provider is a change of known shape:
  - add `internal/provider/twilio.go` implementing `worker.ProviderClient`;
  - switch the constructor in `internal/worker/manager.go` to the new type;
  - add Twilio credentials to `internal/config`;
  - add a per-provider rate limiter alongside the per-channel one — Twilio's account-level quotas are independent of our internal SMS rate limit and need their own token bucket.
  Nothing in the worker scheduling, queue, or API layer changes.
- The runtime image already supports real HTTPS providers without modification — `distroless-static:nonroot` ships the CA bundle (see ADR-0007). When we adopt Twilio or SES, no Dockerfile change is required.
- One environment variable (`WEBHOOK_URL`) replaces what would otherwise be per-channel provider configuration. Operationally simple now; the channel-aware fan-out will appear in the worker manager when (and only when) a second provider lands.

## Alternatives Considered

- **Integrate Twilio + SES + FCM/APNs for real.** Out of scope per the brief, and each is a multi-day undertaking on its own: per-provider auth (Twilio account SID / auth token, SES SigV4, FCM service-account JWTs, APNs p8 certificates), per-provider response shapes, per-provider rate semantics (Twilio is per-account, SES has sandbox-vs-production modes, FCM has per-project quotas), per-provider retry advice. Adding even one would outweigh the rest of the system in code and configuration volume. The `ProviderClient` interface is the deliberate boundary that lets us defer this without painting ourselves into a corner.
- **Mock provider for tests, real providers in production.** The brief explicitly names webhook.site. Substituting real providers behind its back is not what we were asked to deliver.
- **In-process no-op provider (skip the HTTP call entirely).** Faster, but it hides the failure surface — HTTP client configuration, timeout layering, SSRF defense, retry classification — that is the most likely source of production incidents in the delivery path. We pay the cost of a real outbound call on purpose.
- **`WEBHOOK_URL` per channel (`WEBHOOK_URL_SMS`, `WEBHOOK_URL_EMAIL`, ...).** Premature. The brief specifies one endpoint, and a single `WEBHOOK_URL` keeps the worker manager free of channel-specific provider construction. When a real second provider appears, the dispatch table lands in the manager alongside it, not as four parallel env vars that all point at the same stub today.
