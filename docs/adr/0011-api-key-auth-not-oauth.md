# ADR-0011: API key auth with scopes, not OAuth/OIDC

## Status

Accepted (2026-05-16)

## Context

The notification system exposes an HTTP API that must authenticate every caller and authorize per route. The caller population is narrow and well-defined: internal services in the same operator estate, plus a small set of operator teams driving the admin surface. There are no end users, no browser-based login flows, no third-party app developers. The threat model is "an unauthenticated request reaches a privileged route," not "a confused-deputy delegation across organizations."

Authorization is also coarse. Reads (`GET /v1/notifications`, status, history) are one privilege class; writes (`POST /v1/notifications`, batch, schedule) are another; operational endpoints (rotate provider keys, replay dead-letters, force-flush outbox) are a third. Anything finer than that is speculation about future requirements we have not been asked to meet.

## Decision

**API key in `Authorization: Bearer <key>` header, validated by middleware, gated by three scopes: `notifications:read`, `notifications:write`, `admin`.**

- Keys are minted out-of-band. A seed migration adds one dev key for local work; the production minting path is an `admin`-scoped endpoint (and a matching CLI subcommand) that returns the plaintext once and never again.
- Storage: an `api_keys` table with the **argon2id hash** of the key (parameters per ADR-0009), a non-secret `prefix` column (first 8 chars of the plaintext) for fast candidate lookup, `scopes text[]`, `created_at`, `revoked_at`, `last_used_at`.
- Auth middleware: parse the `Authorization` header, narrow to one row via the prefix index, verify the argon2id hash, attach an `authedKey` value (id, scopes, prefix) to the request context. On any failure, 401 `application/problem+json` with no detail about which step failed.
- Per-route authorization via a `RequireScope(scope)` middleware composed at mount time in `internal/api/router.go`. Admin routes are bare-minimum: nothing reaches them without `admin` in `authedKey.scopes`.

Implementation: `internal/api/auth.go` (middleware, scope check, hashing helpers).

## Consequences

- One credential type, one verification path, no token-refresh choreography. Reading the auth code is a five-minute exercise.
- Every request hits Postgres for the verify. The prefix index narrows to one row; argon2id verify is ~50ms at our chosen parameters. Acceptable for our throughput target, and the cost is the same cost we already pay for any DB-backed feature on the hot path.
- API keys are static long-lived secrets. If a key leaks, full revocation is a single `UPDATE api_keys SET revoked_at = now()`. Because every request re-reads the row, revocation takes effect on the next request — there is no JWT-style "wait for token expiry" window. We treat this as an active mitigation, not a downside.
- The dev seed key is **not a security boundary**. It is documented as such in `README.md` and `docs/local-development.md`; production deploys must rotate it before exposing the API. A startup check logs a warning if the seed key's prefix is still present in `api_keys` and `APP_ENV != "development"`.
- Three scopes is intentionally coarse. Fine-grained scopes (`notifications:create:high-priority`, `notifications:read:own`) are YAGNI for this assessment, and adding them later is additive — existing keys keep their broader scopes, new keys can be minted with narrower ones.
- Upgrade path to OAuth/OIDC is non-disruptive. A JWKS-fetching middleware would sit in front of the existing `RequireScope` chain, populate `authedKey` from validated JWT claims, and every route would keep working. The `authedKey` context value is the seam — today it carries an API key id and scopes, tomorrow it carries a `jwt.Claims` struct with the same `Scopes []string` field.
- `last_used_at` is stamped asynchronously (best-effort write on a buffered channel, dropped under pressure) so we never make a key's audit metadata block its own request path. Stale-by-a-few-seconds is fine for forensics; head-of-line blocking on a metadata UPDATE is not.
- Constant-time comparison is moot: argon2id verify is already constant-time by construction, and the prefix lookup is a database index probe whose timing leaks at most "a key with this prefix exists," which is not a useful signal to an attacker.

## Alternatives Considered

- **OAuth 2.0 / OIDC with an external issuer (Auth0, Keycloak, Cognito).** The right answer once we have end users authenticating through a browser, federated identity, or third-party apps requesting delegated access. For service-to-service calls with a small operator population, it adds an issuer dependency, JWKS rotation, token-refresh logic, and a claims-to-scope mapping layer — all to authenticate callers who could equally well send us a static key. We will revisit when the caller surface changes shape.
- **mTLS / client certificates.** Cryptographically excellent and the right call inside a fully-meshed service environment. Operationally expensive at our scope: cert distribution to every caller (including hand-`curl`'d operator scripts and customer SDK integrations), rotation tooling, and a revocation path (CRL or OCSP) that is non-trivial to run. Defer until we are inside a service mesh that does this for us.
- **JWT (signed bearer tokens carrying claims).** JWT's value is statelessness — verify the signature, trust the claims, skip the DB lookup. Our DB lookup is one indexed row plus one argon2 verify, which is not the bottleneck we are optimizing. Adopting JWT would still require an issuer, signing keys, key rotation, and a revocation story (since stateless tokens cannot be revoked before expiry without re-introducing a DB check). It buys little and costs the same surface area as OAuth.
- **No auth, "the private network is the boundary."** Rejected on principle. Defense in depth says authenticate at the API regardless of where the API sits. The cost of the middleware is negligible; the cost of a misconfigured firewall rule is total.
