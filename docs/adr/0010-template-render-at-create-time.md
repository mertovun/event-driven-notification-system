# ADR-0010: Templates render at create-time, not delivery-time

## Status

Accepted (2026-05-16)

## Context

`POST /v1/notifications` accepts either a raw `content` field or the pair `template_id` + `variables`. Both shapes have to converge on the same `notifications` row before the outbox publishes anything. The open question is *when* variable substitution runs: synchronously inside the API handler, or lazily inside the worker just before the provider call.

Either placement renders the same bytes on the happy path. They differ on retry, on audit, and on where errors surface to the caller — which is to say, they differ on every path that matters in production.

## Decision

**Variable substitution runs in the API handler, before the `notifications` row is committed.** The worker never sees a template; it sees rendered bytes and pushes them at the provider.

When `POST /v1/notifications` carries `template_id` + `variables`, the handler:

1. Loads the template row via `internal/store/queries/templates.sql`.
2. Rejects with 400 if the template is deprecated (`deprecated_at IS NOT NULL`).
3. Verifies every entry in `required_vars` is present in the request payload; missing keys produce a 400 with the offending name in the problem detail.
4. Parses and executes the body via `text/template` (**not** `html/template` — these are SMS bodies, transactional email bodies, and push payloads, none of them HTML; auto-escaping would mangle legitimate `&` and `<` characters in user data).
5. Sets `Option("missingkey=error")` on the template so any reference to an undefined variable is a render error, not a silent `<no value>` substitution.
6. Validates the rendered length against the channel cap (SMS 1000 bytes, email 256 KB, push 4 KB). Over-cap is a 400, not a worker-side failure.
7. Persists the *rendered* string on `notifications.content`, along with audit columns `template_id` and `template_version` so we can answer "what template produced this row" without joining against a mutable table.

Implementation: `internal/template/template.go` (parser, renderer, length validator) and `internal/api/notifications.go` (handler wiring, problem-detail mapping).

`required_vars` is stored on the template row at create-time, extracted by a regex walk over the body looking for `{{ .X }}` identifiers. The variable model is flat by contract — no `.User.Name` nesting, no method calls — so the regex catches every reference the template engine can resolve, and we avoid wiring up `text/template/parse` AST traversal for a guarantee we can already enforce in schema.

## Consequences

- **Retry determinism.** A worker retry produces byte-identical output because the bytes are already on the row. With delivery-time rendering, a template edited between attempts would have two retries produce different content — exactly the divergence at-least-once delivery requires us to forbid.
- **History immutability.** "What did we send on March 4th?" is answered by `SELECT content FROM notifications WHERE id = ...`. Delivery-time rendering would force us to retain every historical template revision forever, which defeats the soft-deprecation flow that `deprecated_at` exists to support.
- **Errors surface at the caller.** Missing variables, malformed templates, and length-cap violations all become 400s on the request that introduced them. The caller sees the failure on the call that caused it, not as a silent DLQ entry hours later. Operationally this collapses an entire class of "my notification never sent and I cannot tell why" tickets.
- **Cost: one template lookup per create.** Templates are small and cacheable; the query is a single indexed row read.
- **Honest tradeoff: editing a template does not retroactively re-render past notifications.** We consider this a feature — historical sends are immutable artifacts — but it is worth being explicit. An operator who needs a typo-fix applied to in-flight messages PUTs a new template version and replays affected rows through the DLQ admin endpoint, which republishes the *stored rendered content*. The fix is a separate notification, not a mutation of an old one.

## Alternatives Considered

- **Render at delivery time.** Store `template_id` + `variables` on the row, resolve inside the worker just before the provider POST. Rejected on all three axes above: retries lose determinism, audit requires immortal template revisions, and template errors surface as pipeline failures the caller cannot see. The implementation also burdens the worker — already the hottest, most retry-sensitive component — with a parsing and validation step that has no business on the delivery path.
- **Render in a separate "expand" worker between API and delivery.** A third hop that consumes raw template messages and republishes rendered ones. Adds a queue, adds a failure surface, and gains nothing the synchronous render does not already give us at strictly lower latency.
- **`html/template` for safety.** Rejected. The output medium is not HTML for any of our channels. Auto-escaping would corrupt legitimate variable values (an `&` in a customer name, a `<` in a code snippet) and force callers to pre-escape, which is exactly the trap `html/template` is designed to prevent for actual HTML output.
