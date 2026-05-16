# ADR-0016: Chi router over net/http stdlib patterns

## Status

Accepted (2026-05-16)

## Context

Go 1.22 added method-aware patterns to the standard library `net/http` ServeMux: `mux.HandleFunc("GET /v1/notifications/{id}", handler)` now does what every third-party router has done for a decade. For the first time, stdlib routing is competitive on the basics — method-and-path matching, path parameters via `r.PathValue`, longest-pattern precedence. The historical case for pulling in a router dependency ("the stdlib mux doesn't even do path params") evaporated overnight.

Our API surface is twenty-plus routes across `/v1` (read, write) and `/v1/admin` (operational), each subtree needing a distinct middleware chain: correlation-id and access-log everywhere, body-size limit on writes, scope checks per route, an admin-only stack on the admin subtree. The relevant question is not "can stdlib route a path" but "can stdlib express this composition cleanly enough that the routing file remains the readable map of the API."

## Decision

**`github.com/go-chi/chi/v5` (v5.x) as the HTTP router for the entire API surface.**

The router lives in `internal/api/router.go`. A single `chi.NewRouter()` is the root; middleware is attached with `r.Use(...)` (correlation-id, slog logger, access log, metrics, `middleware.Recoverer`); the API subtree is mounted with `r.Route("/v1", ...)` carrying the body-size limit and the bearer-auth middleware; the admin subtree is a nested `r.Route("/admin", ...)` adding `RequireScope(ScopeAdmin)`; individual write routes bind their scope with `r.With(RequireScope(ScopeWrite)).Post(...)`. Path params are read with `chi.URLParam(r, "id")`. The metrics middleware records `route` as `chi.RouteContext(r.Context()).RoutePattern()` — the literal pattern `/v1/notifications/{id}` — so Prometheus cardinality is bounded by route count, not by id space.

Handlers are plain `http.HandlerFunc`. Chi does not wrap the request or response in a custom context type.

## Consequences

- The routing file reads top-to-bottom as the API map. Middleware composition is local to each subtree; there is no per-handler wrapper boilerplate and no reach across files to understand what auth a route enforces.
- One extra dependency (`go-chi/chi/v5`, ~3kLoC, MIT, actively maintained, no transitive dependencies of its own). We accept the cost. The productivity payoff is real for an API in the 20-route range, and the security surface is small.
- Migration ramp is open in both directions. Chi is a thin shim over `net/http`: handlers are `http.HandlerFunc`, the request context is the standard `context.Context`, middleware is `func(http.Handler) http.Handler`. If we ever need to drop chi — to shrink dependencies, to adopt a router we haven't seen yet, or to land on stdlib once it grows the missing primitives — the work is mechanical. There is no `chi.Context` type to unwind and no framework-specific handler signature.
- The `RoutePattern()` metric label is the one chi feature that is genuinely hard to replicate on stdlib today: there is no public API in Go 1.22's ServeMux to recover the pattern that matched a request. Losing it would force either path-template reconstruction in middleware (fragile) or unbounded cardinality (operationally unacceptable).
- We commit to staying on chi v5. Major-version churn in a router is disruptive; chi v5 has been stable since 2021 and shows no signs of a v6.

## Alternatives Considered

- **`net/http` stdlib with Go 1.22 patterns.** The closest competitor and the one we seriously weighed. Method-aware routing and path parameters are now in the box. What is still hand-rolled: per-subtree middleware composition (no `r.Use` / `r.With` equivalent — you wrap handlers manually or build your own chainer), route grouping (you write a helper that takes a prefix and a middleware list and registers handlers under it), and route-pattern recovery for metrics (no public API). None of these are unsolvable; each is 20-50 lines of glue we would own and test. Across an API with two scoped subtrees and five middlewares, that glue starts to look like a worse version of chi.
- **`gorilla/mux`.** The historical default. Mux entered maintenance hibernation in 2022, was un-archived under the Gorilla collective in 2023, and remains less actively developed than chi. The API is also older — `mux.Vars(r)` instead of `chi.URLParam`, no `RouteContext` equivalent. No reason to pick it for a new service in 2026.
- **`gin-gonic/gin`.** A framework, not a router. Bundles its own `gin.Context` that wraps the request/response and does not compose with stdlib middleware (`func(http.Handler) http.Handler`). We would either pay for features we do not use (JSON binding helpers, validator integration, custom rendering) or adapt every middleware we already have. The lock-in is the disqualifier: migrating off gin means rewriting every handler signature.
- **`labstack/echo`.** Same shape as gin — custom context type, framework rather than router, same lock-in concern. Same rejection.
