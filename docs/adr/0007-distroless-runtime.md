# ADR-0007: distroless + nonroot runtime container

## Status

Accepted (2026-05-16)

## Context

The notification system ships as a single Docker image consumed by `deploy/docker-compose.yml` (and, eventually, a Kubernetes manifest). We have to choose a base image for the runtime stage and decide what posture the container runs with at the kernel level. The Go binary is statically linkable, talks to Postgres/Redis/RabbitMQ over the internal compose network, and makes outbound HTTPS calls to external delivery providers (so it needs CA certs). It does not need a shell, a package manager, or any utilities at runtime — those are debugging conveniences, and conveniences in a production image are attack surface.

## Decision

**Multi-stage `Dockerfile`.**

Builder stage uses `golang:1.26-bookworm` with `CGO_ENABLED=0`, `-trimpath`, and `-ldflags="-s -w -X main.Version=..."`. Buildkit cache mounts (`--mount=type=cache,target=/go/pkg/mod` and `target=/root/.cache/go-build`) speed up incremental builds:

```dockerfile
FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath \
      -ldflags="-s -w -X main.Version=${VERSION}" \
      -o /out/notifyd ./cmd/notifyd

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/notifyd /notifyd
COPY --from=build /src/db/migrations /migrations
COPY --from=build /src/api/openapi.yaml /api/openapi.yaml
USER nonroot:nonroot
ENTRYPOINT ["/notifyd"]
```

`ENTRYPOINT` is exec form so SIGTERM propagates directly to the Go process — graceful shutdown drains in-flight HTTP requests and the worker pool cleanly (see ADR-0001).

**Compose runtime flags** in `deploy/docker-compose.yml` harden the container:

- `read_only: true` — root filesystem mounted read-only.
- `cap_drop: [ALL]` — no Linux capabilities.
- `security_opt: ['no-new-privileges:true']` — setuid binaries can't escalate.
- `tmpfs: ['/tmp:size=64m,mode=1777']` — only `/tmp` is writable, and it's a size-bounded tmpfs.
- Two networks: `backend` (internal-only, reaches postgres/redis/rabbitmq) and `frontend` (publishes the API port). The app sits on both; the data-plane services sit only on `backend`.

## Consequences

- Final image is ~20 MB. No shell, no package manager, no busybox, no libc surprises.
- Runs as uid 65532 with zero capabilities on a read-only root. A code-execution bug in `notifyd` lands the attacker in a sandbox with nowhere to write, no tools to pivot with, and no network path to anything outside `backend`/`frontend`.
- CA certs ship with `distroless-static`, so outbound HTTPS to provider APIs works without extra setup.
- **Debugging cost:** there is no `sh`, no `ps`, no `curl` inside the running container. `docker exec -it notifyd sh` will not work. We accept this. Operational visibility comes from structured logs, `/metrics`, `/healthz`, and `/readyz` — production debugging should not require a shell on the running pod. When deeper inspection is needed, attach a debug sidecar (`kubectl debug` / an ephemeral container with the same network namespace) rather than baking tools into the runtime image.
- Migrations and the OpenAPI spec are part of the image, so `notifyd migrate` and the docs endpoint work without volume mounts.

## Alternatives Considered

- **`golang:1.26-alpine` as the final stage.** Still has `sh`, `apk`, busybox. Bigger attack surface for marginal convenience. CGO + musl is a separate class of footgun we'd rather not invite.
- **Debian slim (`debian:12-slim`).** Roughly 30 MB heavier than distroless and ships `apt` and `bash`. The "convenience" argument doesn't hold up — in production we SSH into a host, not into a container. A shell in the runtime image is a liability.
- **`FROM scratch`.** Smallest possible, but no CA bundle (HTTPS to providers breaks) and no `/etc/passwd` (uid 65532 won't resolve to a name, which trips some tooling). `distroless-static:nonroot` gives both for free.
- **Run as root, drop capabilities.** Weaker. Defense in depth says don't be root in the first place; capability dropping is a second layer, not a substitute.

**Follow-up:** the Dockerfile currently references base images by floating tag (`golang:1.26-bookworm`, `gcr.io/distroless/static-debian12:nonroot`). Production builds should pin by digest (`@sha256:...`) with Dependabot configured to bump the digests, so the supply chain is reproducible and the upgrade path is reviewable.
