# syntax=docker/dockerfile:1.7
# Multi-stage build. See docs in the repo for the rationale on each choice.

# ---- Builder ---------------------------------------------------------------
FROM golang:1.25-bookworm AS builder
WORKDIR /src

# Module cache layer first.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Source.
COPY . .

ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_TIME=unknown

ENV CGO_ENABLED=0 \
    GOFLAGS=-trimpath

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build \
      -trimpath \
      -ldflags="-s -w \
        -X main.Version=${VERSION} \
        -X main.Commit=${COMMIT} \
        -X main.BuildTime=${BUILD_TIME}" \
      -o /out/notifyd ./cmd/notifyd

# ---- Final -----------------------------------------------------------------
# In production, pin by digest:
# FROM gcr.io/distroless/static-debian12:nonroot@sha256:<digest>
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /

COPY --from=builder /out/notifyd /notifyd

USER nonroot:nonroot

EXPOSE 8080

ENTRYPOINT ["/notifyd"]
