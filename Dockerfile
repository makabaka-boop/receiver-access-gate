# syntax=docker/dockerfile:1

# ---- builder: Debian-based image ships glibc so -race works too ----
FROM golang:1.23-bookworm AS builder
WORKDIR /src

# Cache dependencies independently of source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# API server image.
FROM builder AS build-api
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/accessgate ./cmd/accessgate

# One-shot acceptance image. Race detector is enabled because the toolchain
# image provides the required C runtime for the build step.
FROM builder AS build-verify
RUN CGO_ENABLED=1 go build -race -trimpath \
    -o /out/verify ./cmd/verify

# ---- minimal runtime ----
FROM gcr.io/distroless/static-debian12:nonroot AS api
COPY --from=build-api /out/accessgate /usr/local/bin/accessgate
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/accessgate"]

# verify needs a shell-less static binary; CGO race build is glibc-linked,
# so base it on a slim Debian instead of distroless static.
FROM debian:12-slim AS verify
COPY --from=build-verify /out/verify /usr/local/bin/verify
ENTRYPOINT ["/usr/local/bin/verify"]
