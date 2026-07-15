# syntax=docker/dockerfile:1.7
#
# Production Dockerfile for the edgeCloud ingress.
#
# The ingress is a single Rust binary (`edge-ingress`) that subscribes
# to NATS for worker heartbeats, maintains an in-memory routing table,
# and pushes Caddyfile-JSON to Caddy's admin API on every change.
# Long-running + Handler (FaaS) routing both go through this same
# process; L4 routing uses the xcaddy-built Caddy image separately
# (not covered by this Dockerfile — opt in via a compose override
# that swaps `image: caddy:2` for the project-built
# `edgecloud/caddy-l4` per .github/workflows/ci.yml → `caddy-image`).
#
# Two execution shapes share this binary:
#   - 80/443 served directly for HTTP/HTTPS to apps.
#   - 2019/tcp  : Caddy admin API target. The ingress writes
#                 Caddyfile-JSON to `CADDY_ADMIN_URL` (default
#                 `http://127.0.0.1:2019`). Operators MUST set
#                 `CADDY_ADMIN_LISTEN=0.0.0.0:2019` on the Caddy
#                 container so this admin port is reachable from
#                 outside Caddy's netns — `localhost` inside
#                 Docker is container-scoped (issue #279).
#   - 9091/tcp  : The ingress's own /metrics endpoint (per
#                 `edge-ingress/src/config.rs:307-308`, env
#                 `INGRESS_METRICS_LISTEN`). Distinct from the
#                 worker's 9090 because both report on the same host
#                 if edge-cloud is colocated.
#
# The ingress is stateless at the filesystem level (Caddy owns
# runtime state). No volumes are declared in the image — compose
# doesn't need to mount anything onto this container.

# ── Stage 1a: dep fetch ────────────────────────────────────────────
FROM rust:1-bookworm AS deps

ENV CARGO_BUILD_RUSTC_WRAPPER="" \
    CARGO_TARGET_DIR=/build/target \
    CARGO_NET_RETRY=10

WORKDIR /workspace

# Cargo.lock is NOT COPY'd from the build context (the workspace
# gitignores it). Generate it inside the builder stage via
# `cargo generate-lockfile` below; see edge-control-plane/Dockerfile
# for the full rationale.
COPY Cargo.toml ./
COPY edge-runtime/Cargo.toml                          edge-runtime/Cargo.toml
COPY edge-runtime/bin/wasm2cwasm/Cargo.toml           edge-runtime/bin/wasm2cwasm/Cargo.toml
COPY edge-config/Cargo.toml                           edge-config/Cargo.toml
COPY edge-spool/Cargo.toml                            edge-spool/Cargo.toml
COPY edge-worker/Cargo.toml                           edge-worker/Cargo.toml
COPY edge-ingress/Cargo.toml                          edge-ingress/Cargo.toml
COPY edge-ingress-sidecar/Cargo.toml                  edge-ingress-sidecar/Cargo.toml
COPY edge-migrate/edge-migrate-lib/Cargo.toml         edge-migrate/edge-migrate-lib/Cargo.toml
COPY edge-migrate/edge-migrate-bin/Cargo.toml         edge-migrate/edge-migrate-bin/Cargo.toml
COPY edge-js-runtime/Cargo.toml                       edge-js-runtime/Cargo.toml
COPY edge-js-runtime-long/Cargo.toml                  edge-js-runtime-long/Cargo.toml
COPY edge-test-helpers/Cargo.toml                     edge-test-helpers/Cargo.toml

RUN mkdir -p \
      edge-runtime/src \
      edge-runtime/bin/wasm2cwasm/src \
      edge-config/src \
      edge-spool/src \
      edge-worker/src \
      edge-ingress/src \
      edge-ingress-sidecar/src \
      edge-migrate/edge-migrate-lib/src \
      edge-migrate/edge-migrate-bin/src \
      edge-js-runtime/src \
      edge-js-runtime/benches \
      edge-js-runtime-long/src \
      edge-test-helpers/src && \
    echo "" > edge-runtime/src/lib.rs && \
    echo "" > edge-runtime/bin/wasm2cwasm/src/main.rs && \
    echo "" > edge-config/src/lib.rs && \
    echo "" > edge-spool/src/lib.rs && \
    echo "" > edge-worker/src/lib.rs && \
    echo "" > edge-worker/src/main.rs && \
    echo "" > edge-ingress/src/lib.rs && \
    echo "" > edge-ingress/src/main.rs && \
    echo "" > edge-ingress-sidecar/src/lib.rs && \
    echo "" > edge-migrate/edge-migrate-lib/src/lib.rs && \
    echo "" > edge-migrate/edge-migrate-bin/src/main.rs && \
    echo "" > edge-js-runtime/src/lib.rs && \
    echo "" > edge-js-runtime/benches/warm_vs_cold.rs && \
    echo "" > edge-js-runtime-long/src/lib.rs && \
    echo "" > edge-test-helpers/src/lib.rs

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo generate-lockfile && \
    cargo fetch --locked

# ── Stage 1b: actual build ─────────────────────────────────────────
FROM rust:1-bookworm AS builder

ENV CARGO_BUILD_RUSTC_WRAPPER="" \
    CARGO_TARGET_DIR=/build/target \
    CARGO_NET_RETRY=10 \
    RUSTFLAGS="-C opt-level=3 -C debuginfo=0"

WORKDIR /workspace

# Cargo.lock is NOT COPY'd from the build context (the workspace
# gitignores it); the deps stage above generated one via
# `cargo generate-lockfile`. `cargo build --release` regenerates it
# on the fly from the manifests + cached registry, which is fine
# because the registry is already deterministic.
COPY Cargo.toml ./
COPY edge-runtime/   edge-runtime/
COPY edge-config/    edge-config/
COPY edge-spool/     edge-spool/
COPY edge-worker/    edge-worker/
COPY edge-ingress/   edge-ingress/

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --bin edge-ingress && \
    cp /build/target/release/edge-ingress /out/edge-ingress

# ── Stage 2: runtime ───────────────────────────────────────────────
#
# Stateless. No filesystem writes beyond what metrics + tracing do.
# Non-root user `ingress` (uid 1002) — distinct from the worker's
# 1001 in case we colocate them on the same host.

FROM debian:bookworm-slim AS runtime

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
        tini \
        wget && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd -g 1002 ingress && \
    useradd -u 1002 -g ingress -m -d /data ingress && \
    mkdir -p /data && \
    chown -R ingress:ingress /data

LABEL org.opencontainers.image.title="edgecloud-ingress" \
      org.opencontainers.image.description="edgeCloud ingress — Caddy controller (HTTP/HTTPS reverse-proxy + L4 routing)" \
      org.opencontainers.image.source="https://github.com/poyrazK/edgecloud" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY --from=builder --chown=1002:1002 /out/edge-ingress /usr/local/bin/edge-ingress

USER ingress:ingress
WORKDIR /data

ENV RUST_LOG=edge_ingress=info \
    PATH=/usr/local/bin:/usr/bin:/bin \
    HOME=/data

EXPOSE 80 443 2019 9091

# Healthcheck: TCP probe on the HTTP listener. The ingress rejects
# requests without a valid TLS cert (production) or upstream
# `host` header (catch-all), but always opens the listening socket.
# Cheap process-liveness indicator; deep readiness is the heartbeats
# NATS feed (if NATS is down, ingress keeps the last known good
# Caddyfile — by design, so TCP-listening is the right thing to
# gate on).
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=3 \
    CMD ["/bin/sh", "-c", "wget -q -O- http://127.0.0.1:9091/ > /dev/null 2>&1 || exit 1"]

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/edge-ingress"]
