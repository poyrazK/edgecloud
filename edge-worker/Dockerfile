# syntax=docker/dockerfile:1.7
#
# Production Dockerfile for the edgeCloud worker.
#
# The worker is a single Rust binary (`edge-worker`) that subscribes
# to NATS for desired-app updates, instantiates WASM components, and
# hosts their HTTP servers. Two execution models are picked at link
# time by edge-worker/src/detect.rs (LongRunning vs. Handler/FaaS);
# this image bundles both.
#
# Heavy deps the build pulls in (per edge-worker/Cargo.toml):
#   - wasmtime =45.0.3 (the runtime engine; ~600 MB of deps)
#   - wasmtime-wasi-http =45.0.3
#   - axum, hyper, hyper-util (HTTP server for Handler dispatch)
#   - prometheus 0.13 (metrics endpoint; issue #49)
#   - rustls 0.23 + ring (TLS for FaaS, Handler TLS per issue #209)
#   - ed25519-dalek 2.2 (artifact-signing verifier, issue #307)
#
# Build-context strategy: stages 1a/1b split dep-fetch from source-copy
# so PR rebuilds that only touch Rust sources skip the
# `cargo fetch --locked` step. Mirror of edge-control-plane/Dockerfile's
# pattern. The committed `.cargo/config.toml` sets
# `rustc-wrapper = "sccache"` and target-dir to a config-relative
# `../target-cache/edgecloud` — both are wrong for Docker, so we
# override them with env vars (CARGO_TARGET_DIR and
# CARGO_BUILD_RUSTC_WRAPPER prefix the Docker bind-mount target).

# ── Stage 1a: dep fetch (workspace + all member manifests) ──────────
FROM rust:1-bookworm AS deps

ENV CARGO_BUILD_RUSTC_WRAPPER="" \
    CARGO_TARGET_DIR=/build/target \
    CARGO_NET_RETRY=10

WORKDIR /workspace

# Workspace root + every Cargo.toml the worker depends on (directly
# or transitively, per edge-worker/Cargo.toml):
#   edge-worker → edge-runtime, edge-spool, edge-config, edge-cli, edge-ingress
# We narrow the COPY to the manifests + a stub src/lib.rs+main.rs per
# crate so `cargo fetch --locked` resolves the package graph without
# pulling sources the worker doesn't use at runtime.
#
# `Cargo.lock` is NOT COPY'd from the build context (the workspace
# gitignores it). Generate it inside the builder stage from the
# freshly-COPY'd Cargo.toml set; see edge-control-plane/Dockerfile
# for the full rationale.
COPY Cargo.toml ./
COPY edge-runtime/Cargo.toml                          edge-runtime/Cargo.toml
COPY edge-runtime/bin/wasm2cwasm/Cargo.toml           edge-runtime/bin/wasm2cwasm/Cargo.toml
COPY edge-config/Cargo.toml                           edge-config/Cargo.toml
COPY edge-spool/Cargo.toml                            edge-spool/Cargo.toml
COPY edge-worker/Cargo.toml                           edge-worker/Cargo.toml
COPY edge-cli/Cargo.toml                              edge-cli/Cargo.toml
COPY edge-ingress/Cargo.toml                          edge-ingress/Cargo.toml
COPY edge-ingress-sidecar/Cargo.toml                  edge-ingress-sidecar/Cargo.toml
COPY edge-migrate/edge-migrate-lib/Cargo.toml         edge-migrate/edge-migrate-lib/Cargo.toml
COPY edge-migrate/edge-migrate-bin/Cargo.toml         edge-migrate/edge-migrate-bin/Cargo.toml
COPY edge-js-runtime/Cargo.toml                       edge-js-runtime/Cargo.toml
COPY edge-js-runtime-long/Cargo.toml                  edge-js-runtime-long/Cargo.toml
COPY edge-test-helpers/Cargo.toml                     edge-test-helpers/Cargo.toml

# Stub sources so cargo sees each manifest's package graph. The real
# src/lib.rs + src/main.rs land in the builder stage.
RUN mkdir -p \
      edge-runtime/src \
      edge-runtime/bin/wasm2cwasm/src \
      edge-config/src \
      edge-spool/src \
      edge-worker/src \
      edge-cli/src \
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
    echo "" > edge-cli/src/lib.rs && \
    echo "" > edge-cli/src/main.rs && \
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

# Copy the (now-resolved) registry cache from the deps stage as a
# baseline. Cold cache PRs still pay ~10 minutes for wasmtime — the
# BuildKit `--mount=type=cache` on `/usr/local/cargo/registry` is the
# reusable counterpart.
COPY --from=deps /usr/local/cargo/registry /usr/local/cargo/registry

# Source slices the worker needs. Each is its own COPY so a change
# in `edge-worker/src/` does NOT bust the layer for `edge-runtime/`
# (which dominates the build time).
#
# Cargo.lock is intentionally NOT COPY'd from the build context (the
# workspace gitignores it). The deps stage above generated one via
# `cargo generate-lockfile` and the registry cache mount preserves it
# in /usr/local/cargo/registry; `cargo build --release` regenerates
# /workspace/Cargo.lock on the fly from the manifests and the cached
# registry index, which is fine because the registry is already
# deterministic.
COPY Cargo.toml ./
COPY edge-runtime/   edge-runtime/
COPY edge-config/    edge-config/
COPY edge-spool/     edge-spool/
COPY edge-worker/    edge-worker/

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --bin edge-worker && \
    cp /build/target/release/edge-worker /out/edge-worker

# ── Stage 2: runtime ───────────────────────────────────────────────
#
# Debian bookworm slim runtime. The Rust `ring` crypto backend
# (used by rustls, reqwest's rustls-tls, and reqwest's default) is
# pure Rust + tiny asm, so libssl is NOT strictly required — but
# keeping it future-proofs against a tree-sitter / aws-lc-rs
# transitive bump that re-introduces C deps.
#
# Permissions: non-root `worker` user (uid 1001). All writable paths
# the worker uses at runtime are owned by this user:
#   /data/.worker-cache   (downloader + identity.key EWIS record)
#   /data/.worker-spool   (edge-spool JSONL log batches)
# Compose binds host-side volumes here.

FROM debian:bookworm-slim AS runtime

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
        tini \
        wget && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd -g 1001 worker && \
    useradd -u 1001 -g worker -m -d /data worker && \
    mkdir -p /data/.worker-cache /data/.worker-spool && \
    chown -R worker:worker /data

LABEL org.opencontainers.image.title="edgecloud-worker" \
      org.opencontainers.image.description="edgeCloud worker — Wasmtime host for tenant-supplied WASM components (LongRunning + Handler/FaaS)" \
      org.opencontainers.image.source="https://github.com/poyrazK/edgecloud" \
      org.opencontainers.image.licenses="Apache-2.0"

COPY --from=builder --chown=1001:1001 /out/edge-worker /usr/local/bin/edge-worker

USER worker:worker
WORKDIR /data

ENV RUST_LOG=edge_worker=info,edge_runtime=info \
    PATH=/usr/local/bin:/usr/bin:/bin \
    HOME=/data

# METRICS_ADDR defaults to 0.0.0.0:9090 in edge-worker/src/config.rs.
# Healthcheck is on the metrics port: the endpoint fail-closes 401
# without a token but is always reachable on TCP — a port-check is
# enough to know the worker process is up.
EXPOSE 8081 9090

HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=3 \
    CMD ["/bin/sh", "-c", "wget -q -O- http://127.0.0.1:9090/ > /dev/null || (ss -ltn | grep -q ':9090' || exit 1)"]

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/edge-worker"]
