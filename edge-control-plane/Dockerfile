# syntax=docker/dockerfile:1.7
#
# Production Dockerfile for the edgeCloud control plane.
#
# Produces a single image with three binaries on PATH:
#   - `migrate`   : schema migrator (edge-control-plane/cmd/migrate)
#   - `api`       : HTTP API server (edge-control-plane/cmd/api)
#   - `edge-migrate`  : tree-sitter C/Rust → WASI source transformer
#                       (edge-migrate/edge-migrate-bin; default path
#                       referenced by `internal/handler/migration.go`'s
#                       `exec.LookPath("edge-migrate")` and the CP's
#                       `internal/config/config.go:1172` default
#                       `edge_migrate_path: "edge-migrate"`).
#   - `wasm2cwasm`: AOT precompile helper (edge-runtime/bin/wasm2cwasm;
#                   optional via the BUILD_WITH_WASM2CWASM build arg —
#                   set to `false` for a lean demo image; the worker
#                   JIT-compiles on first load when .cwasm is absent,
#                   so omitting it is a perf hit but not a correctness
#                   one).
#
# The `docker-entrypoint.sh` runs `migrate -up` then `exec api` so a
# fresh container applies pending migrations on boot before serving
# traffic. The migrate step is non-fatal on standalone startup — set
# `SKIP_MIGRATE_ON_BOOT=true` to skip it (useful for read-only API
# replicas once migrations are managed by an init container / job).
#
# Both cmd/api and cmd/migrate hardcode `config.Load("config.yaml")`
# (no -config flag). WORKDIR is /etc/edge where the bundled config.yaml
# lives, so both binaries resolve the file from CWD at startup. The
# config.yaml in the image is the canonical repo-root one; operators
# override values via DATABASE_* / JWT_* / EDGE_* env vars (config.go
# reads env after the YAML load).

# ── Stage 1a: Go builder for the control plane ─────────────────────
FROM golang:1.25-bookworm AS go-builder

WORKDIR /src/edge-control-plane
# Module cache lives in a BuildKit mount so sibling PR builds reuse it.
COPY edge-control-plane/go.mod edge-control-plane/go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY edge-control-plane/ ./
# Build-time copy of the openapi spec (cmd/api/main.go:24's `//go:generate`
# relies on a `docs/api/openapi.yaml` next to main.go at build time).
COPY edge-control-plane/docs/api/openapi.yaml docs/api/openapi.yaml

ENV CGO_ENABLED=0 GOOS=linux

# Compile the two production binaries in one invocation. Tags + trimpath
# strip the path prefix + symbol table; `-s -w` strips DWARF/Go-internal
# tables. The two binaries share ~80% of the build graph so a single
# `./...` build is faster than two separate `go build` invocations on
# cold cache.
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    go build -trimpath -ldflags="-s -w" -o /out/migrate ./cmd/migrate && \
    go build -trimpath -ldflags="-s -w" -o /out/api     ./cmd/api && \
    go build -trimpath -ldflags="-s -w" -o /out/printpub ./cmd/printpub

# ── Stage 1b: Rust builder for edge-migrate + wasm2cwasm ───────────
#
# Both targets are sub-builds of the workspace but the CP image only
# needs their outputs. We override CARGO_BUILD_RUSTC_WRAPPER="" and
# CARGO_TARGET_DIR because the committed `.cargo/config.toml` wraps
# rustc with sccache and points target-dir at a relative path
# outside the workspace — neither of which work inside Docker. The
# builder stage exports those env vars so cargo's precedence rules
# (env > config-file) keep the Docker build hermetic.

FROM rust:1.85-bookworm AS rust-builder

ARG BUILD_WITH_WASM2CWASM=true

ENV CARGO_BUILD_RUSTC_WRAPPER="" \
    CARGO_TARGET_DIR=/build/target \
    CARGO_NET_RETRY=10

WORKDIR /workspace

# Vendor Cargo manifests first so the cache layer invalidates only on
# workspace dependency changes, not on every source edit.
#
# `Cargo.lock` is intentionally NOT COPY'd from the build context —
# it's in .gitignore (the workspace is a mix of libraries and binaries;
# gitignore-the-lockfile is the conservative choice). On a fresh CI
# clone, the build context has no `Cargo.lock`, and `cargo fetch
# --locked` requires one. Generate it inside the builder stage from
# the freshly-COPY'd Cargo.toml set instead. This keeps the lockfile
# generation cost inside the BuildKit cache layer (keyed off
# `Cargo.toml` content), so a subsequent rebuild that hasn't touched
# any manifest skips the regeneration.
COPY Cargo.toml ./
COPY edge-runtime/Cargo.toml                  edge-runtime/Cargo.toml
COPY edge-runtime/bin/wasm2cwasm/Cargo.toml   edge-runtime/bin/wasm2cwasm/Cargo.toml
COPY edge-config/Cargo.toml                   edge-config/Cargo.toml
COPY edge-spool/Cargo.toml                    edge-spool/Cargo.toml
COPY edge-worker/Cargo.toml                   edge-worker/Cargo.toml
COPY edge-ingress/Cargo.toml                  edge-ingress/Cargo.toml
COPY edge-ingress-sidecar/Cargo.toml          edge-ingress-sidecar/Cargo.toml
COPY edge-cli/Cargo.toml                      edge-cli/Cargo.toml
COPY edge-migrate/edge-migrate-lib/Cargo.toml edge-migrate/edge-migrate-lib/Cargo.toml
COPY edge-migrate/edge-migrate-bin/Cargo.toml edge-migrate/edge-migrate-bin/Cargo.toml
COPY edge-js-runtime/Cargo.toml               edge-js-runtime/Cargo.toml
COPY edge-js-runtime-long/Cargo.toml          edge-js-runtime-long/Cargo.toml
COPY edge-test-helpers/Cargo.toml             edge-test-helpers/Cargo.toml

# Pre-fetch deps (separate RUN so the cache layer is keyed off Cargo.lock
# only). The empty-stub src/lib.rs files let cargo resolve the package
# graph without paying the bandwidth for actual sources.
#
# Two-step: first generate Cargo.lock from the manifests just COPY'd
# in (since the build context doesn't carry one — see the comment on
# the COPY block above). Then `cargo fetch --locked` validates the
# freshly-generated lockfile against the manifest set and pre-fetches
# every registry entry to the cache mount.
RUN mkdir -p \
      edge-runtime/src \
      edge-runtime/bin/wasm2cwasm/src \
      edge-config/src \
      edge-spool/src \
      edge-worker/src \
      edge-ingress/src \
      edge-ingress-sidecar/src \
      edge-cli/src \
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
    echo "" > edge-cli/src/lib.rs && \
    echo "" > edge-cli/src/main.rs && \
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

# Now copy the real sources and build both bins unconditionally.
# edge-migrate is required by `internal/handler/migration.go`'s
# exec.LookPath("edge-migrate") lookup — building it always keeps
# the migration handler's wire contract working. wasm2cwasm is
# optional (used by `internal/service/precompile.go:46-47`); set
# BUILD_WITH_WASM2CWASM=false to skip it. The placeholder file is
# always present so the runtime COPY below doesn't fail.
COPY edge-runtime/                            edge-runtime/
COPY edge-runtime/bin/wasm2cwasm/             edge-runtime/bin/wasm2cwasm/
COPY edge-config/                             edge-config/
COPY edge-migrate/                            edge-migrate/

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --bin edge-migrate && \
    cargo build --release --manifest-path edge-runtime/bin/wasm2cwasm/Cargo.toml --bin wasm2cwasm && \
    mkdir -p /out && \
    cp /build/target/release/edge-migrate /out/edge-migrate && \
    if [ "$BUILD_WITH_WASM2CWASM" = "true" ]; then \
        cp /build/target/release/wasm2cwasm /out/wasm2cwasm; \
    else \
        : > /out/wasm2cwasm; \
    fi

# `/out` is a populated directory in the Rust builder stage. The Go
# binary output lives alongside it (under `/out/{migrate,api,printpub}`
# from the go-builder stage). The runtime stage copies both builder
# outputs below.

# ── Stage 2: runtime ───────────────────────────────────────────────

# ── Stage 2: runtime ───────────────────────────────────────────────
#
# Distroless `base-debian12:nonroot` (NOT `static`) because we want
# `/bin/sh` available so docker-entrypoint.sh can shell-exec migrate
# then `exec` api in the same PID 1. The `nonroot` flavor runs as
# uid 65532 by default — meets the "don't run as root in containers"
# baseline without baking our own user.

FROM gcr.io/distroless/base-debian12:nonroot AS runtime

# OCI labels — `org.opencontainers.image.source` lets GHCR wire
# "View on GitHub" links for the built image.
LABEL org.opencontainers.image.title="edgecloud-control-plane" \
      org.opencontainers.image.description="edgeCloud control plane (HTTP API + Postgres + NATS publisher)" \
      org.opencontainers.image.source="https://github.com/poyrazK/edgecloud" \
      org.opencontainers.image.licenses="Apache-2.0"

WORKDIR /etc/edge

# Registry storage root — STORAGE_ARTIFACT_PATH defaults to
# /var/edgecloud/registry, which the operator overrides via env.
# /var/edgecloud is the conventional filesystem backend path used
# by edge-control-plane/internal/storage. A volume is mounted here
# in docker-compose.prod.yml so artifact blobs survive container
# restarts.
WORKDIR /var/edgecloud/registry

# Runtime config — the canonical edge-control-plane/config.yaml,
# edited in place by Dockerfile semantics. config.Load reads env
# vars AFTER YAML values, so DATABASE_PASSWORD / JWT_SECRET /
# EDGE_* operator overrides take precedence on top of these defaults.
COPY --chown=65532:65532 edge-control-plane/config.yaml /etc/edge/config.yaml

# Binaries from the Go + Rust builders. The Rust-builder /out/ is
# populated conditionally (see the BUILD_*_WASM2CWASM / BUILD_EDGE_MIGRATE
# gates above). A missing source path will fail the COPY, so the
# default image includes both — build the lean variant with
# `--build-arg BUILD_WITH_WASM2CWASM=false` and the runtime treats the
# absence as a no-op (the CP's `precompile.go:46-47` short-circuits
# on empty EDGE_WASM2CWASM_PATH).
COPY --from=go-builder         --chown=65532:65532 /out/migrate      /usr/local/bin/migrate
COPY --from=go-builder         --chown=65532:65532 /out/api          /usr/local/bin/api
COPY --from=go-builder         --chown=65532:65532 /out/printpub     /usr/local/bin/printpub
COPY --from=rust-builder       --chown=65532:65532 /out/edge-migrate /usr/local/bin/edge-migrate
COPY --from=rust-builder       --chown=65532:65532 /out/wasm2cwasm   /usr/local/bin/wasm2cwasm

# Entrypoint: migrate up → exec api. Both binaries resolve
# `config.yaml` from CWD; WORKDIR is /etc/edge for the lifetime of
# the container, so this works without per-binary flag plumbing.
# The shell script must be in the distroless /bin/sh — base-debian12
# ships dash, which can run this script unmodified.
COPY --chown=65532:65532 scripts/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

USER 65532:65532
ENV EDGE_WASM2CWASM_PATH=/usr/local/bin/wasm2cwasm \
    EDGE_MIGRATE_PATH=/usr/local/bin/edge-migrate \
    APP_HOST=0.0.0.0 \
    APP_PORT=8080 \
    APP_ENV=production \
    PATH=/usr/local/bin:/usr/bin:/bin

EXPOSE 8080

# Process-level liveness; deep readiness (DB ping + NATS flush per
# issue #48) is at /ready — K8s readinessProbe should point there.
# wget is in distroless base-debian12; we point at the always-200
# /health endpoint rather than /ready because most container
# orchestrators don't read JSON for a 503.
HEALTHCHECK --interval=15s --timeout=3s --start-period=30s --retries=3 \
    CMD ["/bin/sh", "-c", "wget -q -O- http://127.0.0.1:8080/health > /dev/null || exit 1"]

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["api"]
