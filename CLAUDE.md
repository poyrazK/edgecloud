# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

edgeCloud is a managed WebAssembly edge computing platform. Developers compile a service to a WASI Preview 2 component, run `edge deploy`, and the platform runs it on worker nodes close to end users. Tenant code is sandboxed in Wasmtime, scoped per-tenant, metered per request, and can call platform-provided host interfaces (`edge:*`) for HTTP, KV, cache, scheduling, etc.

The repository contains five modules. There is **no top-level Cargo workspace** — each Rust crate is built independently via `--manifest-path`, and the Go module is built on its own. The Rust crates reference each other via `path = "..."`.

| Module | Language | Role |
|---|---|---|
| `edge-runtime/` | Rust | Wasmtime-based host library. Exposes `create_engine`, `create_store`, `RuntimeState`, `RequestMeter`. Implements the `edge:cloud@0.1.0` WIT world. |
| `edge-worker/` | Rust | Per-node supervisor binary. Subscribes to NATS for desired-app updates, instantiates WASM components, hosts their HTTP servers. |
| `edge-control-plane/` | Go | HTTP API + Postgres (sqlx) + NATS publisher. Two binaries: `cmd/api` (the server) and `cmd/migrate` (DB migrations). JWT auth for workers, API-key auth for tenants. |
| `edge-cli/` | Rust | Developer CLI (`edge init | build | deploy | dev | activate | env | ...`). Persists local state to `.edge/state.json`. |
| `edge-migrate/` | Rust workspace | Standalone `edge-migrate` developer CLI binary + `edge-migrate-lib` (tree-sitter C → WASI C analysis/transform library imported by the Go control plane). |

**Documentation:** `whitepaper.md` is the broad design doc (2026-06-14). Per-tool design docs (e.g. `edge-migrate/docs/design.md`) are scoped to one tool and may be newer — **when the two conflict, trust the per-tool design doc**. Treat any design doc as the source of intent, but always verify against the actual code.

## Build & Test

Each Rust module is built with `--manifest-path`. There is no workspace `Cargo.toml` at the repo root, so don't try to `cargo build` from there.

```bash
# edge-runtime (the Wasmtime host library)
cargo build  --manifest-path edge-runtime/Cargo.toml
cargo test   --manifest-path edge-runtime/Cargo.toml
cargo build  --manifest-path edge-runtime/Cargo.toml --release

# edge-worker (the supervisor)
cargo build  --manifest-path edge-worker/Cargo.toml
cargo test   --manifest-path edge-worker/Cargo.toml
# Integration tests in edge-worker/tests/ need Docker (testcontainers + wiremock).
# They self-skip when CI=true or SKIP_INTEGRATION_TESTS=1.

# edge-cli
cargo build  --manifest-path edge-cli/Cargo.toml
cargo test   --manifest-path edge-cli/Cargo.toml

# edge-migrate (its own internal workspace with lib + bin)
cargo build  --manifest-path edge-migrate/edge-migrate-lib/Cargo.toml
cargo build  --manifest-path edge-migrate/edge-migrate-bin/Cargo.toml

# edge-control-plane (Go)
cd edge-control-plane && go build ./... && go test ./...
```

## Lint & CI

```bash
# Rust
cargo fmt --check --manifest-path edge-runtime/Cargo.toml
cargo clippy --all-targets --all-features --manifest-path edge-runtime/Cargo.toml -- -D warnings

# Go
cd edge-control-plane && gofmt -l . && go vet ./...
```

CI runs in two places:

- **`.gitlab-ci.yml`** — runs fmt, clippy, audit, test, build:debug, build:release on **`edge-runtime` only**. Other Rust crates and Go are not covered here.
- **`.github/workflows/`** — covers `edge-control-plane` (go-fmt, go-vet, go-test) per recent commits.

If you change `edge-worker`, `edge-cli`, or `edge-migrate`, lint/test them manually — GitLab CI won't catch regressions in those crates.

## End-to-End Architecture

A request flows through the system like this:

1. **Build** — developer runs `edge build` → `cargo build --target wasm32-wasip2 --release` → `.wasm` component.
2. **Deploy** — `edge deploy` POSTs the artifact to `POST /api/deploy/{appName}` on the control plane with a Bearer API key. Control plane SHA-256-hashes the blob and stores it on the filesystem at `/registry/{tenant_id}/{app_name}/{deployment_id}.wasm`, plus a row in the `deployments` table.
3. **Activate** — `edge activate <deployment_id>` flips the `active_deployments` row, which causes the control plane to publish a `TaskMessage` over NATS JetStream to `edgecloud.tasks.<region>`.
4. **Reconcile** — `edge-worker` subscribes to that subject. `Supervisor::handle_task_message` diffs desired apps vs. running apps and starts/stops accordingly. Starting an app means: acquire a port from `PortPool`, download the artifact (cached locally), instantiate the component, and spawn `run_app_loop`.
5. **Execute** — the guest calls `edge:http-server.start(port, host)` to open a real TCP socket on the worker. Each accepted request goes through `httparse`, into an `mpsc`, the guest polls it via `edge:http-server.poll`, calls `respond`, and the server writes bytes back. Each request bumps `RequestMeter::count` for billing.
6. **Heartbeat** — the worker publishes `HeartbeatMessage{app_status, request_count}` to `edgecloud.heartbeats.<region>` every **30s** (whitepaper §5.6) so the control plane can bill and monitor.

### Key contracts

**NATS subjects** (all JetStream-durable; per whitepaper §8.4 the streams are configured with retention `workqueue`, replication factor 3, max age 24h):
- `edgecloud.tasks.<region>` — control plane → workers (`TaskMessage{timestamp, tenant_id, apps}`)
- `edgecloud.heartbeats.<region>` — workers → control plane (`HeartbeatMessage{timestamp, worker_id, region, apps}`)
- `edgecloud.deployments.<tenantID>` — tenant-scoped deployment events (per whitepaper §4.2)

**PostgreSQL schema** (control plane; see whitepaper §4.3 for full DDL): `tenants`, `quotas`, `api_keys`, `deployments`, `active_deployments`, `app_env`, `workers`, `worker_status`. IDs are prefixed: tenants `t_`, deployments `d_`, API keys `k_`, workers `w_<region>_`.

**Control-plane HTTP surface** (see `edge-control-plane/cmd/api/main.go`):
- Public: `POST /api/tenants`, `POST /api/keys`, `GET /health`
- Tenant-authenticated (Bearer API key, SHA-256-hashed in `api_keys`): deploy, activate, env, status, apps, etc.
- Admin (owner role): `/api/admin/tenants/*`, `DELETE /api/admin/apps/{appName}`
- Internal (Worker JWT, HMAC-SHA256, 24h TTL per whitepaper §9.3): `/api/internal/download/{deploymentID}`, `/api/internal/workers*`

**Migration flow** (server-side; the dev CLI is a thin uploader — per `edge-migrate/docs/design.md` v0.3):
1. Developer runs either (the `--language` flag selects between C and Rust; default is `c`):
   - `edge-migrate [--language c|rust] hello_world.{c,rs}` — single-file mode, app name derived from the file stem.
   - `edge-migrate --language c|rust --tree ./my_project/ [--app-name NAME]` — tree mode. C: walks `.c`/`.h` files. Rust: walks `.rs` files. Both skip `build/`, `target/`, `node_modules/`, etc.
2. The CLI POSTs to one of two endpoints:
   - `POST /api/migrate` (single-file) — multipart: `file`, `filename`, `language: "c"|"rust"`.
   - `POST /api/migrate-tree` (tree) — either multipart parts + a `tree` JSON manifest (`{"files":[...]}`) + one `file` part per entry, **or** a single `tree` part with `Content-Type: application/zip`. Required form field: `app_name` (must match `^[a-z0-9][a-z0-9-]{0,62}$`). 50 MiB body cap. Accepted extensions: `.c`/`.h` (C) or `.rs` (Rust), depending on `language`.
3. Control plane's `MigrationService` invokes **`edge-migrate` as a subprocess** (`exec.CommandContext(... "edge-migrate" "--transform" "--language" <lang> <path>)` — see `edge-control-plane/internal/service/migration.go`) for tree-sitter analysis + auto-transformation. C path: POSIX → WASI C patterns (`socket()` → `create-tcp-socket()`, `bind()` → `start-bind()`/`finish-bind()`, `recv`/`send` → wasi:io streams). Rust path: `std::net::TcpListener::bind` → `TcpSocket::new(AddressFamily::Ipv4)?.start_bind(...)...`, `std::fs::File::open` → `wasi::filesystem::open(...)`, etc. (see `edge-migrate/docs/design.md` §4.4). The Go control plane does **not** import the Rust library directly; it shells out and parses the JSON `MigrationReport` from stdout. In tree mode, it also runs `edge-migrate --analyze-json --language <lang> <path>` per file to populate per-file `FileReport` fields.
4. Transformed source is compiled via wasi-sdk's `clang --target=wasm32-wasip2 -nostdlib` (C) or `rustc --target wasm32-wasip2 --crate-type=cdylib --edition 2021` (Rust; requires `rustup target add wasm32-wasip2` on the server, path controlled via `RUSTC_PATH` env var). Tree mode compiles all transformed files together in a single invocation. The wasm size is checked against `MaxArtifactSize` (100 MiB) on **both** endpoints.
5. Wasm stored at `/registry/{tenant_id}/{app_name}/{deployment_id}.wasm`; a `deployments` row is written with status `migrated` (no `active_deployments` row yet).
6. Response is returned: single-file mode returns `MigrationReport`; tree mode returns `TreeMigrationReport` with a per-file `FileReport` array (`{path, status, patterns_detected, transformations, manual_review, errors, preprocessor}`). The developer activates via `edge deploy <app> --id <id>`.

## edge-runtime Deep Dive

The runtime library is structured around the WIT world in `src/wit/edge.wit` (loaded at `src/lib.rs:13-15` via `wasmtime::component::bindgen!({path: "src/wit/edge.wit"})` — **not inline in `lib.rs`**, contrary to the previous CLAUDE.md). The macro generates Rust bindings at compile time.

### Core modules

| File | Role |
|------|------|
| `src/lib.rs` | Public re-exports; loads WIT via `bindgen!`. |
| `src/engine.rs` | wasmtime `Engine` with security-hardened config (no threads, no reference types, SIMD on, component model on, epoch interruption on). Engine is meant to be shared across apps so compilation is cached. |
| `src/runtime.rs` | `RuntimeState` — implements every WIT `Host` trait by **delegating** to per-interface sub-structs. Three constructors: `new()` (`#[cfg(test)]` only, ephemeral), `with_env(env, tenant_id)` (tenant isolation), `with_env_and_meter(env, meter, tenant_id)` (per-deployment billing). All three make per-tenant persistent stores via `EDGE_KV_STORE_PATH/{tenant_id}/`, `EDGE_CACHE_PATH/{tenant_id}/`, `EDGE_SCHEDULING_PATH/{tenant_id}/`. |
| `src/linker.rs` | `create_component_linker` wires every WIT interface in via the macro-generated `EdgeRuntime::add_to_linker`. |
| `src/store.rs` | `HasStoreLimits` public trait + `create_store<T: HasStoreLimits>`. Store data `T` embeds a `StoreLimits` field; `create_store` calls `set_store_limits` before constructing the `Store`, then wires wasmtime's limiter callback via a lifetime-bounded closure `\|data\| data.store_limits_mut()`. No `Box::leak`, no `'static` extension, no `unsafe`. `StaticLimiter` was removed in issue #176 fix. |
| `src/memory.rs` | `read_string`/`write_string`/`read_bytes`/`write_bytes`/`allocate`/`get_memory` for crossing the wasm boundary. `get_memory` must be called **after** any wasm execution because `memory.grow()` invalidates the `Memory` handle. |
| `src/limits.rs` | `StoreLimitsBuilder` config (memory size + table elements + instances/memories counts). |
| `src/metering.rs` | `RequestMeter` — atomic per-deployment request counter, snapshotted into heartbeats. |
| `src/interfaces/` | Per-interface host implementations (feature-gated). |

### The `edge:*` interfaces

Each interface lives in its own feature-gated module under `src/interfaces/`. `RuntimeState` holds one instance of each and implements the bindgen-generated `Host` trait by delegating to the inner struct.

| Interface | Module | Notes |
|---|---|---|
| `http-client` | `http_client.rs` | `reqwest::Client` (async) with `tokio::time::sleep` for backoff (no `spawn_blocking`; the previous blocking-client approach panicked under async contexts — fixed in commit `d2399f4`). 3 retries with exponential backoff capped at 10s; retryable = timeout/connect/`429`/`502`/`503`/`504`. W3C `traceparent` validation + `tracestate` forwarding; one global `OnceLock<Arc<Client>>` for pooling. |
| `kv-store` | `kv_store.rs` | `RwLock<HashMap>`; optional on-disk persistence to `<EDGE_KV_STORE_PATH>/store.json` via atomic rename; TTL cleanup every 100 writes; batch `get/set/delete_many`. Initial load runs in a separate thread+runtime so `block_on` doesn't panic inside an existing tokio context. |
| `cache` | `cache.rs` | Same persistence pattern as kv-store but with an LRU cap. |
| `observe` | `observe.rs` | Wraps the `metrics` crate: counters, gauges, histograms, `emit_log`. |
| `time` | `time.rs` | `now`/`sleep`/`resolution` via the `clock` crate. |
| `scheduling` | `scheduling.rs` | Delayed + repeating tasks via tokio; persistent; uses an `Instant ↔ Unix` boot-time offset. |
| `process` | `process.rs` | Env vars + args + cwd + **clean exit mechanism**: `exit(code)` stores an `AtomicU32` then returns; the resulting wasmtime trap is later distinguished from a real error by `RuntimeState::exit_requested()`. Has a defensive env blocklist (`AWS_*`, `*SECRET*`, `*API_KEY*`, …) — best-effort, not exhaustive. |
| `networking` | `networking.rs` + `dns.rs` | DNS resolution with a 60-entry cache, shared with `http-client` for outbound lookups. |
| `http-server` | `http_server.rs` | The largest module (880 lines). tokio TCP server with `httparse`, optional TLS via `rustls`, gzip above 512 bytes, 10 MB body cap, `mpsc` queue to hand requests to guest code, tracks requests by `u64` id. Calls `RequestMeter::record_request` on every accepted connection. |

### Feature flags

Each interface has a Cargo feature in `edge-runtime/Cargo.toml`. The `default` feature set enables all nine. To add a new interface: add it to `src/wit/edge.wit`, create `src/interfaces/<name>.rs`, wire it through `src/interfaces/mod.rs` + `src/runtime.rs`, and add a `#[cfg(feature = "...")] pub mod` entry. Run `cargo build` to regenerate bindings.

### WIT world

```wit
package edge:cloud@0.1.0;
world edge-runtime {
  import http-client; import networking; import kv-store; import cache;
  import observe; import time; import scheduling; import process; import http-server;
}
```

No exports — the guest is fully host-provided. All nine interfaces are pulled in unconditionally at the WIT level; only the Rust implementations are feature-gated.

## Conventions & Gotchas

- **No top-level Cargo workspace.** Don't add `[workspace]` at the repo root — each crate is independent. The `edge-migrate` sub-directory has its own internal workspace.
- **`edge-runtime` engine is meant to be shared.** Create it once (e.g., per worker process) so wasmtime can cache compilation across app instances.
- **Bridge sync → async.** `HttpClientHost::fetch` and `HttpServerHost` calls still use `tokio::runtime::Handle::current().block_on(...)` to call the now-async `http_client.fetch()` / `http_server` methods from sync WIT trait impls. The historical foot-gun (blocking reqwest runtime panic when dropped in an async context) was fixed by `d2399f4` — but the sync→async bridge in `runtime.rs` remains. Don't move async work outside that bridge.
- **Guest exit vs. wasm trap.** Always check `RuntimeState::exit_requested()` after a guest call returns `Err` — a clean `process.exit` looks like a trap to wasmtime.
- **`edge-migrate` placement.** Per `edge-migrate/docs/design.md` v0.3 (2026-06-19, the more authoritative doc for this tool), `edge-migrate` is a **standalone binary** (`cargo install edge-migrate`), not a subcommand of `edge-cli`. The older whitepaper §10.2 (2026-06-14) still describes it as an `edge migrate` subcommand — that description is **superseded by design.md**. The current code has a stub in `edge-cli/src/migrate/transformer.rs`; the real transform lives in `edge-migrate/edge-migrate-lib` and is invoked by the Go control plane as a subprocess via `edge-migrate --transform [--language c|rust] <path>`. Don't add new logic to the CLI stub.
- **`edge-migrate` language dispatch (M3).** The bin accepts `--language c` (default) or `--language rust`. The Rust path is feature-gated in `edge-migrate-lib` (`features = ["rust"]` pulls in `tree-sitter-rust`) and emits a `wasi::socket` + `wasi::filesystem` `use` prelude plus per-pattern rewrites. The Go control plane compiles Rust output with `rustc --target wasm32-wasip2 --crate-type=cdylib --edition 2021` (`RUSTC_PATH` env var to override the default `rustc`). Server hosts must have `rustup target add wasm32-wasip2` installed. The handler language gate accepts `c` or `rust`; anything else returns 400 `only c and rust are supported`. See `edge-migrate/docs/design.md` §4.4 and §5.4.
- **`edge-migrate` preprocessor.** When `clang` is reachable (PATH lookup, falling back to `$WASI_SDK_PATH/bin/clang`), the analyzer runs the source through `clang -E -nostdinc` before tree-sitter parsing. Patterns hidden behind `#define` macros (e.g. `#define socket(x) make_socket(x)`) become visible. `MigrationReport` and `TransformResult` carry a `preprocessor: Option<PreprocessorInfo>` field. When clang is missing, the analyzer silently falls back to the unexpanded source. See `edge-migrate/docs/design.md` §2.2.
- **Egress allowlist.** Per whitepaper §9.5, tenants can specify allowed outbound destinations (e.g. `api.stripe.com`). The `http-client` interface does not currently enforce this; enforcement is the worker's job (or a future middleware).
- **`WORKER_TENANT_ID` required for edge-worker.** The worker requires this env var at startup — it is the **operator's** tenant ID (the tenant whose apps this worker hosts). It's stamped into every JWT `tenant_id` claim and scopes all outbound `/api/internal/*` calls (downloads, logs, sync). The worker is architecturally multi-tenant (it can host apps from different tenants via NATS task messages), but because the JWT is signed once at boot, every HTTP call carries this single tenant ID. A worker with `WORKER_TENANT_ID=t_a` cannot download artifacts or forward logs for apps belonging to `t_b`. Setting it to the wrong tenant ID will cause downloads/sync to return 404 for every app on this worker. A future change should make the JWT per-request rather than per-boot.
- **Port pool exhaustion** in `edge-worker/src/supervisor.rs` calls `.expect("port pool exhausted")` — should probably surface as `Err` instead.
- **Artifact integrity.** `edge-worker` SHA-256-verifies every downloaded artifact against `AppSpec.deployment_hash` before instantiation (`edge-worker/src/downloader.rs::verify_hash`). Empty hash, malformed hash, or mismatch causes `get_artifact` to return `Err`; the supervisor releases the port and logs both expected and actual hashes. The wire format is bare lowercase hex (64 chars), not `sha256:<hex>` (the latter only appears in `whitepaper.md` examples and is a docs bug).
- **Persisted interfaces** (kv-store, cache, scheduling) honor `EDGE_KV_STORE_PATH` / `EDGE_CACHE_PATH` / `EDGE_SCHEDULING_PATH` env vars. Absent or invalid → ephemeral in-memory only.