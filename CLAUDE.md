# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

edgeCloud is a managed WebAssembly edge computing platform. Developers compile a service to a WASI Preview 2 component, run `edge deploy`, and the platform runs it on worker nodes close to end users. Tenant code is sandboxed in Wasmtime, scoped per-tenant, metered per request, and can call platform-provided host interfaces (`edge:cloud/*`) for HTTP, KV, cache, observe, time, scheduling, process, and WebSocket — plus the full `wasi:*` surface from `wasi:cli/command@0.2.1`.

### Repo layout

This is a **Cargo workspace** at the repo root (`Cargo.toml`, `[workspace] resolver = "2"`) with **9 members** plus one excluded crate. There is also a Go module (`edge-control-plane/`). Every Rust crate in the workspace can be built together via `cargo --workspace`.

| Crate | Language | Role |
|---|---|---|
| `edge-runtime/` | Rust | Wasmtime host library. Exposes `create_engine`, `create_store`, `RuntimeState`, `RequestMeter`, `EgressPolicy`. Implements `edge:cloud@0.2.0` (two worlds). Hosts the per-tenant KV/cache/scheduler and the egress policy. |
| `edge-runtime/bin/wasm2cwasm/` | Rust | AOT pre-compile helper binary (issue #315). Reads `.wasm`, writes `.cwasm`. Invoked by the control plane's `precompile.go` after activation. |
| `edge-worker/` | Rust | Per-node supervisor binary. Subscribes to NATS for desired-app updates, instantiates WASM components, hosts their HTTP servers. Two execution models: long-running and FaaS (Handler). |
| `edge-ingress/` | Rust | Public-facing Caddy controller. Subscribes `edgecloud.heartbeats.<region>`, renders Caddyfile-JSON that maps `<tenant>-<app>.edgecloud.dev` to a worker host:port. DDoS caps + per-IP rate limit. |
| `edge-cli/` | Rust | Developer CLI (`edge init \| build \| deploy \| dev \| activate \| env \| ...`). The package is `edge-cli` but the installed binary is named `edge` (`[[bin]] name = "edge"` in edge-cli/Cargo.toml). Persists local state to `.edge/state.json`. Reads `~/.config/edgecloud/config.toml` via `edge-config`. `edge init --lang=rust` (issue #576) scaffolds a FaaS-shaped `src/lib.rs` + `Cargo.toml` modeled on `samples/hello/`, plus a vendored `wit/` tree (embedded into the binary via `include_dir!` — `edge-cli/src/scaffold/wit.rs`) so the scaffolded project builds offline outside the monorepo. |
| `edge-js-sdk/` | JS | JS-side shim package (`@edgecloud/sdk` on npm) that delegates to `globalThis.EdgeCloud.*` host functions injected by `edge-js-runtime` at request time. Resolved by `edge init --lang=js` from npm (issue #424 — earlier versions referenced the in-tree SDK via a `file:` path that only worked inside the monorepo). |
| `edge-config/` | Rust | Shared helpers for `~/.config/edgecloud/config.toml` loading. Used by `edge-cli` and `edge-migrate-bin` so a config-schema change ships in one crate. |
| `edge-spool/` | Rust | Append-only JSONL disk spool for worker log-batch durability. Worker side, between `LogForwarder`'s in-memory buffer and the control plane's `POST /api/internal/logs`. |
| `edge-test-helpers/` | Rust | Shared test harness builders (Supervisor, RuntimeState) for integration tests. |
| `edge-migrate/edge-migrate-lib/` | Rust | tree-sitter C → WASI C analyzer + transformer, plus `--language rust` for `std::net`/`std::fs` rewrites. Library used only by the bin. |
| `edge-migrate/edge-migrate-bin/` | Rust | Standalone CLI (`edge-migrate --transform [--language c\|rust] <path>`). Invoked as a subprocess by the Go control plane. |
| `edge-control-plane/` | Go | HTTP API + Postgres (sqlx) + NATS publisher. Three binaries: `cmd/api` (the server), `cmd/migrate` (DB schema migrations via rubenv/sql-migrate), `cmd/printpub` (debug tool that prints NATS messages). |

**Excluded from the workspace** (`Cargo.toml [workspace.exclude]`):
- `edge-worker/tests/fixtures/handler` — built separately by the Phase E fixture-build script. Uses `wasm32-unknown-unknown` and an older `wit-bindgen` pin.

`edge-js-runtime` (QuickJS runtime for the FaaS / `edge-runtime-handler` world, issue #317) and `edge-js-runtime-long` (rlib for the long-running / `edge-runtime` world, issue #426) are also workspace members (issue #510). The `register_*` namespace helpers are factored into `edge-js-runtime/src/register.rs` and reused (via duplication) by the LR sibling; the LR crate is rlib-only (NOT cdylib) because the canonical world requires `start: func()` as an export, and a cdylib in this crate would land a second `start` symbol in the shim's final link and clash.

**Documentation map:**
- `whitepaper.md` is the broad design doc (2026-06-14). Per-tool design docs (e.g. `edge-migrate/docs/design.md`) are scoped to one tool and may be newer — **when the two conflict, trust the per-tool design doc**. Treat any design doc as the source of intent, but always verify against the actual code.
- `docs/jwt-bootstrap.md`, `docs/nats-auth.md` — operational runbooks.
- `edge-ingress/README.md` — operator runbook for the Caddy controller.

## Build, Test, Lint, CI

The repo is a Cargo workspace — `cargo --workspace` is the default. Use `--manifest-path` only when you need to scope a build to one crate (e.g., a fast iteration loop).

```bash
# All Rust crates
cargo build    --workspace
cargo test     --workspace               # or `cargo nextest run --workspace` (CI uses nextest)
cargo fmt      --all -- --check
cargo clippy   --workspace --all-targets -- -D warnings
cargo deny     check                     # license/advisory gate (CI runs this)
cargo audit                              # advisory gate (CI runs this; non-blocking)

# Single crate (fast iteration)
cargo build  --manifest-path edge-runtime/Cargo.toml
cargo test   --manifest-path edge-worker/Cargo.toml

# Go
cd edge-control-plane && go build ./... && go test ./...
cd edge-control-plane && go test -tags=integration ./migrations/...   # roundtrip every *.sql via testcontainers
cd edge-control-plane && gofmt -l . && go vet ./...
```

### Local dev: shared target cache

The workspace pulls in heavy crates (wasmtime, tree-sitter, wasmtime-wasi-http). Each `git worktree` owns its own working tree, and Cargo's default `target/` lives inside it — running 5 agents in parallel worktrees can balloon to 20 GB+. To keep local dev light, the repo is wired to share `target/` and wrap `rustc` with `sccache`:

- **`target/` is shared across worktrees.** `.cargo/config.toml:36-37` (committed) sets `build.target-dir = "../target-cache/edgecloud"` — config-relative, so the resolved path is `<workspace>/target-cache/edgecloud/`. Every worktree compiles into the same dir; content-addressed fingerprinting means only changed sources rebuild.
- **`rustc` is wrapped with sccache.** Same file sets `build.rustc-wrapper = "sccache"`. Install once: `brew install sccache` (or `cargo install sccache`). sccache itself stores its cache at `~/.cache/sccache-edgecloud` (override with `SCCACHE_DIR`).
- **Dev profile is trimmed.** `Cargo.toml` sets `[profile.dev] debug = "line-tables-only"` so backtraces still resolve file:line but `.dwp`/`.dwo` bloat is dropped.

Verify after a fresh clone:

```bash
sccache --version                       # ≥ 0.7
cargo build --workspace                 # cold; observe "Compiling ..." via sccache
du -sh ../target-cache/edgecloud        # single shared target (one dir up from the workspace root)
```

If a build fails with `could not execute wrapper 'sccache'`, install sccache or unset the wrapper locally with `CARGO_BUILD_RUSTC_WRAPPER=""`. CI does not use sccache — `Swatinem/rust-cache@v2` already caches cold builds, and the per-job `RUSTFLAGS` set `-C debuginfo=0` so CI builds stay lean.

### Vendored WASI Preview 1 reactor adapter

`edge build --lang=js` shells out to `wasm-tools component new --adapt <adapter>` to wrap the `wasm32-wasip1` core module produced by `edge-js-runtime` into a WASI Preview 2 component the host linker accepts. The adapter is committed at `edge-cli/adapters/wasi_snapshot_preview1.reactor.wasm` (52,238 bytes, SHA-256 `49fafbdac877303ac91bd178d8ad6b14041aca5136362fe6864f96c8413b5bea`). Pinned against wasmtime **v45.0.3** — byte-identical to the asset at <https://github.com/bytecodealliance/wasmtime/releases/download/v45.0.3/wasi_snapshot_preview1.reactor.wasm>. SHA-256 is checked by `sha256sum -c SHA256SUMS` (sidecar at `edge-cli/adapters/SHA256SUMS`); that step runs in the `rust-js-build` CI job. Issue #423.

A wasmtime bump in `edge-runtime/Cargo.toml` requires re-vendoring the adapter: download the new release asset, drop it at `edge-cli/adapters/wasi_snapshot_preview1.reactor.wasm`, and update `SHA256SUMS` so the CI check stays green.

`edge-cli/src/commands/build.rs::resolve_wasi_adapter()` probes three sources in priority order — `$EDGE_JS_WASI_ADAPTER` env override, the vendored file, then a fallback cargo-registry glob for dev machines with `wasi-preview1-component-adapter-provider-45.0.x` cached locally from another project. The vendored file wins on a fresh clone.

### CI

`.github/workflows/ci.yml` runs on every push to `main` and every PR. There is **no `.gitlab-ci.yml` in this repository** — earlier docs that mentioned GitLab CI were stale.

CI jobs:

| Job | What it does |
|---|---|
| `rust-lint` | `cargo fmt --check --all`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo deny check`, `cargo audit` |
| `rust-test` | `cargo nextest run --workspace` |
| `rust-semver` | `cargo semver-checks` on the public API of `edge-runtime` and `edge-worker` |
| `rust-nightly` | nightly-only checks (cargo-udeps etc.) |
| `openapi-validate` | validates `edge-control-plane/docs/api/openapi.yaml` |
| `ts-client` | regenerates and typechecks `edge-control-plane/internal/generated/api-types.ts` |
| `go-fmt` | `gofmt -l edge-control-plane/` |
| `golangci-lint` | golangci-lint v6 (latest) |
| `go-test` | `go test -coverprofile=... ./...` |
| `go-test-integration` | `go test -tags=integration -v ./migrations/...` against a postgres:16 service |
| `typos` | crate-ci/typos across the whole repo |
| `coverage-rust` | cargo-llvm-cov (informational) |
| `rust-js-build` | Exercises the JS build pipeline end-to-end (`rustup target add wasm32-wasip1` + esbuild + `wasm-tools component new --adapt <vendored adapter>` against `samples/hello-js-ws/`); also verifies the vendored WASI Preview 1 reactor adapter via `sha256sum -c edge-cli/adapters/SHA256SUMS` (issue #423). |

`.github/workflows/preview.yml` is a `deploy-preview` job that runs on every PR `opened`/`synchronize` event (issue #308). The composite action at `.github/actions/deploy-preview/action.yml` builds the CLI via `cargo install --root $CARGO_HOME`, then runs `edge deploy --preview --pr-number=${{ github.event.pull_request.number }}`. The action includes a `Expose edge CLI on PATH` step that appends `$CARGO_HOME/bin` to `$GITHUB_PATH` — without it the next bash step fails with `edge: command not found` (rc=127). The URL is parsed from the CLI's stdout and exposed as the `preview-url` step output; the workflow's `Comment PR` step posts it on the PR when `EDGECLOUD_API_KEY` is set (fork PRs lack the secret and silently no-op).

## Agent Behavior

These rules govern how this repo expects Claude (or any other agent reading `CLAUDE.md`) to operate during a session. They override any default agent instincts where they conflict.

### Stick to the problem. Don't run away.

- **When you spot a problem in passing, don't ignore it.** If you're working on task A and notice a real bug, missing test, or stale doc in an adjacent area, surface it — either fix it as part of the current change (if it's tiny and obviously related) or file it as a separate issue via `gh issue create` and keep moving. Never silently leave known defects in the working tree.
- **Don't bounce.** If a tool fails, a build breaks, or a test fails, dig in until you understand the failure mode and either fix it or hand back a precise `needs input:` describing the unblock. Don't loop the same failing command five times hoping it'll start working.
- **When you can't make progress, ask.** Use `needs input:` for decisions where guessing costs more than a round-trip. Use `failed:` when the framing itself is wrong (wrong repo, missing binary, premise contradicted by the code). Use plain text when a sensible default exists — make the call, note the assumption, keep going.

### Don't spawn subagents unless it's truly necessary.

- **Do the work yourself.** Use `Read` / `Grep` / `Glob` / `Bash` / `Edit` / `Write` directly. Subagents are for fan-out searches across many files when you only need the conclusion, not the file dumps — and even then, prefer targeted `Grep` first.
- **Justified subagent use:** sweeping the repo for all references to a symbol that's about to be renamed; cross-cutting audit work that needs to read dozens of files in parallel; reproducing a flaky CI failure that requires stepping through a long log. Each one should be a deliberate `Agent` call with a tight scope, not a habit.
- **Avoid subagents for:** reading one file, making one edit, running one test, debugging a stack trace you can see. Those are faster (and cheaper) inline.
- **Don't chain subagents.** A subagent that spawns a subagent that spawns a subagent is almost always wrong — by the third hop you can't reason about what any of them actually saw.

### Ship the work, don't stop at "ready to ship."

- **Small commits on a fresh branch from `main`.** Each commit should be a self-contained, reviewable unit. Don't bundle an unrelated refactor with a bug fix. Don't dump 30 files in one commit.
- **Verify before pushing.** Run the relevant tests (`cargo nextest run --workspace`, `go test ./...`, `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`) and read the output. If a test fails, fix it — don't push red.
- **Push and open a draft PR.** Use `gh pr create --draft`. Never push to `main` / `master` directly, never force-push, never merge your own PRs.
- **Watch CI until it's green.** After opening the PR, poll `gh pr checks <number>` (or `gh run watch`) until all required jobs pass. If a job fails, debug from the logs (`gh run view <id> --log-failed`), fix the cause, push, re-poll. Don't mark the task done with red checks pending.
- **Pull latest `main` after merge.** Once the PR is merged (the user will usually say "merged" or it'll show up in `gh pr list --state merged`), run `git checkout main && git pull --ff-only`. If `main` has moved while your branch was open, the next task starts on top of the new tip.

### Code is not done until tests and docs ship.

- **Write tests for every behavior change.** New Rust code → `cargo test` in the affected crate + integration tests under `tests/` if the change crosses module boundaries. New Go service → unit tests in `internal/service/<name>_test.go` and, if it touches SQL, a roundtrip test in `migrations/` tagged `integration` that runs against a `postgres:16` service container (per `go-test-integration` job).
- **Update docs to match.** If you change a public API, an interface, a CLI subcommand, an env var, or a deployment contract, update the relevant doc (`CLAUDE.md`, `whitepaper.md`, `edge-migrate/docs/design.md`, `edge-ingress/README.md`, or a `docs/*.md` runbook) in the same commit. Don't let the doc lag the code.
- **Cite sources in prose.** When a section claims "X is at Y", the reader should be able to grep for it. Use `path:line` form (`edge-runtime/src/runtime.rs:836`) wherever practical; cite the file without a line only when the line number is unstable.

## End-to-End Architecture

A request flows through the system like this:

1. **Build** — developer runs `edge build` → for Rust, `cargo build --target wasm32-wasip2 --release` → `.wasm` component. For JS (issue #317), the JS source is bundled and executed in the QuickJS runtime via `edge-js-runtime` (FaaS world `edge-runtime-handler`) or via a shim that pulls `register_*` from `edge-js-runtime-long` (long-running world `edge-runtime`, issue #426). The shim is the one that produces the cdylib; the LR crate is rlib-only and only supplies the helpers. The FaaS JS pipeline (`edge build --lang=js`) builds `edge-js-runtime` directly for `wasm32-wasip2` — the `wasm32-wasip2` cargo target emits a complete WASI Preview 2 component natively, so no `wasm-tools component new --adapt` wrap step or wasi-preview1 reactor adapter is needed (the earlier wasip1 path was dropped on the `feat/edge-js-runtime-wasip2` branch; the lockstep test against `wasi-preview1-component-adapter-provider 45.0.3` is gone with it). The LR JS pipeline (`edge-js-runtime-long/`, `samples/hello-js-ws/`) still targets `wasm32-wasip1` and is wrapped with the wasi-preview1 reactor adapter — that scope is a follow-up. The JS pipeline additionally requires `wasm-tools 1.252.x` on PATH (for the Rust guest pipeline's `wasm-tools component new` wrap step — `edge build --lang=rust`, `edge-migrate`, the worker fixture build) and the `wasm32-wasip2` Rust target (one-time host prereqs installed by `scripts/dev-install.sh`); the SDK package `@edgecloud/sdk` is pulled from npm at scaffold time (`edge init --lang=js`, issue #424).
2. **Sign** — `edge deploy` POSTs the artifact to `POST /api/v1/deploy/{appName}` with a Bearer API key. The control plane (`edge-control-plane/internal/service/deployment.go`) SHA-256-hashes the blob, stores it via `storage.ArtifactStore.Save` (defaulting to `/registry/{tenant_id}/{app_name}/{deployment_id}.wasm`), signs `(sha256_raw_32_bytes || deployment_id)` with Ed25519, and writes the row + signature to `deployments`.
3. **Pre-compile** — `edge activate <deployment_id>` flips `active_deployments`, then the control plane's `precompile.PrecompileCwasm` (best-effort) shells out to `wasm2cwasm` and stores the result via `ArtifactStore.SaveFormat(..., "cwasm", ...)` next to the `.wasm`.
4. **Activate** — the control plane's `deployment.Service.ActivateDeployment` then publishes a `TaskMessage` over NATS JetStream to `edgecloud.tasks.<region>`. In parallel, `cache_pusher` PUTs the artifact to each per-region edge-cache binary (3-second timeout, best-effort) and updates `active_deployments.regions_cached` / `regions_cache_failed`.
5. **Reconcile** — `edge-worker` subscribes to that subject. `Supervisor::handle_task_message` (`edge-worker/src/supervisor.rs`) diffs desired apps vs. running apps and starts/stops accordingly. Starting an app means: acquire a port from `PortPool` (with 60s cooldown), download the artifact (cached locally as `.wasm` + `.cwasm`), verify SHA-256 + Ed25519 signature, instantiate the component, and spawn either `run_app_loop` (long-running) or `HandlerDispatch::serve` (FaaS).
6. **Execute** — there are two execution models, picked structurally at link time by `edge-worker/src/detect.rs`:
   - **Long-running** — the guest's `_start` opens a real TCP socket on the worker via `wasi:sockets/tcp`. Each accepted request goes through `httparse`, into an `mpsc`, the guest polls it, calls `respond`, and the server writes bytes back. Each request bumps `RequestMeter::record_request` for billing. On guest trap or `process.exit`, the supervisor restarts with exponential backoff (max 5 attempts, then `Crashed` status).
   - **Handler (FaaS)** — the guest exports `wasi:http/incoming-handler`. The worker hosts one HTTP/1 server per app, calls the guest once per request via `wasmtime_wasi_http::ProxyPre::instantiate_async`, returns the synthetic response.
7. **Heartbeat** — the worker publishes `HeartbeatMessage{app_status, request_count, outbound_bytes}` to `edgecloud.heartbeats.<region>` every **30s** (whitepaper §5.6) so the control plane can bill and monitor. Each `AppStatus` carries a `dedupe_id` (issue #418) — a stable `(worker_id, deployment_id, 30s_bucket)` token the CP uses to skip re-applying the same delta on JetStream redelivery or reconcile replay. `edge-ingress` subscribes to the same subject to learn routing. The `dedupe_id` is stamped on `AppStatus` in the JSON body (not as a NATS header) so the token survives `edge-ingress` re-publishes to a downstream subscriber.
8. **Quota enforcement** (issue #420 / #44 part 2) — heartbeat-driven counters in `quotas` drive two enforcement points. **Deploy-time** (`service.Deploy` in `edge-control-plane/internal/service/deployment.go`): returns **402 PAYMENT_REQUIRED** when (a) `billing_subscriptions.status` is `past_due`/`canceled` (reason `subscription_past_due`), (b) a free-tier tenant has `tenants.disabled_at IS NOT NULL` (reason `free_tier_exceeded`), (c) `quotas.quota_lock_grace_until > now()` (reason `quota_lock_grace_active`), (d) `quotaRepo.VerifyUnderCap` returns false (reason `quota_will_be_exceeded`), or (e) `quotaRepo.VerifyMemoryUnderCap` returns false (reason `memory_quota_will_be_exceeded` — issue #44 part 2; checks `quotas.used_memory_mb + perAppMemory > quotas.MaxMemoryMB`, with `MaxMemoryMB < 0` the unlimited sentinel for `enterprise` and `quotas.used_memory_mb` maintained transactionally inside `activateDeployment` / `rollbackDeployment` via `QuotaRepository.WithTx(tx).AddMemoryMB`). The existing deploy-count cap (`count >= quota.MaxDeployments`) still returns 429 `QUOTA_EXCEEDED` — distinct from 402 because "you have too many deploys" is a throttle, not a billing boundary. **Request-time**: `edge-ingress` polls `GET /api/v1/internal/quota/{tenantID}` every `QUOTA_FETCH_INTERVAL` (default 30s) and injects a Caddy `static_response 402` block before the reverse_proxy route for any tenant where `over_cap=true`. Free-tier lockdown reuses `tenants.disabled_at` — when the heartbeat pipeline crosses cap for a free tenant, it dual-writes `quotas.quota_lock_grace_until` (deploy-time fires immediately) and `tenants.disabled_at` (request-time fires after the grace clock expires). Operator escape hatch: `POST /api/v1/admin/tenants/{id}/quota-override` (owner-role, audit-logged) sets `overage_allowed_until`, clears `disabled_at`, or clears grace.

### Key contracts

**NATS subjects** (all JetStream-durable; stream declared in `edge-control-plane/cmd/api/main.go:53-61` with **retention `InterestPolicy`**, replication factor from `cfg.NATS.Replicas`, max age 24h):
- `edgecloud.tasks.<region>` — control plane → workers (`TaskMessage{timestamp, tenant_id, apps}` — `TaskUpdate`, `FullSync`, or `TaskPurge` variants). `TaskPurge` carries `{tenant_id, app_name?, reason: app_deleted|tenant_offboarded}` and triggers `edge_runtime::purge_tenant` on receipt (issue #569); empty `app_name` = tenant-wide. Same wire subject, distinct `type` discriminator.
- `edgecloud.heartbeats.<region>` — workers → control plane **and** ingress (`HeartbeatMessage{timestamp, worker_id, region, apps}`)
- `edgecloud.deployments.<tenantID>` — tenant-scoped deployment events (per whitepaper §4.2)

**PostgreSQL schema** (control plane; full DDL in `edge-control-plane/migrations/*.up.sql`):

| Table | Purpose |
|---|---|
| `tenants` | Tenant rows; `id` prefixed `t_`. |
| `quotas` | Per-tenant quota (CPU/memory/reqs/outbound bytes). |
| `api_keys` | API keys, SHA-256 hashed; `id` prefixed `k_`. |
| `deployments` | Every uploaded artifact (status, hash, signature, regions). |
| `active_deployments` | Currently-active deployment per `(tenant, app)`, plus `regions_cached` / `regions_cache_failed`. |
| `app_env` | Per-app env vars (optionally encrypted via the secrets encryptor). |
| `apps` | App metadata (name, language, rate limit, desired replicas). |
| `workers` | Worker registry; `id` prefixed `w_<region>_`. |
| `worker_status` | Heartbeat-derived status per worker. |
| `app_traffic_splits` | Canary / blue-green splits. |
| `domains` | Custom FQDN bindings. |
| `webhooks` | Per-tenant webhook config. |
| `webhook_deliveries` | Webhook delivery attempts + outcomes. |
| `logs` | Worker-ingested log entries (TTL'd via `log_gc`). |
| `audit_logs` | Admin/owner action audit trail. |
| `autoscale_events` | Scale up/down events. |
| `outbox` | Durable-publish queue for `task_update` NATS messages (issue #42). The row is written in the same transaction as the `active_deployments` mutation it accompanies, and relayed by `service.OutboxDrainer` (`edge-control-plane/internal/service/outbox_drainer.go`). Rows transition `pending` → `in_flight` → `published` (or `failed` after `OUTBOX_MAX_ATTEMPTS` retries). `FullSync` messages from the reconcile loop are NOT outboxed. |

IDs are prefixed: tenants `t_`, deployments `d_`, API keys `k_`, workers `w_<region>_`.

**Control-plane HTTP surface** (see `edge-control-plane/internal/app/app.go` for the full route table):
- **Public**: `POST /api/v1/tenants` (self-signup, IP-rate-limited), `GET /health` (pings DB + NATS), `GET /docs/` (Swagger UI).
- **Tenant-authenticated** (Bearer API key, SHA-256-hashed in `api_keys`): `/api/v1/deploy/{appName}`, `/api/v1/apps/{appName}/activate/{deploymentID}`, `/api/v1/apps/{appName}/rollback`, `/api/v1/apps/{appName}/promote/{deploymentID}`, `/api/v1/apps/{appName}/env*`, `/api/v1/keys*`, `/api/v1/webhooks*`, `/api/v1/apps/{appName}/domains*`, `/api/v1/egress*`, `/api/v1/metrics`, etc.
- **Admin (owner role only)**: `/api/v1/admin/tenants*`, `DELETE /api/v1/admin/apps/{appName}`, `/api/v1/admin/cluster*`, `/api/v1/admin/secrets/*`, `POST /api/v1/admin/tenants/{id}/quota-override` (issue #420 — operator escape hatch for the billing umbrella).
- **Internal, worker JWT (HMAC-SHA256, 24h TTL per whitepaper §9.3 — keys live in `cfg.JWT.Keys`, `cfg.JWT.ActiveKID` selects the current kid)**: `/api/internal/workers*`, `/api/internal/logs`, `/api/internal/apps/{appName}/auto-rollback`, `/api/internal/domains*`, `/api/internal/tls-allowed`. The worker JWT is gated by role (`ingest` for domains/TLS, default for the rest).
- **Internal, `X-Internal-Token`**: `/api/v1/internal/traffic/{tenantID}/{appName}` (read by `edge-ingress`), `/api/v1/internal/rate-limits/{tenantID}/{appName}` (read by `edge-ingress`), `/api/v1/internal/quota/{tenantID}` (read by `edge-ingress` — issue #420), `/api/v1/admin/secrets/*`.
- **Dual-auth**: `GET /api/internal/download/{deploymentID}` accepts either a worker JWT or `X-Internal-Token` (used by `edge-ingress`).
- **Deprecated unversioned paths** (`/api/tenants`, `/api/deploy/...`, `/api/admin/...`, etc.) redirect to `/api/v1/...` with a `Sunset` header. Sunset date `2026-09-20`.

**Migration flow** (server-side; the dev CLI is a thin uploader — per `edge-migrate/docs/design.md` v0.3):
1. Developer runs either (the `--language` flag selects between C and Rust; default is `c`):
   - `edge-migrate [--language c|rust] hello_world.{c,rs}` — single-file mode, app name derived from the file stem.
   - `edge-migrate --language c|rust --tree ./my_project/ [--app-name NAME]` — tree mode. C: walks `.c`/`.h` files. Rust: walks `.rs` files. Both skip `build/`, `target/`, `node_modules/`, etc.
2. The CLI POSTs to one of two endpoints:
   - `POST /api/v1/migrate` (single-file) — multipart: `file`, `filename`, `language: "c"|"rust"`.
   - `POST /api/v1/migrate-tree` (tree) — either multipart parts + a `tree` JSON manifest (`{"files":[...]}`) + one `file` part per entry, **or** a single `tree` part with `Content-Type: application/zip`. Required form field: `app_name` (must match `^[a-z0-9][a-z0-9-]{0,62}$`). 50 MiB body cap. Accepted extensions: `.c`/`.h` (C) or `.rs` (Rust), depending on `language`.
3. Control plane's `MigrationService` invokes **`edge-migrate` as a subprocess** (`exec.CommandContext(... "edge-migrate" "--transform" "--language" <lang> <path>)` — see `edge-control-plane/internal/service/migration.go`) for tree-sitter analysis + auto-transformation. C path: POSIX → WASI C patterns (`socket()` → `create-tcp-socket()`, `bind()` → `start-bind()`/`finish-bind()`, `recv`/`send` → wasi:io streams). Rust path: `std::net::TcpListener::bind` → `TcpSocket::new(AddressFamily::Ipv4)?.start_bind(...)...`, `std::fs::File::open` → `wasi::filesystem::open(...)`, etc. (see `edge-migrate/docs/design.md` §4.4). The Go control plane does **not** import the Rust library directly; it shells out and parses the JSON `MigrationReport` from stdout. In tree mode, it also runs `edge-migrate --analyze-json --language <lang> <path>` per file to populate per-file `FileReport` fields.
4. Transformed source is compiled via wasi-sdk's `clang --target=wasm32-wasip2 -nostdlib` (C) or `rustc --target wasm32-wasip2 --crate-type=cdylib --edition 2021` (Rust; requires `rustup target add wasm32-wasip2` on the server, path controlled via `RUSTC_PATH` env var). Tree mode compiles all transformed files together in a single invocation. The wasm size is checked against `MaxArtifactSize` (100 MiB) on **both** endpoints.
5. Wasm stored at `/registry/{tenant_id}/{app_name}/{deployment_id}.wasm`; a `deployments` row is written with status `migrated` (no `active_deployments` row yet). Ed25519 signature over `(sha256_raw_32_bytes || deployment_id)` is persisted on the row.
6. Response is returned: single-file mode returns `MigrationReport`; tree mode returns `TreeMigrationReport` with a per-file `FileReport` array (`{path, status, patterns_detected, transformations, manual_review, errors, preprocessor}`). The developer activates via `edge activate <id>`.

## edge-runtime Deep Dive

The runtime library is structured around the WIT world in `src/wit/edge-cloud.wit` (loaded at `src/lib.rs` via the bindgen! macro — two worlds, two submodules: `edge_runtime_long` and `edge_runtime_handler`). The macro generates Rust bindings at compile time.

### WIT world

```wit
package edge:cloud@0.2.0;

world edge-runtime {
  include wasi:cli/command@0.2.1;   // wasi:sockets/*, wasi:filesystem/*, wasi:io/*, wasi:clocks/*, wasi:random/*, wasi:cli/*

  import kv-store;
  import cache;
  import observe;
  import time;
  import scheduling;
  import process;
  import websocket;
}

world edge-runtime-handler {
  include wasi:cli/command@0.2.1;

  import kv-store;
  import cache;
  import observe;
  import time;
  import scheduling;
  import process;
  import websocket;

  export wasi:http/incoming-handler@0.2.1;   // FaaS dispatch entry point
}
```

`edge-runtime` is the **long-running** world — the guest's `_start` runs forever, opens its own TCP sockets via `wasi:sockets`, and uses `edge:*` for KV/cache/observe/etc. `edge-runtime-handler` is the **FaaS** world — the guest exports `wasi:http/incoming-handler` and the worker calls it once per request via `wasmtime_wasi_http::ProxyPre`.

The `include wasi:cli/command@0.2.1` is what brings in the entire `wasi:sockets/*` family (the egress gap below) plus the filesystem/IO/clocks/random/CLI surfaces the linker registers with `wasmtime_wasi::p2::add_to_linker_async` + `wasmtime_wasi_http::p2::add_only_http_to_linker_async` (`src/linker.rs`).

The WIT version bump from `0.1.0` → `0.2.0` is **strict** — a component compiled against `0.1.0` will not import-match against the current world. Existing deployed components need a recompile.

### Core modules

| File | Role |
|------|------|
| `src/lib.rs` | Public re-exports; loads WIT via `bindgen!`; declares `pub mod` for every module. |
| `src/engine.rs` | wasmtime `Engine` with security-hardened config (no threads, no reference types, SIMD on, component model on, epoch interruption on). Engine is meant to be shared across apps so compilation is cached. |
| `src/runtime.rs` | `RuntimeState` — implements every WIT `Host` trait by **delegating** to per-interface sub-structs. Two constructors: `new()` (`#[cfg(test)]` only, ephemeral) and `with_env_and_meter(env, meter, tenant_id)` (per-deployment billing — the only public constructor, used by both Handler and LongRunning paths in `edge-worker/src/supervisor.rs`). Each constructor makes per-tenant persistent stores via `EDGE_KV_STORE_PATH/{tenant_id}/`, `EDGE_CACHE_PATH/{tenant_id}/`, `EDGE_SCHEDULING_PATH/{tenant_id}/`. Implements `WasiHttpHooks::send_request` (egress first defense) and `WasiHttpView`. |
| `src/linker.rs` | Wires `wasmtime_wasi::p2::add_to_linker_async` + `wasmtime_wasi_http::p2::add_only_http_to_linker_async` + the bindgen-generated `edge_cloud_add_to_linker_get_host` for each edge:cloud interface. |
| `src/store.rs` | `HasStoreLimits` trait + `create_store<T: HasStoreLimits>` that attaches a `StoreLimits` (memory + table elements + instances/memories) before constructing the `Store`. Uses a lifetime-bounded closure `\|data\| data.store_limits_mut()` — no `Box::leak`, no `'static` extension. |
| `src/memory.rs` | `read_string`/`write_string`/`read_bytes`/`write_bytes`/`allocate`/`get_memory` for crossing the wasm boundary. `get_memory` must be called **after** any wasm execution because `memory.grow()` invalidates the `Memory` handle. |
| `src/limits.rs` | `StoreLimitsBuilder` config: `memory_size`, `table_elements(100_000)`, `instances(10)`, `memories(1)`. The 10-instance floor is required for WASI P2 components that embed multiple core wasm instances internally. |
| `src/metering.rs` | `RequestMeter` — atomic per-deployment request counter, snapshotted into heartbeats and recorded on every accepted connection (HTTP server) and every FaaS request. |
| `src/egress.rs` | `EgressPolicy` — per-tenant outbound allowlist (`check(url)` first defense). Hard-deny for loopback/link-local/private/multicast/broadcast IPs and known metadata hostnames. |
| `src/egress_transport.rs` | DNS-rebinding guard (second defense). Clones `wasmtime_wasi_http::p2::default_send_request_handler`, pre-resolves via `tokio::net::lookup_host`, validates each candidate IP with `EgressPolicy::check_resolved_ip`, and connects to the validated IP literal. |
| `src/socket_egress.rs` | Third defense for `wasi:sockets/*` calls — `SocketEgressPolicy` (default: hard-deny everything) applied via `RuntimeState::socket_mode`. Includes a dormant `SocketEgressPolicy::HostnamePinned` variant. Per-app selection is `AppSpec.socket_mode` (NATS, issue #412) — the worker resolves `spec.socket_mode.unwrap_or(self.config.socket_mode)` at `edge-worker/src/supervisor.rs::socket_mode_for_spec` and threads the result into both the Handler `HandlerConfig.socket_mode_for_app` and the LongRunning `execute_app` parameter. The `HostnamePinned` arm additionally requires the worker-wide `EDGE_EGRESS_HOSTNAME_PINNING=true` (compose rule, enforced at `edge-worker/src/dispatch.rs::handle_request`). |
| `src/interfaces/` | Per-interface host implementations (feature-gated). |

### The `edge:cloud/*` host interfaces

| Interface | Module | Notes |
|---|---|---|
| `kv-store` | `interfaces/kv_store.rs` | `RwLock<HashMap>`; optional on-disk persistence to `<EDGE_KV_STORE_PATH>/store.json` via atomic rename; TTL cleanup every 100 writes; batch `get/set/delete_many`. Initial load runs in a separate thread+runtime so `block_on` doesn't panic inside an existing tokio context. |
| `cache` | `interfaces/cache.rs` | Same persistence pattern as `kv-store` but with an LRU cap. |
| `observe` | `interfaces/observe.rs` | Wraps the `metrics` crate: counters, gauges, histograms, `emit_log`. Per-app `MetricsAccumulator` is shared with the supervisor and snapshotted into heartbeats. |
| `time` | `interfaces/time.rs` | `now`/`sleep`/`resolution` via the `clock` crate. |
| `scheduling` | `interfaces/scheduling.rs` | Delayed + repeating tasks via tokio; persistent; uses an `Instant ↔ Unix` boot-time offset. |
| `process` | `interfaces/process.rs` | Env vars + args + cwd + **clean exit mechanism**: `exit(code)` stores an `AtomicU32` then returns; the resulting wasmtime trap is later distinguished from a real error by `RuntimeState::exit_requested()`. Has a defensive env blocklist (`AWS_*`, `*SECRET*`, `*API_KEY*`, …) — best-effort, not exhaustive. |
| `websocket` | `interfaces/websocket.rs` | WebSocket connection hosting per RFC 6455 (issue #312). `listen`/`accept`/`send`/`receive`/`close`. Allocates a port from the worker's pool (under `EDGE_WS_PORT` env). |

WASI surfaces (`wasi:http/*`, `wasi:sockets/*`, `wasi:filesystem/*`, `wasi:io/*`, `wasi:clocks/*`, `wasi:random/*`, `wasi:cli/*`) come from the `wasi:cli/command@0.2.1` include. The runtime impls are feature-gated under Cargo features.

### Egress flow (three layers)

1. **URL-level allowlist** — `EgressPolicy::check(url)` in `runtime.rs`'s `WasiHttpHooks::send_request` rejects hard-deny hosts/IPs and non-allowlisted destinations before DNS.
2. **DNS-rebinding guard** — `egress_transport.rs` pre-resolves the host, validates every candidate IP with `EgressPolicy::check_resolved_ip`, and connects to the IP literal so the kernel can't re-resolve to a poisoned record on the second query.
3. **`wasi:sockets/*` allowlist** — `socket_egress.rs` provides `SocketEgressPolicy`, applied via `RuntimeState::socket_mode` (default: hard-deny everything). **Per-app override:** `AppSpec.socket_mode` (issue #412) selects the per-app mode; the worker resolves `spec.socket_mode.unwrap_or(self.config.socket_mode)` at `edge-worker/src/supervisor.rs::socket_mode_for_spec`. **Note:** there is also a `SocketEgressPolicy::HostnamePinned` variant (issue #309 follow-up, PR #391 follow-up) gated behind the worker-wide `EDGE_EGRESS_HOSTNAME_PINNING` knob — currently **dormant** (code merged, but the knob is off and the documentation in `runtime.rs` warns this is not yet production-ready). The compose rule (issue #412, enforced at `edge-worker/src/dispatch.rs::handle_request`): `HostnamePinned` activates only when **both** the per-app field is `HostnamePinned` AND `EDGE_EGRESS_HOSTNAME_PINNING=true`.

### Feature flags

Each `edge:cloud/*` interface has a Cargo feature in `edge-runtime/Cargo.toml`. The `default` feature set enables `kv-store`, `cache`, `observe`, `time`, `scheduling`, `process`, and `egress-tls`. To add a new interface:

1. Add it to `src/wit/edge-cloud.wit` (and bump the WIT version if the change is breaking).
2. Create `src/interfaces/<name>.rs`.
3. Add `pub mod <name>;` to `src/interfaces/mod.rs` (feature-gated).
4. Add a `pub <name>: ...` field on `RuntimeState` and delegate to it from the bindgen-generated `Host` trait impl in `src/runtime.rs`.
5. `cargo build` to regenerate bindings.

## edge-worker Deep Dive

### Execution model detection

`edge-worker/src/detect.rs` inspects a compiled `Component` structurally — if the component exports `wasi:http/incoming-handler`, the supervisor uses the **Handler (FaaS)** path with `HandlerDispatch`; otherwise it uses the **LongRunning** path with `run_app_loop`. The linker factory is picked to match (`create_component_linker_handler` vs `create_component_linker_long_running` in `edge-runtime/src/linker.rs`).

### LongRunning path

- `Supervisor::start_app` (`edge-worker/src/supervisor.rs`) acquires a port from `PortPool`, downloads the artifact (SHA-256 + Ed25519 verified), instantiates the component, and spawns `run_app_loop`.
- Each app gets a per-app **epoch ticker** (a `tokio::spawn` in `supervisor.rs`) that calls `engine.increment_epoch()` on a 10 ms cadence. The store's epoch deadline is set per-request; the guest traps if it exceeds the budget. The ticker aborts with the app when the app stops.
- On guest trap, the supervisor restarts with exponential backoff (max 5 attempts, then status `Crashed`).
- A panic inside the host task (e.g. a `panic!` in an `edge:cloud/*` host import or a `unwrap()` that fires) is captured by `run_app_loop`'s `tokio::spawn` wrapper and routed through the same restart/`Crashed` arm as a wasm trap (issue #45). The supervisor never re-raises; a single bad app cannot tear down the worker. The symmetric guarantee on the shutdown path lives in `stop_app`, which logs the panic payload at `error!` and continues the stop sequence instead of `panic::resume_unwind`. The shared trap/panic arm is factored into `Supervisor::handle_app_crash` (`edge-worker/src/supervisor.rs:1755`) so both arms stay in lock-step.
- `process.exit(code)` looks like a trap to wasmtime — check `RuntimeState::exit_requested()` to distinguish.

### Handler (FaaS) path

- `HandlerDispatch::serve` (`edge-worker/src/dispatch.rs`) binds `0.0.0.0:port`, accepts connections with hyper 1.x, calls `wasmtime_wasi_http::ProxyPre::instantiate_async` for each request, and returns the guest's response.
- Uses a **dedicated std::thread** for `engine.increment_epoch()` (the FaaS path is per-request, so the supervisor's tokio ticker can't keep up under load — see `dispatch.rs` header comment).
- Returns a synthetic 500 (`synthetic_500`) on guest trap — never `Err` (hyper 1.x closes the connection mid-message if the service returns `Err`).
- Reuses engines from `StandbyPool` (`supervisor.rs`); evicts LRU when the pool is exhausted.

### StandbyPool / engine sharing

`edge-runtime::create_engine` is meant to be called **once** per worker process (so wasmtime can cache compilation). `StandbyPool` maintains warm engines keyed by `(tenant_id, app_name)` and reclaims them on eviction. Both execution models share the same pool.

### Downloader (`edge-worker/src/downloader.rs`)

Per-app, per-deployment:

1. **Artifact download** — `GET /api/internal/download/{deploymentID}` with worker JWT (or `X-Internal-Token`). Cached locally under `.worker-cache/{deploymentID}.wasm` and `.worker-cache/{deploymentID}.cwasm` (the latter populated by `precompile.PrecompileCwasm` on the control-plane side after activation).
2. **SHA-256 verification** — bare lowercase hex (64 chars), not `sha256:<hex>`. Empty hash, malformed hash, or mismatch causes `get_artifact` to return `Err`; the supervisor releases the port and logs both expected and actual hashes.
3. **Ed25519 signature verification** — `verifier.rs` verifies the signature stored on the `deployments` row over `(sha256_raw_32_bytes || deployment_id)`. **Critical:** the verifier operates on the **raw 32-byte hash**, not the lowercase hex form. The Go signer (`edge-control-plane/internal/signing/signer.go`) builds the message the same way; any divergence breaks verification.

### Ed25519 verifier (`edge-worker/src/verifier.rs`)

- Holds a **keyring** of Ed25519 public keys indexed by operator-chosen `kid`. Loaded from `EDGE_SIGNING_KEYRING` (inline) or `EDGE_SIGNING_KEYRING_PATH` (file with `<kid> = <32-byte-seed-hex>` lines). The worker pubkey file format mirrors the CP keyring (`edge-control-plane/internal/signing/keyring.go`) so operators learn one shape. Legacy `EDGE_SIGNING_PUBKEY` / `EDGE_SIGNING_PUBKEY_PATH` still work (deprecation warning).
- The verifier is constructed once in `crate::main` and threaded through `Downloader::new`. Per-deployment signature verification selects the matching `kid` from the deployment row.
- Signature wire format is **base64url, no padding** — standard base64 (`+/=`) is rejected at decode.
- Signed payload is `sha256_raw_32_bytes || deployment_id` — binding the deployment_id prevents DB-replay attacks (an attacker can't lift a signature from row A onto row B).

### Port pool (`edge-worker/src/port_pool.rs`)

- Sequential allocation starting at `config.starting_port` (default `8081`), pre-populated with 100 ports for O(1) `acquire`.
- Released ports enter a **60-second cooldown** to avoid TIME_WAIT collisions.
- `acquire() -> Option<u16>` — currently returns `None` when truly exhausted, but every call site in `supervisor.rs` uses `.expect("port pool exhausted")`, which **panics the worker process on exhaustion**. Treat this as a known hazard; the fix is to surface `None` as `Err` and let the supervisor log and continue.

### Logging (`edge-worker/src/log_forwarder.rs` + `edge-spool`)

- `LogForwarder` buffers log entries in memory and POSTs batches to `POST /api/internal/logs` every flush tick (default 1s).
- On POST failure (5xx, timeout, network), the batch is appended to `edge-spool::Spool`'s JSONL file at `<spool-dir>/spool.jsonl`. The next flush tick drains the spool, retries, and re-appends on continued failure.
- The spool does **not** fsync per record (deliberate throughput choice — a worker crash between OS write and disk commit can lose one batch).
- `LogGC` in the control plane TTLs `logs` rows (default 7 days, tunable via `LOG_RETENTION` / `LOG_GC_INTERVAL`).

### Bootstrap (`edge-worker/src/bootstrap.rs`)

- On first startup, the worker has no JWT. `bootstrap.rs` hits an internal CP endpoint with `X-Internal-Token` to mint the worker JWT.
- Subsequent restarts load the JWT from local cache. The cached JWT carries `tenant_id = "*"` (or `WORKER_TENANT_ID` if set) — every per-tenant request then uses that single tenant ID until a per-request JWT lands.

## edge-control-plane Deep Dive

### Composition root (`edge-control-plane/internal/app/app.go`)

`app.New()` is the only place every internal package is wired together — 12 repositories, 12 services, 16 handlers, 8 middlewares, and a single `http.Handler`. Adding a new service means touching `app.go` (constructor wiring + optional setter call), `internal/handler/<name>.go` (the HTTP surface), and `internal/service/<name>.go` (the business logic). Routes are registered on two `http.ServeMux` instances (`api` for tenant-authenticated, `admin` for owner-only); both get wrapped in `authMiddleware.Authenticate` then `tenantLimiter`.

### Auth model

- **Tenants** — Bearer API key, SHA-256-hashed in `api_keys`. The middleware extracts the key from the `Authorization` header, hashes it, looks up the row, and stamps `tenant_id` on the request context. Lookup-hash is now indexed (`007_api_key_lookup_hash_not_null` migration).
- **Workers** — JWT signed with the HMAC-SHA256 keys in `cfg.JWT.Keys` (selected by `cfg.JWT.ActiveKID`). 24h TTL. Roles: `owner` (admin endpoints), `ingest` (domains + TLS allowed-list read).
- **`X-Internal-Token`** — a shared secret presented as a header by `edge-ingress` for read-only internal endpoints (traffic, secrets admin). Dual-mounted on `/api/internal/download/{deploymentID}` so both worker JWT and the internal token are accepted.

### Deployment lifecycle

`DeploymentService` (`edge-control-plane/internal/service/deployment.go`) owns the state machine:

- **Deploy** — SaveAndHash (atomic temp-rename), Ed25519 sign, DB row insert, blob store. Manual rollback (DeleteByID + blob Delete) on failure. Note: Deploy and Activate are NOT in a single DB transaction by default; partial failures are compensated, not atomic.
- **Activate** — flips `active_deployments`, runs `precompile.PrecompileCwasm` (best-effort, logs and continues on failure), **enqueues a `task_update` `outbox` row inside the same transaction as the `active_deployments` mutation**, then `publishSwap` (now cache-only) runs the per-region cache-push best-effort. NATS publish is owned by `service.OutboxDrainer` (issue #42): `FOR UPDATE SKIP LOCKED` claim + exponential backoff, `pending` → `in_flight` → `published` (or `failed` after `OUTBOX_MAX_ATTEMPTS`). `regions_cached` / `regions_cache_failed` track per-region outcome; the next activate uses `regions_cached` for incremental caching.
- **Rollback** — restores `last_good_deployment_id` (set by `005_add_last_good` migration); enqueues its `task_update` `outbox` row inside the same tx.
- **Promote** — explicit move of a deployment into active status (used in canary workflows); delegates to `activateDeployment` and inherits its outbox behavior.
- **Env edits** (`EnvService.SetEnv` / `DeleteEnv`, issue #560) — write the `app_env` row AND, if the app has an active deployment, enqueue a `task_update` `outbox` row in the same tx as the env upsert/delete. Apps with no active deployment skip the publish silently. The tenant-disabled gate mirrors `activateDeployment`'s (issue #440): `tenants FOR UPDATE` would be the rigorous form, but the env write path instead does a same-tx `SELECT … FROM tenants` plus `IsDisabled()` check and returns `ErrTenantDisabled` (handler maps to 409) if set. The env write itself rolls back with the rest of the tx on any failure. The shared TaskMessage marshaling lives at `internal/service/publish_payload.go` (`(*publishBuilder).buildPublishPayload`) — same instance is wired into both `DeploymentService` and `EnvService` from `internal/app/app.go`. Note: until issue #561 lands (worker-side restart-on-env-change), the published message reaches the worker but does not trigger a restart; the env value is consumed on the next activate / FullSync, closing the previous "running apps serve stale env until next activate" gap within seconds (well under the 5-minute reconcile window).
- **App / tenant deletion → `task_purge`** (issue #569) — `AppService.DeleteApp` and `TenantService.DeleteTenant` each enqueue a `task_purge` `outbox` row inside the same tx as the cascade deletes (Commit 4/5 of PR for #569). Per-app deletion enqueues one row scoped to that app; per-tenant deletion enumerates the tenant's apps first and enqueues one row per app (per-app keeps the worker's `handle_purge` path uniform with the app-deletion handler and avoids forcing the worker to enumerate its in-memory `(tenant, app)` keys). `OutboxDrainer.processRow` (Commit 2 of #569) switches on `row.Kind` and routes `task_purge` rows to the new `Publisher.PublishPurge`. Forward-compatible with #475/#476 (durable CP-side `tenant_kv` tier) — when those ship, sibling `tenant_kv BatchDelete` rows land at the same call site.

### Reconcile (`edge-control-plane/internal/service/reconcile.go`)

`ReconcileService.Run` ticks every `RECONCILE_INTERVAL` (default 5 min) and publishes a `TaskMessage::FullSync` per `(tenant, region)`. This is the safety net for "message lost in NATS stream / consumer crashed mid-diff / max_age exceeded." `RequestSync` is the on-demand entry (called from the worker register hook) — publishes immediately for one `(tenant, region)`.

### Autoscaler (`edge-control-plane/internal/autoscale/`)

`autoscale.Service` subscribes to `edgecloud.heartbeats.>`, maintains a per-region fleet view, and on each decision tick either calls `CloudProvider.Provision` / `Deprovision` or records a `noop` row when in-band or cooldown-gated. The `cloud` subpackage abstracts the provisioner (in-tree: `noop`, `mock`; pluggable). Disabled by default — opt in via `cfg.Autoscale.Enabled`.

### Background goroutines (`app.RunBackground`)

- `WorkerSvc.SubscribeHeartbeats` — NATS heartbeat consumer.
- `LogGC.Run` — TTL'd log deletion.
- `ReconcileSvc.Run` — periodic full_sync.
- `WorkerGC.Run` — evicts workers that haven't heartbeated in `WORKER_MAX_AGE` (default 15 min).
- `AutoscaleSvc.Subscribe` — no-op when disabled.
- `PreviewGC.Run` — issue #308. TTL'd preview deployment GC: every `PREVIEW_GC_INTERVAL` (default 1h), sweep deployments whose `preview_expires_at < NOW()`, delete their artifact blobs FIRST, then delete the rows. Mirror of `LogGC.Run`; same batched-delete + immediate-first-sweep shape.
- `DeploymentGC.Run` — TTL'd deployment-row GC (older than `DEPLOYMENT_GC_MAX_AGE`, default 30 days; not preview deployments — those are `PreviewGC`).
- `CacheRetrySweep.Run` — issue #501. Background sweep that re-attempts per-region artifact-cache pushes for deployments whose previous push landed in `regions_cache_failed`. Tick interval `cfg.CacheRetry.IntervalS` (env `REGION_CACHE_RETRY_INTERVAL`, default 5m). Per-region attempt cap `cfg.CacheRetry.MaxAttempts` (env `REGION_CACHE_RETRY_MAX_ATTEMPTS`, default 10): over-cap regions are routed to a `giveUp` partition (drop from `regions_cache_failed` with a WARN log). The per-region counter is reset on every activation (`publishSwap` calls `ResetRegionCacheRetryCount` inside the cache-state transaction), so the cap is per-deployment, not per-tenant-app-lifetime. Set `MaxAttempts<=0` to disable the cap entirely (escape hatch for environments that want unbounded retries).

### Background-GC metrics (issue #581)

All three background GCs above (`LogGC`, `PreviewGC`, `CacheRetrySweep`) emit Prometheus metrics via the hand-rolled `service.MetricsAggregator` (no `prometheus/client_golang` dependency — see `edge-control-plane/internal/service/metrics.go`). Each GC takes a typed sink closure in its constructor; the aggregator exposes `NewLogGCSink`, `NewPreviewGCSink`, `NewPreviewBlobFailureRecorder`, `NewCacheRetrySweepSink` — wired in `app.New` (`edge-control-plane/internal/app/app.go:705-731`).

20 label-free families are exposed on BOTH `/metrics` (unauthenticated, operator) AND `/api/v1/metrics` (per-tenant) — the per-tenant endpoint appends the global GC families after the per-tenant/app series so tenants can see fleet-wide GC health on their own page:

- **`edge_log_gc_*`** (4 families, `edge-control-plane/internal/service/log_gc.go`): `ticks_total`, `rows_deleted_total`, `errors_total`, `last_tick_timestamp_seconds` (Unix seconds — alert if older than `2 * LOG_GC_INTERVAL`).
- **`edge_preview_gc_*`** (7 families, `edge-control-plane/internal/service/preview_gc.go`): `ticks_total`, `blobs_deleted_total`, `rows_deleted_total`, `batches_swept_total`, `errors_total`, `blob_delete_failures_total` (per-blob granular), `last_tick_timestamp_seconds`.
- **`edge_cache_retry_sweep_*`** (9 families, `edge-control-plane/internal/service/cache_retry_sweep.go`): `ticks_total`, `batches_swept_total`, `rows_touched_total`, `pushed_ok_total`, `still_failing_total`, `config_missing_total`, `given_up_total` (exhaustion signal — alert if non-zero over a sustained window), `errors_total`, `last_tick_timestamp_seconds`.

The sink is invoked once per sweep tick (success or error). The refused-to-run path (zero/negative interval, negative retention, pre-cancelled context) does NOT bump `ticks_total` — this is intentional so an "alert on never-ticked" operator rule fires correctly when a GC is misconfigured or stuck at boot.

### Secrets encryption (`edge-control-plane/internal/service/secrets.go`)

- `cfg.Secrets.ActiveKeyID` + `cfg.Secrets.Keys` (keyring, multi-key) — only path; supports rotation via the keyring key IDs.
- `cfg.SecretsMasterKey` — legacy single-key shim, wrapped into a one-entry keyring at startup with a `default` kid and a deprecation log.

Operator endpoints: `GET /api/v1/admin/secrets/keys` and `POST /api/v1/admin/secrets/re-encrypt` (both X-Internal-Token gated).

### Signing keyring (`edge-control-plane/internal/signing/keyring.go`)

- Multi-key keyring for Ed25519 signing. Loaded from `EDGE_SIGNING_KEYRING` (inline) or `EDGE_SIGNING_KEYRING_PATH` (file). Legacy single-key env vars (`EDGE_SIGNING_KEY`, `EDGE_SIGNING_KEY_PATH`) are still honored as a 1-entry keyring with a `default` kid.
- The **active kid** for new signatures is `EDGE_SIGNING_KEY_ID`. If unset, the keyring must contain a `default` entry. Set-but-missing fails startup.
- Workers verify against the same kid that the CP signed with — the kid travels on the `deployments` row alongside the signature, so per-deployment key rotation is invisible to the worker (it just looks up the kid).

## Storage (`edge-control-plane/internal/storage/`)

Three backends, picked at startup via `cfg.Storage.ArtifactBackend`:

| Backend | Default | Notes |
|---|---|---|
| `fs` | yes | Filesystem. `Save` writes to `{cfg.Storage.ArtifactPath}/{tenantID}/{appName}/{deploymentID}.{format}` via temp-rename. |
| `s3` | no | S3-compatible. `SaveFormat("cwasm", ...)` writes alongside `.wasm`. |
| `remote` | no | HTTP pull-through. Used by `edge-ingress` or as a front for a private cache. |

The `ArtifactStore` interface (`storage/artifact.go`) covers `Save`/`Open`/`Delete`/`SaveFormat`/`OpenFormat`. Path components (tenant ID, app name, deployment ID) are validated by `validatePathComponent` to reject `..`, `/`, NUL, and other traversal chars. `MaxArtifactSize` (100 MiB) is enforced on both the read side (`Open`) and the write side (`Save`); handlers additionally cap request bodies via `http.MaxBytesReader` (`internal/middleware/maxbody.go`).

## Conventions & Gotchas

- **Cargo workspace at the root.** `[workspace]` is in `/Cargo.toml`; 11 crates listed under `[workspace.members]` (9 host crates + 2 JS runtime crates, issue #510). `cargo --workspace` is the default; use `--manifest-path` only for surgical single-crate work. Adding a new crate: edit `[workspace.members]` and (if it can't share resolver-2 defaults) add to `[workspace.exclude]`. `edge-test-helpers/` is a path-dep pulled into workspace builds by `edge-worker`/`edge-ingress`'s `[dev-dependencies]` but is not an explicit member (kept off `[workspace.members]` so its `testcontainers` + `async-nats` transitive closure doesn't widen the workspace build for every member).
- **`cargo-udeps` ignore-list schema.** `[package.metadata.cargo-udeps.ignore]` is the canonical place (used by the `rust-nightly` CI job, `cargo +nightly udeps --workspace --all-targets`). The schema is **nested** — `normal = ["..."]` covers `[dependencies]`, `development = ["..."]` covers `[dev-dependencies]`. A flat `ignore = ["..."]` key is silently dropped. Add an entry whenever a dep is referenced only from a `#[cfg(target_arch = "wasm32")]` module (host udeps scan cannot see those use sites) or only when a non-default feature is enabled. `edge-js-runtime` / `edge-js-runtime-long` ignore `wit-bindgen`/`rquickjs` for exactly that reason — they're shipped via `wasm32-wasip1`/`wasm32-wasip2` builds; see those crates' `Cargo.toml`s.
- **`edge-runtime` engine is meant to be shared.** Create one engine per worker process via `edge_runtime::create_engine()`. Per-app `StandbyPool` reuses it across instances.
- **Bridge sync → async.** The WIT trait impls in `runtime.rs` are sync; async work (`http_client.fetch()`, `http_server` accept loops, `egress_transport::spawn_send_request_handler`) is bridged via `tokio::runtime::Handle::current().block_on(...)`. Don't move async work outside that bridge — the historical foot-gun was a blocking reqwest runtime panic when dropped in an async context.
- **Guest exit vs. wasm trap.** Always check `RuntimeState::exit_requested()` after a guest call returns `Err` — a clean `process.exit` looks like a trap to wasmtime.
- **Egress hardening.** URL-level allowlist (`EgressPolicy::check`) is the first defense; DNS-pre-resolve + IP allowlist (`egress_transport.rs`) is the second; `socket_egress::SocketEgressPolicy` (default hard-deny) is the third for `wasi:sockets/*`. The `HostnamePinned` variant is dormant behind `EDGE_EGRESS_HOSTNAME_PINNING` — do not enable in production without reading the warning at `edge-runtime/src/runtime.rs` (~line 836-849). **Per-app `socket_mode` (issue #412):** the per-app selector on `AppSpec.socket_mode` resolves to `spec.socket_mode.unwrap_or(self.config.socket_mode)` at `edge-worker/src/supervisor.rs::socket_mode_for_spec`; the FaaS compose rule at `edge-worker/src/dispatch.rs::handle_request` requires BOTH `spec.socket_mode == HostnamePinned` AND `hostname_pinning_enabled = true` to activate the dormant arm.
- **`WORKER_TENANT_ID`.** Defaults to `"*"` (no longer required at startup, per `edge-worker/src/config.rs`). When set, it's stamped into the worker JWT and scopes all `/api/internal/*` calls — the worker is multi-tenant by `TaskMessage` content but the JWT carries this single tenant ID. Per-request JWT issuance is a follow-up.
- **Port pool exhaustion** in `edge-worker/src/supervisor.rs` calls `.expect("port pool exhausted")` — known hazard; `acquire()` already returns `Option<u16>`, so the call sites should `match` and surface `Err` instead.
- **App-task panic propagation (issue #45).** `stop_app`'s `try_into_panic()` arm logs the panic payload at `tracing::error!` (with `tenant_id` / `app_name` / `panic_payload` structured fields) and returns `Ok(())` instead of `panic::resume_unwind`. The panic-in-`run_app_loop` path is symmetric: `execute_app` is `tokio::spawn`-ed, and the new `Ok(Err(join_err))` arm in the `select!` body routes the synthetic `anyhow::Error` through `Supervisor::handle_app_crash`. Do NOT reintroduce `panic::resume_unwind` for panicking app tasks — that's the bug the worker is now hardened against. Add `Self::handle_app_crash` to the `match` arms if you ever add a new failure class (OOM, store-limit-exceeded, etc.).
- **Artifact integrity.** SHA-256 first, then Ed25519 over `(sha256_raw_32_bytes || deployment_id)`. Hash wire format is **bare lowercase hex** (64 chars), not `sha256:<hex>` (the latter only appears in `whitepaper.md` examples and is a docs bug). Signature wire format is **base64url, no padding**.
- **Persisted interfaces** (kv-store, cache, scheduling) honor `EDGE_KV_STORE_PATH` / `EDGE_CACHE_PATH` / `EDGE_SCHEDULING_PATH` env vars. Absent or invalid → ephemeral in-memory only. **Lifecycle:** stop / crash / rebalance keep these dirs in place. Deletion happens via issue #569 — `DELETE /api/v1/admin/apps/{appName}` and `DELETE /api/v1/admin/tenants/{id}` publish a `task_purge` TaskMessage variant to each worker via the outbox (Commit 4/5 of PR for #569); the worker calls `edge_runtime::runtime::purge_tenant` which removes the in-memory registry entry and the on-disk directories. Forward-compatible with #475/#476 (CP-side durable `tenant_kv` Postgres backend) — when those ship, the same outbox call site adds a sibling `tenant_kv BatchDelete` row. **KV / cache / scheduling are per-tenant by design** (issue #558) — two apps of the same tenant share one namespace (apps opt in to sharing; task IDs are UUIDs so scheduling cross-app collision is theoretical). **Filesystem preopens (`EDGE_FS_PATH`) are per-app**: the guest's `/` is mounted at `{EDGE_FS_PATH}/{tenant_id}/{app_name}/`. Per-tenant would let app A corrupt app B's SQLite (miserable to debug), so preopens get a different default.
- **CLI retry classification lives on `ApiError` (issue #571).** `ApiError::is_retryable()` at `edge-cli/src/api/client.rs:57-69` is the single source of truth for "should the caller retry?" — `Transient { .. }` and `Rejected { 429 }` are retryable; every other `Rejected` is deterministic and must NOT retry. `From<serde_json::Error>` for `ApiError` returns `Rejected { status: 200, body: "invalid response body: ..." }` (issue #571 retry-propagation follow-up) so wire-shape mismatches are deterministic — the server returns the same bytes on retry, retrying wastes the budget. The canonical retry loop is `commands::retry::call_with_retry<T, F>(...)` at `edge-cli/src/commands/retry.rs`. Every retry-aware CLI command (`edge deploy`, `edge env delete`, `edge env list`, `edge traffic set`, `edge traffic show`, `edge keys revoke`, `edge keys list`, `edge domains remove`, `edge domains list/check`, `edge egress set/clear/show`) routes through this one loop. Generic over `T`, closure-typed so callers pass borrowed arguments and the same closure shape covers deploy, env, traffic, egress, keys, and domains. `edge traffic set` and `edge env delete` expose `--max-retries` / `--retry-base-ms` / `--retry-cap-ms` (operator-tunable tier, defaults 3 / 500 / 8_000, `--max-retries` clamped to 0..=20 by `value_parser`); the rest use hardcoded defaults from `commands::retry::{DEFAULT_MAX_RETRIES, DEFAULT_RETRY_BASE_MS, DEFAULT_RETRY_CAP_MS}` (single source of truth — no per-module duplication). Non-deploy callers should use the `commands::retry::call_with_retry_no_interrupt` shim — it constructs a fresh `AtomicBool::new(false)` internally so the 11 retry-aware sites don't carry dead-weight `AtomicBool` ceremony; the deploy caller keeps the explicit `&AtomicBool` parameter because it installs its own ctrlc handler. The `EDGE_CLI_RETRY_BASE_MS` env var is a **test-only hook** (not a stable user-facing API) used by the wiremock integration tests to shrink the backoff below the 500ms default; production callers must NOT rely on it. Endpoints that need a server-side `Idempotency-Key` header (`edge env set`, `edge keys create`, `edge domains add`) are NOT routed through `call_with_retry` yet — see the Phase-2 follow-up issue. Wrap the error with `anyhow::Error::new(api_err).context("...")` (NOT plain `anyhow!("...")`) so the typed `ApiError` survives the chain and `is_anyhow_retryable` (the canonical helper at `commands::retry::is_anyhow_retryable` — was `commands::deploy::is_anyhow_retryable` pre-follow-up) can downcast it: walk `e.chain()`, downcast to `ApiError` first (defer to `is_retryable`), then to `reqwest::Error` (`!is_builder()` → transient, `is_builder()` → deterministic). Any error in the chain that isn't one of those is treated as deterministic (don't retry). The deploy-side shim `commands::deploy::deploy_with_retry` is a 5-line wrapper that calls `call_with_retry("deploy", ...)` — the public surface (clap flags, `run_upload` body) is unchanged.
- **`edge-migrate` placement.** Per `edge-migrate/docs/design.md` v0.3 (the more authoritative doc for this tool), `edge-migrate` is a **standalone binary** (`cargo install edge-migrate`), not a subcommand of `edge-cli`. The older whitepaper §10.2 still describes it as an `edge migrate` subcommand — that description is **superseded by design.md**. The current code has a stub in `edge-cli/src/migrate/`; the real transform lives in `edge-migrate/edge-migrate-lib` and is invoked by the Go control plane as a subprocess via `edge-migrate --transform [--language c|rust] <path>`. Don't add new logic to the CLI stub.
- **`edge-migrate` language dispatch.** The bin accepts `--language c` (default) or `--language rust`. The Rust path is feature-gated in `edge-migrate-lib` (`features = ["rust"]` pulls in `tree-sitter-rust`) and emits a `wasi::socket` + `wasi::filesystem` `use` prelude plus per-pattern rewrites. The Go control plane compiles Rust output with `rustc --target wasm32-wasip2 --crate-type=cdylib --edition 2021` (`RUSTC_PATH` env var to override the default `rustc`). Server hosts must have `rustup target add wasm32-wasip2` installed. The handler language gate accepts `c` or `rust`; anything else returns 400 `only c and rust are supported`. See `edge-migrate/docs/design.md` §4.4 and §5.4.
- **`edge-migrate` preprocessor.** When `clang` is reachable (PATH lookup, falling back to `$WASI_SDK_PATH/bin/clang`), the analyzer runs the source through `clang -E -nostdinc` before tree-sitter parsing. Patterns hidden behind `#define` macros (e.g. `#define socket(x) make_socket(x)`) become visible. `MigrationReport` and `TransformResult` carry a `preprocessor: Option<PreprocessorInfo>` field. When clang is missing, the analyzer silently falls back to the unexpanded source. See `edge-migrate/docs/design.md` §2.2.
- **`EDGE_QUEUE_GROUP` (issue #86, intra-region HA pinning).** Optional NATS JetStream queue group name. When set, every worker in the same region that joins the same `queue_group` shares a single delivery of each `TaskMessage` — exactly one worker per group starts the app, preventing the multi-worker duplicate-start that fan-out would cause. Empty string (the default) preserves the historical fan-out behavior (issue #316) where every worker in the region receives every `TaskMessage` and the supervisor's diff-and-reconcile logic handles duplicates. Read once at worker startup and threaded into `NatsClientImpl::build_consumer_config`. Mechanically: all group members share ONE durable JetStream consumer named after the group (`nats.rs::effective_consumer_name`) and queue-subscribe to its deliver subject under `deliver_group` — per-worker durable names would each receive a full copy of the stream and defeat the pinning (each JetStream durable is an independent cursor; `deliver_group` only load-balances subscribers of a single consumer). Self-tests: `nats.rs::tests::consumer_config_queue_group_sets_deliver_group` and `consumer_config_queue_group_shares_one_durable`.
- **`QUOTA_FETCH_INTERVAL` (issue #420, ingress 402 enforcement).** How often `edge-ingress` polls `GET /api/v1/internal/quota/{tenantID}` to refresh its per-tenant quota cache (default 30s, set on `edge-ingress` via `edge_ingress::config::Config::quota_fetch_interval`). The cache drives Caddy `static_response 402` injection: tenants with `over_cap=true` get a terminal 402 block placed before their reverse_proxy route. Fail-open default: a tenant not in the cache is treated as under-cap (no 402 injected). The 30s default matches the heartbeat tick so the ingress reacts to a free-tier lockdown within one tick of the worker's `applyTenantDelta` call.
- **WIT version is `0.2.0`.** Components compiled against `0.1.0` will not import-match. Bump the version deliberately when adding breaking interface changes.
- **Adding a new `edge:cloud/*` interface.** See the feature-flags section above — the five-step recipe.
- **Adding a new control-plane service.** Implement in `internal/service/<name>.go`, wire dependencies in `internal/app/app.go::New`, expose the HTTP surface in `internal/handler/<name>.go`, and register the route in `app.go` on `api` or `admin`. Use `sqlx` repos from `internal/repository/` for DB access.
