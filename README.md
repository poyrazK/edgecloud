# edgeCloud

> Multi-tenant Wasm edge runtime — Rust supervisor, Rust ingress, Go control plane.
>
> Status: edgeCloud is in active development. See [whitepaper.md](./whitepaper.md) for design intent.

## Binaries

| Binary | Source | Role |
|---|---|---|
| `edge-cli` | `edge-cli/src/main.rs` (clap `name = "edge"` — invoked as `edge`) | Developer CLI for tenants — deploy, activate, manage apps, inspect state. |
| `edge-worker` | `edge-worker/src/main.rs` | Rust supervisor — pulls artifacts, hosts Wasmtime instances, publishes heartbeats. |
| `edge-ingress` | `edge-ingress/src/main.rs` | Public ingress — terminates TLS, maintains a routing table by tenant and app. |
| `edge-migrate` | `edge-migrate/edge-migrate-bin/src/main.rs` | Standalone source-to-source migrator — the tool the Go control plane shells out to per [edge-migrate/docs/design.md](./edge-migrate/docs/design.md). |
| `api` | `edge-control-plane/cmd/api/main.go` | Go control plane — HTTP API for tenants and operators. |
| `migrate` | `edge-control-plane/cmd/migrate/main.go` | Go DB migrator — schema migrations for the control plane. |

## Quick start (macOS)

> One command brings up the full stack — Postgres, NATS, control plane, worker, ingress, Caddy, and a seeded test deployment — in the foreground with prefixed logs and Ctrl+C cleanup.
>
> ```sh
> make dev-install   # one-time: brew formulae + rustup target
> make dev           # bring up the stack
> ```
>
> After `READY` is printed, the script prints both the direct worker URL and the Caddy-routed URL. See `scripts/dev-up.sh` for what each phase does. The four-terminal recipe below remains valid for users who want manual control.

## Module Map

```
   developers                              public traffic
       │                                       │
       │ edge deploy / activate                 │
       ▼                                       ▼
+------------------------+            +------------------------+
| edge-cli  (CLI)        |            | edge-ingress  (Rust)   |
| edge-cli/              |            | edge-ingress/          |
+-----------+------------+            +-----------+------------+
            │                                     │
            │ POST /api/deploy, ...               │ forward
            ▼                                     ▼
+------------------------+            +------------------------+
| api  (Go control plane)│◀──heartbeat──│ edge-worker  (Rust)   |
| edge-control-plane/    │   ──NATSTask▶│ edge-worker/          |
|   cmd/api/             │              | -- Wasmtime host      |
+----------+-------------+              +-----------+-----------+
           │                                       │
           │ (DB schema)                           │ host lib
           ▼                                       ▼
+------------------------+              +------------------------+
| migrate  (Go DB migr.) |              | edge-runtime  (lib)   |
| edge-control-plane/    |              | edge-runtime/         |
|   cmd/migrate/         |              +------------------------+
+------------------------+

Standalone tools (separate from the request flow):

+-------------------------+   +----------------------------+
| edge-migrate  (Rust)    |   | edge-test-helpers          |
| edge-migrate/           |   | edge-test-helpers/  ¹     |
| -- source-to-source     |   +----------------------------+
| -- invoked by `api`     |
+-------------------------+

Internal crates (no user-facing binary):
    edge-config, edge-spool, edge-migrate-lib  ²

¹ edge-test-helpers lives outside the Cargo workspace — dev-only
  test harness, never linked into prod binaries.
² edge-migrate-lib — workspace member; the bin forces
  `features = ["rust"]` on it, so the C-only path is only
  exercised by direct library consumers.
```

## Build

```sh
cargo build --workspace                            # all Rust crates
cargo build --manifest-path edge-worker/Cargo.toml # single crate
(cd edge-control-plane && go build ./...)          # Go control plane
```

Per-crate gotchas — Docker requirements for integration tests, `CI=true` skip flags, the `edge-migrate-lib` `rust` feature flag — are documented in [CLAUDE.md](./CLAUDE.md#build--test).

## Test

```sh
cargo test --workspace                             # Rust unit tests
(cd edge-control-plane && go test ./...)           # Go unit tests
```

Integration tests self-skip without Docker — see [CLAUDE.md](./CLAUDE.md#build--test) for flags.

## Local Development

### Prerequisites

- Docker (for Postgres and NATS via `docker compose`)
- Rust nightly (via `rustup`)
- Go 1.23+
- A Caddy binary on `$PATH` (for the ingress)

### Quick start (four terminals)

**Terminal 1 — Infrastructure:**
```sh
# First run only: copy the dev defaults (`.env` is gitignored).
cp .env.example .env

make infra-up      # Postgres :5432 + NATS :4222 in the background
```

**Terminal 2 — Database schema:**
```sh
make migrate       # apply all pending schema migrations
```

**Terminal 3 — Control plane:**
```sh
make run-api       # starts the Go API on :8080
```

**Terminal 4 — Worker + Ingress:**
```sh
# Worker: env var values must match what the control plane expects.
#   - WORKER_ID     e.g. w_fra_dev
#   - REGION        must match the control plane's CONTROL_PLANE_REGION (default: "global")
#   - WORKER_TENANT_ID   the tenant whose apps this worker hosts
#   - WORKER_JWT_SECRET  must match JWT_SECRET from edge-control-plane/config.yaml
export REGION=global WORKER_ID=w_global_dev \
  WORKER_TENANT_ID=t_system \
  WORKER_JWT_SECRET=change-me-in-production \
  CONTROL_PLANE_URL=http://localhost:8080
make run-worker    # starts edge-worker

# Ingress (separate terminal):
cargo run --bin edge-ingress
```

### Local Postgres password

The Postgres password is no longer hardcoded. On first run:

```sh
cp .env.example .env       # one-time; .env is gitignored
# (optional) edit .env and change POSTGRES_PASSWORD / DATABASE_PASSWORD
make infra-up              # Makefile auto-sources .env via `set -a; . ./.env; set +a`
```

`docker compose` will refuse to start without `POSTGRES_PASSWORD` set in `.env` (strict-fail via `${VAR:?msg}`). The control plane binary additionally fails closed at startup if `DATABASE_PASSWORD` is empty or matches a known placeholder (`edgecloud`, `postgres`, `changeme`, ...) — this catches the case where `.env.example` was copied verbatim into a non-local environment. CI uses its own ephemeral `POSTGRES_PASSWORD=test` and is unaffected.

### Seeding test data

Once the stack is running (`docker compose`, `migrate`, `api`), register a test tenant and deploy an app:

```sh
# Sign up a tenant
edge auth signup --plan free

# Create an app
cd /tmp && mkdir myapp && cd myapp
cargo init --lib
# Write your WASI component, then:
edge deploy
```

### Makefile targets

| Target | Description |
|---|---|
| `make dev` | Bring up the full stack in the foreground (Postgres + NATS + CP + worker + ingress + Caddy + seeded sample). Ctrl+C to stop. |
| `make dev-install` | One-time prereq install (brew formulae + `rustup target add wasm32-wasip2`). |
| `make dev-prereqs` | Verify prereqs without installing anything. |
| `make dev-config` | Regenerate `~/.edgecloud/env.sh` and `edge-control-plane/config.local.yaml` without launching the stack. |
| `make dev-down` | Stop Postgres + NATS containers (preserves the volume). |
| `make dev-clean` | Stop everything and wipe `~/.edgecloud`. Add `--purge` to also wipe the artifact registry. |
| `make infra-up` | Start Postgres + NATS containers |
| `make infra-reset` | Wipe volumes, restart, re-migrate |
| `make migrate` | Run pending DB migrations |
| `make run-api` | Start the Go control plane |
| `make run-worker` | Start the Rust worker (set env vars first) |
| `make help` | Show all targets |

### Troubleshooting

| Symptom | Likely cause |
|---|---|
| worker heartbeats fail with FK error | Worker not registered — auto-registration was added in PR #284, make sure you're on latest `main` |
| control plane logs DB connection errors | Postgres not yet ready after `docker compose up -d` — wait, then retry |
| `TASK_STREAM_REPLICAS` error | Non-clustered NATS needs `TASK_STREAM_REPLICAS=1` (see PR #268) |
| Caddy fails to read TLS cert | When running in Docker, cert paths must be container-accessible (issue #281) |

## Doc / Business

| File | Role |
|---|---|
| [whitepaper.md](./whitepaper.md) | Design intent — 13-section architecture, deployment artifact format, security model, roadmap. |
| [CLAUDE.md](./CLAUDE.md) | Build/test commands, lint, per-crate gotchas, integration-test flags. (Written for AI agents, equally useful for humans hacking on the repo.) |
| [edge-migrate/docs/design.md](./edge-migrate/docs/design.md) | Migration spec — transformation rules, AST contracts, C preprocessor handling. |
| [edge-control-plane/docs/storage.md](./edge-control-plane/docs/storage.md) | Operator guide for the control-plane artifact-storage backends (`fs` / `s3` / `remote`). |
| [edge-control-plane/docs/api/openapi.yaml](./edge-control-plane/docs/api/openapi.yaml) | OpenAPI 3 spec for the `api` binary's HTTP surface. |
| [edge-ingress/README.md](./edge-ingress/README.md) | Operator runbook for `edge-ingress`. |
| [edge-worker/tests/fixtures/README.md](./edge-worker/tests/fixtures/README.md) | Test fixture builder reference (`wasm32-unknown-unknown` + `wasm-tools`, L1–L10 layers). |
| [docs/welcome.md](./docs/welcome.md) | Tenant-facing quickstart map — first-account + first-deploy flow, language picker, "what to read next" (issue #550 welcome doc). |
| [docs/recipes/databases.md](./docs/recipes/databases.md) | Tenant recipes for Neon / Turso / Upstash from a Rust or JS guest (issue #550). |

## Layout

```
edgeCloud/
├── Cargo.toml              # Cargo workspace (8 members)
├── Cargo.lock
├── deny.toml
├── _typos.toml
├── whitepaper.md
├── CLAUDE.md
├── edge-cli/               # → edge-cli binary (invoked as edge)
├── edge-config/
├── edge-control-plane/     # Go module (cmd/api, cmd/migrate)
├── edge-ingress/           # → edge-ingress binary
├── edge-migrate/
│   ├── edge-migrate-lib/
│   └── edge-migrate-bin/   # → edge-migrate binary
├── edge-runtime/           # Wasmtime host library
├── edge-spool/
├── edge-test-helpers/      # standalone, NOT in workspace
└── edge-worker/            # → edge-worker binary
```

## License

Proprietary — see [LICENSE](./LICENSE). All rights reserved.
Unauthorized use is prohibited. Contact the copyright holder at
`hpk.poyraz@gmail.com` to request a license.
