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

## Docs

| File | Role |
|---|---|
| [whitepaper.md](./whitepaper.md) | Design intent — 13-section architecture, deployment artifact format, security model, roadmap. |
| [CLAUDE.md](./CLAUDE.md) | Build/test commands, lint, per-crate gotchas, integration-test flags. (Written for AI agents, equally useful for humans hacking on the repo.) |
| [edge-migrate/docs/design.md](./edge-migrate/docs/design.md) | Migration spec — transformation rules, AST contracts, C preprocessor handling. |
| [edge-control-plane/docs/storage.md](./edge-control-plane/docs/storage.md) | Operator guide for the control-plane artifact-storage backends (`fs` / `s3` / `remote`). |
| [edge-control-plane/docs/api/openapi.yaml](./edge-control-plane/docs/api/openapi.yaml) | OpenAPI 3 spec for the `api` binary's HTTP surface. |
| [edge-ingress/README.md](./edge-ingress/README.md) | Operator runbook for `edge-ingress`. |
| [edge-worker/tests/fixtures/README.md](./edge-worker/tests/fixtures/README.md) | Test fixture builder reference (`wasm32-unknown-unknown` + `wasm-tools`, L1–L10 layers). |

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
