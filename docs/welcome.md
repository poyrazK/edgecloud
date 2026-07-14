# Welcome to edgeCloud

edgeCloud runs your WASI Preview 2 components on edge nodes close to
your users, meters each request, and gives you per-tenant host
interfaces (`edge:cloud/*`) for KV, cache, scheduling, observe, time,
process, and WebSocket — plus the full `wasi:cli/command@0.2.1`
surface for filesystem, clocks, sockets, and outbound HTTP.

This page is the in-repo map of the tenant-facing documentation. It
exists as a cross-link target for the docs site (the F2 site data
section admin row, tracked separately); until the site row is in
place, the table below is the canonical entry point.

## What to read next

| You want to… | Start here |
|---|---|
| Get a developer account and your first API key | [edge-cli/README.md#auth](../edge-cli/README.md) — `edge auth signup`, `edge auth login`, `edge auth whoami`. |
| Build and deploy your first component | [README.md#local-development](../README.md#local-development) + the [Quick start](#quick-start) below. |
| Pick a language | Rust (`edge init --lang=rust` → `samples/hello/`) or JavaScript (`edge init --lang=js` → `samples/hello-js/`). JS guests run in the QuickJS runtime (`edge-js-runtime`) and use `@edgecloud/sdk` for the `edge:cloud/*` namespaces. |
| Talk to a database from your component | [docs/recipes/databases.md](./recipes/databases.md) — Neon / Turso / Upstash recipes, both Rust and JS. |
| Understand the egress allowlist | [edge-ingress/README.md](../edge-ingress/README.md) + the egress rows in [docs/recipes/databases.md#troubleshooting](./recipes/databases.md#troubleshooting). |
| Operate a worker | [edge-worker/src/bootstrap.rs](../edge-worker/src/bootstrap.rs) is the runtime-side code; this repo's operator runbooks live under `docs/` (`jwt-bootstrap.md`, `jwt-secret-rotation.md`, `nats-auth.md`). |
| Understand the platform's architecture | [whitepaper.md](../whitepaper.md) + [CLAUDE.md](../CLAUDE.md) (the latter is also the AI-agent briefing). |

## Quick start

> One terminal per piece (Postgres, NATS, control plane, worker +
> ingress). Skip the four-terminal split and use
> `make dev` to bring the whole stack up in the foreground — see
> [README.md](../README.md#quick-start-macos) for the one-shot flow.

```sh
# 1. Get an account + API key (writes ~/.config/edgecloud/config.toml)
edge auth signup --plan free

# 2. Scaffold a Rust or JS app
cd /tmp && mkdir my-app && cd my-app
edge init --lang=rust            # or --lang=js

# 3. Edit src/lib.rs (Rust) or src/handler.js (JS), then build + deploy
edge build
edge deploy
edge activate                   # pick a deployment ID from `edge apps`

# 4. Open the URL printed by `edge deploy` — your component is live.
```

A JS example that uses the platform's outbound-HTTP shim to talk to
Neon:

```js
import { http, process } from "@edgecloud/sdk";

globalThis.handleRequest = () => {
  const res = http.fetch("https://ep-xyz.aws.neon.tech/sql", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: `Bearer ${process.getEnv("NEON_API_KEY")}`,
    },
    body: JSON.stringify({ query: "SELECT now()", params: [] }),
  });
  return { status: 200, body: res.body, contentType: "application/json" };
};
```

A Rust equivalent lives in
[docs/recipes/databases.md#recipe-neon-serverless-driver-postgres-over-https](./recipes/databases.md#recipe-neon-serverless-driver-postgres-over-https).

## Concepts you'll trip over

- **Egress is per-tenant; env is per-app.** One `edge egress set` call
  governs every app under that tenant. `edge env set --app <name>`
  only touches that one app. The recipes page has the
  [full scope table](./recipes/databases.md#before-you-start).
- **Components are isolated in Wasmtime, scoped per tenant.** The
  preopen at `EDGE_FS_PATH/{tenant_id}/{app_name}/` is per-app, not
  per-tenant (issue #558), so two apps of the same tenant don't
  share a filesystem.
- **Deploys are signed.** SHA-256 + Ed25519 over
  `(sha256_raw_32_bytes || deployment_id)`. The wire format is bare
  lowercase hex for the hash and base64url-no-padding for the
  signature — see `edge-worker/src/verifier.rs`.
- **Workers enforce egress for you.** You don't need to add an
  allowlist check on the guest side; `WasiHttpHooks::send_request`
  does it. If your call is denied, the host surfaces a 4xx-equivalent
  error to your handler.

## See also

- [whitepaper.md](../whitepaper.md) — design intent.
- [CLAUDE.md](../CLAUDE.md) — repo conventions, build commands, and the
  per-crate gotchas (this file is also what AI agents read first).
- [edge-runtime](../edge-runtime/) — the wasmtime host library. The
  WIT world lives at [wit/edge-cloud.wit](../wit/edge-cloud.wit).
- [edge-control-plane/docs/api/openapi.yaml](../edge-control-plane/docs/api/openapi.yaml) — every HTTP surface the control plane exposes.