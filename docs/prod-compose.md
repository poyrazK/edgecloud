# Production-equivalent Compose Stack (issue #512)

This runbook documents the production-equivalent Docker compose stack for
edgeCloud. The stack mirrors `scripts/dev-up.sh` but bundles everything as
container images and is suitable for a single-VM demo deployment.

> **TL;DR.** Two paths:
>
> **One-command bootstrap** (recommended for demos / fresh installs):
> `cp .env.prod.example .env.prod && make prod-bootstrap` — generates
> keyring + TLS + signs up a tenant + deploys `samples/hello` and
> asserts HTTP 200 through Caddy end-to-end.
>
> **Manual** (when you want to control every secret yourself):
> `cp .env.prod.example .env.prod`, fill the `replace-me-*`
> placeholders, then `make prod-secrets && make prod-up && make
> prod-smoke`.
>
> `make prod-down` stops, `make prod-reset` wipes volumes.

## What runs where

Six services in one compose file (`docker-compose.prod.yml`):

| Service          | Image                                  | Notes |
|------------------|----------------------------------------|-------|
| `postgres`       | `postgres:16-alpine`                   | Same as dev; sized to fit a demo VM. |
| `nats`           | `nats:2.10-alpine`                     | Single-node JetStream file store. `NATS_REPLICAS=1` required for the CP's stream-declaration to succeed on a one-node cluster (CP logs a WARN if the rate-limit stream falls back to a degraded state). |
| `cp`             | `edgecloud/control-plane:latest` (built locally from `edge-control-plane/Dockerfile`) | Runs `cmd/migrate -up` then `exec cmd/api` on every boot (via `scripts/docker-entrypoint.sh`). Set `SKIP_MIGRATE_ON_BOOT=true` once the schema is owned by an external migration job. |
| `worker`         | `edgecloud/worker:latest` (built locally from `edge-worker/Dockerfile`) | Mounts the operator-supplied `secrets/signing-keyring` for artifact verification (`EDGE_REQUIRE_SIGNATURE=true` is the secure default). |
| `ingress`        | `edgecloud/ingress:latest` (built locally from `edge-ingress/Dockerfile`) | Public-facing on `:80`/`:443`; speaks to Caddy on the `edgecloud` network. |
| `caddy`          | `caddy:2`                              | Stock upstream image. Swap for `edgecloud/caddy-concurrent` (built by `caddy-image` CI job) if you want the per-app concurrent-cap module; swap for `edgecloud/caddy-l4` if you want L4 TCP routing. |

Image versions are commit-pinned per build — `latest` here means "the
image built from `HEAD` of the running checkout." Tag-bumping +
`compose pull && up` is the production deploy path (tracked as a
follow-up to #578).

## One-command bootstrap

For a demo VM or a fresh install on a Linux host, `make prod-bootstrap`
collapses the five operator steps (keyring, TLS, .env edit, prod-up,
manual smoke) into one invocation:

```bash
cp .env.prod.example .env.prod
make prod-bootstrap
```

What `scripts/bootstrap-prod.sh` does, in order:

1. **Preflight.** `docker`, `docker compose`, `openssl`, `jq`, `cargo`,
   and the `wasm32-wasip2` rustup target must all be on `$PATH`. The
   script exits with a precise `bootstrap: FATAL: <tool>` message
   otherwise.
2. **Ed25519 signing keyring.** Writes `./secrets/signing-keyring`
   with `k1 = <32-byte-hex-seed>` (mode 0600) the first time. Re-runs
   reuse the existing file so the worker's `EDGE_REQUIRE_SIGNATURE=true`
   contract is preserved across resets.
3. **Self-signed TLS cert.** Writes `./tls/cert.pem` + `./tls/key.pem`
   with a SAN that covers `*.edgecloud.dev`, `edgecloud.dev`,
   `localhost`, and `127.0.0.1`. Curl-based smoke runs work without DNS
   rewrites as long as the consumer honours `Host:` headers.
4. **`.env.prod` population.** The script copies `.env.prod.example`
   if you haven't, then for each of `DATABASE_PASSWORD`,
   `POSTGRES_PASSWORD`, `JWT_SECRET`, `EDGE_INTERNAL_TOKEN`,
   `BOOTSTRAP_SECRET`, `METRICS_AUTH_TOKEN`, `CADDY_ADMIN_TOKEN`,
   `EDGE_SIGNING_KEY_ID`, `EDGE_SIGNING_KEYRING`: if the value is
   empty or still a `replace-me-*` token, it's overwritten with a
   freshly-generated secret. Anything you've set yourself is left
   alone (the check is grep-based, not strict-fail). It also resets
   a stale `EDGE_WORKER_ADDR=*.example.com` to the compose-internal
   `worker` since the ingress appends the per-app port from
   heartbeats (issue #641).
5. **`make prod-up`.** Builds the three service images and starts
   all six services. The CP entrypoint runs `migrate -up` then execs
   `api` (`scripts/docker-entrypoint.sh`).
6. **`/ready` poll.** Up to `EDGECLOUD_PROD_READY_TIMEOUT` seconds
   (default 120). Status `ok` or `degraded` per issue #48 — the deep
   readiness snapshot covers DB ping + NATS flush + per-loop health.
7. **Build edge-cli** on the host (`cargo build --release --bin
   edge`).
8. **Signup.** `edge auth signup --name ... --plan free --key-name
   default --force`. Captures `tenant_id` + `api_key` from the CLI's
   config persistence at `~/.config/edgecloud/config.toml`.
9. **Build + deploy + activate `samples/hello`** in
   `samples/hello/` (the canonical FaaS sample). Reuses the
   persisted deployment if the existing activation is still current.
10. **Caddy route poll.** Waits (default 120s) for
    `/config/` to publish a `host` matcher for
    `t_<id>-hello.edgecloud.dev`. This is the ingress's
    heartbeat-driven route install.
11. **HTTP smoke assert.** `curl -H "Host: t_<id>-hello.edgecloud.dev"
    http://127.0.0.1:80/hello` and asserts HTTP 200. On failure,
    dumps redacted compose logs to stderr for diagnosis — secret
    values are scrubbed in the dump so they don't leak into CI
    artefacts.
12. **Persists `state/seed.json`** (mode 0600) with `{tenant_id,
    tenant_name, api_key, app_name, deployment_id, host,
    generated_at}` so subsequent runs skip the signup step.

### Idempotency

`make prod-bootstrap` is safe to re-run. It:

- Reuses `secrets/signing-keyring`, `tls/cert.pem`, and `tls/key.pem`
  if present.
- Reuses `state/seed.json`'s `api_key` and re-validates it via
  `edge auth whoami` — if the CP rejects it (DB reset in between),
  falls through to a fresh signup.
- Always redeploys `samples/hello` so the smoke check exercises the
  full activate → heartbeat → ingress path, catching regressions
  that a stale deployment would mask.

### Skipping the manual steps

If you only need the smoke (no signup, no sample deploy), run
`make prod-up` then curl directly: the existing `make prod-smoke`
just aliases `make prod-bootstrap`, so to skip the heavy steps
without losing the convenience:

```bash
make prod-up
until curl -fsS http://127.0.0.1:8080/ready | jq -e '.status == "ok" or .status == "degraded"'; do
  sleep 2
done
```

### Required host binaries

- `docker` 24+ with the `compose` plugin.
- `openssl` 1.1 or 3.x.
- `jq` 1.6+.
- `cargo` + rustup stable + `rustup target add wasm32-wasip2`.
- A reachable daemon (`docker info` succeeds).

These mirror the dev `scripts/dev-install.sh` shape; on macOS the
existing `make dev-install` preflight handles most of them.

## Pre-flight: secrets

Every `${VAR:?msg}` reference in `docker-compose.prod.yml` strict-fails
if the variable is unset. The bundle lives in `.env.prod` (gitignored)
and is seeded from `.env.prod.example`:

```bash
cp .env.prod.example .env.prod
# Edit .env.prod — every "replace-me" token MUST be replaced.
# Generate values via the inline comments: openssl rand -base64 48,
# openssl rand -hex 32, etc.
```

Then render operator-specific files:

```bash
make prod-secrets
# Writes docker-compose.prod/caddy.local.json with your CADDY_ADMIN_TOKEN
# substituted in. Caddy refuses to start with a placeholder bearer.
mkdir -p secrets
echo "k1=$(openssl rand -hex 32)" > secrets/signing-keyring
chmod 0600 secrets/signing-keyring

# Generate TLS certs (self-signed works for the smoke; prod uses
# cert-manager or a manually-provisioned cert from the CA).
mkdir -p tls
openssl req -x509 -newkey rsa:2048 -nodes -keyout tls/key.pem \
  -out tls/cert.pem -days 365 -subj "/CN=*.edgecloud.dev"
```

The operator MUST:

- Set `DATABASE_PASSWORD` to at least 16 bytes — the CP's
  `validateDBPassword` (issue #626) refuses to start otherwise.
- Set `JWT_SECRET` to at least 32 bytes — the control plane rejects
  placeholder values at startup.
- Generate a real `CADDY_ADMIN_TOKEN` — the dev default doesn't ship
  in this file.
- Generate or supply an Ed25519 keypair for artifact signing
  (`EDGE_SIGNING_KEYRING`) — workers refuse to start with
  `EDGE_REQUIRE_SIGNATURE=true` (the default) and an empty / missing
  keyring file.

## Booting the stack

```bash
make prod-up
```

This runs:

1. `make prod-secrets` if `caddy.local.json` doesn't exist yet.
2. `docker compose -f docker-compose.prod.yml up -d --build` —
   builds the three service images from their Dockerfiles, then
   starts all six services.

The compose dependency graph is healthcheck-gated:

- `postgres` (`pg_isready`) → `nats` (`/healthz`) →
- `cp` (auto-migrate → `/health`) → `worker` (TCP probe on
  `METRICS_ADDR:9090`) →
- `ingress` + `caddy` in parallel.

Bring-up time on a warm-cache host is ~30 seconds; cold cache pays
~10 minutes for the worker's wasmtime first-compile.

## Verifying the boot

```bash
make prod-smoke           # alias for prod-bootstrap (idempotent)
```

If you've already brought up the stack via `make prod-up` and just
need a status check without re-running the deploy:

```bash
# Status check only — /ready (deep readiness) and /health (liveness):
curl -fsS http://localhost:8080/ready | jq .
curl -fsS http://localhost:8080/health
```

A 200 through Caddy after bootstrap confirms the full path
(Caddy → ingress → worker → wasmtime → guest):

```bash
# Reads tenant_id + host from state/seed.json written by prod-bootstrap:
TENANT_ID=$(jq -r .tenant_id state/seed.json)
HOST=$(jq -r .host state/seed.json)
curl -H "Host: $HOST" http://localhost/hello | jq .
# Expect: 200 + JSON body from samples/hello
```

## Daily operations

| Action                                       | Command |
|----------------------------------------------|---------|
| Tail logs                                    | `make prod-logs` |
| Container status                             | `make prod-ps` |
| Stop (keeps volumes)                         | `make prod-down` |
| Nuke volumes + cold restart                  | `make prod-reset` |
| Apply migrations without restart             | `make prod-migrate` |
| Re-render operator files (.env.prod → caddy.local.json) | `make prod-secrets` |
| Update a single image (no full re-build)     | `docker compose -f docker-compose.prod.yml build cp && docker compose -f docker-compose.prod.yml up -d cp` |

## Image-build caching

CI runs `build-images` on every push/PR with BuildKit's
`cache-from: type=gha,mode=max` + `cache-to: type=gha,mode=max,duration=336h`,
sharing compiled layers across runs. The first CI build of the day pays
~10 minutes for wasmtime's first compile; subsequent runs reuse that
cache for ~30-60 second rebuilds.

To mimic this locally (warm cache):

```bash
docker buildx create --use --driver docker-container
DOCKER_BUILDKIT=1 BUILDKIT_INLINE_CACHE=1 \
  docker build -f edge-worker/Dockerfile -t edgecloud/worker:dev \
  --cache-from type=local,src=/tmp/edge-build-cache \
  --cache-to   type=local,dest=/tmp/edge-build-cache,mode=max .
```

`scripts/lib/build-cache.sh` (TODO: not committed yet) wraps the
above for local dev iteration.

## What's NOT in this compose (yet)

- **Multi-worker / multi-region topologies.** Single worker, single
  NATS, single Postgres. Multi-node NATS would need `NATS_REPLICAS>1`
  and CP WARN at boot would no longer fire.
- **TLS provisioning.** Self-signed works for the smoke; production
  needs cert-manager or a manual cert swap. Tracked under #514.
- **Image push to GHCR.** Tagged image publishing for a real
  demo-deploy is #578's scope. Until it lands, this compose builds
  from the running checkout.
- **`HostnamePinned` egress mode.** Per-app egress overrides
  (`EDGE_EGRESS_SOCKET_MODE`) stay at the default `block-all` for
  the demo — the `HostnamePinned` arm is dormant behind
  `EDGE_EGRESS_HOSTNAME_PINNING=true` until #384's follow-up
  designates that path production-ready.
- **Multi-label app names.** Apps with `.` in the name (e.g.
  `myapp.v2`) generate hosts deeper than two labels under
  `edgecloud.dev`. Operators must provision wildcard DNS + certs
  for `*.*.edgecloud.dev` to route them — see the issue #438 docs.

## Files referenced by this runbook

| File                              | Purpose |
|-----------------------------------|---------|
| `docker-compose.prod.yml`         | The compose file itself. |
| `docker-compose.prod/caddy.json`  | Caddyfile-equivalent JSON template (committed). |
| `docker-compose.prod/caddy.local.json` | Rendered from `caddy.json` with `CADDY_ADMIN_TOKEN` substituted (gitignored). |
| `.env.prod.example`               | Template; copy to `.env.prod`. |
| `.dockerignore`                   | Build-context filter — keeps build layers small, secrets out. |
| `edge-control-plane/Dockerfile`   | Multi-stage: Go builder for `api`/`migrate`/`printpub`, Rust builder for `edge-migrate`/`wasm2cwasm`, distroless runtime. |
| `edge-worker/Dockerfile`          | Multi-stage: Rust builder → Debian slim runtime. |
| `edge-ingress/Dockerfile`         | Multi-stage: Rust builder → Debian slim runtime. |
| `scripts/docker-entrypoint.sh`    | CP image entrypoint: migrate up → exec api. |
| `Makefile`                        | `prod-*` targets (`prod-up`, `prod-down`, `prod-reset`, `prod-bootstrap`, `prod-smoke`, `prod-migrate`, `prod-secrets`). |
| `scripts/bootstrap-prod.sh`       | One-command bootstrap. Brings up the stack, signs up a tenant, deploys `samples/hello`, smoke-asserts HTTP 200, persists `state/seed.json`. Idempotent. |
| `state/seed.json`                 | Output of `prod-bootstrap` (mode 0600). Re-read by re-runs to skip signup + reuse the persisted api_key. |
| `.github/workflows/ci.yml`        | `build-images` job (PR+main), `compose-smoke` job (main only). |
