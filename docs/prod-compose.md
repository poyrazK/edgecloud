# Production-equivalent Compose Stack (issue #512)

This runbook documents the production-equivalent Docker compose stack for
edgeCloud. The stack mirrors `scripts/dev-up.sh` but bundles everything as
container images and is suitable for a single-VM demo deployment.

> **TL;DR.** `make prod-up` (after `cp .env.prod.example .env.prod` and
> filling in secrets). `make prod-smoke` waits on `/ready` and prints
> the next operator step. `make prod-down` stops, `make prod-reset`
> wipes volumes.

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
make prod-smoke
# Polls http://localhost:8080/ready until status is "ok" or "degraded";
# prints the next step — manually `docker compose exec cp bash` →
# `cd /samples/hello && edge deploy . && edge activate`.
```

A 200 through Caddy after `edge activate` confirms the full path:

```bash
# From your laptop, with a tunnel/SSH to the host's :80:
curl -H "Host: t_smoke-hello.edgecloud.dev" http://localhost/hello
# Expect: 200, body "Hello, edgeCloud!"
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
| `Makefile`                        | `prod-*` targets (prod-up / prod-down / prod-reset / prod-smoke / prod-migrate / prod-secrets). |
| `.github/workflows/ci.yml`        | `build-images` job (PR+main), `compose-smoke` job (main only). |
