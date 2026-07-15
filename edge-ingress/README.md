# edge-ingress

Public ingress / edge proxy for edgeCloud. A small Rust binary that wraps a
Caddy process: subscribes to NATS heartbeats from `edge-worker`, maintains an
in-memory `app_name → {worker_addr, port}` routing table, and re-renders the
Caddyfile JSON on every change. Terminates TLS with a pre-provisioned
`*.edgecloud.dev` wildcard cert.

Hosts are formatted as `<tenant_id>-<app_name>.edgecloud.dev` (e.g.
`https://t_acme-api.edgecloud.dev`) — the `tenant_id` prefix avoids
cross-tenant name collisions on the shared wildcard.

> **Note (issue #438):** `app_name` may contain `.` or `_` (the unified
> validator allows `^[a-z0-9][a-z0-9.\-_]{0,62}$`). For dotted names
> like `myapp.v2`, the public hostname is a two-label host under
> `edgecloud.dev` (`https://t_acme-myapp.v2.edgecloud.dev`), which the
> single-level `*.edgecloud.dev` wildcard DNS record and TLS cert do
> NOT cover. **Operators must additionally provision
> `*.*.edgecloud.dev` DNS + a matching multi-label cert** before dotted
> names are routable. Load the multi-label cert via `TLS_CERT_FILE_2`
> and `TLS_KEY_FILE_2` env vars on `edge-ingress`; otherwise the
> per-route `tls.on_demand: {}` fall-through triggers ACME on first hit
> for the unknown host. Single-label wildcard names (`myapp-v2`,
> `myapp_v2`) keep working under the existing single-level wildcard
> with no new operator config.

## Architecture

```
                 +-----------------------------+
                 |   edge-ingress (Rust)       |
                 |   one per region, beside    |
                 |   the Caddy it controls     |
                 +--------------+--------------+
                                |
              subscribes        | POSTs Caddyfile JSON
              NATS heartbeats   v
  worker --publish-> NATS ---->  +-----------------------------+
                                 |   Caddy (admin :2019)      |
   *.edgecloud.dev  (TLS)  <---- |  reverse_proxy upstreams   |
   traffic             ---->     +-----------------------------+
                                 |
                                 v
                       http://<worker>:<port>
```

## Quick start (local dev)

Prereqs: Docker, a NATS server, a self-signed `*.edgecloud.dev` wildcard
cert. Generate one with `caddy trust` or
[`mkcert`](https://github.com/FiloSottile/mkcert) `"*.edgecloud.dev"`.

```sh
# 1. Caddy — the reverse-proxy this binary controls. Exposes the JSON
#    admin API on :2019 and binds the public ports :80/:443.
#
#    Issue #548 + #663: stock `caddy:2` has no `apps.layer4` AND
#    no in-flight concurrency counter primitive for HTTP routes.
#    Use the xcaddy-built image `edgecloud/caddy-concurrent:latest`
#    (built from `edge-ingress/Dockerfile.caddy-concurrent`) which
#    is a strict superset: includes both the `mholt/caddy-l4`
#    plugin (for #548) AND the first-party `tenant_concurrent`
#    HTTP middleware (for #663, sub-feature #2 of #305). The HTTP
#    path is byte-identical to stock for unconfigured routes. The
#    L4-only `edgecloud/caddy-l4:latest` image is still built (see
#    `Dockerfile.caddy-l4`) for environments that do not need
#    concurrent caps.
docker run --rm -p 2019:2019 -p 80:80 -p 443:443 \
  -v ~/.edgecloud/tls:/etc/caddy/tls:ro \
  -e CADDY_ADMIN_TOKEN=dev-token \
  edgecloud/caddy-concurrent:latest

# 2. edge-ingress
#    CADDY_ADMIN_LISTEN keeps Caddy's admin API reachable from the host after
#    each /load push rewrites the admin block inside the Docker container.
INGRESS_REGION=fra \
CADDY_ADMIN_TOKEN=dev-token \
CADDY_ADMIN_LISTEN=0.0.0.0:2019 \
TLS_CERT_FILE=/path/to/cert.pem \
TLS_KEY_FILE=/path/to/key.pem \
cargo run --manifest-path edge-ingress/Cargo.toml

# 3. edge-worker (note EDGE_WORKER_ADDR is REQUIRED — startup fails if unset)
EDGE_WORKER_ADDR=127.0.0.1 \
WORKER_ID=w_fra_test \
REGION=fra \
CONTROL_PLANE_URL=http://localhost:8080 \
cargo run --manifest-path edge-worker/Cargo.toml

# 4. Publish a synthetic heartbeat. The route appears in Caddy within
#    ~1s + the debounce window.
nats pub edgecloud.heartbeats.fra '{
  "type":"heartbeat","timestamp":"2026-06-17T12:00:00Z",
  "worker_id":"w_fra_test","region":"fra","worker_addr":"127.0.0.1",
  "apps":{"myapp":{"deployment_id":"d_x","status":"running",
  "exit_code":null,"request_count":0,"tenant_id":"t_acme","port":8081}}
}'

# 5. The Caddy admin should now show a route for t_acme-myapp.edgecloud.dev.
curl http://127.0.0.1:2019/config/ | jq .apps.http.servers.edge_https.routes
```

## Environment variables

### edge-ingress (required)

| Var                 | Notes                                                            |
|---------------------|------------------------------------------------------------------|
| `INGRESS_REGION`    | Region this ingress serves (e.g. `fra`). Subscribes to `edgecloud.heartbeats.<region>`. |
| `TLS_CERT_FILE`     | Absolute path to the `*.edgecloud.dev` wildcard cert PEM.        |
| `TLS_KEY_FILE`      | Absolute path to the matching key PEM.                           |

### edge-ingress (optional)

| Var                    | Default                       | Notes                                  |
|------------------------|-------------------------------|----------------------------------------|
| `NATS_URL`             | `nats://localhost:4222`       | Where the binary subscribes for heartbeats. |
| `CADDY_ADMIN_URL`      | `http://127.0.0.1:2019`       | Caddy's JSON admin endpoint.           |
| `CADDY_ADMIN_LISTEN`   | `localhost:2019`              | Listen address written into the rendered Caddy config's `admin` block. **Set to `0.0.0.0:2019` when Caddy runs in Docker** so the host can reach the admin API. Without this, every `POST /load` resets the admin binding to `localhost:2019` inside the container. |
| `CADDY_ADMIN_TOKEN`    | unset                         | If set, sent as `Authorization: Bearer <token>`. **Must match the value on the Caddy process** (`CADDY_ADMIN_TOKEN` env var on the `caddy:2` image). |
| `INGRESS_LISTEN_HTTP`  | `:80`                         | Bind address for the :80 server (308 redirect to HTTPS). |
| `INGRESS_LISTEN_HTTPS` | `:443`                        | Bind address for the :443 server.       |
| `REFRESH_DEBOUNCE_MS`  | `1000`                        | Coalesce bursty heartbeat/stale-cleanup notifications into one Caddy reload. |
| `HTTP_TO_HTTPS`        | `true`                        | If `true`, also listen on :80 and 308-redirect to HTTPS. Set to `false` in environments that handle the redirect elsewhere (e.g. behind another proxy). |
| `RATE_LIMIT_RPS_TENANT_DEFAULT` | `0`                  | Per-tenant default RPS applied to every tenant with no explicit per-tenant override (issue #305 sub-feature #1). `0` = no default cap. |
| `RATE_LIMIT_BURST_TENANT_DEFAULT` | `0`                | Per-tenant default burst paired with `RATE_LIMIT_RPS_TENANT_DEFAULT`. `0` = falls back to the RPS value at the renderer. |
| `TENANT_RATE_LIMIT_FETCH_INTERVAL` | `30s`             | How often the ingress polls `GET /api/v1/internal/rate-limit/{tenantID}` to refresh the per-tenant rate-limit cache. Matches `QUOTA_FETCH_INTERVAL` so both caches refresh on the same beat. `0` disables the fetcher entirely. |
| `GLOBAL_RATE_LIMIT_RPS` | `0`                          | Platform-wide RPS cap applied before any per-tenant route (issue #305 sub-feature #4). Enforced **per Caddy replica** — with N ingress replicas, the effective cap is N × this value. Multi-replica NATS aggregation is a separate follow-up. `0` disables the global cap. |
| `GLOBAL_RATE_LIMIT_BURST` | `0`                        | Global RPS burst paired with `GLOBAL_RATE_LIMIT_RPS`. `0` = falls back to the RPS value at the renderer. |
| `L4_PORT_RANGE_START`  | `31000`                       | Inclusive lower bound of the public-port range reserved for raw-TCP apps. Issue #548. |
| `L4_PORT_RANGE_END`    | `31999`                       | Inclusive upper bound. Provides 1000 ports by default. The CP allocates per-`(tenant_id, app_name)` atomically via `UPDATE … WHERE l4_public_port IS NULL RETURNING` so two ingress instances in the same region cannot collide; this range is the upper bound the CP allocates from. |
| `INGRESS_L4_MAX_CONNS_PER_APP` | `1000`                | Per-app DDoS cap on simultaneous TCP connections. Mirrors the HTTP `Config::max_conns` shape. |
| `INGRESS_L4_MAX_CONNS_PER_IP`  | `100`                 | Per-source-IP cap. Mirrors `Config::max_conns_per_ip`. |
| `L4_PORT_COOLDOWN_SECS` | `60`                          | Port enters cooldown after release to dodge `TIME_WAIT` collisions. Matches `edge-worker/src/port_pool.rs::release`. |

### edge-worker (REQUIRED change for #70)

| Var                 | Notes                                                                                                                       |
|---------------------|-----------------------------------------------------------------------------------------------------------------------------|
| `EDGE_WORKER_ADDR`  | **Required**, fails fast at worker startup. Address the ingress should reverse-proxy to. In private VPCs, set to a routable IP or DNS name (Cloud NAT EIP, internal LB, etc.). |

## End-to-end latency budget

A freshly-deployed app's first request becomes reachable within:

| Stage                                   | Time   |
|-----------------------------------------|--------|
| `edge-worker` → NATS heartbeat          | ≤ 30s  |
| `edge-ingress` debounce                 | ≤ 1s   |
| Caddy `health_checks.active` warmup     | ≤ 30s  |

Plan for up to **~90s** before the first request lands. The `health_checks.active`
interval is configured to `:80` of the upstream — first request *does* work
before the interval fires; the active check is for marking an upstream
unhealthy after a failure, not for the initial admit.

## Cert rotation

Operator's job in v1. `cert-renew` automation lands with **#83** (custom
domains) — that work brings DNS-01 ACME and a per-tenant SAN list. Until
then, regenerate the wildcard cert out-of-band and atomically replace the
PEM files at the paths `TLS_CERT_FILE` / `TLS_KEY_FILE` point at; Caddy's
`load_files` mechanism picks up the new certs on the next config push (i.e.
the next heartbeat burst — trigger one with a NATS `pub` if needed).

## Operational notes

- **Caddy admin auth**: default Caddy admin is unauthenticated on localhost.
  Set `CADDY_ADMIN_TOKEN` on both the Caddy process and `edge-ingress` to
  the same value, otherwise `POST /load` returns 401. The default
  `caddy:2` Docker image has the admin on `:2019` — only expose that port
  to `edge-ingress`, not to the public internet.

- **Caddy admin in Docker**: the default `caddy:2` image binds its admin
  API to `localhost:2019` **inside the container**, making it unreachable
  from the host via port mapping. Use a `Caddyfile` with `admin 0.0.0.0:2019`
  or set `CADDY_ADMIN_LISTEN=0.0.0.0:2019` on the ingress (which writes the
  `admin` block into every config push, so it persists across reloads).

- **Reload volume**: 30s heartbeats × N workers × M apps. `POST /load` on a
  500-route Caddyfile is ~50ms; the ingress handles thousands of routes
  fine. If the route count exceeds ~10k, switch to `PUT /id/<id>/apps/http/servers/edge_https/routes/<n>` patches — see the comment in `src/caddy.rs`.

- **Stale routes**: a 30s tick prunes entries that haven't been refreshed in
  60s (2 missed heartbeats). The route disappears from Caddy on the next
  render. A worker restart in the same region is "free" — the new worker
  publishes a heartbeat within 30s and the route is rewritten. Override
  with `STALE_TIMEOUT` / `PRUNE_INTERVAL`.

- **L4 routing** (issue #548): a parallel `L4RoutingTable` + `L4PortPool`
  carry raw-TCP apps. Heartbeats with `protocol:"tcp"` consult the CP's
  authoritative port allocator (`L4PortCache`, polled every 30s) and
  fall back to the ingress-local pool on cache miss. Each routable L4
  app renders as `apps.layer4.servers.<l4_<port>>` with a single
  `routes[].handle[].handler:"proxy"` forwarder to the worker's
  upstream `worker_addr:port`. Quota over-cap → empty `routes`, which
  Caddy interprets as "close immediately". An xcaddy-built Caddy
  image is mandatory — stock `caddy:2` does not include
  `apps.layer4`. Two images are available:
  - **`edgecloud/caddy-concurrent:latest`** (preferred, built from
    `Dockerfile.caddy-concurrent`) — a strict superset that
    includes the `mholt/caddy-l4` plugin AND the first-party
    `tenant_concurrent` HTTP middleware (issue #663).
  - **`edgecloud/caddy-l4:latest`** (built from `Dockerfile.caddy-l4`)
    — L4-only, for environments that don't need concurrent caps.
  The full L4 design is in [`../docs/l4-ingress.md`](../docs/l4-ingress.md).

- **Cross-region reachability**: the ingress runs in a region (typically
  the same as the workers it serves) and must be able to `dial()` every
  `worker_addr:port`. In a multi-region setup, peer VPCs or tunnel
  traffic between regions — the ingress in `fra` cannot reach a worker in
  `iad` over the public internet unless `EDGE_WORKER_ADDR` is a public IP.
  Multi-region anycast + GeoDNS is **#82**.

- **Worker public-IP auto-detection**: workers in private VPCs must set
  `EDGE_WORKER_ADDR` manually. Cloud metadata endpoints
  (`http://169.254.169.254/`) are a v2 enhancement.

## Out of scope (separate issues)

| Issue | Topic                                                                 |
|-------|-----------------------------------------------------------------------|
| #82   | Multi-region ingress, anycast IPs, GeoDNS, second-region failover.   |
| #83   | Custom domains. Brings per-tenant ACME, DNS-01 challenges, SAN lists. |
| #85   | Autoscale. When an app runs on N workers, the routing table needs a load-balancing strategy. |
| L4 v2 | TLS-on-SNI for raw-TCP. v1 is plain-byte forwarding (`mholt/caddy-l4`'s `handler:"proxy"`); TLS terminates at the worker, not the ingress. |

## Data-plane rate limiting (issue #305)

Per-tenant + global rate limits are enforced at Caddy before traffic
reaches workers. The control plane writes per-tenant caps to the
`quotas` table; the ingress polls
`GET /api/v1/internal/rate-limit/{tenantID}` every
`TENANT_RATE_LIMIT_FETCH_INTERVAL` (default 30s) and renders one
`rate_limit` route per capped tenant plus (optionally) a single
global `rate_limit` route when `GLOBAL_RATE_LIMIT_RPS > 0`.

**Response headers.** Caddy's stock `rate_limit` handler emits
`RateLimit-Limit`, `RateLimit-Remaining`, and `RateLimit-Reset`
(no `X-` prefix). This matches the modern IETF
`draft-ietf-httpapi-ratelimit-headers` convention; the legacy
`X-RateLimit-*` prefix is intentionally not emitted.

**Sub-features in scope of the rendered Caddy config:**
- #1 Per-tenant RPS — `<tenant>-<app>.edgecloud.dev` matched by
  `host_regexp`, capped at the value of `tenant_rate_limit_rps`.
- #2 Per-tenant concurrent-request cap (issue #663) — emits a
  `tenant_concurrent` HTTP handler invocation keyed by
  `tenant-<tenant_id>`. Enforced inside the custom Caddy image
  (`edgecloud/caddy-concurrent:latest`, see
  `edge-ingress/Dockerfile.caddy-concurrent`) by the first-party
  module at `caddy-modules/tenant_concurrent/`. A request whose
  `key` already has `limit` requests in flight receives
  `429 Too Many Requests` with `Retry-After: 1`.
  **Multi-replica caveat:** each Caddy process enforces its own
  copy of the cap. With N ingress replicas, the effective cap is
  `N × concurrent_limit`. Cross-replica NATS aggregation is the
  same shape of follow-up as the per-replica RPS cap (issue #665).
- #4 Global platform RPS — single route keyed on
  `0.0.0.0/0`, capped at `GLOBAL_RATE_LIMIT_RPS`.
- #5 `RateLimit-*` headers — emitted natively by Caddy's
  `rate_limit` handler.

**Sub-features cached but not rendered in this PR** (follow-ups
filed):
- #3 Per-tenant bandwidth throttling — needs Caddy 2.8+ for
  the `rate_limit.bandwidth` field; the `caddy:2` Docker tag is
  a floating tag so this is verification-deferred until the
  production deployment pins a minimum version.

**Per-replica semantics.** `GLOBAL_RATE_LIMIT_RPS` is enforced
per Caddy process. With N ingress replicas, the effective cap is
`N × GLOBAL_RATE_LIMIT_RPS`. Multi-replica NATS aggregation is
a separate follow-up.

## Development

```sh
# Build + unit tests (skips integration tests when Docker is unavailable)
SKIP_INTEGRATION_TESTS=1 cargo test --manifest-path edge-ingress/Cargo.toml

# Lint
cargo fmt --check --manifest-path edge-ingress/Cargo.toml
cargo clippy --all-targets --manifest-path edge-ingress/Cargo.toml -- -D warnings

# Full integration test (requires Docker)
cargo test --manifest-path edge-ingress/Cargo.toml --test integration
```

The integration test (`tests/integration.rs`) uses testcontainers to spin
up a real NATS, publishes a synthetic heartbeat, and asserts the wiremock
Caddy stub received a `POST /load`. The exact rendered Caddyfile JSON shape
is covered by unit tests in `src/caddy.rs`.
