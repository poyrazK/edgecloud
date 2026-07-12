# edgeCloud L4/TCP ingress (issue #548)

Status: v1 (port-based routing). TLS-on-SNI is v2.

edgeCloud's differentiator is raw-TCP apps (Redis, MQTT, custom
protocols). Before #548, `edge-ingress` only fronted Caddy on
HTTP(S) — a Redis-protocol app ran fine on the worker but had no
stable public address except direct `worker-ip:port` (no TLS, no rate
limiting, bypasses everything ingress provides). This doc covers the
v1 plan: each raw-TCP app gets a dedicated public port in
`31000..=31999` on the ingress host; the ingress forwards raw bytes
from `public_port` to `worker_addr:upstream_port` using heartbeat
data.

## Goals

1. **Stable, routable public address** for a long-running raw-TCP
   guest. Operator files DNS or just hands a `host:port` to a
   developer.
2. **DDoS protection**. Per-app and per-IP concurrent-connection
   caps that match the HTTP shape (`Config::max_conns`,
   `max_conns_per_ip`).
3. **Quota enforcement**. A tenant over cap closes the TCP connection
   immediately (fail-closed, mirroring the static_response 402
   behavior on the HTTP path).
4. **No extra moving parts on the worker.** The worker doesn't
   know its public port; the ingress is the only thing that knows
   the public→private mapping. Same model that HTTP apps don't know
   their public hostname.
5. **CP-coordinated allocation.** The same port must not be
   handed to two apps that happen to live in different ingress
   processes in the same region. The `apps.l4_public_port` column
   (`migrations/032_l4_public_port.up.sql`) is the single source
   of truth; the ingress polls
   `GET /api/v1/internal/l4-port/{tenantID}/{appName}` every 30s.

## Wire shape (Rust + Go)

The HeartbeatMessage gains a `protocol` field:

```rust
// edge-worker/src/messages.rs:~308 (Commit 1)
pub struct AppStatus {
    // … existing fields …
    #[serde(default = "default_protocol", skip_serializing_if = "is_default_protocol")]
    pub protocol: String, // "http" (default) or "tcp"
}
```

```go
// edge-control-plane/internal/domain/worker.go:52 (Commit 2)
type AppStatus struct {
    // … existing fields …
    Protocol string `json:"protocol,omitempty"` // "http" or "tcp"
}
```

`#[serde(default)]` + `omitempty` keeps the rolling-upgrade contract:
pre-#548 workers don't emit the field, old CPs and old ingresses
ignore-unknown, and new workers against old CPs default to "http".

## Layer-by-layer path

### Worker (`edge-worker/src/`)

- `messages.rs`: `protocol` added with serde defaults.
- `supervisor.rs`: per-app `app_protocols: HashMap<(tenant, app),
  String>` populated from `EDGE_PROTOCOL` env, stamped by CP via
  `AppConfig.socket_mode = "allow-all"`. Heartbeat stamps
  `protocol` on each `AppStatus`.

No new fields on `AppSpec`. The CP infers L4-ness from the
`AppConfig` it has built.

### Control plane (`edge-control-plane/internal/`)

- `domain/worker.go`: `AppStatus.Protocol` + `AppTarget.Protocol`.
- `nats/publisher.go` `BuildAppConfig`: gains a `socketMode`
  parameter. Callers pass `"allow-all"` when `protocol = "tcp"`.
- `repository/app.go`: new `AllocateL4Port(ctx, tenantID, appName)`
  using `UPDATE apps SET l4_public_port = $1 … WHERE l4_public_port
  IS NULL RETURNING`. Atomic — two concurrent allocations cannot
  return the same row.
- `handler/app.go`: new `GET /api/v1/apps/{appName}/l4-port`
  (tenant-auth) and `GET /api/v1/internal/l4-port/{tenantID}/{appName}`
  (X-Internal-Token). Returns `{public_port: 31042}`.
- `migrations/032_l4_public_port.up.sql`:
  `ALTER TABLE apps ADD COLUMN l4_public_port INTEGER`.

CLI integration: `edge deploy` calls `AllocateL4Port` once
during the pre-deploy phase so the public port is reserved before
the worker heartbeats for the first time. The CLI surfaces the
allocated port in the deploy summary (`edge tcp-info <app>` later).

### Ingress (`edge-ingress/src/`)

- `l4.rs`: `L4RouteEntry`, `L4RoutingTable` (parallel to
  `RoutingTable` in `routing.rs`), `L4PortPool` (modeled on
  `edge-worker/src/port_pool.rs`).
- `heartbeats.rs`: `apply_heartbeat` branches on `app.protocol` —
  `"tcp"` routes into `l4_table`, otherwise the existing HTTP path.
  Both tables evict on stale heartbeat.
- `l4_cache.rs`: per-`(tenant, app)` cache polled every 30s. Falls
  back to the ingress-local `L4PortPool` on miss.
- `caddy.rs`: `render_l4_routes(entries, cfg, quota_cache) -> Value`
  paralleling `render_routes`. `render_full(...)` merges HTTP +
  L4 into a single `/load` payload.

### Caddy image (`edge-ingress/Dockerfile.caddy-l4`)

```dockerfile
FROM caddy:2-builder AS builder
RUN xcaddy build --with github.com/mholt/caddy-l4
FROM caddy:2
COPY --from=builder /usr/bin/caddy /usr/bin/caddy
```

Stock `caddy:2` does not include `apps.layer4`. The xcaddy image
adds the `mholt/caddy-l4` plugin. The HTTP JSON shape is
byte-identical to stock, so the same `render_routes` works on
both — only `render_l4_routes` requires the plugin.

`docker-compose.yml`'s `caddy` service swaps to this image. On
startup, ingress does a single `GET /config/` and logs (does NOT
fail) when `apps.layer4` is missing — surface the drift with an
`ingress.l4.caddy_has_layer4` gauge.

## Port allocation strategy

The CP owns the canonical assignment:

```
CP `apps.l4_public_port`  ←── atomic UPDATE … WHERE null RETURNING
       │
       ▼ (polled every 30s by every ingress in the region)
ingress `L4PortCache`
       │
       ▼ (consulted on cold path only — re-heartbeats reuse the existing entry)
ingress `L4PortPool`  ←── ingress-local fallback for tenants that haven't pre-allocated
```

Why two layers?

| Layer | Authoritative | Rerun cost | Used when |
|---|---|---|---|
| CP `apps.l4_public_port` | yes | DB roundtrip + lock contention | First heartbeat of a new app; refresh every 30s |
| Ingress `L4PortCache` | no | HTTP GET + 30s tick | Cold path consult; cache_miss → L4PortPool |
| Ingress `L4PortPool`  | no | none | Local fallback (pre-alloc hasn't happened yet) |

The local pool is the v1 fallback path. Two ingress instances in
the same region can race on the local pool in that scenario; once
the CLI pre-allocates, both ingresses see the same port via the
cache and the race disappears. The pre-allocation step is
documented in the smoke script (`scripts/dev-l4-smoke.sh`).

## DDoS / quota mapping

| Layer | HTTP shape | L4 shape |
|---|---|---|
| Per-app cap | `Config::max_conns` (HTTP/1.1 connection pool) | `INGRESS_L4_MAX_CONNS_PER_APP` (default 1000) |
| Per-IP cap | `Config::max_conns_per_ip` | `INGRESS_L4_MAX_CONNS_PER_IP` (default 100) |
| Per-app RPS | `Config::max_rps` | **none** — TCP has no requests to key on |
| Quota over-cap | `static_response 402` | empty `routes: []` → Caddy closes immediately |

Per-app RPS does not translate cleanly: TCP has no requests to
key on. A future enhancement would be a payload-aware proxy that
counts "frames sent" instead — out of scope for v1.

## Failure modes

| Scenario | Behavior |
|---|---|
| Ingress restart | `L4PortCache` is empty; cold-path consults the CP, gets the same port (persisted on `apps.l4_public_port`), routes reappear. |
| Worker restart | Heartbeat disappears from NATS for ~30s; the stale-route pruner (Commit 7) evicts the L4 entry; re-heartbeat restores. |
| xcaddy image absent | `apps.layer4` missing from `/config/`; ingress logs `ingress.l4.caddy_has_layer4 = 0` and skips L4 render — HTTP routing continues to work. |
| Quota over-cap | Empty `routes: []` on the L4 server block. Caddy closes the TCP connection immediately. |
| CP unreachable | `L4PortCache` returns `None`; falls back to `L4PortPool` (v1 fallback). Two ingresses in the same region may race; this resolves once CP comes back. |
| Port range exhausted | `UPDATE … RETURNING` returns 0 rows; CP responds with `ErrL4PortRangeExhausted`; CLI surfaces a clear "all 1000 ports in `L4_PORT_RANGE_*` are in use" error. |

## Test recipe (end-to-end)

The bash smoke script (`scripts/dev-l4-smoke.sh`) walks the whole
path:

```sh
#!/usr/bin/env bash
set -euo pipefail
docker build -t edgecloud/caddy-l4:latest -f edge-ingress/Dockerfile.caddy-l4 .
docker compose up -d caddy
( cd samples/hello-tcp && ../../target/release/edge build )
EDGE_API_KEY=dev-key EDGE_API_URL=http://localhost:8080 \
    edge deploy --manifest samples/hello-tcp/edge.toml
sleep 5
public_port=$(curl -sH "Authorization: Bearer $EDGE_API_KEY" \
  http://localhost:8080/api/v1/apps/hello-tcp/l4-port | jq -r .public_port)
echo -e 'PING\r\n' | nc -w 2 localhost "$public_port" | grep -q '^PONG$'
echo "PASS: hello-tcp reachable at tcp://localhost:$public_port"
```

## Manual verification checklist

1. **HTTP regression**: stock `caddy:2` + ingress + worker →
   `samples/hello` still works. The L4 branch is opt-in per
   `protocol:"tcp"`.
2. **L4 happy path**: xcaddy image + ingress + worker +
   `samples/hello-tcp` → `nc localhost <port>` returns `PONG`.
3. **Ingress restart**: `Ctrl-C` ingress, restart →
   `samples/hello-tcp` still routable (CP-persistent allocation).
4. **Worker restart**: kill the worker → within 60s the L4 route
   disappears from Caddy; re-spawn the worker → route reappears.
5. **CLI discovery**: `edge tcp-info <app>` returns the
   `public_port` printed during deploy.
6. **Quota trip**: cross `quotas.MaxMemoryMB` from the CP
   admin endpoint → TCP connection closes immediately on the next
   `nc`.
7. **Compose down/up**: `docker compose down && up` → L4 routes
   reappear (persistence spans restart).
8. **xcaddy image drift**: deploy the HTTP sample against stock
   `caddy:2` (no plugin) → confirm HTTP routes still render; only
   `apps.layer4` is empty.

## Out of scope (v2+)

- **TLS-on-SNI for raw-TCP**: `mholt/caddy-l4` supports
  `handler:"tls"` routes with SNI matching; v1 forwards plaintext
  and terminates TLS at the worker.
- **Per-app RPS** on TCP (frame-counting proxy).
- **Multi-region anycast + GeoDNS** for L4 endpoints (same as HTTP,
  blocked on #82).
- **Auth / rate-limit envelopes for raw-TCP**: requires a
  protocol-aware proxy (e.g., RESP-aware rate limiter in front of
  Redis).
