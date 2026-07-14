# edgeCloud Whitepaper

> **Status:** Draft v0.1
> **Date:** 2026-06-14
> **Version:** 1.0

---

## 1. Overview

edgeCloud is a managed WebAssembly edge computing platform purpose-built for running **backend applications** at the edge. Developers write services in any language that compiles to Wasm, run `edge deploy`, and their application is live globally in seconds.

The platform is NOT for running frontend assets at CDN edge locations. It is for compute-intensive backend workloads — API gateways, data transformation pipelines, authentication services, middleware, and similar — that need to run close to end users without the operational overhead of managing infrastructure.

**Core properties:**
- Cold starts in **under 5 milliseconds** via Wasm sandboxing
- **Polyglot**: Rust, C, Go (as Wasm tooling matures)
- **Migration-first**: existing backend services migrate to edge with minimal code changes
- **Per-request pricing**: customers pay for what they use

**Target customer:** Developers and small-to-medium teams who want global edge infrastructure for backend services without managing servers, containers, or Kubernetes.

---

## 2. Architecture

### 2.1 High-Level Topology

```
┌─────────────────────────────────────────────────────────────────┐
│                        DEVELOPER                                 │
│                                                              │
│   edge init my-service    # Scaffold project                   │
│   edge build              # Compile to .wasm or component      │
│   edge deploy             # Ship to edge                        │
│                                                              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      CONTROL PLANE (Go)                          │
│   REST API for developers (deploy, status, env vars)            │
│   NATS JetStream publisher for worker task distribution          │
│   PostgreSQL for persistent state (multi-region)                │
└─────────────────────────────────────────────────────────────────┘
                              │
              ┌──────────────┼──────────────┐
              │              │              │
              ▼              ▼              ▼
        Frankfurt        NYC          Singapore
        ┌────────┐   ┌────────┐   ┌────────┐
        │ NATS   │   │ NATS   │   │ NATS   │
        │ Server │   │ Server │   │ Server │
        └───┬────┘   └───┬────┘   └───┬────┘
            │            │            │
            ▼            ▼            ▼
     ┌────────────┐ ┌────────────┐ ┌────────────┐
     │  Worker     │ │  Worker    │ │  Worker    │
     │  Supervisor │ │  Supervisor │ │  Supervisor │
     │   (Rust)    │ │   (Rust)    │ │   (Rust)    │
     │  systemd    │ │  systemd    │ │  systemd    │
     └─────┬──────┘ └─────┬──────┘ └─────┬──────┘
           │              │              │
           ▼              ▼              ▼
     ┌───────────┐ ┌───────────┐ ┌───────────┐
     │ wasmtime  │ │ wasmtime  │ │ wasmtime  │
     │ processes │ │ processes │ │ processes │
     │  (many)   │ │  (many)   │ │  (many)   │
     └───────────┘ └───────────┘ └───────────┘
```

End-users reach deployed apps through a region-local `edge-ingress`
process that fronts a Caddy instance — `caddy:2` for HTTP(S) and an
xcaddy-built image (`edgecloud/caddy-l4:latest`, with the
`mholt/caddy-l4` plugin) for **raw-TCP apps**. HTTP apps are routed
by hostname (`<tenant>-<app>.edgecloud.dev`); raw-TCP apps (Redis,
MQTT, custom protocols — issue #548) are routed by public port in
the `L4_PORT_RANGE_*` window (`31000..=31999` by default). The
ingress is the only component that knows the public→private
mapping; the worker stamps the private upstream port onto
`EDGE_HTTP_SERVER_PORT`, and the ingress learns the address from
NATS heartbeats. The full L4 design is in
[`docs/l4-ingress.md`](docs/l4-ingress.md).

### 2.2 Component Responsibilities

| Component | Language | Role |
|-----------|----------|------|
| **edge CLI** | Rust | Developer toolchain: init, build, deploy, manage |
| **Control Plane** | Go | REST API for developers, NATS publisher, PostgreSQL state |
| **Worker Supervisor** | Rust | Process lifecycle, resource limits, health monitoring |
| **edge Runtime** | Rust | Wasmtime-based runtime exposing WASI Preview 2 interfaces |
| **NATS JetStream** | — | Task distribution to workers across regions |
| **edge-ingress** (per region) | Rust | NATS heartbeat-driven Caddy controller. Maps `<tenant>-<app>.edgecloud.dev` to `http://<worker>:<port>`. The L4 path (`apps.layer4`) routes raw-TCP apps (Redis, MQTT, custom protocols) by public port (issue #548). |
| **PostgreSQL** | — | Persistent state: tenants, deployments, API keys, quotas |

---

## 3. Developer CLI (Rust)

### 3.1 Commands

```
edge init <name>           Scaffold new project (edge.toml + Cargo.toml + src/)
edge build [--path PATH]   Compile to .wasm or .wasm component
edge deploy [--path PATH]  Upload artifact to control plane
edge status [--path PATH]  Get deployment status
edge env set <key> <value> Set environment variable for app
edge env list [--path]     List environment variables
edge activate <dep-id>      Activate a specific deployment
edge migrate <path>        Analyze C/Rust source for WASI compatibility
edge dev [--path PATH]     Local dev server with hot-reload
edge open [--path PATH]    Open deployed URL in browser
edge deployments [--path]  List all deployments for app
```

### 3.2 Project Scaffold (`edge init`)

Creates a new directory with:

**`edge.toml`** — Project configuration
```toml
[project]
name = "my-service"
version = "0.1.0"
target = "wasm32-wasip2"  # WASI Preview 2, component model

[deployment]
api = "https://api.edgecloud.dev"
```

**`Cargo.toml`** — Rust package manifest
```toml
[package]
name = "my-service"
version = "0.1.0"
edition = "2024"

[dependencies]
```

**`src/main.rs`** — WASI Preview 2 component template using WIT definitions.

### 3.3 Build Output

`edge build` produces either:
- A **plain `.wasm` module** (for existing WASI Preview 1 binaries)
- A **WASI Preview 2 component** (for WIT-defined interfaces)

The CLI detects which based on the target in `edge.toml`.

### 3.4 Deploy Flow

1. Read `edge.toml` for project name and API endpoint
2. Read the compiled artifact (`.wasm` or component)
3. Send `POST /api/deploy/{appName}` with multipart form (`payload` field)
4. Control plane stores artifact, computes SHA-256 hash, returns deployment ID
5. CLI saves `{ deployment_id, app_name, live_url }` to `.edge/state.json`

---

## 4. Control Plane (Go)

### 4.1 Public REST API

All endpoints require API Key authentication via `Authorization: Bearer <key>` header.

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/deploy/{appName}` | Upload deployment artifact |
| GET | `/api/status/{deploymentID}` | Get deployment status |
| GET | `/api/list/{appName}` | List all deployments for app |
| POST | `/api/apps/{appName}/activate/{deploymentID}` | Activate a deployment |
| GET | `/api/apps/{appName}/active` | Get active deployment |
| POST | `/api/apps/{appName}/env` | Set environment variables |
| GET | `/api/apps/{appName}/env` | Get environment variables |
| GET | `/api/keys` | List API keys |
| POST | `/api/keys` | Create new API key |
| DELETE | `/api/keys/{keyID}` | Revoke API key |
| GET | `/metrics` | Prometheus global metrics |
| GET | `/api/metrics` | Prometheus per-tenant metrics |

**Admin-only (role: owner):**

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/admin/tenants` | List all tenants |
| POST | `/api/admin/tenants` | Create tenant |
| GET | `/api/admin/tenants/{tenantID}` | Get tenant + quota |
| PUT | `/api/admin/tenants/{tenantID}` | Update tenant |
| DELETE | `/api/admin/tenants/{tenantID}` | Delete tenant + all data |

**Health:**

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Liveness probe — returns 200 if the process can respond. |
| GET | `/ready` | Readiness probe — DB ping + NATS flush + per-loop snapshot. 503 only on DB/NATS failure. |

### 4.2 Internal Worker API (NATS)

The control plane does not expose HTTP endpoints for worker communication. Instead, it publishes all worker-facing tasks to NATS JetStream subjects. Workers subscribe to their regional NATS server.

#### NATS Subjects

```
edgecloud.tasks.<region>           # Per-region task subject
edgecloud.deployments.<tenantID>   # Tenant-scoped deployment events
edgecloud.heartbeats.<region>      # Worker heartbeat aggregation
```

#### Message Types Published by Control Plane

**`TaskMessage`** — Published to `edgecloud.tasks.<region>` when a worker's app set changes:
```json
{
  "type": "task_update",
  "timestamp": "2026-06-14T12:00:00Z",
  "tenant_id": "t_abc123",
  "apps": {
    "my-service": {
      "deployment_id": "d_xyz789",
      "deployment_hash": "sha256:abc...",
      "env": { "DATABASE_URL": "postgres://..." },
      "allowlist": ["api.stripe.com", "db.internal"],
      "socket_mode": "block-all"
    }
  }
}
```

**`DeploymentPayload`** — Workers fetch the actual Wasm artifact via HTTP:
```
GET /api/internal/download/{deploymentID}
```
Content-Type: `application/octet-stream`
Response: Raw Wasm bytes

#### Activation Idempotency & Multi-region Publish (issue #127)

`POST /api/apps/{appName}/activate/{deploymentID}?regions=...` is
idempotent across retries. The control plane tracks per-region
publish state on `active_deployments` (see §4.3) so a retry skips
regions already successfully published and always retries regions
that failed on the prior attempt:

```
toPublish = (deployment.Regions ∪ regions_failed) − regions_published
```

If the computed `toPublish` is empty, the activation is a no-op — the
row already flipped, which is the correct semantic. NATS JetStream
workqueue retention dedupes by message id, so re-publishing an
already-published region is a safe no-op on the worker side.

When at least one region's publish fails, the control plane returns
**HTTP 502** with a per-region breakdown:

```json
{
  "error": "activation committed but worker notification failed; please retry",
  "regions_published": ["us-east"],
  "regions_failed": ["eu-west"]
}
```

The tenant can re-issue the activate and only the failed regions will
be re-published. Same envelope applies to the rollback path.

### 4.3 Data Model (PostgreSQL)

```sql
-- Tenants
CREATE TABLE tenants (
    id          TEXT PRIMARY KEY,  -- "t_<uuid>"
    name        TEXT NOT NULL,
    plan        TEXT NOT NULL DEFAULT 'free',
    allowlisted_destinations TEXT[] DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Quotas (per tenant)
CREATE TABLE quotas (
    tenant_id   TEXT PRIMARY KEY REFERENCES tenants(id) ON DELETE CASCADE,
    max_deployments  INT NOT NULL DEFAULT 10,
    max_apps        INT NOT NULL DEFAULT 5,
    max_workers     INT NOT NULL DEFAULT 3,
    max_memory_mb   INT NOT NULL DEFAULT 256,
    max_outbound_mb INT NOT NULL DEFAULT 1000
);

-- API Keys
CREATE TABLE api_keys (
    id          TEXT PRIMARY KEY,  -- "k_<uuid>"
    tenant_id   TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    key_hash    TEXT NOT NULL,  -- SHA-256 of raw key
    role        TEXT NOT NULL DEFAULT 'developer',  -- owner, developer, viewer
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used   TIMESTAMPTZ,
    expires_at  TIMESTAMPTZ
);

-- Deployments
CREATE TABLE deployments (
    id          TEXT PRIMARY KEY,  -- "d_<uuid>"
    tenant_id   TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    app_name    TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'deployed',  -- deployed, active, failed
    hash        TEXT NOT NULL,  -- SHA-256 of Wasm payload
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Active Deployment Mapping
CREATE TABLE active_deployments (
    tenant_id   TEXT NOT NULL,
    app_name    TEXT NOT NULL,
    deployment_id TEXT NOT NULL REFERENCES deployments(id),
    -- Per-region publish state (issue #127, migration 010).
    -- regions_published / regions_failed make ActivateDeployment
    -- idempotent: a retry skips regions already published, always
    -- retries regions that failed previously. The DB row is the
    -- recovery aid; the publish itself is the source of truth.
    regions_published       TEXT[] NOT NULL DEFAULT '{}',
    regions_failed          TEXT[] NOT NULL DEFAULT '{}',
    last_publish_at         TIMESTAMPTZ,
    last_publish_attempt_id UUID,
    PRIMARY KEY (tenant_id, app_name)
);

-- App Environment Variables
CREATE TABLE app_env (
    tenant_id   TEXT NOT NULL,
    app_name    TEXT NOT NULL,
    env_key     TEXT NOT NULL,
    env_value   TEXT NOT NULL,
    PRIMARY KEY (tenant_id, app_name, env_key)
);

-- Workers (registered supervisors)
CREATE TABLE workers (
    id          TEXT PRIMARY KEY,  -- "w_<region>_<uuid>"
    region      TEXT NOT NULL,
    ip          TEXT,
    memory_mb   INT NOT NULL DEFAULT 4096,
    last_seen   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Worker Status Reports
CREATE TABLE worker_status (
    worker_id   TEXT PRIMARY KEY REFERENCES workers(id) ON DELETE CASCADE,
    apps        JSONB NOT NULL DEFAULT '{}',  -- { app_name: { status, exit_code, deployment_id } }
    last_report TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Deployment Artifacts (stored on disk, not in DB)
-- Path: /registry/{tenant_id}/{app_name}/{deployment_id}.wasm
```

### 4.4 Quotas (Default Free Tier)

| Limit | Value |
|-------|-------|
| Max deployments per app | 10 |
| Max apps per tenant | 5 |
| Max workers per tenant | 3 |
| Max memory per worker | 256 MB |
| Max outbound bandwidth | 1000 MB |

### 4.5 Retention GC (issue #574)

Three append-only tables — `audit_logs`, `webhook_deliveries`, `autoscale_events` — grow without bound otherwise. Each is swept by a dedicated background service:

| Table | Default retention | Service | Index |
|-------|-------------------|---------|-------|
| `audit_logs` | 90 days | `AuditGC.Run` | `idx_audit_logs_created_at` (migration 031) |
| `webhook_deliveries` | 30 days | `WebhookDeliveryGC.Run` | `idx_webhook_deliveries_created_at` (migration 031) |
| `autoscale_events` | 14 days | `AutoscaleEventGC.Run` | `idx_autoscale_events_created_at` (migration 031) |

All three services follow the same shape as `LogGC.Run` (issue #581 trio precedent): immediate-first-sweep, ticker at interval, refused-to-run on non-positive interval or retention, server-side `NOW() - make_interval(secs => $1)` cutoff (clock-skew safe), paginated 10k rows per batch with a 1000 batches/sweep cap, ctx-cancellation short-circuit. Intervals and retentions are operator-tunable via env: `AUDIT_GC_INTERVAL`, `AUDIT_RETENTION`, `WEBHOOK_DELIVERY_GC_INTERVAL`, `WEBHOOK_DELIVERY_RETENTION`, `AUTOSCALE_EVENT_GC_INTERVAL`, `AUTOSCALE_EVENT_RETENTION`. Default intervals match `LOG_GC_INTERVAL` (1h).

Per-family Prometheus metrics (`edge_audit_log_gc_*`, `edge_webhook_delivery_gc_*`, `edge_autoscale_event_gc_*` — 4 families each: `ticks_total`, `rows_deleted_total`, `errors_total`, `last_tick_timestamp_seconds`) are emitted on every sweep tick via the same `MetricsAggregator` sink pattern used by the existing GCs. Operators should alert on `last_tick_timestamp_seconds` older than `2 × interval` — a "never-ticked" condition means the GC refused to run or is stuck.

---

## 5. Worker Supervisor (Rust)

### 5.1 Role

The Worker Supervisor is a long-running Rust process on each compute node. It is managed by **systemd** and responsible for:

- Subscribing to NATS JetStream for its region
- Managing the lifecycle of wasmtime processes (start, stop, restart)
- Enforcing resource limits (memory) per app
- Reporting health and status back via NATS
- Downloading Wasm artifacts from the control plane's HTTP download endpoint
- Port assignment for apps listening on TCP

### 5.2 Process Model

```
Worker Supervisor (one per node, systemd-managed)
  │
  ├── App Process 1  →  wasmtime run app1.wasm -S preview2=n -S tcplisten=0.0.0.0:8081
  ├── App Process 2  →  wasmtime run app2.wasm -S preview2=n -S tcplisten=0.0.0.0:8082
  ├── App Process 3  →  wasmtime run app3.wasm -S preview2=n -S tcplisten=0.0.0.0:8083
  └── ...
```

Each app runs as a **separate wasmtime process** (not threads). Wasm's sandboxing provides process-level isolation without containers.

### 5.3 NATS Subscription

The supervisor subscribes to:
```
edgecloud.tasks.<region>   # All task updates for this region
```

On receiving a `TaskMessage`:
1. Compare incoming apps against running apps
2. **Stop** apps no longer in the active set
3. **Start** new or changed apps (hash mismatch = re-download and restart)
4. **Update** env vars for running apps that changed

### 5.4 Artifact Download & Cache

```
.worker-cache/
  ├── d_abc123.wasm   # Cached by deployment ID
  └── d_xyz789.wasm
```

- Cache hit: serve from local disk
- Cache miss or hash changed: `GET /api/internal/download/{deploymentID}`
- Invalidate cache entry when deployment hash changes

#### 5.4.1 Multi-region artifact replication (issue #127)

In a multi-region deployment, each regional control plane can be
configured with one of three artifact backends (`storage.artifact_backend`):

- `fs` (default) — local filesystem at `/registry/...`. Single-region only.
- `s3` — `s3://<bucket>/<key>/<tenant>/<app>/<d>.wasm`. Every CP reads
  directly from the shared bucket; no peer relationship needed.
- `remote` — pull-through cache. Each CP has a local FS cache; on miss
  it `GET`s the artifact from a configured peer CP over HTTPS using a
  shared `X-Internal-Token` header. First request pays cross-region
  latency once, then every subsequent request hits the local cache.

Worker-side download is unchanged: workers always hit their local
control plane via the JWT-authenticated
`/api/internal/download/{deploymentID}` endpoint. The peer-pull path
is invisible to workers. See `edge-control-plane/docs/storage.md`
for the operator-facing config matrix.

The `/api/internal/download/{deploymentID}` endpoint accepts EITHER
a worker JWT (existing) OR an `X-Internal-Token` header (new) — the
two lanes dispatch by which credential is presented.

### 5.5 Port Assignment

Apps request a TCP port via wasmtime's `tcplisten` WASI extension. The supervisor:
1. Maintains a port pool (starting at 8081)
2. Tracks recently freed ports (60-second cooldown to avoid address reuse conflicts)
3. Assigns sequentially, skipping recently freed ports

### 5.6 Health Reporting

Supervisor publishes to NATS subject `edgecloud.heartbeats.<region>`:
```json
{
  "worker_id": "w_fra_abc123",
  "timestamp": "2026-06-14T12:00:00Z",
  "apps": {
    "my-service": { "status": "running", "deployment_id": "d_xyz789" }
  }
}
```

Heartbeat interval: **30 seconds**.

### 5.7 Worker `/metrics` endpoint (issue #49)

Each `edge-worker` process exposes a Prometheus-format metrics endpoint
for scraping per-app counters without going through the control plane.
Binds to `METRICS_ADDR` (default `0.0.0.0:9090`) and serves one
endpoint: `GET /metrics`. Bearer-token auth via `METRICS_AUTH_TOKEN`;
empty token = every request returns 401 (fail-closed).

Metric families:

- `edge_requests_total{deployment_id, app_name}` — monotonic FaaS request counter (mirrors `RequestMeter::record_request`).
- `edge_outbound_bytes_total{deployment_id, app_name}` — monotonic FaaS response byte counter (mirrors `RequestMeter::record_outbound_bytes`).
- `edge_duration_ms_total{deployment_id, app_name}` — monotonic FaaS wall-clock latency (mirrors `RequestMeter::record_duration`).
- `edge_resident_seconds_total{deployment_id, app_name}` — LongRunning resident-time ticker (bumped every heartbeat interval for LR apps only).
- `edge_app_status{deployment_id, app_name, status}` — gauge. Single-row invariant: previous status is cleared before a new one is stamped.
- `edge_worker_uptime_seconds` — worker process uptime.
- `edge_worker_active_apps` — current count of running apps.

The counters are intentionally redundant with `HeartbeatMessage.{request_count, outbound_bytes, duration_ms_total, resident_seconds}`: the heartbeat is for billing (snapshot-and-subtract per heartbeat), the Prometheus counter is for operator dashboards (monotonic running total). They are NOT a duplicate channel — both are wired at the dispatch path.

### 5.8 Graceful Shutdown

On SIGTERM:
1. Stop accepting NATS messages
2. Kill all running wasmtime processes
3. Report final status to NATS
4. Exit cleanly

---

## 6. edge Runtime (Rust)

### 6.1 Role

The edge Runtime is the Rust library that wraps wasmtime and exposes host capabilities to Wasm modules. It is linked into the wasmtime process spawned by the Worker Supervisor (not a separate process).

### 6.2 Supported Interfaces (WASI Preview 2)

The runtime exposes these WIT-defined interfaces:

| Interface | Purpose |
|-----------|---------|
| `edge:http-client` | Outbound HTTP requests |
| `edge:networking` | TCP/UDP/DNS |
| `edge:kv-store` | Key-value persistence |
| `edge:cache` | In-memory LRU cache |
| `edge:observe` | Metrics and logging |
| `edge:time` | Monotonic clock |
| `edge:scheduling` | Delayed execution |
| `edge:process` | Environment variables and args |
| `edge:http-server` | Inbound HTTP serving |

### 6.3 WASI Preview 2 Component Model

The runtime uses `wasmtime`'s WASI Preview 2 implementation with the component model enabled. Components are the preferred artifact format; plain `.wasm` modules are supported for backward compatibility with existing WASI P1 binaries.

### 6.4 Security Configuration

```rust
// wasmtime Engine configuration
config.set_wasm_threads(false);        // Disabled: not needed, reduces attack surface
config.set_wasm_reference_types(false); // Disabled: same reason
config.set_wasm_simd(true);             // Enabled: performance
config.set_wasm_component_model(true); // Required for Preview 2
config.set_epoch_interruption(true);   // CPU time limits
```

### 6.5 Memory Limits

Each wasmtime process is launched with a memory limit derived from the tenant's quota (default 256 MB). The Limiter API enforces this at the wasmtime Store level.

### 6.6 Memory Access Pattern (Host Functions)

All host function implementations follow a consistent pointer-based memory access pattern:

```rust
fn http_client_request(
    caller: &wasmtime::Caller,
    method_ptr: i32,
    method_len: i32,
    url_ptr: i32,
    url_len: i32,
    body_ptr: i32,
    body_len: i32,
) -> i32 {
    // 1. Get wasm memory export
    let mem = caller.get_export("memory").unwrap().into_memory().unwrap();
    let data = mem.data(caller);

    // 2. Read string arguments from wasm linear memory
    let method = read_string(data, method_ptr, method_len);
    let url = read_string(data, url_ptr, url_len);

    // 3. Perform operation

    // 4. Write response back to wasm memory if needed
    //    Return 0 = success, negative = error
}
```

---

## 7. Deployment Artifact Format

### 7.1 Supported Formats

| Format | Extension | Description |
|--------|-----------|-------------|
| WASI Preview 1 Module | `.wasm` | Legacy plain wasm module, supports `wasi_unstable` |
| WASI Preview 2 Module | `.wasm` | Modern wasm module with `wasi:http` interfaces |
| WASI Preview 2 Component | `.wasm` | WIT-defined component with typed interfaces |

The control plane detects format from the file magic bytes and the presence of a component model header.

### 7.2 Storage

Artifacts are stored on the control plane server's filesystem:
```
/registry/{tenant_id}/{app_name}/{deployment_id}.wasm
```

Naming scheme uses deployment ID (not content hash) to avoid conflicts. The SHA-256 hash stored in the database is used for cache invalidation, not for path construction.

---

## 8. Communication Protocols

### 8.1 Developer → Control Plane

HTTPS REST with API Key auth:
```
Authorization: Bearer <raw_api_key>
```

API keys are hashed with SHA-256 on the server; the raw key is returned only once at creation time.

### 8.2 Control Plane → Workers

NATS JetStream publish to regional subjects:
```
edgecloud.tasks.<region>
edgecloud.heartbeats.<region>
```

Workers subscribe to their region's NATS server. The NATS supercluster handles cross-region message replication.

### 8.3 Workers → Control Plane

- **Artifact download**: HTTP GET to control plane's internal download endpoint (JWT auth via query param or header)
- **Health reporting**: NATS publish to `edgecloud.heartbeats.<region>`
- **No direct DB access**: workers never touch PostgreSQL

### 8.4 NATS JetStream Configuration

Each region runs a NATS server configured in JetStream mode. Regional servers form a global supercluster:

```
Frankfurt <── supercluster ──> NYC <── supercluster ──> Singapore
```

Streams are configured with:
- **Retention**: `workqueue` for task subjects (each message processed by one worker)
- **Replication factor**: 3 (across regions for durability)
- **Max age**: 24 hours for task messages

### 8.5 Message Shapes

**TaskMessage** (published by CP → NATS):
```json
{
  "type": "task_update",
  "timestamp": "2026-06-14T12:00:00Z",
  "tenant_id": "t_abc123",
  "apps": {
    "my-service": {
      "deployment_id": "d_xyz789",
      "deployment_hash": "sha256:abc123...",
      "env": { "KEY": "VALUE" },
      "allowlist": ["api.stripe.com"],
      "socket_mode": "block-all"
    }
  }
}
```

**TaskPurge** (published by CP → NATS): Tombstone for per-tenant KV/cache/scheduling data (issue #569). Distinct `type` discriminator on the same `edgecloud.tasks.<region>` subject. Workers stop matching apps (drains in-flight requests), then call `edge_runtime::runtime::purge_tenant` which removes the in-memory registry entry + on-disk directories. Empty `app_name` is reserved for tenant-wide variants — current code always sets it.
```json
{
  "type": "task_purge",
  "timestamp": "2026-07-10T12:00:00Z",
  "tenant_id": "t_abc123",
  "app_name": "my-service",
  "reason": "app_deleted"
}
```
`reason` is `"app_deleted"` (per-app purge) or `"tenant_offboarded"` (per-tenant purge, enqueued once per app).

**HeartbeatMessage** (published by Worker → NATS):
```json
{
  "type": "heartbeat",
  "timestamp": "2026-06-14T12:00:00Z",
  "worker_id": "w_fra_abc123",
  "region": "fra",
  "apps": {
    "my-service": {
      "deployment_id": "d_xyz789",
      "status": "running",
      "exit_code": 0,
      "request_count": 12,
      "outbound_bytes": 4096,
      "resident_seconds": 30,
      "dedupe_id": "w_fra_abc123:d_xyz789:1750000800",
      "last_error": null
    }
  }
}
```

**Field notes** (issues #418, #484, #485):

- `request_count` and `outbound_bytes` — per-interval deltas for the request-count and outbound-bytes metered dimensions (issue #419/#420 quota hot path).
- `resident_seconds` — `null` for Handler (FaaS) apps; an integer for LongRunning apps representing seconds resident in the last heartbeat interval (issue #484). `Some(0)` and `null` are distinct on the wire: the former means "an LR app that started within the current interval"; the latter means "an FaaS app, which contributes nothing to this dimension."
- `dedupe_id` — stable `(worker_id, deployment_id, 30s_bucket)` token the control plane uses to skip re-applying the same delta on JetStream redelivery or reconcile replay (issue #418).
- `last_error` — string if the app transitioned to `Crashed`/`Errored` in the last interval; `null` for `running`.

---

## 9. Security Model

### 9.1 Authentication

**Developer API**: API Key (SHA-256 hashed, Bearer token in Authorization header)
**Worker → Control Plane**: JWT (HMAC-SHA256, 24h TTL, issued by control plane on worker registration)

### 9.2 Authorization

Role-based access control:
- `owner`: full access including tenant management
- `developer`: deploy, env vars, activation, status
- `viewer`: read-only access

### 9.3 Worker JWT

Worker JWT payload:
```json
{
  "iss": "edgecloud",
  "exp": 1750000000,
  "worker_id": "w_fra_abc123",
  "tenant_id": "t_abc123",
  "apps": ["my-service", "auth-service"]
}
```

### 9.4 Input Validation

- App names: rejected if contain `..` or `/`
- Deployment IDs: validated for path traversal (`..`, `/`, null bytes)
- API keys: hashed before storage, raw key shown only once at creation
- Worker IDs: validated format `w_<region>_<uuid>`

### 9.5 Network Security

- Developer API: HTTPS only (TLS termination at load balancer)
- Worker → Control Plane download: HTTPS with JWT validation
- NATS: TLS client certificates for inter-cluster communication
- Egress allowlisting: tenants specify allowed outbound destinations

---

## 10. edge migrate

### 10.1 Purpose

Analyze existing C or Rust source code for WASI compatibility issues and automatically transform POSIX calls to their WASI equivalents.

### 10.2 Integration

`edge migrate` is a subcommand of `edge CLI` (Rust binary):
```bash
edge migrate ./my-c-service     # Analyze and report
edge migrate --auto ./my-c-service  # Transform in place
```

### 10.3 Supported Transformations

| Source | Target |
|--------|--------|
| `socket()` | `WASI sockets` |
| `bind()` | `WASI socks` |
| `read()`/`write()` on sockets | WASI stream I/O |
| `open()`/`read()`/`write()` | WASI filesystem |
| `stdio` | WASI streams |

### 10.4 Analysis Output

```
$ edge migrate ./my-service

Analyzing my-service/main.c...
⚠ POSIX socket detected: main.c:42 socket(AF_INET, SOCK_STREAM, 0)
  → Suggestion: Replace with WASI udp-socket or tcp-socket

⚠ POSIX file open: main.c:78 open("config.json", O_RDONLY)
  → Automatic fix available: --auto

✅ WASI compatible: main.c:15 fprintf(stdout, ...)
```

---

## 11. Pricing

**Per-request model.**

Customers are charged per HTTP request handled by their deployed applications. Pricing tiers are defined by quotas (max deployments, max apps, memory per app, outbound bandwidth).

Tiers:

| Tier | Price | Requests/mo | Max Apps | Max Memory/App |
|------|-------|------------|----------|----------------|
| Free | $0 | 100,000 | 5 | 256 MB |
| Pro | $25/mo | 5,000,000 | 20 | 512 MB |
| Business | $100/mo | 50,000,000 | 50 | 1024 MB |
| Enterprise | Custom | Unlimited | Unlimited | Custom |

Overage pricing applies for requests beyond the tier limit.

---

## 12. Road Map

### Phase 1 — MVP (3 months)

- [ ] edge CLI (init, build, deploy, status, env, activate, migrate)
- [ ] Control Plane (Go) with PostgreSQL
- [ ] Worker Supervisor (Rust) for single region
- [ ] NATS JetStream integration (single region)
- [ ] edge Runtime (Rust) with WASI Preview 2
- [ ] Per-request pricing (free tier + Stripe integration)
- [ ] Developer portal (web dashboard for account management)

### Phase 2 — Multi-Region (Months 4–6)

- [ ] NYC and Singapore compute nodes
- [ ] NATS supercluster across regions
- [ ] PostgreSQL multi-region setup
- [ ] Global latency-based routing

### Phase 3 — Language Support (Months 6–12)

- [ ] C migration tool (full auto-transform)
- [ ] Go WASI support (as Go 1.24+ WASI matures)
- [ ] Python adapter (if viable Wasm path exists)

### Phase 4 — Ecosystem (Months 12–18)

- [ ] `edge logs` command (stream live logs from deployed apps)
- [ ] `edge rollback` command (revert to previous deployment)
- [ ] `edge scale` command (configure per-app scaling)
- [ ] Preview environments (deploy to staging URL before going live)

---

## 13. Glossary

| Term | Definition |
|------|------------|
| **Wasm** | WebAssembly — binary instruction format for stack-based virtual machines |
| **WASI** | WebAssembly System Interface — standardized syscall-like interface for Wasm modules |
| **WASI Preview 2** | Second generation WASI API using WIT interface definitions and the component model |
| **WIT** | WebAssembly Interface Types — IDL for defining component interfaces |
| **Component** | A WASI Preview 2 artifact — a Wasm module with typed, structured interfaces defined in WIT |
| **wasmtime** | Bytecode Alliance's Wasm runtime (Rust implementation) |
| **NATS** | Lightweight message broker supporting JetStream (durable streams, supercluster replication) |
| **NATS Supercluster** | Globally replicated NATS cluster spanning multiple regions |
| **edge Runtime** | edgeCloud's Rust library wrapping wasmtime and exposing edge:* host interfaces |
| **Worker Supervisor** | Rust process on each compute node managing wasmtime app processes |

---

## Appendix A: Component Interaction Sequence

```
Developer                    CLI                     Control Plane              NATS                   Worker Supervisor
   │                          │                          │                       │                         │
   │  edge deploy             │                          │                       │                         │
   │─────────────────────────>│                          │                       │                         │
   │                          │  POST /api/deploy/app    │                       │                         │
   │                          │─────────────────────────>│                       │                         │
   │                          │                          │  Store artifact        │                         │
   │                          │                          │  Publish TaskMessage  │                         │
   │                          │                          │───────────────────────>│                         │
   │                          │  201 { id, url }         │                       │                         │
   │                          │<─────────────────────────│                       │                         │
   │  Deployment URL          │                          │                       │                         │
   │<─────────────────────────│                          │                       │                         │
   │                          │                          │                       │  Receive TaskMessage     │
   │                          │                          │                       │  Compare apps           │
   │                          │                          │                       │  Start new apps         │
   │                          │                          │                       │  GET /download/d_id     │
   │                          │                          │<──────────────────────│                         │
   │                          │                          │  Wasm bytes           │                         │
   │                          │                          │───────────────────────>│                         │
   │                          │                          │                       │  spawn wasmtime         │
   │                          │                          │                       │  (app running)           │
   │                          │                          │                       │                         │
   │                          │                          │  30s heartbeat        │                         │
   │                          │                          │<───────────────────────│                         │
```