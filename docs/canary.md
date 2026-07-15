# Canary / Blue-Green Fan-Out (issue #290)

## What it is

A worker can host **N concurrent deployments** of the same `(tenant,
app_name)` instead of replacing one with the next. The CP emits a
single `TaskMessage` with a `routes[]` list; the worker fans it out
into N independent instances, each with its own port, artifact, and
verification. The ingress renders the weighted split. This is the
end-to-end contract that closes issue #290.

## End-to-end flow

### 1. Control plane emits `routes[]`

`TrafficService.publishTaskUpdate` (`edge-control-plane/internal/service/traffic.go`)
emits `routes[]` on two paths:

  - `PUT /api/v1/apps/{name}/traffic` with explicit weights (e.g.
    `d_v2=20, d_v1=80`).
  - `POST /api/v1/apps/{name}/activate/{deploymentID}?weight=<100` —
    the partial-weight path creates an implicit 2-way split.

`clearTraffic` republishes a single-deployment shape (no `routes`)
so a worker that received a canary earlier drops the canary on the
next sync.

### 2. Worker fans `routes[]` out into per-deployment instances

`expand_routes(apps: HashMap<String, AppSpec>) -> Vec<(app_name,
deployment_id, AppSpec)>` (`edge-worker/src/messages.rs`):

  - `routes == None` → one tuple per app using the primary
    `deployment_id` (legacy single-deployment shape — pre-#290).
  - `routes == Some([d1, d2])` → one tuple per route, each with
    its own per-deployment `deployment_id` / `deployment_hash` /
    `deployment_signature` / `signing_key_id`. The primary fields
    are control-only (`env`, `max_memory_mb`, `allowlist`,
    `socket_mode`, …) and inherited via cloned `AppSpec`.

`handle_task_message` (`edge-worker/src/supervisor.rs`) calls
`expand_routes` immediately after deserializing the wire message,
BEFORE diffing. `compute_app_diff` then sees the per-deployment
view and can apply canary semantics:

  - **Adding a canary:** `current = [(api, d1)]`, `desired =
    [(api, d1), (api, d2)]` → only `(api, d2)` in
    `apps_to_start`. `(api, d1)` is untouched.
  - **Retiring a canary:** `current = [(api, d1), (api, d2)]`,
    `desired = [(api, d1)]` → only `(api, d2)` in
    `apps_to_stop`. `(api, d1)` keeps running.
  - **Blue/green replacement:** `current = [(api, d1)]`,
    `desired = [(api, d2)]` → `(api, d1)` is stopped AND
    `(api, d2)` is started. Pre-#290 semantics for the legacy
    atomic-activate path — opt-in via `routes == None`.

### 3. Worker state holds the canary fleet

`WorkerState.apps` is `HashMap<(String, String, String), …>` keyed by
`(tenant_id, app_name, deployment_id)`. Each canary route gets a
distinct port via `PortPool` and a distinct `AppInstance` with its
own `meter` / `metrics_handle`. The downloader threads the route's
`deployment_hash` + `deployment_signature` into each artifact
fetch — every route downloads and verifies its OWN artifact, never
the primary's.

### 4. Heartbeat emits `"{app_name}:{deployment_id}"` composite keys

`build_heartbeat` stamps each `AppStatus` under
`format!("{}:{}", app_name, deployment_id)`. A canary fan-out
produces N distinct keys (e.g. `api:d_v1` and `api:d_v2`) instead
of one overwrite. `reset_meters_after` splits the key back into
the pair via `split_once(':')` to look up `state.apps` by the
triple.

`edge-ingress` parses the heartbeat key with `split_once(':')`
(`edge-ingress/src/heartbeats.rs`) to recover both halves. Legacy
workers emitting bare `app_name` keys still parse as
`deployment_id: None` — pure forward-compat.

### 5. Ingress renders the weighted split

`edge-ingress` subscribes to `edgecloud.heartbeats.<region>` and
maintains a `HashMap<AppKey, AppRoute>` where
`AppKey { tenant_id, app_name, deployment_id: Option<String> }`.
When canary deployments land, the ingress renders `weight`-based
Caddy routes so e.g. `d_v2=20` of traffic lands on the d_v2 worker
port and `d_v1=80` on the d_v1 worker port.

### 6. Canary purge via `task_purge`

`task_purge` for a per-app delete enumerates every
`(tenant, app_name)` deployment under the tenant's purge
(`handle_purge` synthesizes per-deployment stops) before wiping
KV/cache/scheduler state. So a canary + stable deployment of the
same app are both torn down on app deletion — no orphan instances
holding per-tenant state.

## Wire format

### TaskUpdate with canary (legacy single-deployment ingress still parses)

```json
{
  "type": "task_update",
  "tenant_id": "t_acme",
  "apps": {
    "api": {
      "deployment_id": "d_PRIMARY",
      "deployment_hash": "<unused-when-routes-set>",
      "deployment_signature": null,
      "signing_key_id": null,
      "routes": [
        {
          "deployment_id": "d_v1",
          "deployment_hash": "<sha256-of-v1-artifact>",
          "deployment_signature": "<ed25519-base64url>",
          "signing_key_id": "k1",
          "weight": 80
        },
        {
          "deployment_id": "d_v2",
          "deployment_hash": "<sha256-of-v2-artifact>",
          "deployment_signature": "<ed25519-base64url>",
          "signing_key_id": "k1",
          "weight": 20
        }
      ],
      "env": { "LOG_LEVEL": "info" },
      "max_memory_mb": 256
    }
  }
}
```

### Legacy TaskUpdate (atomic-activate, weight=100)

`routes: None` — single-deployment shape. Pre-#290 behavior, opt-in
to blue/green by sending `routes` instead.

## Compatibility

| Sender → Receiver | Outcome |
|---|---|
| Legacy worker → New ingress | parses `app_name` key as `deployment_id: None` ✓ |
| New worker → Legacy ingress | parses `app_name:deployment_id` via `split_once(':')` ✓ |
| New worker → New ingress | parses `app_name:deployment_id` via `split_once(':')` ✓ |
| Legacy worker → Legacy ingress | parses `app_name` key as `deployment_id: None` ✓ |

The ingress parser change was made **before** this PR (already at
`edge-ingress/src/heartbeats.rs`). The control plane is unchanged —
canary fan-out is opt-in via the existing `routes` field that
`TaskMessage.apps.<app>.routes` has carried on the wire.

## Out of scope / known gaps

- **`FullSync` after a missed canary TaskUpdate.** A `FullSync`
  from the reconcile loop doesn't carry canary state, so a missed
  canary message + an immediate reconcile would shut down a canary
  on the next 5-minute tick. Workaround: send a fresh
  `PUT /api/v1/apps/{name}/traffic` after any missed NATS message.
  The worker-side fix is local (re-read canary state from
  `app_traffic_splits` on `FullSync`) — filed as a follow-up.
- **Per-(tenant, app, deployment_id) task_purge shape.** Today,
  `task_purge` enumerates every deployment of `(tenant, app_name)`
  on receipt. Per-deployment purge is a follow-up.
