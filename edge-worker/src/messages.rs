//! NATS message types for worker ↔ control plane communication.

use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;

/// Deserializes `allowlist` so that absent, `[]`, and `["*"]` all map to
/// `None` (allow-all). Only a non-empty array without a bare `"*"` sentinel
/// becomes `Some(list)`.
///
/// The `["*"]` case handles legacy DB rows written before this PR where
/// `allowlisted_destinations = '{*}'` was the conventional "no restriction"
/// value. `EgressPolicy::new(["*"])` strips the sentinel and produces deny-all,
/// which is the wrong semantic; mapping it to `None` → `allow_all()` is correct.
fn deserialize_allowlist<'de, D>(de: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    let v: Option<Vec<String>> = Option::deserialize(de)?;
    Ok(v.filter(|list| !list.is_empty() && !list.iter().all(|e| e == "*")))
}

/// Default protocol for `AppStatus.protocol` (issue #548) — HTTP/WS
/// long-running or FaaS apps. Matches the historical implicit
/// behaviour pre-#548 so old control planes / ingresses treat missing
/// fields as HTTP.
fn default_protocol() -> String {
    "http".to_string()
}

/// Skip-serializing predicate for `AppStatus.protocol`. We omit the
/// field when it equals the default ("http") to keep the on-wire
/// shape byte-identical for the 99% case (HTTP apps), so old
/// ingresses don't see a new field they don't know how to render.
fn is_default_protocol(s: &String) -> bool {
    s == "http"
}

/// Deserializes `socket_mode` from the wire string into a
/// `SocketEgressPolicy`. The wire vocabulary is the same one
/// `EDGE_EGRESS_SOCKET_MODE` already speaks (see
/// `edge-runtime/src/socket_egress.rs::SocketEgressPolicy::from_str`):
/// `"block-all"`, `"allowlist"`, `"allow-all"`, `"hostname-pinned`".
///
/// Absent / null / `""` / unknown value → `None`. The permissive shape is
/// the rolling-upgrade contract: a future control plane emitting a
/// `socket_mode` value this worker doesn't know (e.g. a variant added in
/// a later release) must not break message decoding — the field falls
/// back to `None` and the per-app selector falls back to the worker-wide
/// `Config::socket_mode`. See issue #412.
fn deserialize_socket_mode<'de, D>(
    de: D,
) -> Result<Option<edge_runtime::socket_egress::SocketEgressPolicy>, D::Error>
where
    D: Deserializer<'de>,
{
    use edge_runtime::socket_egress::SocketEgressPolicy;
    let v: Option<String> = Option::deserialize(de)?;
    Ok(v.and_then(|s| {
        if s.is_empty() {
            None
        } else {
            s.parse::<SocketEgressPolicy>().ok()
        }
    }))
}

/// TaskMessage: received via NATS on `edgecloud.tasks.<region>`.
///
/// Three variants:
///
/// * `task_update` — published when an app set changes (activate / rollback /
///   env edit). Workers diff against current state.
/// * `full_sync` — published periodically (every `RECONCILE_INTERVAL`,
///   default 5 min) and on worker registration, listing the **complete**
///   active app set for the tenant. Workers treat the message as
///   authoritative: stop any app not in the set, start any missing, restart
///   any whose `deployment_id` doesn't match. Closes the gap when a NATS
///   `task_update` is lost (workqueue `WorkQueuePolicy` has no replay
///   support — see issue #53).
/// * `task_purge` — issued by the control plane when a tenant's data
///   lifecycle ends (issue #569). The carrier is an explicit tombstone,
///   NOT the absence of an app from a `task_update`/`full_sync` —
///   stop / crash / rebalance must never delete per-tenant persistent
///   state. The worker drains the in-flight apps for the tenant first,
///   then calls `edge_runtime::runtime::purge_tenant` to remove the
///   KV / cache / scheduler dirs and in-memory registry entries.
///
/// The first two variants share the same `apps` payload shape but carry
/// different semantics; the only difference between them is the
/// observability hook (`reconcile_full_sync_total` counter) so an operator
/// can distinguish a scheduled sync from an event-driven update in metrics.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type")]
pub enum TaskMessage {
    #[serde(rename = "task_update")]
    TaskUpdate {
        #[allow(dead_code)]
        timestamp: String,
        tenant_id: String,
        apps: HashMap<String, AppSpec>,
    },
    #[serde(rename = "full_sync")]
    FullSync {
        #[allow(dead_code)]
        timestamp: String,
        tenant_id: String,
        apps: HashMap<String, AppSpec>,
    },
    #[serde(rename = "task_purge")]
    TaskPurge {
        #[allow(dead_code)]
        timestamp: String,
        tenant_id: String,
        /// Per-app purge when set; tenant-wide purge when empty (every
        /// app currently running for `tenant_id` is stopped before the
        /// per-tenant dirs are removed). Today the CP enqueues per-app
        /// rows (`AppService.Delete`) AND per-app rows for each app
        /// inside `TenantService.DeleteTenant`; the empty-string form
        /// is kept for forward-compat with a future "tenant-wide"
        /// single-publish optimization.
        #[serde(default)]
        #[allow(dead_code)] // consumed by Commit 3 handle_purge
        app_name: String,
        #[allow(dead_code)] // consumed by Commit 3 handle_purge
        reason: PurgeReason,
    },
}

/// Why a `task_purge` was issued (issue #569). The worker logs this for
/// audit but does NOT act on it — the stop-then-purge behavior is the same
/// regardless of which entry point fired. Defined here (rather than in
/// the edge-runtime crate) because the worker is the only consumer and
/// it lives in this crate.
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PurgeReason {
    /// `DELETE /api/v1/admin/apps/{appName}` — a single app was removed.
    AppDeleted,
    /// `DELETE /api/v1/admin/tenants/{id}` — the entire tenant was
    /// offboarded. The CP enqueues one `task_purge` per app, so the
    /// worker still sees the per-app shape; this reason just records
    /// the upstream cause.
    TenantOffboarded,
}

/// DeploymentRoute: a single destination in a weighted traffic split.
#[derive(Debug, Clone, Deserialize)]
pub struct DeploymentRoute {
    #[allow(dead_code)]
    pub deployment_id: String,
    /// SHA-256 of this deployment's wasm artifact. Each route carries its
    /// own hash — the top-level `AppSpec::deployment_hash` only describes
    /// the primary deployment, so without this field the worker would
    /// download the primary's binary for every canary route (and verify it
    /// against the wrong hash, failing for any deployment whose artifact
    /// differs from the primary's).
    #[allow(dead_code)]
    pub deployment_hash: String,
    /// Ed25519 signature over `(sha256(artifact) || deployment_id)` for
    /// this route, base64url no-pad (issue #307). Each route carries its
    /// own signature for the same reason `deployment_hash` is per-route:
    /// the worker downloads each route's artifact separately, and the
    /// signature must match that artifact's hash + that route's id.
    /// Absent (or `None`) for pre-PR2 control planes — workers running
    /// with `EDGE_REQUIRE_SIGNATURE=false` accept unsigned routes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[allow(dead_code)]
    pub deployment_signature: Option<String>,
    /// Logical key id (`"k1"`, `"k2"`, ...) identifying which key in the
    /// worker's keyring signed this route's signature (issue #307 PR1
    /// follow-up — multi-keyring). When absent (or empty string from a
    /// legacy CP), the worker uses its keyring's default key.
    /// `#[serde(default)]` keeps pre-keying messages parseable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[allow(dead_code)]
    pub signing_key_id: Option<String>,
    /// Reserved for canary weight propagation; the worker currently uses 100%
    /// for every route and applies the weight at the ingress layer. Held on the
    /// struct so the wire format stays in sync with `edge-ingress` and the
    /// Go control plane, both of which already read it.
    #[allow(dead_code)]
    pub weight: u8,
}

/// AppSpec: specification for a single deployed app.
#[derive(Debug, Clone, Deserialize)]
pub struct AppSpec {
    pub deployment_id: String,
    pub deployment_hash: String,
    /// Ed25519 signature over `(sha256(artifact) || deployment_id)` for
    /// the primary deployment, base64url no-pad (issue #307). The
    /// `Downloader` reconstructs the signed payload from
    /// `deployment_hash` (decoded from hex to 32 raw bytes) and
    /// `deployment_id`, then verifies the signature against the
    /// worker's configured public key before instantiating the wasm.
    ///
    /// `#[serde(default)]` is the critical bit: pre-PR2 control planes
    /// do NOT emit this field, and the deserializer would otherwise
    /// fail with "missing field `deployment_signature`" on every
    /// task message, which would brick every PR1-or-earlier
    /// deployment in the wild. With `default`, the field is `None`
    /// for legacy messages and the worker's `require_signature`
    /// config decides what to do (default: refuse to instantiate).
    #[serde(default)]
    pub deployment_signature: Option<String>,
    /// Logical key id (`"k1"`, `"k2"`, ...) used to sign `deployment_signature`.
    /// Follow-up-PR1 (multi-keyring). When `None` or `Some("")`, the worker
    /// resolves against its keyring's default key. `#[serde(default)]` keeps
    /// pre-keying messages parseable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signing_key_id: Option<String>,
    /// listed (not just the primary one) concurrently. None = legacy mode
    /// (single deployment_id only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[allow(dead_code)]
    pub routes: Option<Vec<DeploymentRoute>>,
    pub env: HashMap<String, String>,
    /// Per-deployment egress allowlist. `None` means allow-all outbound (field
    /// absent or `[]` on the wire — both safe defaults for pre-enforcement
    /// control planes). `Some(list)` means only the listed hosts are permitted.
    #[serde(default, deserialize_with = "deserialize_allowlist")]
    pub allowlist: Option<Vec<String>>,
    /// Per-deployment `wasip2` socket policy. Selects which arm of
    /// `SocketEgressPolicy` the per-app `RuntimeState::socket_mode`
    /// uses. `None` falls back to the worker-wide `Config::socket_mode`
    /// (the historical pre-#412 behavior). See issue #412.
    ///
    /// Note: `HostnamePinned` additionally requires the worker-wide
    /// `EDGE_EGRESS_HOSTNAME_PINNING=true` (see `dispatch.rs::handle_request`).
    /// That compose rule is enforced at the FaaS dispatch site, not here.
    #[serde(default, deserialize_with = "deserialize_socket_mode")]
    pub socket_mode: Option<edge_runtime::socket_egress::SocketEgressPolicy>,
    pub max_memory_mb: u64,
    #[serde(default)]
    pub cpu_budget_ms: Option<u64>,
    /// Hex preview-id stamped by the control plane when this deploy was
    /// uploaded as a preview (issue #308). The supervisor forwards it to
    /// `edge_runtime::RuntimeState::with_env_and_meter_preview` so the
    /// per-tenant persistent stores (KV / cache / scheduler) get a
    /// `/preview-{id}/` subdirectory — preventing two concurrent previews
    /// of the same app from trampling each other's keys.
    ///
    /// `#[serde(default)]` keeps pre-#308 control-plane messages
    /// parseable; absent → `None` → production / non-preview behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview_id: Option<String>,
    /// Integer GitHub PR number the composite action forwarded via
    /// `?preview-pr-number=`. The supervisor stamps
    /// `EDGE_PREVIEW_PR_NUMBER` into the guest env so the guest can
    /// render PR-aware UI. `None` → no env var is set (the guest's
    /// `process.get_environment` simply does not see the key, which is
    /// the same as the pre-#308 behavior).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview_pr_number: Option<u32>,
}

/// ClusterHeadroom carries capacity info for the autoscaler (issue #85).
///
/// Mirrors the Go `ClusterHeadroom` struct in
/// `edge-control-plane/internal/nats/publisher.go`. `AppSlots` is the
/// only field the autoscaler acts on — the number of free port slots
/// this worker can allocate (i.e., not in cooldown). `cpu_pct` and
/// `mem_pct` are observability-only for now.
///
/// `free_slots` is the same value as `app_slots` duplicated under a
/// second name so the deploy-time 402 gate (issue #641) can read it
/// without reaching for the autoscaler's `app_slots` semantics. Both
/// fields stay in lockstep — pre-#641 control planes that only read
/// `app_slots` continue to work; new control planes preferring
/// `free_slots` read the same number. `#[serde(default)]` keeps
/// pre-#641 workers (no field) parseable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterHeadroom {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_pct: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mem_pct: Option<f64>,
    pub app_slots: u32,
    #[serde(default)]
    pub free_slots: u32,
}

/// HeartbeatMessage: published to `edgecloud.heartbeats.<region>` every 30s.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatMessage {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub timestamp: String,
    pub worker_id: String,
    pub region: String,
    /// Routable address the public ingress should use to reach this worker.
    /// Sourced from the `EDGE_WORKER_ADDR` env var. Optional on the wire so
    /// legacy workers (without the field) still parse; new workers must
    /// always set it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_addr: Option<String>,
    /// The tenant this worker belongs to. Added so the control plane can
    /// auto-register the worker from a heartbeat when the FK constraint on
    /// worker_status trips (fixes issue #297). Optional for backward compat
    /// with old workers; new workers always set it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    pub apps: HashMap<String, AppStatus>,
    /// Capacity headroom for the cluster autoscaler. `None` on pre-#85
    /// workers so old control planes and the autoscaler handle legacy
    /// workers via their fallback (50 assumed slots).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_headroom: Option<ClusterHeadroom>,
    /// Cumulative worker-level `PortPool::acquire() → None` events
    /// since process boot (issue #641). Surfaces apps that exhaust the
    /// pool on first start and never produce an AppStatus row — the
    /// case where a per-app counter would be invisible. Persisted by
    /// the CP's heartbeat-ingest path into
    /// `worker_status.port_pool_exhausted_count`; the deploy-time 402
    /// gate (`SumFreeSlotsByRegion`) reads it to detect fleet-wide
    /// saturation. Reset on worker process restart.
    /// `#[serde(default)]` keeps pre-#641 workers parseable.
    #[serde(default)]
    pub port_pool_exhausted_count: u64,
}

/// AppStatus: status of a single app within a heartbeat.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppStatus {
    pub deployment_id: String,
    pub status: String, // "running" | "starting" | "stopping" | "crashed"
    pub exit_code: Option<i32>,
    /// Number of HTTP requests handled since last heartbeat.
    pub request_count: u64,
    /// Total outbound bytes since the last heartbeat interval.
    /// Covers http-client response bodies received by the guest and
    /// http-server response bodies written back to callers.
    /// Defaults to 0 when absent (old workers) — control plane must
    /// treat a missing field as "no data", not as "zero usage".
    #[serde(default)]
    pub outbound_bytes: u64,
    /// Tenant the app belongs to. Used by the public ingress to render the
    /// host (`<tenant_id>-<app_name>.edgecloud.dev` — see
    /// `edge-ingress::config::ingress_host` and
    /// `edge-control-plane/internal/domain.IngressHost`; the suffix lives
    /// in `edge-ingress::config::INGRESS_HOST_SUFFIX`).
    pub tenant_id: String,
    /// Wire protocol for this app (issue #548). Either `"http"` (the
    /// default — HTTP/WS long-running or FaaS apps, served via the
    /// existing Caddy `reverse_proxy`) or `"tcp"` (raw-TCP long-running
    /// apps served via the L4 ingress port mapping). The default is
    /// omitted from the JSON so the wire shape stays byte-identical
    /// for HTTP apps — old workers / ingresses continue to interoperate.
    /// Old workers that don't send the field deserialize to "http".
    #[serde(
        default = "default_protocol",
        skip_serializing_if = "is_default_protocol"
    )]
    pub protocol: String,
    /// Port the app's HTTP server is listening on, on the worker host.
    /// Used by the public ingress to dial the upstream.
    pub port: u16,
    /// Guest-emitted metrics from `edge:observe` (counters, gauges,
    /// histogram samples) since the last heartbeat. Defaults to empty when
    /// absent so old control planes that don't parse this field keep
    /// working — new control planes ingest the samples and serve them via
    /// the Prometheus endpoints.
    #[serde(default)]
    pub observer_metrics: Vec<MetricSample>,
    /// If present, the port the guest is listening on for WebSocket
    /// upgrade traffic (assigned via `EDGE_WS_PORT` env). The ingress
    /// routes WebSocket connections to this port instead of `port`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ws_port: Option<u16>,
    /// Idempotency token for metering deduplication (issue #418). Stable
    /// across redeliveries within the same `(worker_id, deployment_id,
    /// 30s_bucket)` tuple, rotates per heartbeat interval. The control
    /// plane uses this to skip re-applying the same delta on JetStream
    /// redelivery or reconcile replay. Absent on pre-#418 workers —
    /// legacy control planes ignore it; new control planes treat absence
    /// as "no dedupe" (legacy behaviour preserved).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dedupe_id: Option<String>,
    /// Last error message from a crash / panic-in-spawn / trap
    /// (issue #45). Operators see this on the heartbeat so they can
    /// diagnose *why* the app reached `status: "crashed"` without
    /// grepping the worker's structured logs. `None` for healthy
    /// apps. The control plane surfaces this in the app's status
    /// response and in audit logs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Total resident seconds since the last heartbeat interval (issue
    /// #484, third metered dimension). `None` for Handler (FaaS) apps
    /// that do not contribute (the per-app resident ticker is
    /// LongRunning-only). `Some(0)` for a LongRunning app that started
    /// within the current interval — distinct from None because
    /// `applyTenantDelta` would otherwise fold both to delta=0 and the
    /// control plane would not be able to distinguish "just-started LR"
    /// from "FaaS that doesn't contribute". Absent on pre-#484 workers
    /// (legacy control planes treat absence as no contribution, same as
    /// a Handler app).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resident_seconds: Option<u64>,
    /// Total elapsed wall-clock milliseconds across all FaaS requests
    /// served by this Handler app since the last heartbeat interval
    /// (issue #555, fourth metered dimension). The dispatch path
    /// captures `Instant::now()` after the body-cap 413 early return
    /// and stamps `meter.record_duration(elapsed)` in each of the four
    /// terminal arms of `handle_request`'s `receiver.await` match
    /// (`Ok(Ok)`, `Ok(Err)`, `Err(_dropped)` with `exit_code != 0`,
    /// `Err(_dropped)` with `exit_code == 0`). LongRunning apps leave
    /// this at 0 (the dispatch path never stamps for LR). Defaults to
    /// 0 when absent (legacy workers); legacy control planes ignore
    /// the field, new control planes apply zero contribution via
    /// `checkComputeMs`.
    #[serde(default)]
    pub duration_ms_total: u64,
}

/// Kind of metric emitted via `edge:observe`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MetricKind {
    Counter,
    Gauge,
    HistogramSample,
}

/// A single metric sample shipped inside a heartbeat.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSample {
    pub name: String,
    pub kind: MetricKind,
    /// For counters: the counter value. For gauges: the gauge value.
    /// For histogram samples: the observed sample value.
    pub value: f64,
    pub labels: Vec<(String, String)>,
}

impl HeartbeatMessage {
    /// Create a new heartbeat with the current timestamp.
    pub fn new(worker_id: String, region: String, worker_addr: String, tenant_id: String) -> Self {
        Self {
            msg_type: "heartbeat".to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            worker_id,
            region,
            worker_addr: Some(worker_addr),
            tenant_id: Some(tenant_id),
            apps: HashMap::new(),
            cluster_headroom: None,
            port_pool_exhausted_count: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Lock the wire shape: `HeartbeatMessage::new` must serialize with a
    /// top-level `"worker_addr"` key whose value is the configured address.
    /// The public ingress (`edge-ingress/src/heartbeats.rs`) reads this field
    /// via `hb.worker_addr.as_deref().unwrap_or("")` — if the serde rename
    /// ever drops the field, the ingress silently degrades (no alarm until
    /// traffic stops flowing). This test fails fast on that regression.
    #[test]
    fn heartbeat_wire_format_includes_worker_addr() {
        let hb = HeartbeatMessage::new(
            "w_fra_abc".to_string(),
            "fra".to_string(),
            "203.0.113.10".to_string(),
            "t_test".to_string(),
        );
        let json = serde_json::to_string(&hb).expect("serialize heartbeat");
        assert!(
            json.contains(r#""worker_addr":"203.0.113.10""#),
            "heartbeat wire must include worker_addr; got: {json}"
        );
    }

    /// Empty-string `worker_addr` must serialize as an empty value, NOT be
    /// omitted. The ingress uses field-presence (vs field-emptiness) to
    /// distinguish "field absent" (legacy worker) from "field present but
    /// empty" (misconfigured worker). `skip_serializing_if = "Option::is_none"`
    /// fires only on `None`; `Some("")` round-trips as `""`.
    #[test]
    fn heartbeat_wire_format_preserves_empty_addr_as_empty_string() {
        let hb = HeartbeatMessage::new(
            "w_fra_abc".to_string(),
            "fra".to_string(),
            String::new(),
            "t_test".to_string(),
        );
        let json = serde_json::to_string(&hb).expect("serialize heartbeat");
        assert!(
            json.contains(r#""worker_addr":"""#),
            "empty worker_addr must appear as an empty string, not be omitted; got: {json}"
        );
    }

    /// `None` worker_addr must be omitted from the wire (legacy compat).
    /// The ingress checks `hb.worker_addr.as_deref().unwrap_or("")` and skips
    /// routing if the result is empty; an absent field should reach the same
    /// "skip" branch without ambiguity.
    #[test]
    fn heartbeat_wire_format_omits_worker_addr_when_none() {
        let hb = HeartbeatMessage {
            msg_type: "heartbeat".to_string(),
            timestamp: "2026-06-19T00:00:00Z".to_string(),
            worker_id: "w_fra_abc".to_string(),
            region: "fra".to_string(),
            worker_addr: None,
            tenant_id: None,
            apps: HashMap::new(),
            cluster_headroom: None,
            port_pool_exhausted_count: 0,
        };
        let json = serde_json::to_string(&hb).expect("serialize heartbeat");
        assert!(
            !json.contains("worker_addr"),
            "None worker_addr must be omitted from the wire; got: {json}"
        );
    }

    // ── ClusterHeadroom.free_slots rolling-upgrade contract (issue #641) ──

    /// Old workers that don't send `free_slots` on the heartbeat's
    /// `cluster_headroom` must deserialize to 0 (not fail). The
    /// deploy-time 402 gate (`SumFreeSlotsByRegion`, Commit #3) reads
    /// `free_slots` from the CP's persisted `worker_status` row; a
    /// legacy worker (no field on the wire) appears as `free_slots=0`
    /// and is treated as saturated for safety.
    #[test]
    fn cluster_headroom_free_slots_absent_deserializes_to_zero() {
        let hb: HeartbeatMessage = serde_json::from_str(
            r#"{
                "type":"heartbeat",
                "timestamp":"2026-07-12T00:00:00Z",
                "worker_id":"w_fra",
                "region":"fra",
                "apps":{},
                "cluster_headroom":{"cpu_pct":null,"mem_pct":null,"app_slots":42}
            }"#,
        )
        .expect("deserialize heartbeat");
        let ch = hb.cluster_headroom.expect("cluster_headroom present");
        assert_eq!(ch.free_slots, 0, "absent free_slots → 0 default");
        assert_eq!(ch.app_slots, 42, "legacy app_slots preserved");
    }

    /// Explicit value round-trips correctly — `free_slots=7` survives
    /// a serialize+deserialize cycle and the legacy `app_slots` field
    /// continues to read 42 (both are stamped by the worker).
    #[test]
    fn cluster_headroom_free_slots_present_round_trips() {
        let hb: HeartbeatMessage = serde_json::from_str(
            r#"{
                "type":"heartbeat",
                "timestamp":"2026-07-12T00:00:00Z",
                "worker_id":"w_fra",
                "region":"fra",
                "apps":{},
                "cluster_headroom":{"cpu_pct":null,"mem_pct":null,"app_slots":42,"free_slots":7}
            }"#,
        )
        .expect("deserialize heartbeat");
        let ch = hb.cluster_headroom.expect("cluster_headroom present");
        assert_eq!(ch.free_slots, 7);
        assert_eq!(ch.app_slots, 42);
    }

    // ── HeartbeatMessage.port_pool_exhausted_count rolling-upgrade (issue #641) ──

    /// Old workers (pre-#641) don't send `port_pool_exhausted_count`
    /// on the heartbeat; the field must default to 0 so legacy workers
    /// continue to parse. The CP's deploy-time 402 gate sees `0` and
    /// treats the worker as having no recent exhaustion pressure.
    #[test]
    fn port_pool_exhausted_count_absent_deserializes_to_zero() {
        let hb: HeartbeatMessage = serde_json::from_str(
            r#"{
                "type":"heartbeat",
                "timestamp":"2026-07-12T00:00:00Z",
                "worker_id":"w_fra",
                "region":"fra",
                "apps":{}
            }"#,
        )
        .expect("deserialize legacy heartbeat");
        assert_eq!(
            hb.port_pool_exhausted_count, 0,
            "absent port_pool_exhausted_count must default to 0"
        );
    }

    /// Explicit value round-trips correctly — 7 survives a
    /// serialize+deserialize cycle.
    #[test]
    fn port_pool_exhausted_count_present_round_trips() {
        let hb: HeartbeatMessage = serde_json::from_str(
            r#"{
                "type":"heartbeat",
                "timestamp":"2026-07-12T00:00:00Z",
                "worker_id":"w_fra",
                "region":"fra",
                "apps":{},
                "port_pool_exhausted_count":7
            }"#,
        )
        .expect("deserialize heartbeat");
        assert_eq!(hb.port_pool_exhausted_count, 7);
    }

    /// `HeartbeatMessage::new` must initialise the field to 0 so a
    /// freshly-built heartbeat (before `build_heartbeat` runs the
    /// atomic load) doesn't accidentally report phantom exhaustion
    /// to the CP.
    #[test]
    fn heartbeat_new_initializes_port_pool_exhausted_count_to_zero() {
        let hb = HeartbeatMessage::new(
            "w_fra".to_string(),
            "fra".to_string(),
            "203.0.113.10".to_string(),
            "t_test".to_string(),
        );
        assert_eq!(
            hb.port_pool_exhausted_count, 0,
            "HeartbeatMessage::new must initialise the worker-level counter to 0"
        );
    }

    /// Round-trip: a heartbeat serialized to JSON and deserialized back must
    /// yield the same field values. Catches accidental field renames or
    /// serde attribute changes that would only manifest on the receive side.
    #[test]
    fn heartbeat_round_trip_preserves_worker_addr() {
        let hb = HeartbeatMessage::new(
            "w_fra_abc".to_string(),
            "fra".to_string(),
            "203.0.113.10".to_string(),
            "t_test".to_string(),
        );
        let json = serde_json::to_string(&hb).expect("serialize");
        let parsed: HeartbeatMessage =
            serde_json::from_str(&json).expect("deserialize same wire shape");
        assert_eq!(parsed.worker_addr.as_deref(), Some("203.0.113.10"));
        assert_eq!(parsed.worker_id, "w_fra_abc");
        assert_eq!(parsed.region, "fra");
    }

    // ── outbound_bytes rolling-upgrade contract ───────────────────────────

    fn app_status_from_json(json: &str) -> AppStatus {
        serde_json::from_str(json).expect("deserialize AppStatus")
    }

    /// Old workers that don't send `observer_metrics` must deserialize to
    /// an empty Vec, not fail. Old control planes ignore the field.
    #[test]
    fn observer_metrics_absent_deserializes_to_empty() {
        let s = app_status_from_json(
            r#"{"deployment_id":"d_1","status":"running","exit_code":null,"request_count":5,"tenant_id":"t_1","port":8080}"#,
        );
        assert!(s.observer_metrics.is_empty());
    }

    /// `observer_metrics` round-trips correctly with all three metric kinds.
    #[test]
    fn observer_metrics_round_trips() {
        let s = AppStatus {
            deployment_id: "d_1".into(),
            status: "running".into(),
            exit_code: None,
            request_count: 1,
            outbound_bytes: 0,
            tenant_id: "t_1".into(),
            port: 8080,
            ws_port: None,
            dedupe_id: None,
            protocol: "http".to_string(),
            resident_seconds: None,
            duration_ms_total: 0,
            observer_metrics: vec![
                MetricSample {
                    name: "hits".into(),
                    kind: MetricKind::Counter,
                    value: 42.0,
                    labels: vec![("route".into(), "/api".into())],
                },
                MetricSample {
                    name: "mem".into(),
                    kind: MetricKind::Gauge,
                    value: 512.0,
                    labels: vec![],
                },
                MetricSample {
                    name: "latency_ms".into(),
                    kind: MetricKind::HistogramSample,
                    value: 12.5,
                    labels: vec![],
                },
            ],
            last_error: None,
        };
        let json = serde_json::to_string(&s).expect("serialize");
        let parsed: AppStatus = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.observer_metrics.len(), 3);
        assert_eq!(parsed.observer_metrics[0].name, "hits");
        assert_eq!(parsed.observer_metrics[0].kind, MetricKind::Counter);
        assert_eq!(parsed.observer_metrics[0].value, 42.0);
        assert_eq!(parsed.observer_metrics[1].kind, MetricKind::Gauge);
        assert_eq!(parsed.observer_metrics[2].kind, MetricKind::HistogramSample);
    }

    /// Old workers that don't send `outbound_bytes` must deserialize to 0,
    /// not fail. The control plane treats 0 as "no data for this interval".
    #[test]
    fn outbound_bytes_absent_deserializes_to_zero() {
        let s = app_status_from_json(
            r#"{"deployment_id":"d_1","status":"running","exit_code":null,"request_count":5,"tenant_id":"t_1","port":8080}"#,
        );
        assert_eq!(s.outbound_bytes, 0);
    }

    /// Explicit value round-trips correctly.
    #[test]
    fn outbound_bytes_present_round_trips() {
        let s = app_status_from_json(
            r#"{"deployment_id":"d_1","status":"running","exit_code":null,"request_count":3,"outbound_bytes":1048576,"tenant_id":"t_1","port":8080}"#,
        );
        assert_eq!(s.outbound_bytes, 1_048_576);
    }

    /// Serialization always includes the field (no skip_serializing_if), so
    /// new workers talking to new control planes always send the byte count.
    #[test]
    fn outbound_bytes_always_serialized() {
        let s = AppStatus {
            deployment_id: "d_1".into(),
            status: "running".into(),
            exit_code: None,
            request_count: 2,
            outbound_bytes: 512,
            tenant_id: "t_1".into(),
            port: 8080,
            ws_port: None,
            dedupe_id: None,
            protocol: "http".to_string(),
            resident_seconds: None,
            duration_ms_total: 0,
            observer_metrics: vec![],
            last_error: None,
        };
        let json = serde_json::to_string(&s).expect("serialize");
        assert!(
            json.contains(r#""outbound_bytes":512"#),
            "outbound_bytes must always appear in serialized AppStatus; got: {json}"
        );
    }

    // ── last_error rolling-upgrade contract (issue #45) ───────────────

    /// `last_error` is stamped onto the heartbeat so operators can
    /// diagnose a `status: "crashed"` app without grepping the
    /// worker's structured logs (issue #45). When the field is
    /// `None` it must NOT appear on the wire (skip_serializing_if)
    /// so old control planes don't see an unexpected `last_error`
    /// field; when it is `Some`, it must round-trip exactly.
    #[test]
    fn last_error_round_trips_and_skips_when_none() {
        // None: must be absent from the JSON.
        let none_status = AppStatus {
            deployment_id: "d_1".into(),
            status: "running".into(),
            exit_code: None,
            request_count: 0,
            outbound_bytes: 0,
            tenant_id: "t_1".into(),
            port: 8080,
            ws_port: None,
            dedupe_id: None,
            observer_metrics: vec![],
            last_error: None,
            protocol: "http".to_string(),
            resident_seconds: None,
            duration_ms_total: 0,
        };
        let json_none = serde_json::to_string(&none_status).expect("serialize");
        assert!(
            !json_none.contains("last_error"),
            "last_error must be absent when None (skip_serializing_if); got: {json_none}"
        );

        // Some: round-trip preserves the panic payload verbatim.
        let some_status = AppStatus {
            last_error: Some("app task panicked: synthetic".into()),
            ..none_status.clone()
        };
        let json_some = serde_json::to_string(&some_status).expect("serialize");
        assert!(
            json_some.contains(r#""last_error":"app task panicked: synthetic""#),
            "last_error must serialize verbatim; got: {json_some}"
        );
        let parsed: AppStatus = serde_json::from_str(&json_some).expect("deserialize");
        assert_eq!(
            parsed.last_error.as_deref(),
            Some("app task panicked: synthetic")
        );

        // Old workers that don't send the field must deserialize to None.
        let parsed_old: AppStatus = serde_json::from_str(
            r#"{"deployment_id":"d_1","status":"running","exit_code":null,"request_count":0,"tenant_id":"t_1","port":8080}"#,
        )
        .expect("deserialize legacy heartbeat");
        assert!(
            parsed_old.last_error.is_none(),
            "old workers without last_error must deserialize to None (serde default)"
        );
    }

    // ── protocol rolling-upgrade contract (issue #548) ─────────────────

    /// `protocol` (issue #548) discriminates HTTP apps from raw-TCP
    /// L4 apps. The on-wire contract:
    ///   * "http" (default) MUST be absent from the JSON
    ///     (`skip_serializing_if`) so old ingresses / control planes
    ///     don't see a new field they don't know how to render.
    ///   * "tcp" MUST round-trip verbatim when present.
    ///   * Old workers that don't send the field MUST deserialize to
    ///     "http" (`#[serde(default = "default_protocol")]`) so the
    ///     wire shape stays backward-compatible across the rolling
    ///     upgrade window.
    #[test]
    fn protocol_round_trips_and_skips_when_default() {
        // HTTP (default): must be absent from the JSON.
        let none_status = AppStatus {
            deployment_id: "d_1".into(),
            status: "running".into(),
            exit_code: None,
            request_count: 0,
            outbound_bytes: 0,
            tenant_id: "t_1".into(),
            port: 8080,
            ws_port: None,
            dedupe_id: None,
            observer_metrics: vec![],
            last_error: None,
            protocol: "http".to_string(),
            resident_seconds: None,
            duration_ms_total: 0,
        };
        let json_http = serde_json::to_string(&none_status).expect("serialize");
        assert!(
            !json_http.contains("protocol"),
            "protocol must be absent when \"http\" (skip_serializing_if); got: {json_http}"
        );

        // TCP: round-trip preserves the value verbatim.
        let tcp_status = AppStatus {
            protocol: "tcp".to_string(),
            ..none_status.clone()
        };
        let json_tcp = serde_json::to_string(&tcp_status).expect("serialize");
        assert!(
            json_tcp.contains(r#""protocol":"tcp""#),
            "protocol must serialize verbatim when \"tcp\"; got: {json_tcp}"
        );
        let parsed: AppStatus = serde_json::from_str(&json_tcp).expect("deserialize");
        assert_eq!(parsed.protocol, "tcp", "tcp must round-trip verbatim");

        // HTTP serialized explicitly (not skipped): still round-trips to "http".
        let http_explicit = serde_json::to_string(&AppStatus {
            protocol: "http".to_string(),
            ..none_status.clone()
        })
        .expect("serialize");
        // Note: "http" IS skipped on serialization, so this branch
        // would normally not be exercised — but if a future change
        // drops the predicate, the deserialize-default still keeps
        // us safe. Verify by manually crafting the JSON.
        let forced_http = r#"{"deployment_id":"d_1","status":"running","exit_code":null,"request_count":0,"outbound_bytes":0,"tenant_id":"t_1","port":8080,"protocol":"http"}"#;
        let parsed_forced: AppStatus = serde_json::from_str(forced_http).expect("deserialize");
        assert_eq!(
            parsed_forced.protocol, "http",
            "explicit \"http\" must deserialize to \"http\""
        );
        // Sanity: don't accidentally serialize "http" — the
        // serde default + skip predicate must stay in lock-step.
        assert!(
            !http_explicit.contains("protocol"),
            "explicit-http serialization must still skip the field"
        );

        // Old workers that don't send `protocol` MUST deserialize to "http".
        let parsed_old: AppStatus = serde_json::from_str(
            r#"{"deployment_id":"d_1","status":"running","exit_code":null,"request_count":0,"tenant_id":"t_1","port":8080}"#,
        )
        .expect("deserialize legacy heartbeat");
        assert_eq!(
            parsed_old.protocol, "http",
            "old workers without protocol must deserialize to \"http\" (serde default)"
        );
    }

    /// Sanity check: an AppStatus carrying `protocol = "tcp"` round-trips
    /// through the full HeartbeatMessage envelope without losing the field
    /// or emitting an `"http"` default that would break the L4 routing
    /// branch in the ingress.
    #[test]
    fn protocol_tcp_survives_heartbeat_envelope() {
        let mut apps = HashMap::new();
        apps.insert(
            "my-tcp-app".to_string(),
            AppStatus {
                deployment_id: "d_tcp_1".into(),
                status: "running".into(),
                exit_code: None,
                request_count: 0,
                outbound_bytes: 0,
                tenant_id: "t_acme".into(),
                port: 8081,
                ws_port: None,
                dedupe_id: None,
                observer_metrics: vec![],
                last_error: None,
                protocol: "tcp".to_string(),
                resident_seconds: None,
                duration_ms_total: 0,
            },
        );
        let hb = HeartbeatMessage {
            msg_type: "heartbeat".to_string(),
            timestamp: "0".to_string(),
            worker_id: "w_fra_1".to_string(),
            region: "fra".to_string(),
            worker_addr: Some("10.0.0.1".to_string()),
            tenant_id: Some("t_acme".to_string()),
            apps,
            cluster_headroom: None,
        };
        let json = serde_json::to_string(&hb).expect("serialize heartbeat");
        assert!(
            json.contains(r#""protocol":"tcp""#),
            "tcp protocol must survive HeartbeatMessage serialization; got: {json}"
        );
        let parsed: HeartbeatMessage = serde_json::from_str(&json).expect("deserialize heartbeat");
        assert_eq!(parsed.apps["my-tcp-app"].protocol, "tcp");
    }

    // ── resident_seconds rolling-upgrade contract (issue #484) ────────────

    /// Old workers that don't send `resident_seconds` must deserialize
    /// to None (not fail). The control plane treats None as "no
    /// contribution" — the same way it treats Handler (FaaS) apps.
    #[test]
    fn resident_seconds_absent_deserializes_to_none() {
        let s = app_status_from_json(
            r#"{"deployment_id":"d_1","status":"running","exit_code":null,"request_count":5,"tenant_id":"t_1","port":8080}"#,
        );
        assert_eq!(
            s.resident_seconds, None,
            "absent resident_seconds must deserialize to None (FaaS-like)"
        );
    }

    /// Explicit value round-trips correctly — Some(N) → Some(N).
    #[test]
    fn resident_seconds_present_round_trips() {
        let s = app_status_from_json(
            r#"{"deployment_id":"d_1","status":"running","exit_code":null,"request_count":3,"outbound_bytes":0,"tenant_id":"t_1","port":8080,"resident_seconds":90}"#,
        );
        assert_eq!(s.resident_seconds, Some(90));
    }

    /// `skip_serializing_if = "Option::is_none"` drops the field when
    /// None — old control planes that don't parse `resident_seconds`
    /// must keep working. The serialized JSON must NOT contain
    /// "resident_seconds" when None.
    #[test]
    fn resident_seconds_skipped_when_none() {
        let s = AppStatus {
            deployment_id: "d_1".into(),
            status: "running".into(),
            exit_code: None,
            request_count: 0,
            outbound_bytes: 0,
            tenant_id: "t_1".into(),
            port: 8080,
            ws_port: None,
            dedupe_id: None,
            protocol: "http".to_string(),
            last_error: None,
            resident_seconds: None,
            duration_ms_total: 0,
            observer_metrics: vec![],
        };
        let json = serde_json::to_string(&s).expect("serialize");
        assert!(
            !json.contains("resident_seconds"),
            "resident_seconds must be skipped from serialized AppStatus when None; got: {json}"
        );
    }

    /// `Some(0)` is distinct from `None`: a just-started LongRunning
    /// app has accumulated 0 resident seconds, while a Handler (FaaS)
    /// app has no concept of resident time at all. Both fold to
    /// delta=0 in applyTenantDelta, but the wire shape preserves the
    /// distinction for future debugging and for the worker's own
    /// reasoning about whether it ever spawned the resident ticker.
    #[test]
    fn resident_seconds_zero_serializes() {
        let s = AppStatus {
            deployment_id: "d_1".into(),
            status: "starting".into(),
            exit_code: None,
            request_count: 0,
            outbound_bytes: 0,
            tenant_id: "t_1".into(),
            port: 8080,
            ws_port: None,
            dedupe_id: None,
            protocol: "http".to_string(),
            last_error: None,
            resident_seconds: Some(0),
            duration_ms_total: 0,
            observer_metrics: vec![],
        };
        let json = serde_json::to_string(&s).expect("serialize");
        assert!(
            json.contains(r#""resident_seconds":0"#),
            "Some(0) must serialize as 0 (NOT skipped); got: {json}"
        );
        let parsed: AppStatus = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.resident_seconds, Some(0));
    }

    // ── duration_ms_total rolling-upgrade contract (issue #555) ──────────

    /// Old workers that don't send `duration_ms_total` must deserialize
    /// to 0 (not fail). The control plane treats 0 as "no FaaS
    /// duration this interval" — same way it treats `outbound_bytes`
    /// absent. Mirrors `outbound_bytes_absent_deserializes_to_zero`
    /// (issue #210).
    #[test]
    fn duration_ms_total_absent_deserializes_to_zero() {
        let s = app_status_from_json(
            r#"{"deployment_id":"d_1","status":"running","exit_code":null,"request_count":5,"tenant_id":"t_1","port":8080}"#,
        );
        assert_eq!(s.duration_ms_total, 0);
    }

    /// Explicit value round-trips correctly. Mirrors
    /// `outbound_bytes_present_round_trips` (issue #210).
    #[test]
    fn duration_ms_total_present_round_trips() {
        let s = app_status_from_json(
            r#"{"deployment_id":"d_1","status":"running","exit_code":null,"request_count":3,"outbound_bytes":1048576,"duration_ms_total":250,"tenant_id":"t_1","port":8080}"#,
        );
        assert_eq!(s.duration_ms_total, 250);
    }

    /// Serialization always includes the field (no
    /// `skip_serializing_if`), so new workers talking to new control
    /// planes always send the duration_ms count. LongRunning apps
    /// stamp 0 (the dispatch path never fires for LR); control
    /// planes treat 0 as "no FaaS contribution this interval".
    #[test]
    fn duration_ms_total_always_serialized() {
        let s = AppStatus {
            deployment_id: "d_1".into(),
            status: "running".into(),
            exit_code: None,
            request_count: 2,
            outbound_bytes: 512,
            tenant_id: "t_1".into(),
            port: 8080,
            ws_port: None,
            dedupe_id: None,
            resident_seconds: None,
            duration_ms_total: 150,
            observer_metrics: vec![],
            protocol: "http".to_string(),
            last_error: None,
        };
        let json = serde_json::to_string(&s).expect("serialize");
        assert!(
            json.contains(r#""duration_ms_total":150"#),
            "duration_ms_total must always appear in serialized AppStatus; got: {json}"
        );
    }
    // ── deserialize_allowlist rolling-upgrade contract ────────────────────

    fn app_spec_from_json(json: &str) -> AppSpec {
        serde_json::from_str(json).expect("deserialize AppSpec")
    }

    /// Absent `allowlist` field → `None` (allow-all). Old control planes that
    /// don't send the field must not trigger deny-all on every app.
    #[test]
    fn allowlist_absent_deserializes_to_none() {
        let spec = app_spec_from_json(
            r#"{"deployment_id":"d_1","deployment_hash":"abc","env":{},"max_memory_mb":256}"#,
        );
        assert!(
            spec.allowlist.is_none(),
            "absent allowlist must deserialize to None (allow-all)"
        );
    }

    /// Explicit `[]` → `None` (allow-all). Old control planes that send an
    /// empty array as a Go zero-value must not trigger deny-all either.
    #[test]
    fn allowlist_empty_array_deserializes_to_none() {
        let spec = app_spec_from_json(
            r#"{"deployment_id":"d_1","deployment_hash":"abc","env":{},"max_memory_mb":256,"allowlist":[]}"#,
        );
        assert!(
            spec.allowlist.is_none(),
            "empty-array allowlist must deserialize to None (allow-all)"
        );
    }

    /// Legacy `["*"]` sentinel → `None` (allow-all). Pre-enforcement DB rows that
    /// stored `allowlisted_destinations = '{*}'` must not trigger deny-all after
    /// deployment; `EgressPolicy::new(["*"])` strips the sentinel and produces
    /// deny-all, so we intercept here and map to None → allow_all() instead.
    #[test]
    fn allowlist_star_sentinel_deserializes_to_none() {
        let spec = app_spec_from_json(
            r#"{"deployment_id":"d_1","deployment_hash":"abc","env":{},"max_memory_mb":256,"allowlist":["*"]}"#,
        );
        assert!(
            spec.allowlist.is_none(),
            "legacy [\"*\"] sentinel must deserialize to None (allow-all), not Some([\"*\"]) which becomes deny-all"
        );
    }

    /// Non-empty array → `Some(list)`. The worker must enforce this list.
    #[test]
    fn allowlist_non_empty_deserializes_to_some() {
        let spec = app_spec_from_json(
            r#"{"deployment_id":"d_1","deployment_hash":"abc","env":{},"max_memory_mb":256,"allowlist":["api.stripe.com","*.sendgrid.net"]}"#,
        );
        assert_eq!(
            spec.allowlist,
            Some(vec![
                "api.stripe.com".to_string(),
                "*.sendgrid.net".to_string()
            ])
        );
    }

    // ── deserialize_socket_mode rolling-upgrade contract (issue #412) ─────

    use edge_runtime::socket_egress::SocketEgressPolicy;

    /// Absent `socket_mode` field → `None`. Pre-#412 control planes that
    /// don't emit the field must continue to work; the per-app selector
    /// falls back to the worker-wide `Config::socket_mode`.
    #[test]
    fn socket_mode_absent_deserializes_to_none() {
        let spec = app_spec_from_json(
            r#"{"deployment_id":"d_1","deployment_hash":"abc","env":{},"max_memory_mb":256}"#,
        );
        assert!(
            spec.socket_mode.is_none(),
            "absent socket_mode must deserialize to None (fall back to worker-wide)"
        );
    }

    /// Explicit `null` → `None`. Same contract as absent.
    #[test]
    fn socket_mode_null_deserializes_to_none() {
        let spec = app_spec_from_json(
            r#"{"deployment_id":"d_1","deployment_hash":"abc","env":{},"max_memory_mb":256,"socket_mode":null}"#,
        );
        assert!(
            spec.socket_mode.is_none(),
            "null socket_mode must deserialize to None"
        );
    }

    /// Empty string → `None`. Some Go zero-value paths may produce `""`
    /// rather than `null`; treat them as "no override".
    #[test]
    fn socket_mode_empty_string_deserializes_to_none() {
        let spec = app_spec_from_json(
            r#"{"deployment_id":"d_1","deployment_hash":"abc","env":{},"max_memory_mb":256,"socket_mode":""}"#,
        );
        assert!(
            spec.socket_mode.is_none(),
            "empty-string socket_mode must deserialize to None"
        );
    }

    /// Each of the 4 known `SocketEgressPolicy` variants round-trips.
    #[test]
    fn socket_mode_known_variants_round_trip() {
        for (raw, expected) in [
            (r#""block-all""#, SocketEgressPolicy::BlockAll),
            (r#""allowlist""#, SocketEgressPolicy::AllowList),
            (r#""allow-all""#, SocketEgressPolicy::AllowAll),
            (r#""hostname-pinned""#, SocketEgressPolicy::HostnamePinned),
        ] {
            let json = format!(
                r#"{{"deployment_id":"d_1","deployment_hash":"abc","env":{{}},"max_memory_mb":256,"socket_mode":{raw}}}"#
            );
            let spec = app_spec_from_json(&json);
            assert_eq!(
                spec.socket_mode,
                Some(expected),
                "raw {raw} must round-trip to {expected:?}"
            );
        }
    }

    /// Unknown string → `None`. Rolling-upgrade contract: a future control
    /// plane emitting a value the worker doesn't recognise must not brick
    /// message decoding. The unknown value is dropped, the per-app
    /// selector falls back to worker-wide.
    #[test]
    fn socket_mode_unknown_string_deserializes_to_none() {
        let spec = app_spec_from_json(
            r#"{"deployment_id":"d_1","deployment_hash":"abc","env":{},"max_memory_mb":256,"socket_mode":"future-mode-v2"}"#,
        );
        assert!(
            spec.socket_mode.is_none(),
            "unknown socket_mode string must deserialize to None (rolling-upgrade safety)"
        );
    }

    // ── TaskMessage::FullSync wire format (issue #53) ─────────────────────

    /// `task_update` parses into the `TaskUpdate` variant (regression
    /// guard for the existing path; included here so a future variant
    /// rename doesn't silently break the old path).
    #[test]
    fn task_update_deserializes_to_task_update_variant() {
        let json = r#"{
            "type": "task_update",
            "timestamp": "2026-06-19T00:00:00Z",
            "tenant_id": "t_1",
            "apps": {
                "myapp": {
                    "deployment_id": "d_1",
                    "deployment_hash": "abc",
                    "env": {},
                    "max_memory_mb": 256
                }
            }
        }"#;
        let msg: TaskMessage = serde_json::from_str(json).expect("deserialize task_update");
        match msg {
            TaskMessage::TaskUpdate {
                tenant_id, apps, ..
            } => {
                assert_eq!(tenant_id, "t_1");
                assert_eq!(apps.len(), 1);
                assert!(apps.contains_key("myapp"));
            }
            TaskMessage::FullSync { .. } => panic!("task_update parsed as FullSync"),
            TaskMessage::TaskPurge { .. } => panic!("task_update parsed as TaskPurge"),
        }
    }

    /// `full_sync` parses into the `FullSync` variant with the same `apps`
    /// payload shape as `task_update`. Lock the wire so a future spec change
    /// on either side fails fast here.
    #[test]
    fn full_sync_deserializes_to_full_sync_variant() {
        let json = r#"{
            "type": "full_sync",
            "timestamp": "2026-06-19T00:00:00Z",
            "tenant_id": "t_1",
            "apps": {
                "myapp": {
                    "deployment_id": "d_1",
                    "deployment_hash": "abc",
                    "env": {"KEY": "value"},
                    "max_memory_mb": 256,
                    "allowlist": ["api.stripe.com"],
                    "cpu_budget_ms": 500
                },
                "other": {
                    "deployment_id": "d_2",
                    "deployment_hash": "def",
                    "env": {},
                    "max_memory_mb": 128
                }
            }
        }"#;
        let msg: TaskMessage = serde_json::from_str(json).expect("deserialize full_sync");
        let (tenant_id, apps) = match msg {
            TaskMessage::FullSync {
                tenant_id, apps, ..
            } => (tenant_id, apps),
            TaskMessage::TaskUpdate { .. } => panic!("full_sync parsed as TaskUpdate"),
            TaskMessage::TaskPurge { .. } => panic!("full_sync parsed as TaskPurge"),
        };
        assert_eq!(tenant_id, "t_1");
        assert_eq!(apps.len(), 2);
        assert_eq!(apps["myapp"].deployment_id, "d_1");
        assert_eq!(
            apps["myapp"].env.get("KEY").map(String::as_str),
            Some("value")
        );
        assert_eq!(
            apps["myapp"].allowlist,
            Some(vec!["api.stripe.com".to_string()])
        );
        assert_eq!(apps["myapp"].cpu_budget_ms, Some(500));
        assert_eq!(apps["other"].deployment_hash, "def");
        assert_eq!(apps["other"].max_memory_mb, 128);
        assert_eq!(apps["other"].cpu_budget_ms, None);
    }

    /// Unknown `type` values must fail to deserialize rather than silently
    /// fall through to a default variant. The CP and worker ship together,
    /// so this guards against a divergent deployment.
    #[test]
    fn unknown_type_field_fails_to_deserialize() {
        let json = r#"{
            "type": "bogus",
            "timestamp": "2026-06-19T00:00:00Z",
            "tenant_id": "t_1",
            "apps": {}
        }"#;
        let res: Result<TaskMessage, _> = serde_json::from_str(json);
        assert!(res.is_err(), "unknown type field must fail to parse");
    }

    // ── TaskMessage::TaskPurge wire format (issue #569) ────────────────────

    /// `task_purge` with `app_name` and `reason=app_deleted` parses into
    /// the `TaskPurge` variant. Locks the wire shape the CP's
    /// `AppService.Delete` enqueues.
    #[test]
    fn task_purge_deserializes_to_task_purge_variant() {
        let json = r#"{
            "type": "task_purge",
            "timestamp": "2026-07-10T00:00:00Z",
            "tenant_id": "t_1",
            "app_name": "myapp",
            "reason": "app_deleted"
        }"#;
        let msg: TaskMessage = serde_json::from_str(json).expect("deserialize task_purge");
        match msg {
            TaskMessage::TaskPurge {
                tenant_id,
                app_name,
                reason,
                ..
            } => {
                assert_eq!(tenant_id, "t_1");
                assert_eq!(app_name, "myapp");
                assert_eq!(reason, PurgeReason::AppDeleted);
            }
            TaskMessage::TaskUpdate { .. } | TaskMessage::FullSync { .. } => {
                panic!("task_purge parsed as TaskUpdate or FullSync")
            }
        }
    }

    /// `task_purge` with `reason=tenant_offboarded` parses. This is the
    /// per-app row the CP enqueues from `TenantService.DeleteTenant`
    /// (one row per app currently owned by the tenant).
    #[test]
    fn task_purge_tenant_offboarded_reason_round_trip() {
        let json = r#"{
            "type": "task_purge",
            "timestamp": "2026-07-10T00:00:00Z",
            "tenant_id": "t_off",
            "app_name": "demo",
            "reason": "tenant_offboarded"
        }"#;
        let msg: TaskMessage = serde_json::from_str(json).expect("deserialize task_purge");
        let (tenant_id, app_name, reason) = match msg {
            TaskMessage::TaskPurge {
                tenant_id,
                app_name,
                reason,
                ..
            } => (tenant_id, app_name, reason),
            TaskMessage::TaskUpdate { .. } | TaskMessage::FullSync { .. } => {
                panic!("task_purge parsed as TaskUpdate or FullSync")
            }
        };
        assert_eq!(tenant_id, "t_off");
        assert_eq!(app_name, "demo");
        assert_eq!(reason, PurgeReason::TenantOffboarded);
    }

    /// Empty `app_name` (tenant-wide purge) parses with `app_name=""`.
    /// Today the CP doesn't emit this shape — both call sites enqueue
    /// per-app rows — but the receiver must accept it for
    /// forward-compat with a future "single tenant-wide publish"
    /// optimization. `#[serde(default)]` lets the field be omitted
    /// entirely too.
    #[test]
    fn task_purge_empty_app_name_parses_as_tenant_wide() {
        let json = r#"{
            "type": "task_purge",
            "timestamp": "2026-07-10T00:00:00Z",
            "tenant_id": "t_wide",
            "reason": "app_deleted"
        }"#;
        let msg: TaskMessage =
            serde_json::from_str(json).expect("deserialize task_purge (no app_name)");
        match msg {
            TaskMessage::TaskPurge { app_name, .. } => {
                assert_eq!(app_name, "", "missing app_name must default to empty");
            }
            _ => panic!("task_purge parsed as wrong variant"),
        }

        let json_empty = r#"{
            "type": "task_purge",
            "timestamp": "2026-07-10T00:00:00Z",
            "tenant_id": "t_wide",
            "app_name": "",
            "reason": "tenant_offboarded"
        }"#;
        let msg_empty: TaskMessage =
            serde_json::from_str(json_empty).expect("deserialize task_purge (app_name='')");
        match msg_empty {
            TaskMessage::TaskPurge { app_name, .. } => {
                assert_eq!(app_name, "");
            }
            _ => panic!("task_purge parsed as wrong variant"),
        }
    }

    /// Unknown `reason` values fail to deserialize — keeps the
    /// `PurgeReason` enum closed at the wire boundary, mirroring the
    /// strict-unknown-tag invariant on `type`.
    #[test]
    fn task_purge_unknown_reason_fails_to_deserialize() {
        let json = r#"{
            "type": "task_purge",
            "timestamp": "2026-07-10T00:00:00Z",
            "tenant_id": "t_1",
            "app_name": "myapp",
            "reason": "scheduled_cleanup"
        }"#;
        let res: Result<TaskMessage, _> = serde_json::from_str(json);
        assert!(
            res.is_err(),
            "unknown purge reason must fail to parse (closed enum)"
        );
    }

    /// `task_update` still parses as `TaskUpdate` after adding the
    /// `TaskPurge` variant — regression guard so the new enum arm
    /// doesn't accidentally shadow the existing paths via serde's
    /// tag dispatch.
    #[test]
    fn legacy_task_update_still_parses_after_purge_added() {
        let json = r#"{
            "type": "task_update",
            "timestamp": "2026-07-10T00:00:00Z",
            "tenant_id": "t_1",
            "apps": {
                "myapp": {
                    "deployment_id": "d_1",
                    "deployment_hash": "abc",
                    "env": {},
                    "max_memory_mb": 256
                }
            }
        }"#;
        let msg: TaskMessage = serde_json::from_str(json).expect("deserialize task_update");
        match msg {
            TaskMessage::TaskUpdate { apps, .. } => {
                assert_eq!(apps.len(), 1);
                assert!(apps.contains_key("myapp"));
            }
            _ => panic!("task_update parsed as wrong variant after adding TaskPurge"),
        }
    }
}
