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

/// TaskMessage: received via NATS on `edgecloud.tasks.<region>`.
///
/// Two variants share the same `apps` payload shape but carry different
/// semantics:
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
///
/// Both variants use the same handler logic; the only difference is the
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
    pub max_memory_mb: u64,
    #[serde(default)]
    pub cpu_budget_ms: Option<u64>,
}

/// ClusterHeadroom carries capacity info for the autoscaler (issue #85).
///
/// Mirrors the Go `ClusterHeadroom` struct in
/// `edge-control-plane/internal/nats/publisher.go`. `AppSlots` is the
/// only field the autoscaler acts on — the number of free port slots
/// this worker can allocate (i.e., not in cooldown). `cpu_pct` and
/// `mem_pct` are observability-only for now.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterHeadroom {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_pct: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mem_pct: Option<f64>,
    pub app_slots: u32,
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
        };
        let json = serde_json::to_string(&hb).expect("serialize heartbeat");
        assert!(
            !json.contains("worker_addr"),
            "None worker_addr must be omitted from the wire; got: {json}"
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
            observer_metrics: vec![],
        };
        let json = serde_json::to_string(&s).expect("serialize");
        assert!(
            json.contains(r#""outbound_bytes":512"#),
            "outbound_bytes must always appear in serialized AppStatus; got: {json}"
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
}
