//! NATS message types for worker ↔ control plane communication.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// TaskMessage: received via NATS on `edgecloud.tasks.<region>`.
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
}

/// AppSpec: specification for a single deployed app.
#[derive(Debug, Clone, Deserialize)]
pub struct AppSpec {
    pub deployment_id: String,
    pub deployment_hash: String,
    pub env: HashMap<String, String>,
    #[allow(dead_code)]
    pub allowlist: Vec<String>,
    pub max_memory_mb: u64,
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
    pub apps: HashMap<String, AppStatus>,
}

/// AppStatus: status of a single app within a heartbeat.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppStatus {
    pub deployment_id: String,
    pub status: String, // "running" | "starting" | "stopping" | "crashed"
    pub exit_code: Option<i32>,
    /// Number of HTTP requests handled since last heartbeat.
    pub request_count: u64,
    /// Tenant the app belongs to. Used by the public ingress to render the
    /// host (`<tenant_id>-<app_name>.edgecloud.dev` — see
    /// `edge-ingress::config::ingress_host` and
    /// `edge-control-plane/internal/domain.IngressHost`; the suffix lives
    /// in `edge-ingress::config::INGRESS_HOST_SUFFIX`).
    pub tenant_id: String,
    /// Port the app's HTTP server is listening on, on the worker host.
    /// Used by the public ingress to dial the upstream.
    pub port: u16,
}

impl HeartbeatMessage {
    /// Create a new heartbeat with the current timestamp.
    pub fn new(worker_id: String, region: String, worker_addr: String) -> Self {
        Self {
            msg_type: "heartbeat".to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            worker_id,
            region,
            worker_addr: Some(worker_addr),
            apps: HashMap::new(),
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
        let hb = HeartbeatMessage::new("w_fra_abc".to_string(), "fra".to_string(), String::new());
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
            apps: HashMap::new(),
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
        );
        let json = serde_json::to_string(&hb).expect("serialize");
        let parsed: HeartbeatMessage =
            serde_json::from_str(&json).expect("deserialize same wire shape");
        assert_eq!(parsed.worker_addr.as_deref(), Some("203.0.113.10"));
        assert_eq!(parsed.worker_id, "w_fra_abc");
        assert_eq!(parsed.region, "fra");
    }
}
