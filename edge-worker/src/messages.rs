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
