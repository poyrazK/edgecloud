//! NATS message types mirrored from edge-worker. Every field uses
//! `#[serde(default)]` so legacy heartbeats (pre-EDGE_WORKER_ADDR, pre-port)
//! still parse cleanly.

use serde::Deserialize;
use std::collections::HashMap;

/// HeartbeatMessage: received on `edgecloud.heartbeats.<region>`. Several
/// fields are accepted for forward-compat with the worker's wire format but
/// are not consulted on the ingress side — we only need `worker_addr` and
/// the per-app `tenant_id`/`port`/`status`.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct HeartbeatMessage {
    #[serde(rename = "type", default)]
    pub msg_type: String,
    #[serde(default)]
    pub timestamp: String,
    #[serde(default)]
    pub worker_id: String,
    #[serde(default)]
    pub region: String,
    #[serde(default)]
    pub worker_addr: Option<String>,
    #[serde(default)]
    pub apps: HashMap<String, AppStatus>,
}

/// AppStatus: status of a single app within a heartbeat.
///
/// Several fields are kept for forward-compatibility with the wire format
/// (so legacy and future heartbeats still parse) but are not consulted on
/// the ingress side — we only need `tenant_id`, `port`, and `status`.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
pub struct AppStatus {
    #[serde(default)]
    pub deployment_id: String,
    /// "running" | "starting" | "stopping" | "crashed" | "hung"
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub exit_code: Option<i32>,
    #[serde(default)]
    pub request_count: u64,
    #[serde(default)]
    pub tenant_id: String,
    /// Optional on the wire — older workers didn't carry it.
    #[serde(default)]
    pub port: Option<u16>,
}
