//! edge-ingress: NATS heartbeat-driven Caddy controller for edgeCloud.
//!
//! Subscribes to `edgecloud.heartbeats.<region>`, maintains an in-memory
//! routing table of `<tenant>-<app>.edgecloud.dev → worker:port`, and
//! reloads a local Caddy process with the rendered Caddyfile-JSON on every
//! change. See `edge-ingress/README.md` for the operator runbook.

pub mod caddy;
pub mod config;
pub mod heartbeats;
pub mod messages;
pub mod routing;
pub mod traffic;
