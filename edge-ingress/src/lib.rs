//! edge-ingress: NATS heartbeat-driven Caddy controller for edgeCloud.
//!
//! Subscribes to `edgecloud.heartbeats.<region>`, maintains an in-memory
//! routing table of `<tenant>-<app>.edgecloud.dev → worker:port`, and
//! reloads a local Caddy process with the rendered Caddyfile-JSON on every
//! change. With `CONTROL_PLANE_URL` set, a second 30s poller fetches
//! the tenant's custom FQDN bindings from
//! `GET /api/internal/domains` and adds per-FQDN routes with on-demand
//! TLS. See `edge-ingress/README.md` for the operator runbook.

pub mod caddy;
pub mod config;
pub mod domains;
pub mod heartbeats;
pub mod l4;
pub mod l4_cache;
pub mod messages;
pub mod quota;
pub mod ratelimit;
pub mod routing;
pub mod tenant_ratelimit;
pub mod traffic;
