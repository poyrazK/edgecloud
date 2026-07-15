//! edge-worker library
//!
//! Re-exports public types for integration tests and external consumers.

pub mod auth;
pub mod backoff;
pub mod bootstrap;
pub mod config;
pub mod detect;
pub mod dispatch;
pub mod downloader;
pub mod log_forwarder;
pub mod messages;
pub mod metering_dedupe;
pub mod metrics;
pub mod metrics_server;
pub mod nats;
pub mod port_pool;
pub mod state;
pub mod supervisor;
pub mod tracing_layer;
pub mod verifier;
pub mod worker_key;
