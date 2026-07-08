//! edge-worker library
//!
//! Re-exports public types for integration tests and external consumers.

pub mod auth;
pub mod bootstrap;
pub mod config;
pub mod detect;
pub mod dispatch;
pub mod downloader;
pub mod log_forwarder;
pub mod messages;
pub mod nats;
pub mod port_pool;
pub mod state;
pub mod supervisor;
pub mod tracing_layer;
pub mod verifier;
