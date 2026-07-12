//! CLI commands.

pub mod activate;
pub mod apps;
pub mod auth;
pub mod billing;
pub mod build;
pub mod completions;
pub mod deploy;
pub mod deployments;
pub mod dev;
pub mod domains;
pub mod egress;
pub mod env;
pub mod ingress;
pub mod init;
pub mod logs;
pub mod migrate;
pub mod open;
pub mod quota;
pub mod rollback;
pub(crate) mod state_io;
pub mod status;
pub mod traffic;
pub mod webhooks;

/// Shared retry loop for transient-failure mutations across the
/// CLI (issue #571 propagation follow-up). Lifts the loop, the
/// classifier, and the backoff math out of `commands::deploy.rs`
/// into a single module so other mutation endpoints (`env delete`,
/// `traffic set`, `keys revoke`, `domains remove`, `egress set`, …)
/// route through one canonical home.
pub mod retry;
