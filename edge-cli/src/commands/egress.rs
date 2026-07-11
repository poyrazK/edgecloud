//! `edge egress` — manage the outbound host allowlist.
//!
//! * `edge egress` / `edge egress show` — display the current allowlist.
//! * `edge egress set <hosts...>` — replace the allowlist with one or more hosts.
//! * `edge egress clear` — clear the allowlist (allow all outbound traffic).
//!
//! Retry-aware paths route through
//! `commands::retry::call_with_retry_no_interrupt` with the
//! centralized defaults `DEFAULT_MAX_RETRIES` /
//! `DEFAULT_RETRY_BASE_MS` / `DEFAULT_RETRY_CAP_MS`.

use anyhow::Result;
use clap::Subcommand;
use std::path::Path;

use super::retry::{
    call_with_retry_no_interrupt, DEFAULT_MAX_RETRIES, DEFAULT_RETRY_BASE_MS, DEFAULT_RETRY_CAP_MS,
};
use crate::api::ApiClient;
use crate::config::EdgeToml;

/// Subcommand enum for `edge egress`. Mirrors the dispatch in
/// `main.rs::Command::Egress`.
#[derive(Subcommand)]
pub enum EgressAction {
    /// Show the current egress allowlist.
    Show,
    /// Set the egress allowlist. Accepts hostnames (e.g. `api.stripe.com`)
    /// and wildcard patterns (e.g. `*.sendgrid.net`). Replaces the entire
    /// list with the provided entries.
    Set {
        /// One or more hostnames or wildcard patterns to allowlist.
        hosts: Vec<String>,
    },
    /// Clear the egress allowlist (allow all outbound traffic).
    Clear,
}

/// Display the current egress allowlist. Naturally idempotent
/// (read); the call routes through [`call_with_retry`] with
/// hardcoded sensible defaults.
#[cfg(feature = "network")]
pub fn show(path: &Path) -> Result<()> {
    let edge_toml = EdgeToml::from_path(path)?;
    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    let egress = call_with_retry_no_interrupt(
        "egress show",
        || client.get_egress(),
        DEFAULT_MAX_RETRIES,
        DEFAULT_RETRY_BASE_MS,
        DEFAULT_RETRY_CAP_MS,
    )?;

    if egress.allowlist.is_empty() {
        println!("No egress restrictions — all outbound traffic is allowed.");
    } else {
        println!("Egress allowlist ({} entry):", egress.allowlist.len());
        for host in &egress.allowlist {
            println!("  {host}");
        }
    }
    Ok(())
}

/// Replace the egress allowlist with the given hosts. Naturally
/// idempotent (PUT-replaces; the same final state replays). The
/// retry path uses hardcoded sensible defaults.
#[cfg(feature = "network")]
pub fn set(path: &Path, hosts: &[String]) -> Result<()> {
    let edge_toml = EdgeToml::from_path(path)?;
    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    let egress = call_with_retry_no_interrupt(
        "egress set",
        || client.set_egress(hosts),
        DEFAULT_MAX_RETRIES,
        DEFAULT_RETRY_BASE_MS,
        DEFAULT_RETRY_CAP_MS,
    )?;

    if egress.allowlist.is_empty() {
        println!("Egress allowlist cleared — all outbound traffic is allowed.");
    } else {
        println!(
            "Egress allowlist updated ({} entry):",
            egress.allowlist.len()
        );
        for host in &egress.allowlist {
            println!("  {host}");
        }
    }
    Ok(())
}

/// Clear the egress allowlist (allow all outbound traffic).
/// Naturally idempotent (PUT-replaces with empty list).
#[cfg(feature = "network")]
pub fn clear(path: &Path) -> Result<()> {
    set(path, &[])
}

#[cfg(not(feature = "network"))]
pub fn show(_path: &Path) -> Result<()> {
    anyhow::bail!("egress show requires network support; rebuild with --features network")
}

#[cfg(not(feature = "network"))]
pub fn set(_path: &Path, _hosts: &[String]) -> Result<()> {
    anyhow::bail!("egress set requires network support; rebuild with --features network")
}

#[cfg(not(feature = "network"))]
pub fn clear(_path: &Path) -> Result<()> {
    anyhow::bail!("egress clear requires network support; rebuild with --features network")
}
