//! `edge ingress` — show the ingress target (worker address and port)
//! for a running app.

use anyhow::{Context, Result};
use std::path::Path;

use super::state_io::{load_state_optional, resolve_app_name};
use crate::api::ApiClient;
use crate::config::EdgeToml;

/// Show the ingress target for an app.
///
/// App name is resolved with precedence: positional arg > `.edge/state.json`.
/// API base URL must come from `edge.toml`.
#[cfg(feature = "network")]
pub fn run(path: &Path, app: &str) -> Result<()> {
    let state = load_state_optional(path)?;
    let app_name = resolve_app_name("edge ingress", app, state.as_ref())?;
    let edge_toml = EdgeToml::from_path(path)
        .with_context(|| "edge ingress requires edge.toml with [deployment] api = \"<url>\"")?;
    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    let ingress = client.get_ingress(&app_name)?;

    if !ingress.ready {
        println!("App '{}' is not currently running on any worker.", app_name);
        return Ok(());
    }

    println!("App:     {}", ingress.app_name);
    println!("Worker:  {}", ingress.worker_id.unwrap_or_default());
    println!("Region:  {}", ingress.region.unwrap_or_default());
    println!(
        "Target:  {}:{}",
        ingress.worker_addr.unwrap_or_default(),
        ingress.port.unwrap_or(0)
    );
    Ok(())
}

#[cfg(not(feature = "network"))]
pub fn run(_path: &Path, _app: &str) -> Result<()> {
    anyhow::bail!("edge ingress requires network support; rebuild with --features network")
}
