//! `edge status` — get deployment status.

use anyhow::{Context, Result};
use std::path::Path;

use crate::api::ApiClient;
use crate::config::EdgeToml;
use crate::output;
use crate::state::State;

/// Get deployment status.
#[cfg(feature = "network")]
pub fn run(path: &Path) -> Result<()> {
    let state =
        State::load(path).with_context(|| "no deployment found — run `edge deploy` first")?;
    let edge_toml = EdgeToml::from_path(path)?;

    let client = ApiClient::new(edge_toml.deployment.api.clone())?;
    let status = client.status(&state.deployment_id)?;

    output::section("Deployment Status");
    println!("Deployment: {}", status.id);
    println!("Status: {}", status.status);
    println!("Created: {}", status.created_at);
    if let Some(url) = status.url {
        println!("URL: {}", url);
    }
    Ok(())
}

#[cfg(not(feature = "network"))]
pub fn run(_path: &Path) -> Result<()> {
    anyhow::bail!("status requires network support; rebuild with --features network")
}
