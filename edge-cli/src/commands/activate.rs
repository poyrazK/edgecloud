//! `edge activate` — activate a specific deployment.

use anyhow::{Context, Result};
use std::path::Path;

use crate::api::ApiClient;
use crate::config::EdgeToml;
use crate::output;
use crate::state::State;

/// Activate a specific deployment.
#[cfg(feature = "network")]
pub fn run(path: &Path, deployment_id: &str) -> Result<()> {
    let state =
        State::load(path).with_context(|| "no deployment found — run `edge deploy` first")?;
    let edge_toml = EdgeToml::from_path(path)?;

    let client = ApiClient::new(edge_toml.deployment.api.clone())?;
    client.activate(&state.app_name, deployment_id)?;

    output::success(&format!("Deployment {} activated", deployment_id));
    Ok(())
}

#[cfg(not(feature = "network"))]
pub fn run(_path: &Path, _deployment_id: &str) -> Result<()> {
    anyhow::bail!("activate requires network support; rebuild with --features network")
}
