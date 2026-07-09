//! `edge deployments` — list all deployments for the app.

use anyhow::{Context, Result};
use std::path::Path;

use crate::api::ApiClient;
use crate::config::EdgeToml;
use crate::state::State;

/// List all deployments for the app.
#[cfg(feature = "network")]
pub fn run(path: &Path) -> Result<()> {
    let state =
        State::load(path).with_context(|| "no deployment found — run `edge deploy` first")?;
    let edge_toml = EdgeToml::from_path(path)?;

    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    let deployments = client.list_deployments(&state.app_name)?;

    if deployments.is_empty() {
        println!("No deployments found.");
    } else {
        println!("{:<12} {:<10} {:<20} URL", "ID", "STATUS", "CREATED");
        println!("{}", "-".repeat(60));
        for d in deployments {
            println!(
                "{:<12} {:<10} {:<20} {}",
                d.id, d.status, d.created_at, d.url,
            );
        }
    }
    Ok(())
}

#[cfg(not(feature = "network"))]
pub fn run(_path: &Path) -> Result<()> {
    anyhow::bail!("deployments requires network support; rebuild with --features network")
}
