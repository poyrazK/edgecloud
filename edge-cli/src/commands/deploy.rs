//! `edge deploy` — upload the artifact to the control plane.

use anyhow::Result;
use std::path::Path;

use crate::api::ApiClient;
use crate::config::EdgeToml;
use crate::output;
use crate::state::State;

/// Upload the artifact to the edgeCloud control plane.
#[cfg(feature = "network")]
pub fn run(path: &Path) -> Result<()> {
    let edge_toml = EdgeToml::from_path(path)?;
    let artifact = path
        .join("target")
        .join("wasm32-wasip2")
        .join("release")
        .join(format!("{}.wasm", edge_toml.project.name));

    let wasm_bytes = std::fs::read(&artifact).map_err(|e| {
        output::error(&format!("failed to read {}: {}", artifact.display(), e));
        e
    })?;

    let client = ApiClient::new(edge_toml.deployment.api.clone())?;
    let resp = client.deploy(&edge_toml.project.name, &wasm_bytes)?;

    let live_url = resp.url.clone();
    let state = State {
        deployment_id: resp.id,
        app_name: edge_toml.project.name,
        live_url,
    };
    state.save(path)?;

    output::success("Deployed successfully");
    println!("  URL: {}", resp.url);
    Ok(())
}

#[cfg(not(feature = "network"))]
pub fn run(_path: &Path) -> Result<()> {
    anyhow::bail!("deploy requires network support; rebuild with --features network")
}
