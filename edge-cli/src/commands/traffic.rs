//! `edge traffic` — get or set traffic splits for an app.

use anyhow::{Context, Result};
use std::path::Path;

use crate::api::ApiClient;
use crate::config::EdgeToml;
use crate::output;
use crate::state::State;

/// Get current traffic splits for the app.
#[cfg(feature = "network")]
pub fn get(path: &Path) -> Result<()> {
    let state = State::load(path).context("no deployment found — run `edge deploy` first")?;
    let edge_toml = EdgeToml::from_path(path)?;

    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    let splits = client.get_traffic(&state.app_name)?;

    if splits.is_empty() {
        output::info("No traffic splits configured — all traffic goes to the active deployment");
    } else {
        output::info(&format!("Traffic splits for {}:", state.app_name));
        for (id, weight) in &splits {
            println!("  {:5}%  {}", weight, id);
        }
        let total: u32 = splits.iter().map(|(_, w)| *w as u32).sum();
        println!("  -----");
        println!("  {:5}%  total", total);
        if total != 100 {
            output::error(&format!("WARNING: weights sum to {}%, not 100%", total));
        }
    }
    Ok(())
}

/// Set traffic splits for the app.
/// `splits` is a slice of "deployment_id=weight" strings, e.g. ["d_v1=95","d_v2=5"].
#[cfg(feature = "network")]
pub fn set(path: &Path, splits: &[(String, u8)]) -> Result<()> {
    let state = State::load(path).context("no deployment found — run `edge deploy` first")?;
    let edge_toml = EdgeToml::from_path(path)?;

    let total: u32 = splits.iter().map(|(_, w)| *w as u32).sum();
    if total != 100 {
        anyhow::bail!("weights must sum to 100 (got {})", total);
    }

    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    client.set_traffic(&state.app_name, splits)?;

    output::success(&format!("Traffic splits set for {}", state.app_name));
    for (id, weight) in splits {
        println!("  {:5}%  {}", weight, id);
    }
    Ok(())
}

#[cfg(not(feature = "network"))]
pub fn get(_path: &Path) -> Result<()> {
    anyhow::bail!("traffic get requires network support; rebuild with --features network")
}

#[cfg(not(feature = "network"))]
pub fn set(_path: &Path, _splits: &[(String, u8)]) -> Result<()> {
    anyhow::bail!("traffic set requires network support; rebuild with --features network")
}
