//! `edge env set` and `edge env list`.

use anyhow::{Context, Result};
use std::path::Path;

use crate::api::ApiClient;
use crate::config::EdgeToml;
use crate::output;
use crate::state::State;

/// Set an environment variable for the app.
#[cfg(feature = "network")]
pub fn set_var(path: &Path, key: &str, value: &str) -> Result<()> {
    let state =
        State::load(path).with_context(|| "no deployment found — run `edge deploy` first")?;
    let edge_toml = EdgeToml::from_path(path)?;

    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    client.set_env(&state.app_name, key, value)?;

    output::success(&format!("{} set", key));
    Ok(())
}

/// List environment variables for the app.
#[cfg(feature = "network")]
pub fn list_vars(path: &Path) -> Result<()> {
    let state =
        State::load(path).with_context(|| "no deployment found — run `edge deploy` first")?;
    let edge_toml = EdgeToml::from_path(path)?;

    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    let vars = client.list_env(&state.app_name)?;

    if vars.is_empty() {
        println!("No environment variables set.");
    } else {
        for var in vars {
            println!("{} = {}", var.key, var.value);
        }
    }
    Ok(())
}

#[cfg(not(feature = "network"))]
pub fn set_var(_path: &Path, _key: &str, _value: &str) -> Result<()> {
    anyhow::bail!("env requires network support; rebuild with --features network")
}

#[cfg(not(feature = "network"))]
pub fn list_vars(_path: &Path) -> Result<()> {
    anyhow::bail!("env requires network support; rebuild with --features network")
}
