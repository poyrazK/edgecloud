//! `edge env set`, `edge env list`, and `edge env delete`.

use anyhow::{Context, Result};
use std::path::Path;

use super::state_io::{load_state_optional, resolve_app_name};
use crate::api::ApiClient;
use crate::config::EdgeToml;
use crate::output;

/// Set an environment variable for the app.
///
/// App name is resolved with precedence: positional `app` >
/// `.edge/state.json.app_name`. A missing state.json is tolerated
/// when the user passed a positional — the otherwise unhelpful
/// "no deployment found" message is replaced with the
/// `resolve_app_name` standard phrasing.
#[cfg(feature = "network")]
pub fn set_var(path: &Path, app: &str, key: &str, value: &str) -> Result<()> {
    let state = load_state_optional(path)?;
    let app_name = resolve_app_name("edge env set", app, state.as_ref())?;
    let edge_toml = EdgeToml::from_path(path)
        .with_context(|| "edge env set requires edge.toml with [deployment] api = \"<url>\"")?;

    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    client.set_env(&app_name, key, value)?;

    output::success(&format!("{} set", key));
    Ok(())
}

/// List environment variables for the app.
///
/// App name resolution mirrors `set_var` above.
#[cfg(feature = "network")]
pub fn list_vars(path: &Path, app: &str) -> Result<()> {
    let state = load_state_optional(path)?;
    let app_name = resolve_app_name("edge env list", app, state.as_ref())?;
    let edge_toml = EdgeToml::from_path(path)
        .with_context(|| "edge env list requires edge.toml with [deployment] api = \"<url>\"")?;

    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    let vars = client.list_env(&app_name)?;

    if vars.is_empty() {
        println!("No environment variables set.");
    } else {
        for var in vars {
            println!("{} = {}", var.key, var.value);
        }
    }
    Ok(())
}

/// Delete an environment variable for the app.
///
/// App name resolution mirrors `set_var` above.
#[cfg(feature = "network")]
pub fn delete_var(path: &Path, app: &str, key: &str) -> Result<()> {
    let state = load_state_optional(path)?;
    let app_name = resolve_app_name("edge env delete", app, state.as_ref())?;
    let edge_toml = EdgeToml::from_path(path)
        .with_context(|| "edge env delete requires edge.toml with [deployment] api = \"<url>\"")?;

    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    client.delete_env(&app_name, key)?;

    output::success(&format!("{} deleted", key));
    Ok(())
}

#[cfg(not(feature = "network"))]
pub fn set_var(_path: &Path, _app: &str, _key: &str, _value: &str) -> Result<()> {
    anyhow::bail!("env requires network support; rebuild with --features network")
}

#[cfg(not(feature = "network"))]
pub fn list_vars(_path: &Path, _app: &str) -> Result<()> {
    anyhow::bail!("env requires network support; rebuild with --features network")
}

#[cfg(not(feature = "network"))]
pub fn delete_var(_path: &Path, _app: &str, _key: &str) -> Result<()> {
    anyhow::bail!("env requires network support; rebuild with --features network")
}
