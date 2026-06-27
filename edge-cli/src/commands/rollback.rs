//! `edge rollback` — swap the active deployment back to the stored
//! `last_good_deployment_id`.

use anyhow::{Context, Result};
use std::path::Path;

use super::state_io::{load_state_optional, resolve_app_name};
use crate::api::ApiClient;
use crate::config::EdgeToml;
use crate::output;

/// Roll back the active deployment of `app`.
///
/// App name is resolved with precedence: positional `app` > `.edge/state.json.app_name`.
/// The CLI updates `.edge/state.json` so subsequent `edge open` /
/// `edge status` reflect the rolled-back deployment id. The `live_url`
/// field is left as-is — refreshing it cleanly would require the server
/// to return the new URL in the rollback response body, which is
/// deferred to a follow-up (issue #74 follow-up).
#[cfg(feature = "network")]
pub fn run(path: &Path, app: &str) -> Result<()> {
    // load_state_optional semantics: missing state.json is OK if the
    // user passed an explicit app name; otherwise surface a clear error.
    let state = load_state_optional(path)?;
    let app_name = resolve_app_name("edge rollback", app, state.as_ref())?;

    let edge_toml = EdgeToml::from_path(path)
        .with_context(|| "edge rollback requires edge.toml with [deployment] api = \"<url>\"")?;
    let base_url = edge_toml.api_url("https://api.edgecloud.dev");

    let client = ApiClient::new(base_url)?;
    let resp = client.rollback(&app_name)?;

    // Update .edge/state.json so subsequent commands (status / open)
    // see the rolled-back id. We only persist when state.json existed
    // and matches the app we just rolled back — never overwrite state
    // for a different app.
    if let Some(mut s) = state {
        if s.app_name == app_name {
            s.deployment_id = resp.deployment_id.clone();
            s.save(path)?;
        }
    }

    output::success(&format!("Rolled back to deployment {}", resp.deployment_id));
    output::hint("verify with `edge status` or open in a browser with `edge open`");
    Ok(())
}

#[cfg(not(feature = "network"))]
pub fn run(_path: &Path, _app: &str) -> Result<()> {
    anyhow::bail!("rollback requires network support; rebuild with --features network")
}

#[cfg(test)]
mod tests {
    // resolve_app_name is exercised in commands/state_io.rs::tests;
    // this placeholder keeps `cargo test` happy with at least one
    // test per commands/* module file (clippy's -D warnings turns
    // empty `#[cfg(test)] mod tests` into a build failure).
    #[test]
    fn placeholder_for_centralized_resolve_tests() {
        // Intentionally empty: real coverage lives in state_io.
    }
}
