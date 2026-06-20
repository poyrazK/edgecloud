//! `edge activate` — activate a specific deployment.

use anyhow::Result;
use std::path::Path;

use super::state_io::load_state_optional;
use crate::api::ApiClient;
use crate::config::EdgeToml;
use crate::output;
use crate::state::State;

/// Activate a specific deployment.
///
/// If `.edge/state.json` exists and matches the app being activated,
/// update its `deployment_id` so subsequent commands (status, open,
/// rollback) see the new active id. We deliberately do NOT touch
/// `live_url` here — refreshing it cleanly would require the server
/// to return the new URL in the activate response body, which is
/// deferred to a follow-up (issue #74 follow-up).
#[cfg(feature = "network")]
pub fn run(path: &Path, deployment_id: &str) -> Result<()> {
    let state = load_state_optional(path)?;
    let app_name = resolve_app_name(&state)?;

    let edge_toml = EdgeToml::from_path(path)?;
    let base_url = edge_toml.api_url("https://api.edgecloud.dev");

    let client = ApiClient::new(base_url)?;
    client.activate(&app_name, deployment_id)?;

    // Update state.json if it exists for this app. Never overwrite
    // state for a different app.
    if let Some(mut s) = state {
        if s.app_name == app_name {
            s.deployment_id = deployment_id.to_string();
            s.save(path)?;
        }
    }

    output::success(&format!("Deployment {} activated", deployment_id));
    Ok(())
}

/// Resolve the app name. Requires an existing `.edge/state.json`
/// (unlike the deploy path which can fall back to edge.toml).
fn resolve_app_name(state: &Option<State>) -> Result<String> {
    match state {
        Some(s) if !s.app_name.is_empty() => Ok(s.app_name.clone()),
        _ => anyhow::bail!("edge activate requires .edge/state.json — run `edge deploy` first"),
    }
}

#[cfg(not(feature = "network"))]
pub fn run(_path: &Path, _deployment_id: &str) -> Result<()> {
    anyhow::bail!("activate requires network support; rebuild with --features network")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state_with(name: &str) -> State {
        State {
            deployment_id: "d_test".to_string(),
            app_name: name.to_string(),
            live_url: "https://example.test".to_string(),
        }
    }

    #[test]
    fn resolve_returns_app_from_state() {
        let s = Some(state_with("myapp"));
        assert_eq!(resolve_app_name(&s).unwrap(), "myapp");
    }

    #[test]
    fn resolve_errors_when_no_state() {
        let err = resolve_app_name(&None).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("requires .edge/state.json"),
            "expected missing-state error, got: {msg}"
        );
    }

    #[test]
    fn resolve_treats_empty_state_as_missing() {
        let s = Some(state_with(""));
        let err = resolve_app_name(&s).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("requires .edge/state.json"),
            "expected missing-state error, got: {msg}"
        );
    }
}
