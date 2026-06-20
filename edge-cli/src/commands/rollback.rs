//! `edge rollback` — swap the active deployment back to the stored
//! `last_good_deployment_id`.

use anyhow::{Context, Result};
use std::path::Path;

use super::state_io::load_state_optional;
use crate::api::ApiClient;
use crate::config::EdgeToml;
use crate::output;
use crate::state::State;

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
    let app_name = resolve_app_name(app, state.as_ref())?;

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

/// Resolve the app name to use for rollback.
///
/// Precedence: non-empty positional `app` wins; otherwise read from
/// `state.json`; otherwise error.
fn resolve_app_name(app: &str, state: Option<&State>) -> Result<String> {
    if !app.is_empty() {
        return Ok(app.to_string());
    }
    match state {
        Some(s) if !s.app_name.is_empty() => Ok(s.app_name.clone()),
        _ => anyhow::bail!(
            "edge rollback requires an app name; pass it positionally \
             or run from a directory with .edge/state.json"
        ),
    }
}

#[cfg(not(feature = "network"))]
pub fn run(_path: &Path, _app: &str) -> Result<()> {
    anyhow::bail!("rollback requires network support; rebuild with --features network")
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
    fn resolve_positional_wins_over_empty() {
        let got = resolve_app_name("myapp", None).unwrap();
        assert_eq!(got, "myapp");
    }

    #[test]
    fn resolve_falls_back_to_state_when_positional_empty() {
        let s = state_with("from-state");
        let got = resolve_app_name("", Some(&s)).unwrap();
        assert_eq!(got, "from-state");
    }

    #[test]
    fn resolve_positional_wins_over_state() {
        let s = state_with("from-state");
        let got = resolve_app_name("positional", Some(&s)).unwrap();
        assert_eq!(got, "positional");
    }

    #[test]
    fn resolve_errors_when_no_inputs() {
        let err = resolve_app_name("", None).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("requires an app name"),
            "expected 'requires an app name' in error, got: {msg}"
        );
    }

    #[test]
    fn resolve_treats_empty_state_as_missing() {
        let s = state_with("");
        let err = resolve_app_name("", Some(&s)).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("requires an app name"),
            "expected 'requires an app name' in error, got: {msg}"
        );
    }
}
