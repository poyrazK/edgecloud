//! `edge deploy` — upload the artifact to the control plane, or activate a stored one.

use anyhow::{Context, Result};
use std::path::Path;

use crate::api::ApiClient;
use crate::config::EdgeToml;
use crate::output;
use crate::state::State;

/// Dispatch to the upload or activate path based on whether `--id` was given.
///
/// `regions` is forwarded to the upload path so the server can fan
/// out the activate-time `TaskMessage` to each region. The activate
/// path (with `--id`) ignores `regions` because regions are baked
/// into the deployment row at upload time; activation reads them from
/// the row, not from the CLI flag.
#[cfg(feature = "network")]
pub fn run(path: &Path, app: &str, id: Option<&str>, regions: &[String]) -> Result<()> {
    if let Some(deployment_id) = id {
        return run_activate(path, app, deployment_id);
    }
    run_upload(path, app, regions)
}

/// Upload the project's compiled artifact to the control plane.
///
/// `app`: positional app-name override. When empty, read from `edge.toml`.
/// `regions`: list of regions to replicate to. Empty slice = server's
/// default region.
#[cfg(feature = "network")]
fn run_upload(path: &Path, app: &str, regions: &[String]) -> Result<()> {
    let edge_toml = EdgeToml::from_path(path)?;
    let app_name = if !app.is_empty() {
        app.to_string()
    } else {
        edge_toml.project.name.clone()
    };
    let artifact = path
        .join("target")
        .join("wasm32-wasip2")
        .join("release")
        .join(format!("{}.wasm", app_name));

    let wasm_bytes = std::fs::read(&artifact).map_err(|e| {
        output::error(&format!("failed to read {}: {}", artifact.display(), e));
        e
    })?;

    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    let resp = client.deploy(&app_name, &wasm_bytes, regions)?;

    let live_url = resp.url.clone();
    // Persist the regions the server actually accepted (it may
    // dedupe, apply a default, or reject). This keeps state.json in
    // sync with the deployment row even when the user passed an
    // empty `--regions` and the server filled in its own default.
    let persisted_regions = if !resp.regions.is_empty() {
        resp.regions.clone()
    } else {
        regions.to_vec()
    };
    let state = State {
        deployment_id: resp.id,
        app_name,
        live_url,
        regions: persisted_regions,
    };
    state.save(path)?;

    output::success("Deployed successfully");
    println!("  URL: {}", resp.url);
    Ok(())
}

/// Activate a previously-stored deployment by ID (e.g. from `edge migrate`).
///
/// App name is resolved with precedence: positional `app` > `.edge/state.json.app_name`.
/// API base URL must come from `edge.toml` (matches `edge activate` semantics).
#[cfg(feature = "network")]
fn run_activate(path: &Path, app: &str, deployment_id: &str) -> Result<()> {
    let state = load_state_optional(path)?;
    let app_name = resolve_app_name(app, state.as_ref())?;

    let edge_toml = EdgeToml::from_path(path)
        .with_context(|| "edge deploy --id requires edge.toml with [deployment] api = \"<url>\"")?;
    let base_url = edge_toml.api_url("https://api.edgecloud.dev");

    let client = ApiClient::new(base_url)?;
    client.activate(&app_name, deployment_id)?;

    output::success("Activated successfully");
    println!("  ID: {deployment_id}");
    // Only show the URL from state.json when it corresponds to the app we just
    // activated. A stale state.json from a different app would be misleading.
    match state {
        Some(s) if s.app_name == app_name => println!("  URL: {}", s.live_url),
        _ => println!("  (run `edge status` to view)"),
    }
    Ok(())
}

/// Load `.edge/state.json` if it exists. Suppress only `NotFound`; surface
/// parse/IO errors so the user gets a real diagnostic instead of a generic
/// "requires an app name" message.
fn load_state_optional(path: &Path) -> Result<Option<State>> {
    match State::load(path) {
        Ok(s) => Ok(Some(s)),
        Err(e) => {
            let is_not_found = e.chain().any(|c| {
                c.downcast_ref::<std::io::Error>()
                    .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound)
            });
            if is_not_found {
                Ok(None)
            } else {
                Err(e)
            }
        }
    }
}

/// Resolve the app name to use for the activate path.
///
/// Precedence: non-empty positional `app` wins; otherwise read from `state.json`;
/// otherwise error. An empty string in state is treated as missing.
fn resolve_app_name(app: &str, state: Option<&State>) -> Result<String> {
    if !app.is_empty() {
        return Ok(app.to_string());
    }
    match state {
        Some(s) if !s.app_name.is_empty() => Ok(s.app_name.clone()),
        _ => anyhow::bail!(
            "edge deploy --id requires an app name; pass it positionally \
             or run from a directory with .edge/state.json"
        ),
    }
}

#[cfg(not(feature = "network"))]
pub fn run(_path: &Path, _app: &str, _id: Option<&str>, _regions: &[String]) -> Result<()> {
    anyhow::bail!("deploy requires network support; rebuild with --features network")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state_with(name: &str) -> State {
        State {
            deployment_id: "d_test".to_string(),
            app_name: name.to_string(),
            live_url: "https://example.test".to_string(),
            // Empty regions for the resolve_* tests — those helpers
            // don't touch regions. The serde round-trip tests for
            // state.json live in state/mod.rs.
            regions: vec![],
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

    #[test]
    fn load_state_optional_returns_none_when_missing() {
        // A directory with no .edge/state.json at all.
        let dir = tempfile::tempdir().unwrap();
        let got = load_state_optional(dir.path()).unwrap();
        assert!(got.is_none());
    }

    #[test]
    fn load_state_optional_surfaces_parse_error() {
        // A .edge/state.json that exists but is not valid JSON — must surface
        // the error rather than silently treating it as "no state".
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(".edge")).unwrap();
        std::fs::write(
            dir.path().join(".edge").join("state.json"),
            "{not valid json",
        )
        .unwrap();
        let err = load_state_optional(dir.path()).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("failed to parse") || msg.contains("parse"),
            "expected a parse error, got: {msg}"
        );
    }
}
