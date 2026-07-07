//! `edge deploy` — upload the artifact to the control plane, or activate a stored one.

use anyhow::{Context, Result};
use std::path::Path;

use super::state_io::load_state_optional;
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
///
/// `auto_rollback` is the tenant opt-in for the worker-driven auto-
/// rollback + heartbeat-driven stability-window promotion (issue
/// #74). It is forwarded to the upload path; the activate path
/// ignores it because the flag was already set at upload time and
/// is read from the deployment row by ActivateDeployment.
#[cfg(feature = "network")]
pub fn run(
    path: &Path,
    app: &str,
    id: Option<&str>,
    regions: &[String],
    auto_rollback: bool,
    file: Option<&Path>,
) -> Result<()> {
    if let Some(deployment_id) = id {
        return run_activate(path, app, deployment_id);
    }
    run_upload(path, app, regions, auto_rollback, file)
}

/// Upload the project's compiled artifact to the control plane.
///
/// `app`: positional app-name override. When empty, read from `edge.toml`.
/// `regions`: list of regions to replicate to. Empty slice = server's
/// default region.
/// `auto_rollback`: tenant opt-in for issue #74 — when true, the
/// deployment row and (at activate time) the active_deployments row
/// get `auto_rollback_enabled = true`, which gates the
/// worker-driven auto-rollback trigger and the heartbeat-driven
/// stability-window promotion.
#[cfg(feature = "network")]
fn run_upload(
    path: &Path,
    app: &str,
    regions: &[String],
    auto_rollback: bool,
    file: Option<&Path>,
) -> Result<()> {
    let edge_toml = EdgeToml::from_path(path)?;
    let app_name = if !app.is_empty() {
        app.to_string()
    } else {
        edge_toml.project.name.clone()
    };
    let artifact = match file {
        Some(f) => f.to_path_buf(),
        None => path
            .join("target")
            .join("wasm32-wasip2")
            .join("release")
            .join(format!("{}.wasm", app_name)),
    };

    let wasm_bytes = std::fs::read(&artifact).map_err(|e| {
        output::error(&format!("failed to read {}: {}", artifact.display(), e));
        e
    })?;

    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    let resp = client.deploy(&app_name, &wasm_bytes, regions, auto_rollback)?;

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
/// On success, `.edge/state.json` is updated so subsequent commands
/// (`status`, `open`, `rollback`) see the new active id.
#[cfg(feature = "network")]
fn run_activate(path: &Path, app: &str, deployment_id: &str) -> Result<()> {
    let state = load_state_optional(path)?;
    let app_name = resolve_app_name(app, state.as_ref())?;

    let edge_toml = EdgeToml::from_path(path)
        .with_context(|| "edge deploy --id requires edge.toml with [deployment] api = \"<url>\"")?;
    let base_url = edge_toml.api_url("https://api.edgecloud.dev");

    let client = ApiClient::new(base_url)?;
    client.activate(&app_name, deployment_id, None)?;

    // Capture the URL we'll print BEFORE `state` is moved into the save
    // block below. The save only mutates `deployment_id`, so the URL we
    // capture here is byte-identical to what we'd re-read from disk
    // afterwards — but we avoid one redundant state.json read on the
    // success path. Empty URLs (or a state for a different app) suppress
    // the URL line entirely instead of printing a misleading blank.
    let url_to_print = url_to_print(state.as_ref(), &app_name);

    // Update state.json if it exists for this app.
    if let Some(mut s) = state {
        if s.app_name == app_name {
            s.deployment_id = deployment_id.to_string();
            s.save(path)?;
        }
    }

    output::success("Activated successfully");
    println!("  ID: {deployment_id}");
    match url_to_print {
        Some(url) => println!("  URL: {url}"),
        None => println!("  (run `edge status` to view)"),
    }
    Ok(())
}

/// Promote a preview deployment to production.
/// Activates the deployment under the real app name (the CP's PromoteDeployment
/// relaxes the app_name check so a deployment stored under `myapp--preview-xxx`
/// can be activated under `myapp`).
#[cfg(feature = "network")]
pub fn run_promote(path: &Path, app: &str, deployment_id: &str) -> Result<()> {
    let edge_toml =
        EdgeToml::from_path(path).with_context(|| "edge deploy --promote requires edge.toml")?;
    let app_name = if !app.is_empty() {
        app.to_string()
    } else {
        edge_toml.project.name.clone()
    };
    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    client.promote(&app_name, deployment_id)?;
    output::success("Promoted successfully");
    println!("  ID: {deployment_id}");
    println!("  App: {app_name}");
    Ok(())
}

/// Decide whether to print `  URL: <live_url>` for the just-activated
/// app. Returns Some(url) only when state.json exists, belongs to the
/// same app we activated, and has a non-empty `live_url`; otherwise
/// None (caller prints the "run `edge status` to view" hint).
fn url_to_print(state: Option<&State>, app_name: &str) -> Option<String> {
    state
        .filter(|s| s.app_name == app_name && !s.live_url.is_empty())
        .map(|s| s.live_url.clone())
}

/// Resolve the app name to use for the activate path.
///
/// Delegates to `state_io::resolve_app_name` so the precedence rule
/// stays in one place. An empty string in state is treated as missing.
fn resolve_app_name(app: &str, state: Option<&State>) -> Result<String> {
    super::state_io::resolve_app_name("edge deploy --id", app, state)
}

#[cfg(not(feature = "network"))]
pub fn run(
    _path: &Path,
    _app: &str,
    _id: Option<&str>,
    _regions: &[String],
    _auto_rollback: bool,
    _file: Option<&Path>,
) -> Result<()> {
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
    fn url_to_print_is_some_when_state_app_matches_with_url() {
        // The just-activated app has a state.json with a populated
        // live_url — we should print it.
        let s = state_with("myapp");
        let got = url_to_print(Some(&s), "myapp");
        assert_eq!(got, Some("https://example.test".to_string()));
    }

    #[test]
    fn url_to_print_is_none_when_state_app_differs() {
        // state.json belongs to a different app — printing its URL
        // would be misleading. Suppress the URL line.
        let s = state_with("state-app");
        let got = url_to_print(Some(&s), "different-app");
        assert_eq!(got, None);
    }

    #[test]
    fn url_to_print_is_none_when_state_live_url_is_empty() {
        // state.json exists for the right app but live_url is empty
        // (e.g. it was never set by an `edge deploy` upload). Printing
        // "  URL: " with nothing after it is a UX bug — suppress.
        let s = State {
            deployment_id: "d_test".to_string(),
            app_name: "myapp".to_string(),
            live_url: String::new(),
            regions: vec![],
        };
        let got = url_to_print(Some(&s), "myapp");
        assert_eq!(got, None);
    }

    #[test]
    fn url_to_print_is_none_when_state_is_missing() {
        // No state.json at all — the caller has nothing to print.
        let got = url_to_print(None, "myapp");
        assert_eq!(got, None);
    }
}
