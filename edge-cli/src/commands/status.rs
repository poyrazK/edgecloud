//! `edge status` — get deployment status.
//!
//! Two subcommands:
//!
//! * `edge status deployment` (also the no-arg `edge status` form for
//!   backward compat): DB-row view — `deployed` / `active` /
//!   `failed` / `migrated`. Sourced from
//!   `GET /api/v1/status/{deployment_id}` via `ApiClient::status`.
//! * `edge status runtime <app>`: worker-reported runtime view —
//!   `running` / `starting` / `stopping` / `crashed` / `hung` /
//!   `unknown`. Sourced from
//!   `GET /api/v1/apps/{appName}/status` via `ApiClient::app_status`.

use anyhow::{Context, Result};
use std::path::Path;

use super::state_io::{load_state_optional, resolve_app_name};
use crate::api::ApiClient;
use crate::config::EdgeToml;
use crate::output;
use crate::state::State;

/// Get deployment status (legacy DB-row view).
#[cfg(feature = "network")]
pub fn run(path: &Path) -> Result<()> {
    let state =
        State::load(path).with_context(|| "no deployment found — run `edge deploy` first")?;
    let edge_toml = EdgeToml::from_path(path)?;

    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    let status = client.status(&state.deployment_id)?;

    output::section("Deployment Status");
    println!("Deployment: {}", status.id);
    println!("Status: {}", status.status);
    println!("Created: {}", status.created_at);
    println!("URL: {}", status.url);
    Ok(())
}

#[cfg(not(feature = "network"))]
pub fn run(_path: &Path) -> Result<()> {
    anyhow::bail!("status requires network support; rebuild with --features network")
}

/// Get the worker-reported runtime status of an app.
///
/// TTY-only output (matches `edge traffic` and `edge deployments` —
/// the majority convention for app-level commands). The endpoint
/// returns `last_heartbeat` raw; we do not apply a staleness check
/// (that's `edge logs`' job — this command is a data dump for
/// scripting / debugging).
#[cfg(feature = "network")]
pub fn runtime(path: &Path, app: &str) -> Result<()> {
    // Same resolution pattern as `edge logs` and `edge rollback`:
    // positional arg wins, else fall back to `.edge/state.json`,
    // else error. State.json is OPTIONAL because the user may pass
    // the app explicitly without ever having deployed (e.g. just
    // inspecting a peer's app by name).
    let state = load_state_optional(path)?;
    let app_name = resolve_app_name("edge status runtime", app, state.as_ref())?;
    let edge_toml = EdgeToml::from_path(path).with_context(|| {
        "edge status runtime requires edge.toml with [deployment] api = \"<url>\""
    })?;
    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;

    let s = client.app_status(&app_name)?;

    output::section(&format!("Runtime Status — {app_name}"));
    println!("Status:         {}", s.status);
    println!("Region:         {}", s.region);
    println!("Worker:         {}", s.worker_id);
    match s.last_heartbeat.as_deref() {
        Some(hb) => println!("Last heartbeat: {hb}"),
        // The endpoint returns 200 with {status: "unknown"} for apps
        // no worker has reported on — the truthful answer, not an
        // error. We surface it as "(none — no worker has reported)"
        // so the user can tell apart "unknown" from a populated
        // status whose last_heartbeat happens to be missing.
        None => println!("Last heartbeat: (none — no worker has reported)"),
    }
    if let Some(code) = s.exit_code {
        println!("Exit code:      {code}");
    }
    Ok(())
}

#[cfg(not(feature = "network"))]
pub fn runtime(_path: &Path, _app: &str) -> Result<()> {
    anyhow::bail!("status requires network support; rebuild with --features network")
}
