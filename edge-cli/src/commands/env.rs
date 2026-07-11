//! `edge env set`, `edge env list`, and `edge env delete`.

use anyhow::{Context, Result};
use std::path::Path;
use std::sync::atomic::AtomicBool;

use super::retry::call_with_retry;
use super::state_io::{load_state_optional, resolve_app_name};
use crate::api::ApiClient;
use crate::config::EdgeToml;
use crate::output;

/// Hardcoded sensible defaults for endpoints that don't expose the
/// `--max-retries` / `--retry-base-ms` / `--retry-cap-ms` flag
/// triple. Matches `edge deploy`'s defaults so a transient outage
/// during `edge env list` / `edge keys list` / `edge domains list`
/// is treated the same as a transient during `edge deploy`.
const HARD_CODED_MAX_RETRIES: u32 = 3;
const HARD_CODED_RETRY_BASE_MS: u64 = 500;
const HARD_CODED_RETRY_CAP_MS: u64 = 8_000;

/// Set an environment variable for the app.
///
/// App name is resolved with precedence: positional `app` >
/// `.edge/state.json.app_name`. A missing state.json is tolerated
/// when the user passed a positional — the otherwise unhelpful
/// "no deployment found" message is replaced with the
/// `resolve_app_name` standard phrasing.
///
/// **Phase-2 deferred (issue #571 follow-up).** `edge env set` is a
/// POST upsert and a retried POST could double-apply or 409 on
/// replay — the right fix is CP-side `Idempotency-Key` schema
/// extension beyond `migrations/026_idempotency_keys.sql`. Until
/// that lands, `set_var` does NOT route through `call_with_retry`:
/// the bare `client.set_env(...)` call fails fast on transient
/// 5xx, which is the conservative behavior (no risk of duplicate
/// env writes). The retry umbrella covers `delete_var` and
/// `list_vars`, which are naturally idempotent.
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
/// App name resolution mirrors `set_var` above. Naturally
/// idempotent (read), so the call routes through [`call_with_retry`]
/// with hardcoded sensible defaults — a transient 5xx on `edge env
/// list` is retried the same way as on `edge deploy`.
#[cfg(feature = "network")]
pub fn list_vars(path: &Path, app: &str) -> Result<()> {
    let state = load_state_optional(path)?;
    let app_name = resolve_app_name("edge env list", app, state.as_ref())?;
    let edge_toml = EdgeToml::from_path(path)
        .with_context(|| "edge env list requires edge.toml with [deployment] api = \"<url>\"")?;

    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    let interrupt = AtomicBool::new(false);
    let vars = call_with_retry(
        "env list",
        || client.list_env(&app_name),
        HARD_CODED_MAX_RETRIES,
        HARD_CODED_RETRY_BASE_MS,
        HARD_CODED_RETRY_CAP_MS,
        &interrupt,
    )?;

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
/// App name resolution mirrors `set_var` above. Naturally
/// idempotent (DELETE-by-primary-key; the second call returns 404
/// with no side effect). The retry flags are operator-tunable —
/// `main.rs` wires `--max-retries` / `--retry-base-ms` /
/// `--retry-cap-ms` to this function (matches `edge deploy`'s
/// shape) so a CI script can tune the retry budget without code
/// changes.
#[cfg(feature = "network")]
pub fn delete_var(
    path: &Path,
    app: &str,
    key: &str,
    max_retries: u32,
    retry_base_ms: u64,
    retry_cap_ms: u64,
) -> Result<()> {
    let state = load_state_optional(path)?;
    let app_name = resolve_app_name("edge env delete", app, state.as_ref())?;
    let edge_toml = EdgeToml::from_path(path)
        .with_context(|| "edge env delete requires edge.toml with [deployment] api = \"<url>\"")?;

    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    let interrupt = AtomicBool::new(false);
    call_with_retry(
        "env delete",
        || client.delete_env(&app_name, key),
        max_retries,
        retry_base_ms,
        retry_cap_ms,
        &interrupt,
    )?;

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
pub fn delete_var(
    _path: &Path,
    _app: &str,
    _key: &str,
    _max_retries: u32,
    _retry_base_ms: u64,
    _retry_cap_ms: u64,
) -> Result<()> {
    anyhow::bail!("env requires network support; rebuild with --features network")
}
