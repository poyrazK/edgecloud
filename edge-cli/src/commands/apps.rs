//! `edge apps` — list all apps, create a new one, show details, or
//! delete (issue #573).
//!
//! * `edge apps` (no subcommand) → list all apps for the tenant.
//! * `edge apps create <name>` → create a new app.
//! * `edge apps get <name>` → show details for a specific app.
//! * `edge apps delete <name> --yes` → hard-delete an app (owner-role
//!   required; irreversible cascade on the server side).

use anyhow::{Context, Result};
use std::io::IsTerminal;
use std::path::Path;

use super::retry::{
    call_with_retry_no_interrupt, DEFAULT_MAX_RETRIES, DEFAULT_RETRY_BASE_MS, DEFAULT_RETRY_CAP_MS,
};
use crate::api::{ApiClient, ApiError};
use crate::config::EdgeToml;
use crate::output;

/// List all apps for the authenticated tenant.
#[cfg(feature = "network")]
pub fn list(path: &Path) -> Result<()> {
    let edge_toml = EdgeToml::from_path(path)?;
    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    let apps = client.list_apps()?;

    if apps.is_empty() {
        println!("No apps found.");
    } else {
        println!("{:<24} {:<24} CREATED", "ID", "NAME");
        println!("{}", "-".repeat(64));
        for a in apps {
            println!("{:<24} {:<24} {}", a.id, a.name, a.created_at);
        }
    }
    Ok(())
}

#[cfg(not(feature = "network"))]
pub fn list(_path: &Path) -> Result<()> {
    anyhow::bail!("edge apps requires network support; rebuild with --features network")
}

/// Create a new app.
#[cfg(feature = "network")]
pub fn create(path: &Path, name: &str, description: Option<&str>) -> Result<()> {
    let edge_toml = EdgeToml::from_path(path)?;
    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    let app = client.create_app(name, description)?;

    output::success(&format!("Created app '{}'", app.name));
    println!("  ID:          {}", app.id);
    if let Some(ref d) = app.description {
        println!("  Description: {d}");
    }
    println!("  Created:     {}", app.created_at);
    Ok(())
}

#[cfg(not(feature = "network"))]
pub fn create(_path: &Path, _name: &str, _description: Option<&str>) -> Result<()> {
    anyhow::bail!("edge apps requires network support; rebuild with --features network")
}

/// Show details for a specific app.
#[cfg(feature = "network")]
pub fn get(path: &Path, name: &str) -> Result<()> {
    let edge_toml = EdgeToml::from_path(path)?;
    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    let app = client
        .get_app(name)
        .with_context(|| format!("fetching app '{name}'"))?;

    println!("ID:          {}", app.id);
    println!("Name:        {}", app.name);
    println!("Tenant ID:   {}", app.tenant_id);
    match app.description {
        Some(ref d) => println!("Description: {d}"),
        None => println!("Description: (none)"),
    }
    println!("Created:     {}", app.created_at);
    Ok(())
}

#[cfg(not(feature = "network"))]
pub fn get(_path: &Path, _name: &str) -> Result<()> {
    anyhow::bail!("edge apps requires network support; rebuild with --features network")
}

/// Hard-delete an app. Owner-role gated server-side
/// (`RequireRole("owner")` on `/api/v1/admin/apps/{appName}`); the
/// `check_owner_role` pre-flight surfaces an actionable error if
/// the loaded bearer is not owner-flavored, before the destructive
/// round-trip lands.
///
/// `--yes` is required so a typo'd app name can't cascade-delete
/// the wrong tenant's app. The server cascade is irreversible:
/// artifact blobs, env rows, active deployments, plus a
/// `task_purge` outbox row that tears down the per-app KV / cache /
/// scheduler dirs on every worker.
///
/// The DELETE is naturally idempotent (second call returns 404 with
/// no side effect), so it routes through `call_with_retry_no_interrupt`
/// — same justification as `edge webhooks remove`. Issue #573.
#[cfg(feature = "network")]
pub fn delete(path: &Path, name: &str, yes: bool) -> Result<()> {
    // Confirmation prompt. Matches the `keys_revoke` UX (commands/
    // auth.rs:520-532): non-TTY shells must pass --yes (no stdin
    // to read from); on a TTY we prompt for `y/N`. The cascade
    // reaches beyond the CP — task_purge tears down worker
    // in-memory + on-disk dirs on every region — but the prompt
    // is the established project UX, so we mirror it. The `--yes`
    // flag still works for CI / scripting where a confirm would
    // hang the pipeline.
    if !yes {
        if !std::io::stderr().is_terminal() {
            anyhow::bail!(
                "apps delete is irreversible — pass --yes (or -y) in non-interactive shells.\n\
                 This will:\n  - delete the app row + all deployment rows\n  - delete env vars\n\
                  - delete active deployments + artifact blobs\n  - publish a task_purge to every worker \
                 (which tears down per-app KV/cache/scheduler dirs)"
            );
        }
        let confirmed = output::confirm(&format!("Delete app '{name}'? [y/N] "))?;
        if !confirmed {
            output::info("aborted");
            return Ok(());
        }
    }

    let edge_toml = EdgeToml::from_path(path)?;
    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;

    // Pre-flight role check: the CLI has no separate admin-token
    // path (ApiKey::load returns a single bearer), so the user must
    // already be authenticated with an owner-role key. The CP's
    // `RequireRole("owner")` middleware returns a generic 403 with
    // no hint; we surface a clear "you need an owner key" message
    // before the round-trip. Costs one extra GET — fine for a
    // destructive action.
    check_owner_role(&client)?;

    call_with_retry_no_interrupt(
        "apps delete",
        || client.delete_app(name).map_err(anyhow::Error::new),
        DEFAULT_MAX_RETRIES,
        DEFAULT_RETRY_BASE_MS,
        DEFAULT_RETRY_CAP_MS,
    )
    .map_err(|e| match find_api_error(&e) {
        Some(ApiError::Rejected { status, .. }) if status.as_u16() == 404 => anyhow::anyhow!(
            "app '{name}' not found (404)\n  hint: run `edge apps` to list your apps, or check spelling"
        ),
        Some(ApiError::Rejected { status, .. }) if status.as_u16() == 401 => anyhow::anyhow!(
            "authentication failed (401) — your API key is invalid or expired\n  \
             hint: run `edge auth login` to re-authenticate, or `edge auth keys create` to mint a new key"
        ),
        Some(ApiError::Rejected { status, body }) => {
            anyhow::anyhow!("apps delete rejected by server ({status}): {body}")
        }
        Some(ApiError::Transient { source }) => {
            anyhow::anyhow!("apps delete failed after retries: {source}")
        }
        None => e.context(format!("deleting app '{name}'")),
    })?;

    println!("Deleted app '{name}'.");
    Ok(())
}

#[cfg(not(feature = "network"))]
pub fn delete(_path: &Path, _name: &str, _yes: bool) -> Result<()> {
    anyhow::bail!("edge apps delete requires network support; rebuild with --features network")
}

/// Pre-flight role guard for `apps delete`. Calls `whoami` and
/// bails with a multi-line actionable error if the loaded bearer
/// is not owner-flavored. The hint cites `edge auth keys create
/// --role owner` (to mint a new owner key) since that is the
/// in-CLI path for upgrading role; the alternative is re-running
/// `edge auth login` with an existing owner key.
fn check_owner_role(client: &ApiClient) -> Result<()> {
    let info = client
        .auth()
        .whoami_anyhow()
        .context("apps delete requires whoami — failed to fetch caller identity")?;
    if info.role == "owner" {
        return Ok(());
    }
    anyhow::bail!(
        "apps delete requires an owner-role API key\n\
         current key role: {role}\n\
         mint an owner key with: edge auth keys create --role owner <name>\n\
         or re-run `edge auth login` with an existing owner key",
        role = info.role
    )
}

/// Walk `e.chain()` and return the first `ApiError` found. The
/// `call_with_retry_no_interrupt` wrapper preserves the typed
/// `ApiError` through the chain (we wrap with
/// `anyhow::Error::new(api_err)` per `commands/retry.rs:143`),
/// so this downcast is reliable. Used by `delete()` to surface
/// dedicated 404/401 messages instead of the generic
/// `rejected by server: <status> <body>` from `ApiError::Display`.
fn find_api_error(e: &anyhow::Error) -> Option<&ApiError> {
    e.chain().find_map(|c| c.downcast_ref::<ApiError>())
}
