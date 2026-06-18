//! `edge auth {signup, login, whoami, logout}` — manage local credentials
//! and (for `signup`) create a new tenant on the control plane.

use anyhow::{Context, Result};
use clap::Subcommand;
use std::io::Read;

use crate::api::ApiClient;
use crate::config::{load_api_url, ApiKey};
use crate::output;

/// Subcommands of `edge auth`.
#[derive(Subcommand)]
pub enum AuthAction {
    /// Create a new tenant on the control plane and save the API key locally.
    Signup {
        /// Tenant display name.
        #[arg(long)]
        name: String,
        /// Plan tier. Defaults to "free".
        #[arg(long, default_value = "free")]
        plan: String,
    },
    /// Save an existing API key to the local config file. Reads from
    /// stdin if `--key` is not provided.
    Login {
        /// API key value. If omitted, read from stdin.
        #[arg(long)]
        key: Option<String>,
    },
    /// Show the currently-authenticated tenant and API key.
    Whoami,
    /// Remove the locally-saved API key.
    Logout,
}

impl AuthAction {
    pub fn run(self) -> Result<()> {
        match self {
            AuthAction::Signup { name, plan } => signup(&name, &plan),
            AuthAction::Login { key } => login(key.as_deref()),
            AuthAction::Whoami => whoami(),
            AuthAction::Logout => logout(),
        }
    }
}

/// `edge auth signup --name <NAME> [--plan <PLAN>]`
///
/// Hits the public `POST /api/tenants` endpoint, then persists the
/// returned API key to the local config file. Requires network.
#[cfg(feature = "network")]
fn signup(name: &str, plan: &str) -> Result<()> {
    let base_url = load_api_url("https://api.edgecloud.dev");
    let client = ApiClient::new_anonymous(base_url)?;

    output::section(&format!("Creating tenant '{name}'"));
    let created = client.tenants().create(name, plan).with_context(|| {
        format!(
            "signup failed (is the control plane reachable at {}?)",
            client.base_url()
        )
    })?;

    // Persist the key to the user's config file. We do this even though
    // the server has just minted it — that is the whole point of signup.
    let key = ApiKey(created.api_key.clone());
    key.save()
        .with_context(|| "tenant created but failed to save API key to local config")?;

    output::success(&format!("Tenant {} created", created.tenant_id));
    println!("  Tenant ID:   {}", created.tenant_id);
    println!("  API key:     {}", created.api_key);
    if let Some(path) = ApiKey::config_path() {
        output::hint(&format!("Saved to {}", path.display()));
    }
    output::hint("Next: edge build && edge deploy");
    Ok(())
}

#[cfg(not(feature = "network"))]
fn signup(_name: &str, _plan: &str) -> Result<()> {
    anyhow::bail!("auth signup requires network support; rebuild with --features network")
}

/// `edge auth login [--key <KEY>]`
///
/// Saves a key to the local config file. Reads from stdin if `--key` is
/// not provided. After saving, attempts to call `whoami` to confirm the
/// key works. If the server is unreachable, still succeeds on the local
/// write (the user may be working offline).
fn login(key: Option<&str>) -> Result<()> {
    let key_value = match key {
        Some(k) => k.trim().to_string(),
        None => {
            eprintln!("Paste your API key (input is read from stdin, Ctrl-D to cancel):");
            let mut buf = String::new();
            std::io::stdin()
                .lock()
                .read_to_string(&mut buf)
                .context("failed to read API key from stdin")?;
            buf.trim().to_string()
        }
    };

    if key_value.is_empty() {
        anyhow::bail!("API key is empty");
    }

    ApiKey(key_value.clone())
        .save()
        .context("failed to save API key to local config")?;

    output::success("API key saved");
    if let Some(path) = ApiKey::config_path() {
        output::hint(&format!("Saved to {}", path.display()));
    }

    // Try to confirm the key works by calling whoami. We treat any
    // network/HTTP failure as a soft warning so the local write is
    // never rolled back — the user can run `edge auth whoami` later.
    match whoami() {
        Ok(()) => {}
        Err(e) => {
            output::warn(&format!(
                "could not verify key against the control plane: {e}"
            ));
        }
    }
    Ok(())
}

/// `edge auth whoami`
///
/// Calls `GET /api/auth/whoami` and prints the result. Requires a saved
/// or env-supplied API key.
#[cfg(feature = "network")]
fn whoami() -> Result<()> {
    let base_url = load_api_url("https://api.edgecloud.dev");
    let client = ApiClient::new(base_url)?;
    let info = client.auth().whoami().with_context(|| "whoami failed")?;

    println!("  Tenant:    {} ({})", info.tenant_name, info.tenant_id);
    println!("  Plan:      {}", info.plan);
    println!("  API key:   {} ({})", info.api_key_name, info.api_key_id);
    println!("  Role:      {}", info.role);
    println!("  Created:   {}", info.created_at);
    Ok(())
}

#[cfg(not(feature = "network"))]
fn whoami() -> Result<()> {
    anyhow::bail!("auth whoami requires network support; rebuild with --features network")
}

/// `edge auth logout`
///
/// Removes the locally-saved API key. Idempotent: succeeds even if no
/// key was saved.
fn logout() -> Result<()> {
    ApiKey::clear().context("failed to clear API key from local config")?;
    output::success("Logged out");
    Ok(())
}
