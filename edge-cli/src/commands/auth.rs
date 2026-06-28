//! `edge auth {signup, login, whoami, logout, keys}` — manage local
//! credentials and (for `signup`) create a new tenant on the control plane;
//! for `keys create`, mint additional API keys for the current tenant.

use anyhow::{Context, Result};
use clap::Subcommand;
use std::env;
use std::io::{IsTerminal, Read};

use crate::api::{ApiClient, ApiError};
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
        /// Human-readable label for the API key minted for this tenant.
        /// Defaults to "default" (single-tenant CLI model).
        #[arg(long, default_value = "default")]
        key_name: String,
        /// Overwrite an existing saved key without prompting. Required
        /// when an `EDGE_API_KEY` env var is set and a saved key is
        /// present.
        #[arg(long)]
        force: bool,
    },
    /// Save an existing API key to the local config file.
    ///
    /// Reads from stdin if `--key` is not provided. For interactive use,
    /// pass `--no-echo` to suppress terminal echo when pasting the key
    /// (the pasted bytes stay out of scrollback and TTY captures). CI
    /// and pipe scripts should leave `--no-echo` off and pipe the key
    /// via stdin instead — `--no-echo` requires a controlling TTY.
    Login {
        /// API key value. If omitted, read from stdin (or `/dev/tty`
        /// when `--no-echo` is set).
        #[arg(long)]
        key: Option<String>,
        /// Disable terminal echo when reading the key. Requires a
        /// controlling TTY — incompatible with `<<<` and pipe stdin.
        #[arg(long)]
        no_echo: bool,
    },
    /// Show the currently-authenticated tenant and API key.
    Whoami,
    /// Remove the locally-saved API key.
    Logout,
    /// Manage additional API keys for the current tenant.
    Keys {
        #[command(subcommand)]
        action: KeysAction,
    },
}

/// Subcommands of `edge auth keys`.
#[derive(Subcommand)]
pub enum KeysAction {
    /// Mint a new API key for the current tenant. The new key is printed
    /// to stdout and is NOT saved to your local config — the key you
    /// used to authenticate this call keeps working.
    Create {
        /// Human-readable label for the new key.
        #[arg(long)]
        name: String,
        /// Role for the new key (owner | developer | viewer). Defaults
        /// to "developer", matching the server-side default.
        #[arg(long, default_value = "developer")]
        role: String,
    },
    /// List all API keys for the current tenant. Marks the key used
    /// to authenticate this CLI session with `* (current)`.
    List {
        /// Emit the raw JSON response to stdout (pretty-printed) for
        /// piping into `jq` etc.
        #[arg(long)]
        json: bool,
    },
    /// Revoke an API key by id. Refuses to revoke the key currently
    /// authenticating this CLI session unless `--force` is passed;
    /// prompts `y/N` unless `--yes` is passed in non-interactive shells.
    Revoke {
        /// API key id to revoke (e.g. `k_abc123`). Run
        /// `edge auth keys list` to find ids.
        #[arg(long)]
        id: String,
        /// Allow revoking the key currently used to authenticate
        /// this CLI session. The CLI will lose access until you
        /// re-login with another key.
        #[arg(long)]
        force: bool,
        /// Skip the `y/N` confirmation prompt. Required in
        /// non-interactive shells (CI, pipes, heredocs) where stdin
        /// is not a TTY.
        #[arg(long, short = 'y')]
        yes: bool,
    },
}

impl AuthAction {
    pub fn run(self) -> Result<()> {
        match self {
            AuthAction::Signup {
                name,
                plan,
                key_name,
                force,
            } => signup(&name, &plan, &key_name, force),
            AuthAction::Login { key, no_echo } => login(key.as_deref(), no_echo),
            AuthAction::Whoami => whoami(),
            AuthAction::Logout => logout(),
            AuthAction::Keys { action } => keys_run(action),
        }
    }
}

fn keys_run(action: KeysAction) -> Result<()> {
    match action {
        KeysAction::Create { name, role } => keys_create(&name, &role),
        KeysAction::List { json } => keys_list(json),
        KeysAction::Revoke { id, force, yes } => keys_revoke(&id, force, yes),
    }
}

/// `edge auth signup --name <NAME> [--plan <PLAN>] [--key-name <N>] [--force]`
///
/// Hits the public `POST /api/v1/tenants` endpoint, then persists the
/// returned API key to the local config file. Requires network.
#[cfg(feature = "network")]
fn signup(name: &str, plan: &str, key_name: &str, force: bool) -> Result<()> {
    let base_url = load_api_url("https://api.edgecloud.dev");
    let client = ApiClient::new_anonymous(base_url)?;

    // F1: surface the endpoint so the user sees where the request is
    // going. A developer pointing at staging or a local control plane
    // gets a chance to ctrl-C if the URL looks wrong.
    output::info(&format!("Endpoint: {url}", url = client.base_url()));
    output::section(&format!("Creating tenant '{name}'"));

    let created = client
        .tenants()
        .create(name, plan, key_name)
        .with_context(|| {
            format!(
                "signup failed (is the control plane reachable at {}?)",
                client.base_url()
            )
        })?;

    // F2: refuse to silently overwrite a saved key the user may still
    // be relying on. If EDGE_API_KEY is set in the env, the user is
    // actively using *that* key — destroying the saved one is
    // destructive. Otherwise we warn but proceed so a deliberate
    // re-signup is still possible. --force bypasses both checks.
    if !force && ApiKey::load_without_env().is_ok() {
        if env::var("EDGE_API_KEY").is_ok() {
            output::error(&format!(
                "an API key is already saved at {}; signup would overwrite it. \
                 unset EDGE_API_KEY, remove the file, or pass --force.",
                ApiKey::config_path()
                    .map(|p| p.display().to_string())
                    .unwrap_or_else(|| "<unknown>".into())
            ));
            anyhow::bail!("refusing to overwrite saved key while EDGE_API_KEY is set");
        }
        // Warn but proceed.
        output::warn("an API key is already saved locally; signup will replace it");
        output::hint("pass --force to silence this warning");
    }

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
fn signup(_name: &str, _plan: &str, _key_name: &str, _force: bool) -> Result<()> {
    anyhow::bail!("auth signup requires network support; rebuild with --features network")
}

/// `edge auth login [--key <KEY>] [--no-echo]`
///
/// After saving, calls `GET /api/v1/auth/whoami` to verify. Exits 1 if
/// the server rejects the saved key; warns but succeeds if unreachable.
fn login(key: Option<&str>, no_echo: bool) -> Result<()> {
    let key_value = match key {
        Some(k) => k.trim().to_string(),
        // `--no-echo` is silently a no-op when `--key` is provided:
        // `--key` short-circuits the read path, so the secure path
        // never runs. Friendlier than a hard error for scripts that
        // conditionally append flags (e.g. CI configs where
        // `--no-echo "$NO_ECHO"` evaluates to `--no-echo ""`).
        None if no_echo => rpassword::prompt_password("Paste your API key: ")
            .context("failed to read API key from /dev/tty (is one attached?)")?
            .trim()
            .to_string(),
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

    // Verify the just-saved key. Use `new_from_config_only` so an
    // exported `EDGE_API_KEY` cannot shadow the key we just saved
    // (issue #69 review finding F2).
    let base_url = load_api_url("https://api.edgecloud.dev");
    output::info(&format!("Endpoint: {base_url}"));
    let client = match ApiClient::new_from_config_only(base_url) {
        Ok(c) => c,
        Err(e) => {
            // No key in config (shouldn't happen - we just saved one).
            // Treat as transient: leave the saved file alone, warn.
            output::warn(&format!("could not read saved key for verification: {e}"));
            return Ok(());
        }
    };

    match client.auth().whoami() {
        Ok(info) => {
            output::success(&format!(
                "Logged in as {} ({}, plan: {})",
                info.tenant_name, info.tenant_id, info.plan
            ));
            Ok(())
        }
        Err(ApiError::Rejected { status, body }) => {
            output::error(&format!(
                "saved key rejected by server ({status}): {}",
                if body.is_empty() { "<no body>" } else { &body }
            ));
            if let Some(path) = ApiKey::config_path() {
                output::hint(&format!("the key was written to {}", path.display()));
            }
            output::hint("re-run `edge auth login` with the correct key to replace it");
            std::process::exit(1);
        }
        Err(ApiError::Transient { source }) => {
            output::warn(&format!(
                "could not verify key against the control plane: {source}"
            ));
            output::hint("the key was saved; run `edge auth whoami` later to verify");
            Ok(())
        }
    }
}

/// `edge auth whoami`
///
/// Calls `GET /api/v1/auth/whoami` and prints the result. Requires a saved
/// or env-supplied API key.
#[cfg(feature = "network")]
fn whoami() -> Result<()> {
    let base_url = load_api_url("https://api.edgecloud.dev");
    output::info(&format!("Endpoint: {base_url}"));
    let client = ApiClient::new(base_url)?;
    let info = client
        .auth()
        .whoami_anyhow()
        .with_context(|| "whoami failed")?;

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

/// `edge auth keys create --name <N> [--role <R>]`
///
/// Mints an additional API key for the currently-authenticated tenant.
/// Prints the raw token to stdout but does NOT overwrite the on-disk
/// key — the key that was used to authenticate this call keeps
/// working. The caller is responsible for storing the new token
/// somewhere safe (CI secret, password manager, etc.).
#[cfg(feature = "network")]
fn keys_create(name: &str, role: &str) -> Result<()> {
    let base_url = load_api_url("https://api.edgecloud.dev");
    output::info(&format!("Endpoint: {base_url}"));
    let client = ApiClient::new(base_url)?;

    let created = client
        .keys()
        .create(name, role)
        .with_context(|| "failed to create API key")?;

    output::success(&format!("Created key {}", created.id));
    println!("  ID:        {}", created.id);
    println!("  Name:      {}", created.name);
    println!("  Role:      {}", created.role);
    println!();
    output::warn("the raw token below is shown only once and was NOT saved to your config");
    println!("  Token:     {}", created.token);
    if let Some(path) = ApiKey::config_path() {
        output::hint(&format!(
            "your existing key at {} still works",
            path.display()
        ));
    }
    output::hint("store the new token now (e.g. EDGE_API_KEY=<token> edge deploy ...)");
    Ok(())
}

#[cfg(not(feature = "network"))]
fn keys_create(_name: &str, _role: &str) -> Result<()> {
    anyhow::bail!("auth keys requires network support; rebuild with --features network")
}

/// `edge auth keys list [--json]`
///
/// Calls `GET /api/v1/keys` and prints a 5-column table (ID / NAME /
/// ROLE / CREATED / NOTE) for the current tenant's keys. The key used
/// to authenticate this CLI session is annotated with `* (current)`
/// so the user can see which one would be lost on self-revoke.
///
/// `--json` emits the raw `Vec<APIKeySummary>` pretty-printed — used
/// by `edge keys list --json | jq '.[] | select(.role=="owner")'`.
#[cfg(feature = "network")]
fn keys_list(as_json: bool) -> Result<()> {
    let base_url = load_api_url("https://api.edgecloud.dev");
    output::info(&format!("Endpoint: {base_url}"));
    let client = ApiClient::new(base_url)?;

    // `current_id` is the bearer this CLI session is authenticated
    // with — the same string `whoami.api_key_id` would echo back
    // (server-side at edge-control-plane/internal/handler/auth.go).
    // Reading from disk avoids a network round-trip; transient
    // `ApiKey::load()` failures are swallowed (the table just loses
    // the `(current)` marker for that run).
    let current_id: Option<String> = ApiKey::load().ok().map(|k| k.0);

    let keys = client
        .keys()
        .list()
        .with_context(|| "failed to list API keys")?;

    if as_json {
        println!("{}", serde_json::to_string_pretty(&keys)?);
        return Ok(());
    }

    if keys.is_empty() {
        println!("No API keys.");
        return Ok(());
    }

    println!(
        "{:<38} {:<24} {:<12} {:<22} NOTE",
        "ID", "NAME", "ROLE", "CREATED"
    );
    println!("{}", "-".repeat(102));
    for k in &keys {
        let is_current = current_id.as_deref() == Some(k.id.as_str());
        let note = if is_current { "(current)" } else { "" };
        println!(
            "{:<38} {:<24} {:<12} {:<22} {}",
            k.id, k.name, k.role, k.created_at, note
        );
    }
    Ok(())
}

#[cfg(not(feature = "network"))]
fn keys_list(_as_json: bool) -> Result<()> {
    anyhow::bail!("auth keys requires network support; rebuild with --features network")
}

/// `edge auth keys revoke --id <k_xxx> [--force] [--yes]`
///
/// Hard-deletes the API key with the given id via
/// `DELETE /api/v1/keys/{id}`. Refuses to revoke the key currently
/// used to authenticate this CLI session (unless `--force`) and
/// prompts `y/N` (unless `--yes` or non-TTY). The on-disk saved key
/// is NOT auto-cleared on self-revoke — if the user revoked the key
/// their CLI was using, they must run `edge auth login` with a new
/// key.
#[cfg(feature = "network")]
fn keys_revoke(id: &str, force: bool, yes: bool) -> Result<()> {
    let base_url = load_api_url("https://api.edgecloud.dev");
    output::info(&format!("Endpoint: {base_url}"));
    let client = ApiClient::new(base_url)?;

    // Self-revoke guard. whoami tells us which key the bearer token
    // resolves to; if it matches the target, refuse unless --force.
    // If whoami itself fails, fall through and let the server decide
    // — the local check is a UX guard, not a security boundary.
    if !force {
        if let Ok(info) = client.auth().whoami() {
            if info.api_key_id == id {
                output::error(&format!(
                    "refusing to revoke the key currently used to authenticate this CLI \
                     session (id: {}, name: {}); pass --force to override",
                    info.api_key_id, info.api_key_name
                ));
                std::process::exit(2);
            }
        }
    }

    // Confirmation prompt. Only on a TTY (same precedent as
    // `edge auth login --no-echo`). Non-interactive shells must
    // pass --yes — refusing is friendlier than silently bypassing.
    if !yes {
        if !std::io::stderr().is_terminal() {
            anyhow::bail!(
                "refusing to revoke without confirmation: pass --yes in non-interactive shells"
            );
        }
        let label = id.to_string();
        let confirmed = output::confirm(&format!("Revoke key {label}? [y/N] "))?;
        if !confirmed {
            output::info("aborted");
            return Ok(());
        }
    }

    client
        .keys()
        .revoke(id)
        .map_err(|e| match e {
            ApiError::Rejected { status, body } => {
                anyhow::anyhow!("keys revoke failed: {status} {body}")
            }
            ApiError::Transient { source } => source,
        })
        .with_context(|| format!("revoking key {id}"))?;

    output::success(&format!("Revoked key {id}"));

    // If the user just revoked the key this CLI session is
    // authenticated with, warn them — they will not be able to run
    // further CLI commands until they re-login. Compare against the
    // bearer (which the self-guard at line 446-456 already read) so
    // the warning and the guard agree on the source. When EDGE_API_KEY
    // and the on-disk key differ, this fires only if the env-backed
    // bearer is the one revoked — the prior `load_without_env()`-based
    // warning misfired in that case (PR #163 review finding F1).
    //
    // We deliberately do NOT auto-clear the on-disk key here: --force
    // users may be rotating intentionally (e.g. CI), and clearing the
    // config could race with their intent.
    if client.bearer() == id {
        if let Some(path) = ApiKey::config_path() {
            output::warn(&format!(
                "the key just revoked is the one this CLI session is authenticated with \
                 (saved at {}); run `edge auth login` with a new key",
                path.display()
            ));
        }
    }

    Ok(())
}

#[cfg(not(feature = "network"))]
fn keys_revoke(_id: &str, _force: bool, _yes: bool) -> Result<()> {
    anyhow::bail!("auth keys requires network support; rebuild with --features network")
}
