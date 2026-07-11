//! `edge domains` — manage custom FQDNs bound to a deployment (issue #83).
//!
//! Subcommands:
//! - `add <app> <fqdn>` — bind a custom FQDN to an existing app.
//! - `list <app>` — list all custom FQDNs bound to the app.
//! - `check <app> <fqdn>` — fetch a single row's status (incl. any
//!   `last_error` from the v2 Caddy event hook).
//! - `remove <app> <fqdn>` — unbind a custom FQDN.
//!
//! The CLI does NOT persist domain state to `.edge/state.json` — every
//! invocation is a fresh query against the control plane. This matches
//! the design decision in the implementation plan: domains are
//! identity-level bindings, not deployment artifacts, and a stale
//! local copy would risk tenants seeing "domain not found" when
//! reality is the opposite.

use anyhow::{Context, Result};
use std::path::Path;
use std::sync::atomic::AtomicBool;

use super::retry::call_with_retry;
use crate::api::ApiClient;
use crate::config::EdgeToml;

/// Hardcoded sensible defaults for `edge domains` retryable paths
/// (issue #571 propagation). Matches `edge deploy`'s defaults.
const HARD_CODED_MAX_RETRIES: u32 = 3;
const HARD_CODED_RETRY_BASE_MS: u64 = 500;
const HARD_CODED_RETRY_CAP_MS: u64 = 8_000;

/// The four subcommands. Mirrors the route table in
/// `edge-control-plane/internal/handler/domain.go`.
#[derive(Debug)]
pub enum DomainsAction {
    Add { app: String, fqdn: String },
    List { app: String },
    Check { app: String, fqdn: String },
    Remove { app: String, fqdn: String },
}

impl DomainsAction {
    /// Run the action. `path` is the project root, used to load
    /// `edge.toml` (for the control plane URL).
    ///
    /// We intentionally do NOT require `.edge/state.json` here. Every
    /// subcommand takes an explicit `app` arg, so the state file's
    /// `app_name` is redundant. Forcing its presence would mean
    /// `edge domains add myotherapp foo.com` fails when `myotherapp`
    /// has never been deployed — a 404 from the control plane is
    /// the right "no such app" signal.
    ///
    /// **Phase-2 deferred (issue #571 follow-up).** `Add` is a POST
    /// insert and a retried POST could either 409 (duplicate
    /// `(app, fqdn)`) or insert a duplicate row depending on the
    /// server's race window — the right fix is CP-side
    /// `Idempotency-Key` schema extension. Until that lands, `Add`
    /// does NOT route through `call_with_retry`. The retry umbrella
    /// covers `Remove` (DELETE-by-fqdn; second call returns 404 with
    /// no side effect), `List` (read), and `Check` (read).
    #[cfg(feature = "network")]
    pub fn run(self, path: &Path) -> Result<()> {
        let edge_toml = EdgeToml::from_path(path)?;
        let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
        let domains = client.domains();

        match self {
            DomainsAction::Add { app, fqdn } => {
                let d = domains
                    .add(&app, &fqdn)
                    .with_context(|| format!("adding {fqdn} to {app}"))?;
                println!("Added {} (status: {})", d.fqdn, d.status);
                Ok(())
            }
            DomainsAction::List { app } => {
                let interrupt = AtomicBool::new(false);
                let rows = call_with_retry(
                    "domains list",
                    || domains.list(&app),
                    HARD_CODED_MAX_RETRIES,
                    HARD_CODED_RETRY_BASE_MS,
                    HARD_CODED_RETRY_CAP_MS,
                    &interrupt,
                )
                .with_context(|| format!("listing domains for {app}"))?;
                if rows.is_empty() {
                    println!("No custom domains for {app}.");
                } else {
                    println!("{:<12} {:<10} {:<24} CREATED", "ID", "STATUS", "FQDN");
                    println!("{}", "-".repeat(64));
                    for d in rows {
                        println!(
                            "{:<12} {:<10} {:<24} {}",
                            d.id, d.status, d.fqdn, d.created_at
                        );
                    }
                }
                Ok(())
            }
            DomainsAction::Check { app, fqdn } => {
                let interrupt = AtomicBool::new(false);
                let d = call_with_retry(
                    "domains check",
                    || domains.get(&app, &fqdn),
                    HARD_CODED_MAX_RETRIES,
                    HARD_CODED_RETRY_BASE_MS,
                    HARD_CODED_RETRY_CAP_MS,
                    &interrupt,
                )
                .with_context(|| format!("checking {fqdn} for {app}"))?;
                println!("FQDN:     {}", d.fqdn);
                println!("ID:       {}", d.id);
                println!("Status:   {}", d.status);
                println!("Created:  {}", d.created_at);
                if let Some(verified) = d.verified_at {
                    println!("Verified: {verified}");
                } else {
                    println!("Verified: (not yet — waiting on the v2 Caddy event hook)");
                }
                if let Some(err) = d.last_error {
                    println!("Last error: {err}");
                }
                Ok(())
            }
            DomainsAction::Remove { app, fqdn } => {
                let interrupt = AtomicBool::new(false);
                call_with_retry(
                    "domains remove",
                    || domains.remove(&app, &fqdn),
                    HARD_CODED_MAX_RETRIES,
                    HARD_CODED_RETRY_BASE_MS,
                    HARD_CODED_RETRY_CAP_MS,
                    &interrupt,
                )
                .with_context(|| format!("removing {fqdn} from {app}"))?;
                println!("Removed {fqdn} from {app}.");
                Ok(())
            }
        }
    }

    #[cfg(not(feature = "network"))]
    pub fn run(self, _path: &Path) -> Result<()> {
        anyhow::bail!("edge domains requires network support; rebuild with --features network")
    }
}
