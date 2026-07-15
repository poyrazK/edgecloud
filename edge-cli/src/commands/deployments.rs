//! `edge deployments` — list all deployments for the app.

use anyhow::{Context, Result};
use std::path::Path;

use crate::api::ApiClient;
use crate::config::EdgeToml;
use crate::state::State;

/// `edge deployments` — list every deployment for the app, in a
/// single cursor walk.
///
/// Issue #709: the server is cursor-only now; `?offset=` returns 400
/// and `--page` is gone from the CLI. The walker (see
/// [`ApiClient::list_all_deployments`]) terminates when the server
/// returns a `null` / absent `next_cursor`.
///
/// `limit == 0` means "use the server default (20)" — both on the
/// wire and in the page-size argument the walker threads through.
#[cfg(feature = "network")]
pub fn run(path: &Path, limit: u32) -> Result<()> {
    let state =
        State::load(path).with_context(|| "no deployment found — run `edge deploy` first")?;
    let edge_toml = EdgeToml::from_path(path)?;

    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    let items = client.list_all_deployments(&state.app_name, limit)?;

    if items.is_empty() {
        println!("No deployments found.");
    } else {
        println!("{:<12} {:<10} {:<20} URL", "ID", "STATUS", "CREATED");
        println!("{}", "-".repeat(60));
        for d in &items {
            println!(
                "{:<12} {:<10} {:<20} {}",
                d.id, d.status, d.created_at, d.url,
            );
        }
        // Issue #709 — only the cursor walker exists; no page-of-N
        // footer. The walker prints once on termination, so the
        // total is implicit ("here's every deployment"). A
        // single-line "N deployments" header keeps the table
        // output symmetrical with `edge apps`.
        println!();
        println!(
            "{} deployment{}",
            items.len(),
            if items.len() == 1 { "" } else { "s" }
        );
    }

    Ok(())
}

#[cfg(not(feature = "network"))]
pub fn run(_path: &Path, _limit: u32) -> Result<()> {
    anyhow::bail!("deployments requires network support; rebuild with --features network")
}
