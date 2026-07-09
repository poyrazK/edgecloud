//! `edge deployments` — list all deployments for the app.

use anyhow::{Context, Result};
use std::path::Path;

use crate::api::ApiClient;
use crate::config::EdgeToml;
use crate::state::State;

/// Server's default page size when `?limit=` is absent
/// (`internal/handler/deployment.go::List`). Mirrored here so the
/// footer math can compute `last = ceil(total / limit)` even
/// when the wire request carried no `?limit=` (i.e. the user
/// didn't pass `--limit`).
const SERVER_DEFAULT_LIMIT: u32 = 20;

/// List deployments for the app, paginated.
///
/// `page` is 1-indexed (page 1 is the first page). `limit == 0`
/// means "use the server default" — both on the wire and in the
/// footer math.
///
/// The footer is rendered **only** when there is more than one
/// page to navigate (mirrors the UX of `gh pr list` /
/// `kubectl get` — a single page of data renders silently).
#[cfg(feature = "network")]
pub fn run(path: &Path, page: u32, limit: u32) -> Result<()> {
    if page == 0 {
        anyhow::bail!("--page must be >= 1 (page numbers are 1-indexed)");
    }
    let state =
        State::load(path).with_context(|| "no deployment found — run `edge deploy` first")?;
    let edge_toml = EdgeToml::from_path(path)?;

    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;

    // offset = (page - 1) * limit; saturating guards against u32
    // overflow at the math boundary (e.g. `--page u32::MAX
    // --limit u32::MAX`) instead of panicking. `limit.max(1)`
    // means a user who explicitly passes `--limit 0` still gets
    // a finite offset on the wire; the wire handler is lenient
    // about that case (server-side `strconv.Atoi("0")` keeps the
    // default of 20) but the byte arithmetic here would
    // otherwise produce an offset of 0 forever — same as
    // page=1, so the call is equivalent either way.
    let offset = page.saturating_sub(1).saturating_mul(limit.max(1));
    let envelope = client.list_deployments_paginated(&state.app_name, limit, offset)?;

    if envelope.items.is_empty() {
        println!("No deployments found.");
    } else {
        println!("{:<12} {:<10} {:<20} URL", "ID", "STATUS", "CREATED");
        println!("{}", "-".repeat(60));
        for d in &envelope.items {
            println!(
                "{:<12} {:<10} {:<20} {}",
                d.id, d.status, d.created_at, d.url,
            );
        }
    }

    render_footer(&envelope, page, limit);

    Ok(())
}

/// Render the `page X of N · prev/next` footer. Gated on
/// `total > effective_limit` so a single-page result is silent.
///
/// `effective_limit` is the page size actually in use on this
/// request. When the user didn't pass `--limit` (so `limit == 0`
/// on the wire), the server defaults to `SERVER_DEFAULT_LIMIT`
/// and we mirror that here for the local math.
fn render_footer(envelope: &crate::api::client::DeploymentListEnvelope, page: u32, limit: u32) {
    let total: u32 = match u32::try_from(envelope.total) {
        Ok(n) => n,
        Err(_) => {
            // A negative `total` from the server is not a
            // realistic shape today (negative page totals are a
            // future "unknown" sentinel). Bail silently rather
            // than render a misleading footer.
            return;
        }
    };
    let effective_limit = if limit == 0 {
        SERVER_DEFAULT_LIMIT
    } else {
        limit
    };
    if total <= effective_limit {
        return;
    }

    // ceil(total / effective_limit) without floats. Saturating
    // for paranoia at `total == u32::MAX` (table covers
    // 4 billion+ deployments before we wrap).
    let last = total.saturating_sub(1) / effective_limit + 1;
    // Clamp `current` to `[1, last]` so `--page 99` on a 3-page
    // list shows "page 3 of 3" with a useful "prev:" hint
    // instead of inventing an obviously-broken "page 99 of 3".
    let current = page.max(1).min(last);

    println!();
    println!(
        "page {current} of {last}  ·  {total} deployment{}",
        if total == 1 { "" } else { "s" }
    );
    if current > 1 {
        println!("  prev: --page {}", current - 1);
    }
    if current < last {
        println!("  next: --page {}", current + 1);
    }
}

#[cfg(not(feature = "network"))]
pub fn run(_path: &Path, _page: u32, _limit: u32) -> Result<()> {
    anyhow::bail!("deployments requires network support; rebuild with --features network")
}
