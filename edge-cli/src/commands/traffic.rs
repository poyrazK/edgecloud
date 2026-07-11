//! `edge traffic` — get or set traffic splits for an app.
//!
//! Retry-aware paths route through
//! `commands::retry::call_with_retry_no_interrupt` with the
//! centralized defaults `DEFAULT_MAX_RETRIES` /
//! `DEFAULT_RETRY_BASE_MS` / `DEFAULT_RETRY_CAP_MS` from
//! `commands::retry`. The `set` path is operator-tunable (main.rs
//! wires `--max-retries` / `--retry-base-ms` / `--retry-cap-ms` to
//! it); the `get` (Show) path uses the hardcoded defaults.

use anyhow::{Context, Result};
use clap::Subcommand;
use std::path::Path;

use super::retry::{
    call_with_retry_no_interrupt, DEFAULT_MAX_RETRIES, DEFAULT_RETRY_BASE_MS, DEFAULT_RETRY_CAP_MS,
};
use crate::api::ApiClient;
use crate::config::EdgeToml;
use crate::output;
use crate::state::State;

/// Subcommand enum for `edge traffic`. Mirrors the dispatch in
/// `main.rs::Command::Traffic`. Lives in this module so the
/// subcommand variants stay next to their implementations.
#[derive(Subcommand, Debug)]
pub enum TrafficAction {
    /// Print the current traffic split for the app.
    Show,
    /// Set a traffic split. Each argument is `deployment_id=weight`
    /// (e.g. `d_v1=95 d_v2=5`); weights must sum to 100.
    Set {
        /// Space-separated `deployment_id=weight` pairs.
        splits: Vec<String>,

        /// Maximum number of retries on transient failures (issue
        /// #571 propagation): 5xx, network errors, and 429. The
        /// total number of attempts is `1 + max_retries`.
        /// `--max-retries=0` (the default) disables retry (single
        /// attempt, fail fast) — `edge traffic set` is a
        /// PUT-replaces, but the user is unlikely to want retries
        /// on a one-shot CLI invocation; this default is tunable
        /// via the flag. Hard-capped at 20 by `value_parser` to
        /// match the exponent saturation in
        /// `commands::retry::compute_backoff_ms` (2^20 ≈ 1M ms ≈
        /// 17min is the worst-case single sleep).
        #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u32).range(0..=20))]
        max_retries: u32,

        /// Base backoff in milliseconds (issue #571 propagation).
        /// First retry sleeps `retry_base_ms × ±25%` jitter; each
        /// subsequent retry doubles the wait, capped at
        /// `retry-cap-ms`. Ignored when `--max-retries=0`.
        #[arg(long, default_value_t = 500)]
        retry_base_ms: u64,

        /// Maximum backoff in milliseconds (issue #571
        /// propagation). Caps the exponential backoff so a
        /// sustained outage doesn't pin a CI job for minutes.
        /// Hard-capped at 60_000 (60s) by `value_parser`. Ignored
        /// when `--max-retries=0`.
        #[arg(long, default_value_t = 8_000, value_parser = clap::value_parser!(u64).range(1..=60_000))]
        retry_cap_ms: u64,
    },
}

/// Get current traffic splits for the app. Naturally idempotent
/// (read); the call routes through [`call_with_retry`] with
/// hardcoded sensible defaults.
#[cfg(feature = "network")]
pub fn get(path: &Path) -> Result<()> {
    let state = State::load(path).context("no deployment found — run `edge deploy` first")?;
    let edge_toml = EdgeToml::from_path(path)?;

    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    let splits = call_with_retry_no_interrupt(
        "traffic get",
        || client.get_traffic(&state.app_name),
        DEFAULT_MAX_RETRIES,
        DEFAULT_RETRY_BASE_MS,
        DEFAULT_RETRY_CAP_MS,
    )?;

    if splits.is_empty() {
        output::info("No traffic splits configured — all traffic goes to the active deployment");
    } else {
        output::info(&format!("Traffic splits for {}:", state.app_name));
        for (id, weight) in &splits {
            println!("  {:5}%  {}", weight, id);
        }
        let total: u32 = splits.iter().map(|(_, w)| *w as u32).sum();
        println!("  -----");
        println!("  {:5}%  total", total);
        if total != 100 {
            output::error(&format!("WARNING: weights sum to {}%, not 100%", total));
        }
    }
    Ok(())
}

/// Set traffic splits for the app.
/// `splits` is a slice of "deployment_id=weight" strings, e.g. ["d_v1=95","d_v2=5"].
///
/// Naturally idempotent (PUT-replaces; the same final state
/// replays). The retry flags are operator-tunable — main.rs
/// wires `--max-retries` / `--retry-base-ms` / `--retry-cap-ms`
/// to this function (matches `edge deploy`'s shape) so a CI
/// script can tune the retry budget without code changes.
#[cfg(feature = "network")]
pub fn set(
    path: &Path,
    splits: &[(String, u8)],
    max_retries: u32,
    retry_base_ms: u64,
    retry_cap_ms: u64,
) -> Result<()> {
    let state = State::load(path).context("no deployment found — run `edge deploy` first")?;
    let edge_toml = EdgeToml::from_path(path)?;

    let total: u32 = splits.iter().map(|(_, w)| *w as u32).sum();
    if total != 100 {
        anyhow::bail!("weights must sum to 100 (got {})", total);
    }

    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    call_with_retry_no_interrupt(
        "traffic set",
        || client.set_traffic(&state.app_name, splits),
        max_retries,
        retry_base_ms,
        retry_cap_ms,
    )?;

    output::success(&format!("Traffic splits set for {}", state.app_name));
    for (id, weight) in splits {
        println!("  {:5}%  {}", weight, id);
    }
    Ok(())
}

#[cfg(not(feature = "network"))]
pub fn get(_path: &Path) -> Result<()> {
    anyhow::bail!("traffic get requires network support; rebuild with --features network")
}

#[cfg(not(feature = "network"))]
pub fn set(
    _path: &Path,
    _splits: &[(String, u8)],
    _max_retries: u32,
    _retry_base_ms: u64,
    _retry_cap_ms: u64,
) -> Result<()> {
    anyhow::bail!("traffic set requires network support; rebuild with --features network")
}
