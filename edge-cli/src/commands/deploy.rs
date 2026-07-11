//! `edge deploy` — upload the artifact to the control plane, or activate a stored one.

use anyhow::{Context, Result};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::build;
use super::logs::interruptible_sleep;
use super::state_io::load_state_optional;
use crate::api::client::DeployResponse;
use crate::api::{ApiClient, ApiError};
use crate::config::EdgeToml;
use crate::output;
use crate::state::{BuildMetadata, State};
use crate::LangArg;

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
///
/// `lang` is the source-language override. `Some(l)` requires the
/// toml's `[project] language` to also resolve to `l`; mismatches
/// surface as a clear error so a stale `rust` path can't be served
/// for a Javy artifact. `None` defers to the toml entirely.
///
/// `idempotency_key` is the issue-#52 Idempotency-Key to forward
/// to the server. `Some(uuid)` passes through verbatim (CI uses
/// this to dedupe retried jobs); `None` triggers auto-mint in
/// `run_upload` so a laptop retry on a transient network error
/// replays the original deployment. `run_activate` ignores the
/// value — activation is a separate endpoint and the Idempotency-
/// Key header is wired only on the upload path.
///
/// `max_retries` / `retry_base_ms` / `retry_cap_ms` (issue #571)
/// configure the transient-error retry loop on the upload path.
/// `run_activate` ignores them — the activate endpoint is a
/// separate idempotent request keyed on the deployment id, not
/// the artifact, so the same retry policy doesn't apply.
#[allow(clippy::too_many_arguments)]
#[cfg(feature = "network")]
pub fn run(
    path: &Path,
    app: &str,
    id: Option<&str>,
    regions: &[String],
    auto_rollback: bool,
    replicas: usize,
    file: Option<&Path>,
    lang: Option<LangArg>,
    preview_opts: Option<&crate::api::PreviewOpts>,
    idempotency_key: Option<&str>,
    max_retries: u32,
    retry_base_ms: u64,
    retry_cap_ms: u64,
) -> Result<()> {
    if let Some(deployment_id) = id {
        return run_activate(path, app, deployment_id);
    }
    run_upload(
        path,
        app,
        regions,
        auto_rollback,
        replicas,
        file,
        lang,
        preview_opts,
        idempotency_key,
        max_retries,
        retry_base_ms,
        retry_cap_ms,
    )
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
/// `lang`: optional source-language override. `None` reads the
/// project's `edge.toml`; `Some(l)` cross-checks the toml against `l`
/// before resolving the artifact path.
/// `preview_opts`: when `Some`, the deploy is stamped as a preview
/// (issue #308) — the server scopes its KV/cache/scheduler under a
/// per-preview subdirectory and stamps `EDGE_PREVIEW_PR_NUMBER` into
/// the guest env. `None` for normal deploys.
///
/// `idempotency_key`: optional issue-#52 header. `Some(uuid)` is
/// forwarded verbatim. `None` auto-mints a fresh UUID v4 so a CLI
/// retry on a transient network error replays the original
/// deployment instead of minting a duplicate. The minted UUID is
/// echoed to the user via `output::info` so a CI script that
/// wants to retry the exact same key can grab it from logs.
///
/// `max_retries` / `retry_base_ms` / `retry_cap_ms` (issue #571):
/// total attempts = `1 + max_retries`. Backoff doubles per attempt
/// (`base × 2^(attempt-1)`), capped at `retry_cap_ms`, with ±25%
/// jitter via a hand-rolled xorshift RNG (no `rand` dep). Each
/// retry re-calls `client.deploy(...)` with the same `wasm_bytes`
/// and the same `idempotency_key` so the server's
/// Idempotency-Key replay path returns the cached
/// `deployment_id` (200) on the next attempt instead of minting
/// a duplicate. The 429 status is also retried (the deploy handler
/// doesn't emit `Retry-After`); all other 4xx are surfaced
/// immediately as deterministic failures. The backoff sleep is
/// wired through `interruptible_sleep` so Ctrl-C exits within
/// ~100ms instead of up to `retry_cap_ms`.
#[cfg(feature = "network")]
#[allow(clippy::too_many_arguments)]
fn run_upload(
    path: &Path,
    app: &str,
    regions: &[String],
    auto_rollback: bool,
    replicas: usize,
    file: Option<&Path>,
    lang: Option<LangArg>,
    preview_opts: Option<&crate::api::PreviewOpts>,
    idempotency_key: Option<&str>,
    max_retries: u32,
    retry_base_ms: u64,
    retry_cap_ms: u64,
) -> Result<()> {
    let edge_toml = EdgeToml::from_path(path)?;
    let app_name = if !app.is_empty() {
        app.to_string()
    } else {
        edge_toml.project.name.clone()
    };
    let toml_lang = edge_toml.project.language_or_default();
    // Resolve the language used for the artifact-path lookup.
    // `Some(l)` overrides only when it agrees with the toml — that
    // mirrors `edge build`'s cross-check (finding 2 of the PR #221
    // review) and prevents "stale toml says rust, Javy artifact
    // exists" from silently serving the wrong file. `None` defers
    // to the toml, which is the authoritative source.
    let effective_lang = match lang {
        Some(flag_lang) if flag_lang.as_str() != toml_lang => {
            anyhow::bail!(
                "`--lang {flag}` does not match `[project] language = {toml_value:?}` in edge.toml. \
                 Re-run with `--lang {toml_value}` (or remove the `language` line from edge.toml) so \
                 build and deploy stay in sync.",
                flag = flag_lang.as_str(),
                toml_value = toml_lang,
            );
        }
        Some(flag_lang) => flag_lang.as_str().to_string(),
        None => toml_lang.to_string(),
    };
    // Artifact path is language-aware (issue #317): Rust projects
    // land at `target/wasm32-wasip2/release/<name>.wasm`, JS at
    // `target/javy/<name>.wasm`. `build::path_for` is the single
    // source of truth — `commands::build::run` writes through it,
    // we read through it, so the two paths can never disagree.
    // `--file` still overrides when present.
    let artifact = match file {
        Some(f) => f.to_path_buf(),
        None => build::path_for(path, &app_name, &effective_lang)
            .context("resolving deploy artifact path")?,
    };

    let wasm_bytes = std::fs::read(&artifact).map_err(|e| {
        output::error(&format!("failed to read {}: {}", artifact.display(), e));
        e
    })?;

    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    // Issue #307 PR2: read `.edge/build_metadata.json` (written by
    // `edge build`) and forward it as the multipart `build_metadata`
    // part. The control plane uses these fields to populate the SLSA
    // L1 envelope's `predicate.invocation.parameters` and
    // `predicate.buildTools[]` entries. Absent file = "no toolchain
    // info available" — the server still builds an envelope, just with
    // empty `buildTools[]` and a null `invocation.parameters`.
    let build_metadata_json = match BuildMetadata::load_opt(path) {
        Ok(Some(bm)) => Some(serde_json::to_value(&bm).context("serialize BuildMetadata")?),
        Ok(None) => None,
        Err(e) => {
            // A corrupt build_metadata.json shouldn't fail the
            // deploy — log and continue without it. The deploy
            // envelope will have empty buildTools[].
            eprintln!("warning: failed to read build_metadata.json: {e}");
            None
        }
    };
    // Issue #52: resolve the Idempotency-Key header. When the user
    // didn't pass `--idempotency-key`, mint a fresh UUID v4 so a
    // CLI retry on a transient network error replays the original
    // deployment instead of minting a duplicate. The minted value
    // is echoed on stderr so a CI script that wants to grep the
    // exact key out of deploy logs can.
    let idem_key_owned: String;
    let idem_key_slice: &str = match idempotency_key {
        Some(s) if !s.is_empty() => s,
        _ => {
            idem_key_owned = uuid::Uuid::new_v4().to_string();
            output::info(&format!("idempotency-key: {idem_key_owned}"));
            idem_key_owned.as_str()
        }
    };

    // Issue #571: install a SIGINT handler so the retry backoff is
    // interruptible. Without this, an 8s default `retry_cap_ms` blocks
    // Ctrl-C for up to 8s. The flag is shared with the retry helper so
    // a Ctrl-C during the sleep unblocks the loop and the next
    // `client.deploy(...)` call (or the loop's exit branch) returns
    // promptly. Mirrors `commands/logs.rs:178-183`.
    let interrupt = Arc::new(AtomicBool::new(false));
    let interrupt_for_handler = interrupt.clone();
    ctrlc::set_handler(move || {
        interrupt_for_handler.store(true, Ordering::SeqCst);
    })
    .context("installing SIGINT handler for deploy retry")?;

    let resp = deploy_with_retry(
        || {
            client.deploy(
                &app_name,
                &wasm_bytes,
                regions,
                auto_rollback,
                replicas,
                build_metadata_json.as_ref(),
                preview_opts,
                idem_key_slice,
            )
        },
        max_retries,
        retry_base_ms,
        retry_cap_ms,
        &interrupt,
    )?;

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
        deployment_id: resp.id.clone(),
        app_name: app_name.clone(),
        live_url: live_url.clone(),
        regions: persisted_regions,
        desired_replicas: resp.desired_replicas,
        // issue #308: persist preview metadata so `edge status` can
        // surface "expires in 5d 12h" + "PR #123" without re-querying
        // the server. Defaults (empty / 0) round-trip cleanly on
        // non-preview deploys.
        preview_id: resp.preview_id.clone(),
        preview_pr_number: resp.preview_pr_number,
        preview_expires_at: resp.preview_expires_at.clone(),
    };
    state.save(path)?;

    // Issue #307 PR2: persist the signed SLSA L1 envelope to
    // `.edge/attestation.json` so the user can verify it later (or
    // a downstream audit pipeline can ingest it). The server returns
    // the full DSSE wrapper as JSON. We re-serialize pretty so the
    // file is human-readable.
    if let Some(att) = resp.build_attestation.as_ref() {
        let attestation_path = path.join(".edge").join("attestation.json");
        std::fs::create_dir_all(path.join(".edge")).context("create .edge/ for attestation")?;
        let raw = serde_json::to_string_pretty(att).context("serialize build_attestation")?;
        std::fs::write(&attestation_path, raw)
            .with_context(|| format!("failed to write {}", attestation_path.display()))?;
    }

    output::success("Deployed successfully");
    println!("  URL: {}", resp.url);
    Ok(())
}

/// Retry loop around an idempotent single-shot deploy attempt
/// closure (issue #571).
///
/// The closure MUST carry the **same** `Idempotency-Key` and the
/// **same** `wasm_bytes` slice across every call — `ApiClient::deploy`
/// rebuilds the multipart `Form` (and the inner `Cursor`) per call,
/// so the body is not exhausted across retries (the multipart
/// reader is one-shot). The Idempotency-Key is preserved
/// byte-for-byte so the server's replay path returns the cached
/// `deployment_id` (200) on the next attempt instead of minting a
/// duplicate row (the server contract at
/// `edge-control-plane/internal/handler/deployment.go:199-495`).
///
/// `max_retries` is the number of retries *after* the first
/// attempt — `--max-retries=0` means a single attempt, no retries
/// (the `attempt > max_retries` guard short-circuits on the first
/// failure). Backoff grows as `base × 2^(attempt-1)`, capped at
/// `cap_ms`, with ±25% jitter. The retry path **deliberately does
/// not** parse `Retry-After` — the control plane's deploy handler
/// doesn't emit it (per
/// `edge-control-plane/internal/handler/deployment.go::Deploy`),
/// and a future contributor adding header parsing should not
/// regress the bounded backoff (this contract is pinned by
/// `retry_loop_does_not_observe_retry_after_header` in `mod
/// tests` below).
///
/// Only retries on `ApiError::is_retryable()` results — 4xx
/// (other than 429) surface immediately as deterministic failures.
/// The 429 case is handled by the `is_retryable()` override on
/// `Rejected { 429 }`.
///
/// Taking a closure (rather than `&ApiClient`) lets the unit
/// tests in the `tests` module below drive the loop with
/// canned sequences without spinning up wiremock or a real
/// server. The single prod caller `run_upload` passes a closure
/// that delegates to `client.deploy(...)` with the same
/// borrowed arguments on every call.
#[cfg(feature = "network")]
#[allow(clippy::too_many_arguments)]
fn deploy_with_retry<F>(
    attempt: F,
    max_retries: u32,
    retry_base_ms: u64,
    retry_cap_ms: u64,
    interrupt: &AtomicBool,
) -> Result<DeployResponse>
where
    F: FnMut() -> Result<DeployResponse>,
{
    let mut attempt_fn = attempt;
    let mut attempt_no: u32 = 0;
    loop {
        attempt_no += 1;
        match attempt_fn() {
            Ok(resp) => return Ok(resp),
            Err(e) if attempt_no > max_retries || !is_anyhow_retryable(&e) => return Err(e),
            Err(e) => {
                let backoff_ms = compute_backoff_ms(attempt_no, retry_base_ms, retry_cap_ms);
                output::warn(&format!(
                    "retrying deploy (attempt {attempt_no}/{max_retries} after {backoff_ms}ms): {e}"
                ));
                interruptible_sleep(Duration::from_millis(backoff_ms), interrupt);
            }
        }
    }
}

/// Walk the `anyhow::Error` source chain and decide whether the
/// underlying failure is transient (worth retrying) or deterministic
/// (retrying won't help).
///
/// The happy path is finding an [`ApiError`] in the chain — every
/// post-send error path inside `ApiClient::deploy` is funneled
/// through `From<reqwest::Error>`, `From<serde_json::Error>`, or
/// `From<anyhow::Error>` for `ApiError`, so an `ApiError` is the
/// canonical "the HTTP round-trip surfaced an error" marker. We
/// defer to [`ApiError::is_retryable`] for the answer.
///
/// When the chain has **no** `ApiError` (typically because
/// `reqwest::blocking::RequestBuilder::send` failed at the
/// `?` operator before `check_response` could classify the
/// response), we inspect the underlying `reqwest::Error`
/// directly. `reqwest::Error` exposes `is_builder` /
/// `is_connect` / `is_timeout` / `is_request` / `is_body`
/// classifiers:
/// - `is_builder` → URL parse, header validation, multipart
///   construction. **Deterministic** — inputs are locally
///   constructed and a retry hits the same failure. Don't retry.
/// - `is_connect` / `is_timeout` / `is_request` / `is_body` →
///   network-level failure. **Transient** — a retry may reach
///   the server. Retry.
///
/// Anything else (a stray `serde_json::Error`, a non-reqwest
/// anyhow cause) is treated as deterministic. Those failures
/// happen *before* any HTTP traffic — JSON-serializing
/// `BuildMetadata`, mime-str validation — and a retry hits the
/// same broken input.
fn is_anyhow_retryable(e: &anyhow::Error) -> bool {
    for cause in e.chain() {
        if let Some(api) = cause.downcast_ref::<ApiError>() {
            return api.is_retryable();
        }
        if let Some(req) = cause.downcast_ref::<reqwest::Error>() {
            // Builder errors are deterministic — bad URL, bad
            // header, malformed multipart. Everything else
            // (connect, timeout, request, body, decode) is a
            // network/transmission failure and worth retrying.
            return !req.is_builder();
        }
    }
    // No ApiError and no reqwest::Error in the chain — a
    // deterministic pre-send error (JSON serialize of
    // BuildMetadata, mime-str validation, IO). Don't retry.
    false
}

/// Exponential backoff with full jitter (issue #571).
///
/// `attempt` is 1-indexed — the first failure is attempt #1, so
/// the first sleep is `base_ms × 2^0 = base_ms`. Doubles each
/// attempt until `retry_cap_ms` is hit, then plateaus. The
/// ±25% jitter (`× (0.75..=1.25)`) prevents synchronized
/// thundering-herd retries from N parallel CI jobs hitting the
/// same control plane at the same tick.
///
/// Returns at least 1ms so the loop is guaranteed to make
/// forward progress — a future refactor that passes
/// `retry_base_ms = 0` shouldn't accidentally spin.
fn compute_backoff_ms(attempt: u32, base_ms: u64, cap_ms: u64) -> u64 {
    // Clamp the exponent at 20 to keep `2^attempt` from overflowing
    // even with a pathological `--retry-base-ms=1`. 2^20 ≈ 1M,
    // so `1 × 2^20 = 1_048_576ms ≈ 17min` is the worst-case
    // saturated value before `min(cap_ms)` clips it.
    let exp = 2_u64.saturating_pow(attempt.saturating_sub(1).min(20));
    let capped = base_ms.saturating_mul(exp).min(cap_ms);
    // Jitter: random in 0..=50 → scale by 0.75..=1.25 of `capped`.
    // `capped × (75 + jitter)` can saturate u64 if `capped` is
    // close to `u64::MAX` — saturating_mul handles that without
    // overflow. Result floor is 1ms.
    let jitter = xorshift_uniform_u64() % 51;
    capped
        .saturating_mul(75 + jitter)
        .checked_div(100)
        .unwrap_or(0)
        .max(1)
}

/// Hand-rolled xorshift64* RNG (issue #571). No `rand` crate
/// dependency — `edge-cli` already pulls `getrandom` transitively
/// through `uuid` for v4 generation, so adding `rand` would
/// expand the dependency tree for one jitter call site.
///
/// State is a `static AtomicU64` seeded on first call from
/// `SystemTime::now()` nanoseconds (high-entropy, only used once
/// per process lifetime). The state is updated with a
/// fetch-update loop (CAS) so concurrent retry sleeps don't
/// trample each other's state — but the CLI is single-threaded
/// for retry purposes today, so this is belt-and-braces.
/// Period is 2^64 − 1 ≈ 1.8e19, ample for a CLI that runs for
/// seconds-to-minutes.
fn xorshift_uniform_u64() -> u64 {
    use std::sync::OnceLock;
    use std::time::{SystemTime, UNIX_EPOCH};

    static STATE: OnceLock<AtomicU64> = OnceLock::new();
    let state = STATE.get_or_init(|| {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0xCAFE_BABE_DEAD_BEEF);
        // Avoid the all-zero fixed point of xorshift (it would
        // always return 0). `seed | 1` guarantees the LSB is set.
        AtomicU64::new(seed | 1)
    });

    // xorshift64*: state is the value itself; output is the
    // state × a Weyl-sequence constant. CAS the new state back
    // in; retry if a concurrent caller raced us. The retry
    // path is harmless — each iteration just re-derives from
    // the current state value, so callers may see slightly
    // older numbers under contention but the distribution
    // stays uniform.
    //
    // CRITICAL: CAS the *original* loaded state value, not the
    // shifted one. Mutating `x` via `^=` before the CAS would
    // make `expected` diverge from the memory value, so the CAS
    // fails on every iteration and the loop spins forever (the
    // exact symptom this function used to have — issue #571).
    let mut x = state.load(Ordering::Relaxed);
    loop {
        let original = x;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let next = x.wrapping_mul(0x2545_F491_4F6C_DD1D);
        match state.compare_exchange_weak(original, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return next,
            Err(actual) => x = actual,
        }
    }
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
    _replicas: usize,
    _file: Option<&Path>,
    _lang: Option<LangArg>,
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
            desired_replicas: 0,
            preview_id: String::new(),
            preview_pr_number: 0,
            preview_expires_at: String::new(),
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
            desired_replicas: 0,
            preview_id: String::new(),
            preview_pr_number: 0,
            preview_expires_at: String::new(),
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

    // F13 follow-up (issue #571 review): the `is_anyhow_retryable`
    // classifier must distinguish transient reqwest errors
    // (connect refused, timeout, request, body) from deterministic
    // builder errors (URL parse, header validation, multipart
    // construction). Pin both halves below — the regression we
    // want to catch is "treats connection-refused as deterministic
    // and burns the retry budget on a single attempt" or
    // "retries URL-parse failures indefinitely."

    #[test]
    fn is_anyhow_retryable_api_error_defers_to_retryable_classifier() {
        // Transient ApiError in the chain → retry. Mirrors the
        // path that fires when wiremock returns 503.
        let api: ApiError = anyhow::anyhow!("server returned 503").into();
        let wrapped = anyhow::Error::new(api).context("deploy failed");
        assert!(is_anyhow_retryable(&wrapped));
    }

    #[test]
    fn is_anyhow_retryable_api_error_rejected_400_not_retryable() {
        // Rejected ApiError in the chain → defer to
        // ApiError::is_retryable, which returns false for
        // non-429 4xx. Mirrors the wiremock-400 test path.
        let api = ApiError::Rejected {
            status: reqwest::StatusCode::BAD_REQUEST,
            body: "bad request".into(),
        };
        let wrapped = anyhow::Error::new(api).context("deploy failed");
        assert!(!is_anyhow_retryable(&wrapped));
    }

    #[test]
    fn is_anyhow_retryable_no_api_no_reqwest_is_deterministic() {
        // A pre-send error with no reqwest::Error in the chain
        // (e.g. a JSON serialize of BuildMetadata failing). The
        // chain has only the anyhow top frame. Don't retry —
        // the inputs are locally constructed and a retry hits
        // the same broken value.
        //
        // (The reqwest::Error connect/timeout/request/body
        // branches are exercised end-to-end by the manual
        // smoke-test against 127.0.0.1:9 in
        // `tests/deploy.rs`'s comment header; reqwest 0.13
        // doesn't expose a public constructor for those error
        // variants, so we don't try to unit-test them here.)
        let e = anyhow::anyhow!("invalid base url: relative URL without a base");
        assert!(!is_anyhow_retryable(&e));
    }

    // Defensive tests for issue #571's `deploy_with_retry` loop.
    // The loop is gated behind `cfg(feature = "network")` because
    // it threads through `ApiClient::deploy`, which only exists
    // when the `network` feature (and therefore reqwest) is built
    // in. Pin the contracts below — none of these exercise the
    // network; they drive the loop via a closure that returns a
    // canned `Result<DeployResponse, anyhow::Error>` sequence.
    #[cfg(feature = "network")]
    mod retry_loop_tests {
        use super::*;
        use crate::api::client::DeployResponse;
        use std::sync::atomic::{AtomicU32, Ordering};

        fn canned_ok() -> DeployResponse {
            DeployResponse {
                id: "d_canned".to_string(),
                url: "https://canned.test".to_string(),
                regions: vec!["us-west".to_string()],
                desired_replicas: 1,
                preview_id: String::new(),
                preview_pr_number: 0,
                preview_expires_at: String::new(),
                build_attestation: None,
            }
        }

        fn canned_rejected(status: u16, body: &str) -> anyhow::Error {
            anyhow::Error::new(ApiError::Rejected {
                status: reqwest::StatusCode::from_u16(status).unwrap(),
                body: body.to_string(),
            })
            .context("deploy failed")
        }

        /// Build an `ApiError::Transient` (the shape `check_response`
        /// produces for 5xx — see `client.rs::check_response`:
        /// 5xx is NOT `is_client_error()`, so it goes through the
        /// `Transient { source: anyhow!(...) }` arm, NOT the
        /// `Rejected` arm). Tests covering the 5xx retry path
        /// must use this helper, not `canned_rejected` — otherwise
        /// we're testing a code path the loop never sees.
        fn canned_transient(status: u16, body: &str) -> anyhow::Error {
            anyhow::Error::new(ApiError::Transient {
                source: anyhow::anyhow!("server returned {status}: {body}"),
            })
            .context("deploy failed")
        }

        /// Closure factory: returns `(calls_counter, closure)`
        /// where the closure yields `Err(factory())` until
        /// `succeed_after` calls have been made, then yields
        /// `Ok(canned_ok())`. Used by the per-attempt-count
        /// tests below.
        ///
        /// `anyhow::Error: !Clone`, so each failure rebuilds
        /// the error fresh inside the closure; `factory` is
        /// the rebuild template. The counter is wrapped in
        /// an `Arc` so the closure can take ownership while
        /// the caller still observes it.
        fn factory_paced<F>(
            succeed_after: u32,
            factory: F,
        ) -> (Arc<AtomicU32>, impl FnMut() -> Result<DeployResponse>)
        where
            F: Fn() -> anyhow::Error + 'static,
        {
            let calls = Arc::new(AtomicU32::new(0));
            let factory = std::sync::Arc::new(factory);
            let f = factory.clone();
            let calls_for_closure = calls.clone();
            let closure = move || {
                let i = calls_for_closure.fetch_add(1, Ordering::SeqCst);
                if i < succeed_after {
                    Err(f())
                } else {
                    Ok(canned_ok())
                }
            };
            (calls, closure)
        }

        fn no_interrupt() -> AtomicBool {
            AtomicBool::new(false)
        }

        #[test]
        fn retry_loop_returns_first_ok_without_retrying() {
            // Pin that the loop terminator is `Ok` (not
            // `is_retryable` failing) — running off a clean
            // first attempt should call the closure exactly
            // once.
            let calls = AtomicU32::new(0);
            let mut attempt = || {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok::<_, anyhow::Error>(canned_ok())
            };
            let resp = deploy_with_retry(&mut attempt, 3, 1, 1, &no_interrupt())
                .expect("first-attempt Ok should bubble up");
            assert_eq!(resp.id, "d_canned");
            assert_eq!(calls.load(Ordering::SeqCst), 1, "exactly one closure call");
        }

        #[test]
        fn retry_loop_short_circuits_on_max_retries_zero() {
            // `--max-retries=0` semantics: a single attempt, no
            // retries. Pin that `attempt > max_retries` (NOT
            // `attempt >= max_retries`) is the guard — with
            // `>=`, attempt #1 itself would be treated as
            // exhausted and dropped without ever calling the
            // closure.
            let calls = AtomicU32::new(0);
            let mut attempt = || {
                calls.fetch_add(1, Ordering::SeqCst);
                Err::<DeployResponse, _>(canned_transient(503, "transient"))
            };
            let err = deploy_with_retry(&mut attempt, 0, 1, 1, &no_interrupt())
                .expect_err("first failure should bubble on --max-retries=0");
            assert_eq!(calls.load(Ordering::SeqCst), 1, "exactly one attempt");
            // Err chain still carries the original ApiError so
            // an operator-facing log can introspect it.
            assert!(err.chain().any(|c| c.downcast_ref::<ApiError>().is_some()));
        }

        #[test]
        fn retry_loop_stops_on_first_non_retryable_error() {
            // 400 is deterministic per `ApiError::is_retryable`.
            // Pin that the loop bails on the first non-retryable
            // failure, even if `max_retries` would have allowed
            // more attempts — surfaces the 400 to the operator
            // immediately.
            let calls = AtomicU32::new(0);
            let mut attempt = || {
                calls.fetch_add(1, Ordering::SeqCst);
                Err::<DeployResponse, _>(canned_rejected(400, "bad request"))
            };
            let err = deploy_with_retry(&mut attempt, 5, 1, 1, &no_interrupt())
                .expect_err("400 is deterministic and should bubble");
            assert_eq!(
                calls.load(Ordering::SeqCst),
                1,
                "no retry on deterministic 400"
            );
            assert!(err.chain().any(|c| c.downcast_ref::<ApiError>().is_some()));
        }

        #[test]
        fn retry_loop_eventually_retries_exhausts_and_returns_last_err() {
            // 503 (transient) should retry up to `max_retries`
            // times — total attempts `max_retries + 1`. Pin
            // both the call count and that the *last* error
            // (not a synthesized-anyhow one) survives the loop.
            let calls = AtomicU32::new(0);
            let mut attempt = || {
                calls.fetch_add(1, Ordering::SeqCst);
                Err::<DeployResponse, _>(canned_transient(503, "still down"))
            };
            let err = deploy_with_retry(&mut attempt, 3, 1, 1, &no_interrupt())
                .expect_err("503 budget should exhaust");
            assert_eq!(
                calls.load(Ordering::SeqCst),
                4,
                "1 initial attempt + 3 retries = 4 closure calls"
            );
            // The displayed error chain must still be rooted in
            // the original 503 ApiError (not `anyhow!("deploy
            // retries exhausted")` — that flattens the type and
            // breaks the retry-classifier contract from
            // `cli/src/api/client.rs:is_retryable()`).
            let api = err
                .chain()
                .find_map(|c| c.downcast_ref::<ApiError>())
                .expect("ApiError survives the loop");
            assert!(api.is_retryable(), "503 must stay retryable on the way out");
        }

        #[test]
        fn retry_loop_recovers_when_transient_failure_clears() {
            // Two 503s then a 201 — the loop must stop
            // retrying the moment the underlying call
            // succeeds, not after burning through the full
            // `max_retries` budget.
            let (calls, mut attempt) = factory_paced(2, || canned_transient(503, "warming up"));
            let resp = deploy_with_retry(&mut attempt, 5, 1, 1, &no_interrupt())
                .expect("third attempt should succeed");
            assert_eq!(resp.id, "d_canned");
            assert_eq!(calls.load(Ordering::SeqCst), 3, "two 503s + one 201");
        }

        #[test]
        fn retry_loop_treats_rejected_429_as_transient() {
            // 429 is the exception: `ApiError::is_retryable`
            // overrides `Rejected { 429 }` to true even though
            // every other 4xx is deterministic. Pin that the
            // retry budget IS consumed on 429 — otherwise the
            // deploy handler's no-Retry-After contract would
            // surface a 429 immediately to operators as a
            // hard fail.
            let (calls, mut attempt) = factory_paced(1, || canned_rejected(429, "rate"));
            let resp = deploy_with_retry(&mut attempt, 3, 1, 1, &no_interrupt())
                .expect("429 must retry, then succeed");
            assert_eq!(calls.load(Ordering::SeqCst), 2, "one 429 + one 201");
            assert_eq!(resp.id, "d_canned");
        }

        #[test]
        fn retry_loop_does_not_observe_retry_after_header() {
            // Defensive contract: the deploy CP handler does
            // not emit `Retry-After` (per
            // `edge-control-plane/internal/handler/deployment.go::Deploy`),
            // so the retry loop **must not** read or honor it.
            // A future contributor adding `Retry-After`
            // parsing would regress the bounded backoff and
            // unblock a 10-minute CI job on a malicious or
            // buggy server.
            //
            // Pin the contract by exhausting the budget on a
            // sustained 503 storm — every attempt returns a
            // `Transient { 503 }`. With `max_retries=5` and
            // `retry_cap_ms=50` the loop should run 6
            // attempts back-to-back and bail, with the
            // wallclock staying bounded by ~6 × cap_ms =
            // 300ms. If a future change honors a `Retry-After`
            // value (e.g., reads `x-retry-after-ms: 60_000`
            // and sleeps for it), the wallclock floor here
            // would slip dramatically — that's the
            // regression we want to catch.
            let (calls, mut attempt) = factory_paced(u32::MAX, || canned_transient(503, "still"));
            let start = std::time::Instant::now();
            let err = deploy_with_retry(&mut attempt, 5, 1, 50, &no_interrupt())
                .expect_err("budget should exhaust on sustained 503 storm");
            let elapsed = start.elapsed();
            assert!(
                elapsed < Duration::from_millis(2_000),
                "wallclock must stay bounded; elapsed={elapsed:?}"
            );
            assert_eq!(calls.load(Ordering::SeqCst), 6, "1 + 5 retries = 6");
            // The error returned to the operator must STILL be
            // classified as transient — if a future change
            // downgrades the surfaced error type (e.g.,
            // converts the final `Transient` into a plain
            // `anyhow!`), the classifier contract breaks and
            // the retry classifier can't introspect it.
            assert!(
                err.chain().any(|c| matches!(
                    c.downcast_ref::<ApiError>(),
                    Some(ApiError::Transient { .. })
                )),
                "returned Err must keep its Transient type"
            );
        }

        #[test]
        fn retry_loop_aborts_on_interrupt_flag() {
            // Defensive contract: pressing Ctrl-C during a
            // backoff sleep must unblock the loop without
            // waiting out the full `retry_cap_ms`. Without
            // this, an 8s default cap blocks Ctrl-C for up to
            // 8s. The interrupt flag is checked by
            // `interruptible_sleep`; here we set the flag
            // before the loop starts so the first attempt's
            // failure short-circuits through the
            // `interruptible_sleep` return without
            // performing the wait.
            let interrupt = AtomicBool::new(true); // simulate Ctrl-C already raised
            let calls = AtomicU32::new(0);
            let mut attempt = || {
                calls.fetch_add(1, Ordering::SeqCst);
                Err::<DeployResponse, _>(canned_transient(503, "still down"))
            };
            let start = std::time::Instant::now();
            // Pin the *wallclock*: with `retry_cap_ms=5_000`,
            // 3 retries would consume ~5+10=15s without the
            // interrupt guard. The interrupt flag must
            // collapse every `interruptible_sleep` to ~0.
            let _ = deploy_with_retry(&mut attempt, 3, 5_000, 5_000, &interrupt);
            assert!(
                start.elapsed() < Duration::from_secs(1),
                "interrupt must short-circuit the sleep; elapsed={:?}",
                start.elapsed()
            );
        }

        // Defensive tests for `compute_backoff_ms`, the
        // bounded-with-jitter helper used by the retry loop.
        // These pin the math directly — no thread, no
        // closure, no network. A future refactor that
        // accidentally biases the jitter or removes the
        // floor-at-1ms contract should fail one of these.
        #[test]
        fn compute_backoff_first_attempt_is_base_ms_within_jitter() {
            // attempt=1 → exp=1 → capped = base, scaled by
            // 0.75..=1.25. With base=1000 the result should
            // land in [750, 1250].
            for _ in 0..32 {
                let ms = compute_backoff_ms(1, 1_000, 60_000);
                assert!(
                    (750..=1_250).contains(&ms),
                    "attempt=1 base=1000 should be in 750..=1250, got {ms}"
                );
            }
        }

        #[test]
        fn compute_backoff_grows_exponentially_until_cap() {
            // attempt=3 → exp=4 → 500×4=2000ms pre-cap.
            assert!((1_500..=2_500).contains(&compute_backoff_ms(3, 500, 60_000)));
            // attempt=10 → exp=512 → saturates at cap_ms.
            assert!((7_500..=12_500).contains(&compute_backoff_ms(10, 500, 10_000)));
        }

        #[test]
        fn compute_backoff_floor_is_one_ms() {
            // Pathological inputs (base=0, cap=0) must NOT
            // pin the loop at 0 — the floor-at-1ms contract
            // guarantees forward progress on every sleep.
            for attempt in 1..=5 {
                let ms = compute_backoff_ms(attempt, 0, 0);
                assert!(ms >= 1, "attempt={attempt} floor must be >=1, got {ms}");
            }
        }
    }
}
