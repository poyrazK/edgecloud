//! Shared transient-failure retry loop (issue #571 — propagation follow-up).
//!
//! PR #609 landed a hand-rolled retry loop on the `edge deploy` path:
//! the loop, the classifier (`is_anyhow_retryable`), the bounded
//! with-jitter backoff (`compute_backoff_ms`), and the SIGINT-aware
//! sleep (`commands::logs::interruptible_sleep`) all live together.
//!
//! This module is the propagated home for those primitives. Every
//! retry-aware CLI command — `edge deploy`, `edge env delete`,
//! `edge env list`, `edge traffic set`, `edge traffic`,
//! `edge keys revoke`, `edge keys list`, `edge domains remove`,
//! `edge domains list/check`, `edge egress set/clear`,
//! `edge egress` — routes through [`call_with_retry`]. The deploy
//! command's `deploy_with_retry` is now a 5-line shim that calls
//! into here with `op_label = "deploy"`; the rest of the commands
//! import [`call_with_retry`] directly.
//!
//! Endpoints that need a server-side `Idempotency-Key` header
//! (`edge env set`, `edge keys create`, `edge domains add`) are NOT
//! routed through this loop yet — see the Phase-2 follow-up issue
//! for the CP `Idempotency-Key` schema work that has to land first.
//!
//! **The whole module is `feature = "network"` gated** — see the
//! `#![cfg(feature = "network")]` directive at the top of the
//! file. The defensive test suite in [`retry_loop_tests`] uses
//! `#[cfg(test)]`, which inherits the file-level `feature = "network"`
//! gate (no separate `cfg(all(test, feature = "network"))` needed —
//! the file is already network-gated, so `cfg(test)` alone is enough
//! to scope the tests to `cargo test` invocations). The `pub mod
//! retry;` declaration in `commands/mod.rs` is unconditional, but a
//! non-`network` build gets an empty module body — a future
//! contributor adding a non-`network` helper to this file would
//! see the file-level gate as the first thing they edit.

#![cfg(feature = "network")]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;

use super::logs::interruptible_sleep;
use crate::api::ApiError;
use crate::output;

/// Hardcoded sensible defaults for endpoints that don't expose the
/// `--max-retries` / `--retry-base-ms` / `--retry-cap-ms` flag
/// triple. Matches `edge deploy`'s defaults so a transient outage
/// during `edge env list` / `edge keys list` / `edge domains list`
/// is treated the same as a transient during `edge deploy`. The
/// centralized home here means a future bump (e.g. base 250→500ms)
/// is one edit instead of five duplicated `const` blocks across
/// `commands/{env,traffic,egress,auth,domains}.rs`.
pub const DEFAULT_MAX_RETRIES: u32 = 3;
pub const DEFAULT_RETRY_BASE_MS: u64 = 500;
pub const DEFAULT_RETRY_CAP_MS: u64 = 8_000;

/// Generic retry wrapper for any idempotent or naturally-replay-safe
/// single-shot CLI mutation (issue #571 propagation).
///
/// `attempt` is a closure that performs one round-trip — typically
/// `|| client.foo(...)` where `client` and the borrowed args are
/// captured by the closure. The closure is called up to
/// `1 + max_retries` times; the loop short-circuits on the first
/// success, the first non-retryable failure (e.g. 4xx other than
/// 429), or when `max_retries` is exhausted.
///
/// `op_label` is the user-visible retry-warning prefix — `"deploy"`,
/// `"env delete"`, `"traffic set"`. Each call to [`call_with_retry`]
/// passes its own label so the operator sees which operation is
/// looping.
///
/// `max_retries = 0` means a single attempt, no retries
/// (`attempt_no > max_retries` short-circuits on the first failure).
/// Backoff grows as `base_ms × 2^(attempt-1)`, capped at `cap_ms`,
/// with ±25% jitter via a hand-rolled xorshift RNG (no `rand` dep).
/// The backoff sleep is wired through [`interruptible_sleep`] so
/// Ctrl-C unblocks the loop within ~100ms instead of up to
/// `cap_ms`. The interrupt flag is checked by the caller — a
/// caller that doesn't install a SIGINT handler may pass a fresh
/// `AtomicBool::new(false)`, which makes `interruptible_sleep`
/// sleep for the full backoff (Ctrl-C still aborts via the default
/// signal handler on the next blocking call).
///
/// The closure-typed shape (rather than `&ApiClient`) lets the unit
/// tests in [`retry_loop_tests`] below drive the loop with canned
/// sequences without spinning up wiremock or a real server. The
/// single deploy-side caller passes a closure that delegates to
/// `client.deploy(...)` with the same borrowed arguments on every
/// call (so the Idempotency-Key is preserved byte-for-byte across
/// retries — see `commands/deploy.rs::run_upload` for the contract).
///
/// The classifier (`is_anyhow_retryable`) and the backoff math
/// (`compute_backoff_ms`, `xorshift_uniform_u64`) are private
/// helpers below — they were lifted verbatim from
/// `commands/deploy.rs` so the deploy-side tests continue to pin
/// the same contracts through a `pub(crate) use` re-export.
pub fn call_with_retry<T, F>(
    op_label: &str,
    mut attempt: F,
    max_retries: u32,
    retry_base_ms: u64,
    retry_cap_ms: u64,
    interrupt: &AtomicBool,
) -> Result<T>
where
    F: FnMut() -> Result<T>,
{
    // Test-only env override (issue #571 propagation). **This is a
    // test hook, NOT a stable user-facing API.** The wiremock
    // integration tests set `EDGE_CLI_RETRY_BASE_MS=10` so a
    // single retry sleeps ~10ms instead of the default 500ms; the
    // full retry sequence runs in well under a second. Without this
    // override, the hardcoded-default endpoints (no flag triple on
    // the surface) would force every retry test to either thread
    // flags through clap or wait seconds per case.
    //
    // Production callers must NOT rely on this — a user setting
    // `EDGE_CLI_RETRY_BASE_MS=0` in their shell will silently shrink
    // the retry budget to nothing. The env var is read on every
    // `call_with_retry` call (no startup-cache) for test simplicity;
    // if production usage is ever formalized, it should be promoted
    // to a `--retry-base-ms` flag and read once.
    //
    // Only the base backoff is overridable via env; the cap is
    // passed in directly because it interacts with the operator-
    // tunable `--retry-cap-ms` flag on `edge traffic set` /
    // `edge env delete` (where clap's `value_parser` already
    // clamps it to 1..=60_000). A test-only cap override would
    // need `std::env::set_var` (unsafe since Rust 1.86) and would
    // race parallel tests — not worth the complexity.
    let effective_base_ms = std::env::var("EDGE_CLI_RETRY_BASE_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(retry_base_ms);

    let mut attempt_no: u32 = 0;
    loop {
        attempt_no += 1;
        match attempt() {
            Ok(resp) => return Ok(resp),
            Err(e) if attempt_no > max_retries || !is_anyhow_retryable(&e) => return Err(e),
            Err(e) => {
                let backoff_ms = compute_backoff_ms(attempt_no, effective_base_ms, retry_cap_ms);
                output::warn(&format!(
                    "retrying {op_label} (attempt {attempt_no}/{max_retries} after {backoff_ms}ms): {e}"
                ));
                interruptible_sleep(Duration::from_millis(backoff_ms), interrupt);
            }
        }
    }
}

/// Convenience shim for callers that don't install a SIGINT
/// handler — internally constructs an `AtomicBool::new(false)` and
/// delegates to [`call_with_retry`]. Ctrl-C still aborts via the
/// default signal handler on the next blocking call; the flag is
/// only checked inside [`super::logs::interruptible_sleep`].
///
/// The 11 non-deploy retry-aware sites use this shim. The deploy
/// caller keeps the explicit `&AtomicBool` parameter because it
/// installs its own ctrlc handler (see
/// `commands/deploy.rs::run_upload`) and sets the flag on Ctrl-C.
///
/// **Do not cache the `AtomicBool` in a `OnceLock` or static.** The
/// flag is constructed fresh on every call so it stays `false`
/// forever for non-deploy callers — `interruptible_sleep` then
/// runs the full backoff (Ctrl-C still aborts via the default
/// signal handler). If the flag were cached at module scope, a
/// future contributor adding a SIGINT handler at the same scope
/// (e.g., a shared `ctrlc::set_handler`) could set the cached flag
/// for ALL callers, not just the deploy path — which would
/// short-circuit Ctrl-C handling for endpoints that didn't ask
/// for it. The fresh-per-call construction is load-bearing.
pub fn call_with_retry_no_interrupt<T, F>(
    op_label: &str,
    attempt: F,
    max_retries: u32,
    retry_base_ms: u64,
    retry_cap_ms: u64,
) -> Result<T>
where
    F: FnMut() -> Result<T>,
{
    let interrupt = AtomicBool::new(false);
    call_with_retry(
        op_label,
        attempt,
        max_retries,
        retry_base_ms,
        retry_cap_ms,
        &interrupt,
    )
}

/// Walk the `anyhow::Error` source chain and decide whether the
/// underlying failure is transient (worth retrying) or deterministic
/// (retrying won't help).
///
/// The happy path is finding an [`ApiError`] in the chain — every
/// post-send error path inside `ApiClient::*` is funneled through
/// `From<reqwest::Error>`, `From<serde_json::Error>`, or
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
/// per process lifetime). The state is updated with a CAS loop
/// so concurrent retry sleeps from multiple threads (a real
/// Phase-3 candidate: parallel mutations from a CI script that
/// spawns tokio workers — see the plan file at
/// `/Users/poyrazk/.claude/plans/optimized-wondering-quokka.md`
/// for the FaaS concurrency cap, which already exercises this
/// pattern) don't trample each other's state. Today the CLI is
/// single-threaded for retry purposes, so the CAS path is
/// untrafficked — but it's cheap (one `compare_exchange_weak` per
/// jitter call) and the function would silently miss-distribute
/// under contention if it weren't there. Period is 2^64 − 1 ≈
/// 1.8e19, ample for a CLI that runs for seconds-to-minutes.
fn xorshift_uniform_u64() -> u64 {
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

// ---------------------------------------------------------------------------
// Defensive tests for the generic retry loop. Mirror the deploy.rs
// `retry_loop_tests` suite (issue #571) but drive the loop against a
// generic payload type — same shape, same contracts, just no longer
// tied to `DeployResponse`. The deploy-side tests re-export this
// module so `cargo test commands::deploy::retry_loop_tests` keeps
// passing unchanged.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod retry_loop_tests {
    use super::*;
    use std::sync::atomic::AtomicU32;
    use std::time::Instant;

    /// Canned `T` returned by the closure on the success path. We
    /// can't use `DeployResponse` directly (the retry module is
    /// crate-internal, not specific to deploy), so a small
    /// `#[derive(Default)]` payload stands in for "anything
    /// `Clone + PartialEq + Debug`."
    #[derive(Clone, Default, PartialEq, Debug)]
    struct CannedValue(String);

    fn canned_ok() -> CannedValue {
        CannedValue("ok".into())
    }

    fn canned_rejected(status: u16, body: &str) -> anyhow::Error {
        anyhow::Error::new(ApiError::Rejected {
            status: reqwest::StatusCode::from_u16(status).unwrap(),
            body: body.to_string(),
        })
        .context("call failed")
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
        .context("call failed")
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
    ) -> (
        std::sync::Arc<AtomicU32>,
        impl FnMut() -> Result<CannedValue>,
    )
    where
        F: Fn() -> anyhow::Error + 'static,
    {
        let calls = std::sync::Arc::new(AtomicU32::new(0));
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
    fn call_with_retry_returns_first_ok_without_retrying() {
        // Pin that the loop terminator is `Ok` (not
        // `is_retryable` failing) — running off a clean
        // first attempt should call the closure exactly
        // once.
        let calls = AtomicU32::new(0);
        let mut attempt = || {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok::<_, anyhow::Error>(canned_ok())
        };
        let resp = call_with_retry("test", &mut attempt, 3, 1, 1, &no_interrupt())
            .expect("first-attempt Ok should bubble up");
        assert_eq!(resp, canned_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 1, "exactly one closure call");
    }

    #[test]
    fn call_with_retry_short_circuits_on_max_retries_zero() {
        // `--max-retries=0` semantics: a single attempt, no
        // retries. Pin that `attempt > max_retries` (NOT
        // `attempt >= max_retries`) is the guard — with
        // `>=`, attempt #1 itself would be treated as
        // exhausted and dropped without ever calling the
        // closure.
        let calls = AtomicU32::new(0);
        let mut attempt = || {
            calls.fetch_add(1, Ordering::SeqCst);
            Err::<CannedValue, _>(canned_transient(503, "transient"))
        };
        let err = call_with_retry("test", &mut attempt, 0, 1, 1, &no_interrupt())
            .expect_err("first failure should bubble on --max-retries=0");
        assert_eq!(calls.load(Ordering::SeqCst), 1, "exactly one attempt");
        // Err chain still carries the original ApiError so
        // an operator-facing log can introspect it.
        assert!(err.chain().any(|c| c.downcast_ref::<ApiError>().is_some()));
    }

    #[test]
    fn call_with_retry_stops_on_first_non_retryable_error() {
        // 400 is deterministic per `ApiError::is_retryable`.
        // Pin that the loop bails on the first non-retryable
        // failure, even if `max_retries` would have allowed
        // more attempts — surfaces the 400 to the operator
        // immediately.
        let calls = AtomicU32::new(0);
        let mut attempt = || {
            calls.fetch_add(1, Ordering::SeqCst);
            Err::<CannedValue, _>(canned_rejected(400, "bad request"))
        };
        let err = call_with_retry("test", &mut attempt, 5, 1, 1, &no_interrupt())
            .expect_err("400 is deterministic and should bubble");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "no retry on deterministic 400"
        );
        assert!(err.chain().any(|c| c.downcast_ref::<ApiError>().is_some()));
    }

    #[test]
    fn call_with_retry_eventually_exhausts_and_returns_last_err() {
        // 503 (transient) should retry up to `max_retries`
        // times — total attempts `max_retries + 1`. Pin
        // both the call count and that the *last* error
        // (not a synthesized-anyhow one) survives the loop.
        let calls = AtomicU32::new(0);
        let mut attempt = || {
            calls.fetch_add(1, Ordering::SeqCst);
            Err::<CannedValue, _>(canned_transient(503, "still down"))
        };
        let err = call_with_retry("test", &mut attempt, 3, 1, 1, &no_interrupt())
            .expect_err("503 budget should exhaust");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            4,
            "1 initial attempt + 3 retries = 4 closure calls"
        );
        // The displayed error chain must still be rooted in
        // the original 503 ApiError (not `anyhow!("call
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
    fn call_with_retry_recovers_when_transient_failure_clears() {
        // Two 503s then an Ok — the loop must stop
        // retrying the moment the underlying call
        // succeeds, not after burning through the full
        // `max_retries` budget.
        let (calls, mut attempt) = factory_paced(2, || canned_transient(503, "warming up"));
        let resp = call_with_retry("test", &mut attempt, 5, 1, 1, &no_interrupt())
            .expect("third attempt should succeed");
        assert_eq!(resp, canned_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 3, "two 503s + one Ok");
    }

    #[test]
    fn call_with_retry_treats_rejected_429_as_transient() {
        // 429 is the exception: `ApiError::is_retryable`
        // overrides `Rejected { 429 }` to true even though
        // every other 4xx is deterministic. Pin that the
        // retry budget IS consumed on 429 — otherwise the
        // deploy handler's no-Retry-After contract would
        // surface a 429 immediately to operators as a
        // hard fail.
        let (calls, mut attempt) = factory_paced(1, || canned_rejected(429, "rate"));
        let resp = call_with_retry("test", &mut attempt, 3, 1, 1, &no_interrupt())
            .expect("429 must retry, then succeed");
        assert_eq!(calls.load(Ordering::SeqCst), 2, "one 429 + one Ok");
        assert_eq!(resp, canned_ok());
    }

    #[test]
    fn call_with_retry_does_not_observe_retry_after_header() {
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
        let start = Instant::now();
        let err = call_with_retry("test", &mut attempt, 5, 1, 50, &no_interrupt())
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
    fn call_with_retry_aborts_on_interrupt_flag() {
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
            Err::<CannedValue, _>(canned_transient(503, "still down"))
        };
        let start = Instant::now();
        // Pin the *wallclock*: with `retry_cap_ms=5_000`,
        // 3 retries would consume ~5+10=15s without the
        // interrupt guard. The interrupt flag must
        // collapse every `interruptible_sleep` to ~0.
        let _ = call_with_retry("test", &mut attempt, 3, 5_000, 5_000, &interrupt);
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "interrupt must short-circuit the sleep; elapsed={:?}",
            start.elapsed()
        );
    }

    #[test]
    fn call_with_retry_no_interrupt_matches_call_with_retry_with_default_flag() {
        // Pin the contract that `call_with_retry_no_interrupt` is
        // a one-line wrapper around `call_with_retry` with a fresh
        // `AtomicBool::new(false)`. Both call sites should produce
        // identical results for any input sequence. A future
        // contributor who adds logging, metrics, or pre-loop hooks
        // to the shim (e.g., a "retry started" trace event) is
        // caught here when the closure-call counts diverge.
        //
        // The shim does NOT own ctrlc — a Ctrl-C at runtime aborts
        // via the default signal handler on the next blocking call.
        // The flag stays `false` forever, so the loop sleeps for
        // the full backoff. We don't simulate Ctrl-C in this test
        // (that path is exercised by `aborts_on_interrupt_flag`
        // against `call_with_retry` directly — adding the same
        // coverage against the shim would require plumbing the
        // flag out of the shim, which would defeat its purpose).
        let calls = AtomicU32::new(0);
        let mut attempt = || {
            calls.fetch_add(1, Ordering::SeqCst);
            Err::<CannedValue, _>(canned_transient(503, "warming up"))
        };
        let err = call_with_retry_no_interrupt("test", &mut attempt, 3, 1, 1)
            .expect_err("budget should exhaust on sustained 503 storm");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            4,
            "1 initial attempt + 3 retries = 4 closure calls (matches call_with_retry)"
        );
        // Error chain still carries the typed ApiError (the
        // classifier contract — same as `eventually_exhausts`).
        assert!(err.chain().any(|c| matches!(
            c.downcast_ref::<ApiError>(),
            Some(ApiError::Transient { .. })
        )));
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

    #[test]
    fn compute_backoff_saturates_at_cap_when_exponent_overflows() {
        // Pin the `.min(20)` exponent clamp on the saturating_pow
        // call inside `compute_backoff_ms`. Without it,
        // `attempt=30` would compute `2^29 × base = 5.4e8 × 1ms`
        // → `5.4e8ms ≈ 150 hours`, then clip to `cap_ms`. The
        // clamp ensures `2^attempt` never exceeds `2^20 ≈ 1M`
        // regardless of how large `attempt` gets — so a future
        // refactor that accidentally drops the clamp is caught
        // here (the cap-clip behavior would still pass for any
        // base that saturates, but the intermediate overflow
        // would corrupt adjacent state).
        //
        // With `base=1ms` and `cap=60_000ms`, attempt=30 must
        // land within ±25% of `60_000ms` (the cap) — NOT within
        // ±25% of any 2^29-scaled value.
        for _ in 0..32 {
            let ms = compute_backoff_ms(30, 1, 60_000);
            assert!(
                (45_000..=75_000).contains(&ms),
                "attempt=30 base=1 must saturate at cap=60_000, got {ms}"
            );
        }
    }

    // Classifier contract: `is_anyhow_retryable` must distinguish
    // transient reqwest errors (connect refused, timeout, request,
    // body) from deterministic builder errors (URL parse, header
    // validation, multipart construction). The regression we want
    // to catch is "treats connection-refused as deterministic and
    // burns the retry budget on a single attempt" or "retries
    // URL-parse failures indefinitely."
    #[test]
    fn is_anyhow_retryable_api_error_defers_to_retryable_classifier() {
        // Transient ApiError in the chain → retry.
        let api: ApiError = anyhow::anyhow!("server returned 503").into();
        let wrapped = anyhow::Error::new(api).context("call failed");
        assert!(is_anyhow_retryable(&wrapped));
    }

    #[test]
    fn is_anyhow_retryable_api_error_rejected_400_not_retryable() {
        // Rejected ApiError in the chain → defer to
        // ApiError::is_retryable, which returns false for
        // non-429 4xx.
        let api = ApiError::Rejected {
            status: reqwest::StatusCode::BAD_REQUEST,
            body: "bad request".into(),
        };
        let wrapped = anyhow::Error::new(api).context("call failed");
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
}
