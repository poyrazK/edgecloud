//! Backoff math shared by `edge-worker` retry loops.
//!
//! Extracted from `downloader.rs` (PR #676). Second caller is the
//! consume-loop reconnect in `main.rs` (issue #47, same PR family),
//! wired in `main.rs:440-490`. Further callers stay in-crate until a
//! third crate needs them, at which point a shared `edge-retry` /
//! `edge-config` extension is justified per the follow-up note on
//! PR #676's PR body.
//!
//! Conventions (matching `edge-cli/src/commands/retry.rs`):
//! - ±25% jitter band (jitter_factor in `[75, 125]`).
//! - Exponential `base_ms × 2^(attempt-1)`, saturated at `cap_ms`.
//! - Single shared xorshift64 RNG via `OnceLock<AtomicU64>` (CAS-based,
//!   concurrent-safe).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

/// Hand-rolled xorshift64 RNG used by `compute_backoff_ms` for ±25% jitter.
///
/// **Vendored verbatim from `edge-cli/src/commands/retry.rs::xorshift_uniform_u64`
/// (PR #676 implementation).** Cross-crate extraction is out of scope —
/// `edge-cli` is a binary, not a shared lib, so the function is private
/// there. The CAS pitfall below is documented at the original site
/// (commands/retry.rs:316-320) and preserved here: a contributor who
/// attempts to "simplify" the loop into `load → shift → store` will
/// corrupt state under concurrent supervisors; the `compare_exchange_weak`
/// dance is load-bearing.
///
/// Process-global static state means concurrent retry calls share the
/// RNG, but contention is negligible (CAS retry is the slow path) and
/// collisions only widen the jitter distribution. Acceptable.
pub(crate) fn xorshift_uniform_u64() -> u64 {
    static STATE: OnceLock<AtomicU64> = OnceLock::new();
    let state = STATE.get_or_init(|| {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0xCAFE_BABE_DEAD_BEEF);
        AtomicU64::new(seed | 1)
    });
    let mut x = state.load(Ordering::Relaxed);
    loop {
        let original = x;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let next = x.wrapping_mul(0x2545_F491_4F6C_DD1D);
        // CAS the *original* loaded state value (not the shifted `next`),
        // so a concurrent caller that already advanced `x` retries with
        // their updated value. See commands/retry.rs:316-320 for the
        // original write-up of the regression this prevents.
        match state.compare_exchange_weak(original, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return next,
            Err(actual) => x = actual,
        }
    }
}

/// Exponential backoff with ±25% jitter, saturated at `cap_ms`.
///
/// `attempt=1` returns a value near `base_ms` (in `[base × 0.75,
/// base × 1.25]`). Each subsequent attempt doubles the pre-jitter
/// value, capped at `cap_ms`. The cap is applied to the pre-jitter
/// value, so the final result always sits in `[cap × 0.75, cap × 1.25]`
/// once `base × 2^(attempt-1) ≥ cap_ms`.
///
/// Mirrors `edge-cli/src/commands/retry.rs:258` (modulo the explicit
/// `cap_ms` parameter, which the PR #676 version hardcoded via the
/// `RETRY_CAP_MS` const).
///
/// Used by:
/// - `downloader::Downloader::fetch_body_with_retry` — `base_ms = 500`,
///   `cap_ms = 3_200`, across-attempt exponential. (PR #676.)
/// - `main::run_consume_loop` reconnect — `base_ms = backoff`,
///   `cap_ms = 60_000`, per-iteration `attempt = 1` (jitter only);
///   exponential doubling lives at the call site. (Issue #47.)
///
/// `attempt = 0` is **equivalent to `attempt = 1`** — the
/// `saturating_sub(1)` clamps zero to a zero shift, so the pre-jitter
/// value is just `base_ms`. Don't pass zero expecting a smaller backoff;
/// the contract is `attempt ≥ 1`.
pub(crate) fn compute_backoff_ms(attempt: u32, base_ms: u64, cap_ms: u64) -> u64 {
    let exp = attempt.saturating_sub(1).min(31);
    let raw = base_ms.saturating_mul(1u64 << exp);
    let capped = raw.min(cap_ms);
    let jitter_factor = (xorshift_uniform_u64() % 51) + 75; // 75..=125 (i.e. 0.75..=1.25)
    capped.saturating_mul(jitter_factor) / 100
}

#[cfg(test)]
mod tests {
    use super::*;

    /// xorshift64 must produce varying output across many calls. A
    /// regression to a constant seed or a static `return 0;` is caught
    /// here before the more expensive backoff/jitter tests run.
    #[test]
    fn xorshift_produces_distinct_values_across_many_calls() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..1_000 {
            seen.insert(xorshift_uniform_u64());
        }
        // 1 000 calls into a 64-bit RNG must hit well under 1 000 unique
        // values on average — but pathological bugs (constant seed,
        // accidental `return 0`) collapse to ~1 unique value. The
        // threshold "at least 100" is loose enough to survive any
        // genuine xorshift bug fix that shifts the seed distribution.
        assert!(
            seen.len() >= 100,
            "xorshift collapsed: only {} distinct values across 1 000 calls",
            seen.len()
        );
    }

    /// Backoff for `attempt=1` must sit in `[0.75×base, 1.25×base]` —
    /// the jitter band. Anything outside is a sign that `xorshift % 51
    /// + 75` has been refactored without updating the test.
    #[test]
    fn compute_backoff_attempt_1_is_within_pm_25_percent_of_base() {
        let base = 200u64;
        // 200 samples to wash out per-call RNG draw variance.
        for _ in 0..200 {
            let got = compute_backoff_ms(1, base, 3_200);
            assert!(
                got >= (base * 3 / 4) && got <= (base * 5 / 4),
                "attempt 1 backoff {got} out of [150, 250] for base={base}"
            );
        }
    }

    /// Backoff doubles per attempt until it saturates at `cap_ms`.
    /// attempt=3 with base=200 must cap at `3200 × 5/4 = 4000` (the
    /// jitter band above the cap).
    #[test]
    fn compute_backoff_grows_then_caps_at_retry_cap_ms() {
        let cap = 3_200u64;

        // attempt=2 → uncapped raw = 200 × 2 = 400, jitter band 300..500
        for _ in 0..50 {
            let got = compute_backoff_ms(2, 200, cap);
            assert!(
                (300..=500).contains(&got),
                "attempt 2 backoff {got} out of [300, 500]"
            );
        }

        // attempt=3 → uncapped raw = 200 × 4 = 800, but cap=3200
        // is above 800 so the cap doesn't kick in here. Verify the cap
        // does kick in at saturation: ask for a huge base that *would*
        // overflow cap without the `.min(cap_ms)`. With the cap
        // present, the answer must be ≤ cap × 5/4.
        for _ in 0..50 {
            let got = compute_backoff_ms(3, 200, cap);
            // raw=800, no cap, jitter band 600..1000
            assert!(
                (600..=1000).contains(&got),
                "attempt 3 with base 200 got {got}, expected [600, 1000]"
            );
        }

        // Saturation: attempt=20 with base=200 — raw = 200 × 2^19 ≫ cap,
        // capped at 3200, jitter band 2400..4000.
        for _ in 0..50 {
            let got = compute_backoff_ms(20, 200, cap);
            assert!(
                (2400..=4000).contains(&got),
                "attempt 20 backoff {got} out of [2400, 4000] (cap={cap})"
            );
        }
    }
}
