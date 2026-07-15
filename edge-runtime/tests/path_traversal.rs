//! Path-traversal rejection contract for `tenant_id` (issue #620).
//!
//! Verifies the canonical gate `edge_runtime::is_safe_tenant_id`
//! (re-exported from `edge-runtime/src/runtime.rs:411`, the strict
//! regex-based version the worker calls at `start_app` and
//! `handle_purge`) refuses every input that could escape the
//! per-tenant persistence root via a `Path::join`:
//!
//!   * `..` and any substring that contains `..`.
//!   * empty strings.
//!   * POSIX path separators (`/`).
//!   * Windows path separators (`\`).
//!   * NUL bytes.
//!   * colons (Windows drive letters + AltStream markers).
//!   * any non-`[A-Za-z0-9_-]` character.
//!   * strings longer than 64 bytes.
//!
//! The worker boundary at `edge-worker/src/supervisor.rs:2074`
//! (`handle_purge`) and `:2137` (`start_app`) is the authoritative
//! gate — see `supervisor.rs:825-843` for the integration test that
//! drives a `TaskMessage::TaskPurge` carrying `tenant_id = "../etc"`
//! and asserts the supervisor bails without touching port-pool /
//! state-map. These unit tests formalize the rejection set as a
//! named contract so a refactor cannot silently widen the accepted
//! set without breaking this file.
//!
//! These tests assert the validator directly. They do NOT construct
//! a `RuntimeState` because (a) the worker boundary is the
//! authoritative gate and is already covered by
//! `supervisor::handle_purge_rejects_unsafe_tenant_id`, and (b) the
//! new `debug_assert!` at the top of `with_env_and_meter_preview`
//! (this same issue) would now panic on any unsafe input.
//!
//! NOTE: `edge_runtime::is_safe_tenant_id` is the LOOSER of the two
//! tenant-id validators in the crate — a pure `[A-Za-z0-9_-]{1,64}`
//! regex re-exported from `runtime.rs:411`. The persistence helpers
//! (kv_store.rs:222, cache.rs:256, scheduling.rs:387) use the
//! STRICTER `interfaces::is_safe_path_component` (mod.rs:17), which
//! additionally rejects Windows reserved device names (`CON`, `PRN`,
//! `NUL`, `COM1`–`COM9`, `LPT1`–`LPT9`). Both predicates reject the
//! path-traversal threat surface (`..`, `/`, `\`, NUL, `:`); they
//! differ only on Windows reserved names, which the runtime regex
//! allows. Since the worker boundary at supervisor.rs:2074 / :2137
//! (and the `debug_assert!` in this same issue) call the runtime
//! regex, this test asserts the runtime regex contract.

use edge_runtime::is_safe_tenant_id;

// ── Rejection: traversal sequences ─────────────────────────────────────

#[test]
fn is_safe_tenant_id_rejects_dotdot() {
    // Bare `..` — the classic escape-the-parent escape. The strict
    // regex treats `.` as an invalid char, so this falls out for
    // free.
    assert!(
        !is_safe_tenant_id(".."),
        "bare '..' must be rejected (strict regex rejects '.')"
    );
    // `..` as a path component inside an otherwise-safe string.
    assert!(!is_safe_tenant_id("../etc"), "'../etc' must be rejected");
    assert!(
        !is_safe_tenant_id("foo/../bar"),
        "'foo/../bar' must be rejected"
    );
    // `..` as a suffix.
    assert!(
        !is_safe_tenant_id("tenant-a/.."),
        "suffix 'tenant-a/..' must be rejected"
    );
}

// ── Rejection: empty string ─────────────────────────────────────────────

#[test]
fn is_safe_tenant_id_rejects_empty() {
    assert!(
        !is_safe_tenant_id(""),
        "empty string must be rejected (would join to the parent root)"
    );
}

// ── Rejection: path separators ──────────────────────────────────────────

#[test]
fn is_safe_tenant_id_rejects_path_separators() {
    assert!(
        !is_safe_tenant_id("foo/bar"),
        "POSIX '/' separator must be rejected"
    );
    assert!(
        !is_safe_tenant_id("foo\\bar"),
        "Windows '\\\\' separator must be rejected"
    );
}

// ── Rejection: NUL byte and colon ───────────────────────────────────────

#[test]
fn is_safe_tenant_id_rejects_null_byte() {
    // A NUL byte inside a directory name is a classic Unix
    // truncation vector (`foo\0../../../etc/passwd`).
    assert!(!is_safe_tenant_id("foo\0bar"), "NUL byte must be rejected");
}

#[test]
fn is_safe_tenant_id_rejects_colon() {
    // Colon is a Windows drive-letter delimiter AND a macOS
    // AltStream marker (`foo:bar` → resource fork).
    assert!(!is_safe_tenant_id("foo:bar"), "colon must be rejected");
}

// ── Rejection: any other non-`[A-Za-z0-9_-]` character ─────────────────

#[test]
fn is_safe_tenant_id_rejects_whitespace_and_punctuation() {
    // Spot-check the regex alphabet: anything outside
    // `[A-Za-z0-9_-]` must be refused.
    for id in [
        "foo bar",  // space
        "foo\tbar", // tab
        "foo\nbar", // newline
        "foo@bar",  // at sign
        "foo.bar",  // dot
        "foo+bar",  // plus
        "foo=bar",  // equals
        "foo#bar",  // hash
        "foo,bar",  // comma
    ] {
        assert!(
            !is_safe_tenant_id(id),
            "non-alphanumeric tenant id {id:?} must be rejected"
        );
    }
}

// ── Rejection: too long ────────────────────────────────────────────────

#[test]
fn is_safe_tenant_id_rejects_over_64_bytes() {
    let too_long = "a".repeat(65);
    assert!(
        !is_safe_tenant_id(&too_long),
        "tenant_id longer than 64 bytes must be rejected"
    );
    // Boundary: exactly 64 bytes must be accepted.
    let exactly_64 = "a".repeat(64);
    assert!(
        is_safe_tenant_id(&exactly_64),
        "tenant_id of exactly 64 bytes must be accepted"
    );
}

// ── Acceptance: representative valid tenant ids ──────────────────────────

#[test]
fn is_safe_tenant_id_accepts_valid_ids() {
    // The wire shape from the control plane: `t_<base58-or-uuid>`
    // is common. The strict regex only accepts `[A-Za-z0-9_-]`, so
    // spot-check the alphabet and a few realistic ids.
    for id in [
        "abc",
        "my-tenant_42",
        "tenant_with_underscores",
        "a1b2c3",
        "t_acme01HXYZABCDEFGHJKLM",
        "a",
        "a-b-c",
        "MixedCase123",
    ] {
        assert!(
            is_safe_tenant_id(id),
            "valid tenant id {id:?} must be accepted"
        );
    }
}

// ── Defense-in-depth: debug_assert! in with_env_and_meter_preview ─────

// The worker boundary at supervisor.rs is the authoritative gate,
// but the runtime constructor itself asserts `is_safe_tenant_id`
// in debug builds. A future caller that bypasses the worker must
// trip the assert loudly in `cargo test`. This test confirms the
// assert fires for the canonical escape attempt and that the panic
// message identifies the offending tenant id so the failure is
// debuggable.
//
// NOTE: `debug_assert!` is compiled out in `--release` — this test
// only proves the assertion runs in `cargo test` (the default debug
// profile).
#[test]
#[should_panic(expected = "unsafe tenant_id")]
fn with_env_and_meter_preview_panics_on_unsafe_tenant_id() {
    use std::collections::HashMap;
    use std::sync::Arc;

    use edge_runtime::interfaces::observe::{AppLogContext, LogRecord, LogSink};
    use edge_runtime::{EgressPolicy, RuntimeState};

    struct NoopSink;
    impl LogSink for NoopSink {
        fn push(&self, _r: LogRecord, _c: AppLogContext) {}
    }

    let _ = RuntimeState::with_env_and_meter(
        HashMap::new(),
        None,
        "../etc/passwd".to_string(), // rejected: contains '/'
        "app",
        Arc::new(EgressPolicy::allow_all()),
        Arc::new(NoopSink) as Arc<dyn LogSink>,
        AppLogContext {
            app_name: "app".to_string(),
            tenant_id: "../etc/passwd".to_string(),
            deployment_id: "isolation-test".to_string(),
        },
        None,
        edge_runtime::socket_egress::SocketEgressPolicy::default(),
        Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
    );
}
