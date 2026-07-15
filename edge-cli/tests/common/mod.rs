//! Shared helpers for integration tests.
//!
//! Each `tests/<name>.rs` file is compiled by Cargo as its own integration
//! test binary. Files under `tests/<subdir>/` (this file) are modules
//! reachable via `mod common;` and are NOT compiled as separate binaries.
//! That makes this directory the standard Rust 2021 idiom for sharing test
//! helpers across integration tests.
//!
//! Helpers here are pure functions of `TempDir` / `PathBuf` / `Command` —
//! no fixtures, no shared mutable state — so they're safe to share
//! without coupling one test binary's lifecycle to another's.
//!
//! File-specific helpers (e.g. `read_api_key`, `seed_project`,
//! `seed_project_with_state`) stay in the test file that uses them.

use std::io::Write;
use std::path::PathBuf;

use assert_cmd::Command;
use tempfile::TempDir;

/// Returns a fresh tempdir. The caller passes `home.path()` to the
/// child as `HOME` (and on Windows, as `APPDATA`/`USERPROFILE`).
/// This function does not mutate the parent process env, so concurrent
/// tests do not race.
pub fn isolated_home() -> TempDir {
    tempfile::tempdir().expect("tempdir")
}

/// Path to the config file the CLI will actually read/write, given the
/// tempdir we passed as `HOME` (or `APPDATA` on Windows) to the child.
///
/// IMPORTANT: do not call `dirs::config_dir()` here — that resolves
/// against the *test process* env, which is the developer's real home,
/// not the child's overridden home. Tests would then read/write the
/// developer's actual config file and stomp on each other. Instead,
/// compute the path the same way the child will: on macOS, join
/// `Library/Application Support`; on Linux, join `.config` (the XDG
/// default — we deliberately do NOT set `XDG_CONFIG_HOME` for the
/// child, so `dirs::config_dir()` falls back to `$HOME/.config` and
/// the path here matches the path the child sees).
pub fn config_file_for(home: &TempDir) -> PathBuf {
    if cfg!(target_os = "macos") {
        home.path()
            .join("Library")
            .join("Application Support")
            .join("edgecloud")
            .join("config.toml")
    } else if cfg!(target_os = "windows") {
        home.path()
            .join("AppData")
            .join("Roaming")
            .join("edgecloud")
            .join("config.toml")
    } else {
        home.path()
            .join(".config")
            .join("edgecloud")
            .join("config.toml")
    }
}

/// Inject the platform-appropriate env vars so the child CLI resolves
/// its config dir to the test tempdir, not the developer's real home.
///
/// We set `HOME` (and on Windows, `APPDATA`/`USERPROFILE`) and
/// explicitly strip `XDG_CONFIG_HOME`. On Linux, `dirs::config_dir()`
/// prefers `XDG_CONFIG_HOME` over `HOME`; if the test process (or the
/// CI runner) has `XDG_CONFIG_HOME` pointing at the developer's real
/// config, the child would inherit it and read/write the real file
/// instead of the test tempdir. macOS and Windows ignore `XDG_CONFIG_HOME`,
/// but stripping it is harmless there.
pub fn set_platform_env(cmd: &mut Command, home: &TempDir) {
    if cfg!(target_os = "windows") {
        cmd.env("APPDATA", home.path().join("AppData").join("Roaming"));
        cmd.env("USERPROFILE", home.path());
    } else {
        cmd.env("HOME", home.path());
    }
    // Always strip any host-process env vars that could shadow the test.
    cmd.env_remove("XDG_CONFIG_HOME");
    cmd.env_remove("EDGE_API_KEY");
}

/// Pre-seed the tempdir config with a known API key. Used by tests
/// that need the CLI to start already authenticated.
pub fn seed_api_key(home: &TempDir, key: &str) {
    let cfg_path = config_file_for(home);
    if let Some(parent) = cfg_path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    let mut f = std::fs::File::create(&cfg_path).unwrap();
    writeln!(f, "[default]\napi_key = \"{key}\"\n").unwrap();
}

/// Assert that running `cmd` against a wiremock that mounted a single
/// `expect(0)` fence mock surfaces a pre-flight path-component
/// validation error containing `expected_stderr_substr`.
///
/// Used by the issue #671 tests: every call site that interpolates an
/// identifier into a URL path must bail BEFORE the round-trip, so the
/// caller mounts a verb-matching fence with `.expect(0)` and passes
/// the resulting `Command` here. The caller has already `.arg(...)`'d
/// the offending identifier onto `cmd`.
///
/// The test asserts `.failure()` and that stderr contains the
/// substring — the precise substring depends on which validator arm
/// fires (`cannot be empty`, `'..'`, `invalid character`).
///
/// `#[allow(dead_code)]`: each integration test binary is compiled
/// separately, and binaries that don't use this helper (e.g.
/// `tests/auto_rollback.rs`, `tests/auth.rs`) would otherwise flag it
/// as dead. The helper is intentionally shared; the allow is scoped
/// to one line.
#[allow(dead_code)]
pub fn assert_invalid_path_component(mut cmd: Command, expected_stderr_substr: &str) {
    cmd.assert()
        .failure()
        .stderr(predicates::prelude::predicate::str::contains(
            expected_stderr_substr,
        ));
}
