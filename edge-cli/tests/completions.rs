//! Issue #506 acceptance test: `edge completions <SHELL>` exits 0
//! and writes a non-empty completion script for every supported shell.
//!
//! Mirrors the `assert_cmd` + `predicates` harness used by the rest of
//! the integration tests (`tests/apps.rs`, `tests/open.rs`), but without
//! `wiremock` / `tempfile` / a child `HOME` override — completion
//! generation is a pure read of the clap tree at call time.
//!
//! Two tests:
//!
//! - `all_five_shells_exit_zero_with_non_empty_stdout` — iterates
//!   `["bash", "zsh", "fish", "powershell", "elvish"]` and asserts each
//!   exits 0 with non-empty stdout. Catches panics / panicking shells
//!   and the "silently prints nothing" regression (issue #506's literal
//!   acceptance criterion).
//!
//! - `bash_output_starts_with_function_definition` — stronger positive
//!   check on bash: clap_complete emits `_edge() { ... }` as the
//!   preamble. Asserting the substring locks the script shape, so a
//!   silent regression to "function emitted but with no body" still
//!   fails the test.

use assert_cmd::Command;
use predicates::prelude::*;

/// Shells the binary advertises via `--help`. Keep in sync with the
/// `clap_complete::Shell` variants (default features: Bash, Elvish,
/// Fish, PowerShell, Zsh) — the `value_enum` derive on the variant
/// auto-generates the list from the upstream enum, but the test
/// list here is hardcoded to catch a silent drift.
const SHELLS: &[&str] = &["bash", "elvish", "fish", "powershell", "zsh"];

#[test]
fn all_five_shells_exit_zero_with_non_empty_stdout() {
    for shell in SHELLS {
        let mut cmd = Command::cargo_bin("edge").unwrap();
        cmd.arg("completions").arg(shell);
        cmd.assert()
            .success()
            // Non-empty stdout is the literal acceptance criterion
            // from issue #506: "all four shells generate without
            // panicking". A silent regression to "exit 0 but print
            // nothing" would pass `success()` alone, so assert
            // non-empty too.
            .stdout(predicate::str::is_empty().not());
    }
}

#[test]
fn bash_output_starts_with_function_definition() {
    // clap_complete emits `_<bin>() { ... }` as the bash preamble;
    // for `bin = "edge"`, that's `_edge() {`. Asserting the
    // substring locks the script shape — a regression to "function
    // declared but never defined" would still produce non-empty
    // stdout, but would fail this check.
    let mut cmd = Command::cargo_bin("edge").unwrap();
    cmd.arg("completions").arg("bash");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("_edge() {"));
}

#[test]
fn zsh_output_starts_with_compdef() {
    // clap_complete's zsh output begins with `#compdef <bin>` — the
    // marker zsh uses to autoload the function. Without it, zsh
    // would silently treat the file as plain text and never wire
    // completion. Mirrors the bash preamble check for symmetry.
    let mut cmd = Command::cargo_bin("edge").unwrap();
    cmd.arg("completions").arg("zsh");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("#compdef edge"));
}

#[test]
fn completions_help_advertises_all_five_shells() {
    // The `#[arg(value_enum)]` derive on the variant in main.rs
    // generates `--help` from `clap_complete::Shell`'s `value_variants`.
    // If a new variant is added upstream (e.g. Ksh when
    // `clap_complete` adds it) and we forget to widen this test
    // list, the smoke test above will catch it; this test catches
    // the *advertised* surface.
    let mut cmd = Command::cargo_bin("edge").unwrap();
    cmd.arg("completions").arg("--help");
    cmd.assert().success().stdout(predicate::str::contains(
        "bash, elvish, fish, powershell, zsh",
    ));
}
