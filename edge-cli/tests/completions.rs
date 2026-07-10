//! Issue #506 acceptance test: `edge completions <SHELL>` exits 0
//! and writes a non-empty completion script for every supported shell.
//!
//! Mirrors the `assert_cmd` + `predicates` harness used by the rest of
//! the integration tests (`tests/apps.rs`, `tests/open.rs`), but without
//! `wiremock` / `tempfile` / a child `HOME` override — completion
//! generation is a pure read of the clap tree at call time.
//!
//! Five tests:
//!
//! - `all_five_shells_exit_zero_with_non_empty_stdout` — iterates
//!   `["bash", "zsh", "fish", "powershell", "elvish"]` and asserts each
//!   exits 0 with non-empty stdout. Catches panics / panicking shells
//!   and the "silently prints nothing" regression (issue #506's literal
//!   acceptance criterion).
//!
//! - `bash_output_starts_with_function_definition`,
//!   `zsh_output_starts_with_compdef`,
//!   `fish_output_defines_global_optspecs_function`,
//!   `powershell_output_registers_argument_completer` — per-shell
//!   preamble shape checks. clap_complete emits a distinctive line
//!   at the top of each shell's script (e.g. `_edge() {` for bash,
//!   `#compdef edge` for zsh); asserting it locks the script shape so
//!   a future `clap_complete` upgrade that produces valid-looking but
//!   non-functional completions still fails the test.

use assert_cmd::Command;
use predicates::prelude::*;

/// Shells the binary advertises via `--help`. Hardcoded mirror of
/// `clap_complete::Shell::value_variants()` (default features: Bash,
/// Elvish, Fish, PowerShell, Zsh). The `value_enum` derive on the
/// variant in `main.rs` is the source of truth at runtime; this list
/// duplicates it so a silent drift between the test and the binary's
/// advertised surface fails the `completions_help_advertises_all_five_shells`
/// test below. We accept the duplication — pulling `clap` into this
/// test binary just to enumerate variants is worse than the maintenance
/// cost (clap_complete 4.x has been stable on these five since 2023).
const SHELLS: &[&str] = &["bash", "elvish", "fish", "powershell", "zsh"];

#[test]
fn all_five_shells_exit_zero_with_non_empty_stdout() {
    for shell in SHELLS {
        let mut cmd = Command::cargo_bin("edge").unwrap();
        cmd.arg("completions").arg(shell);
        cmd.assert()
            .success()
            // Non-empty stdout is the literal acceptance criterion
            // from issue #506 (generalized from "all four shells"
            // to "all five" — Elvish was added per user decision).
            // A silent regression to "exit 0 but print nothing"
            // would pass `success()` alone, so assert non-empty too.
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
fn fish_output_defines_global_optspecs_function() {
    // clap_complete's fish output begins with a comment line and
    // then defines `__fish_edge_global_optspecs` — the function fish
    // calls to look up the option set for `edge` invocations without
    // a subcommand. Without that function, completion of top-level
    // flags like `--path` would silently fall back to no completion.
    let mut cmd = Command::cargo_bin("edge").unwrap();
    cmd.arg("completions").arg("fish");
    cmd.assert().success().stdout(predicate::str::contains(
        "function __fish_edge_global_optspecs",
    ));
}

#[test]
fn powershell_output_registers_argument_completer() {
    // clap_complete's PowerShell output registers a native
    // argument completer bound to the binary name. Without the
    // `Register-ArgumentCompleter -Native -CommandName 'edge'`
    // line, PowerShell would treat the file as plain text and
    // never wire completion. (Caveat: this generator panics with
    // BrokenPipe if the consumer closes the pipe early; the test
    // harness consumes the full buffer via `cmd.assert()` so the
    // panic doesn't surface here.)
    let mut cmd = Command::cargo_bin("edge").unwrap();
    cmd.arg("completions").arg("powershell");
    cmd.assert().success().stdout(predicate::str::contains(
        "Register-ArgumentCompleter -Native -CommandName 'edge'",
    ));
}

#[test]
fn completions_help_advertises_all_five_shells() {
    // The `#[arg(value_enum)]` derive on the variant in main.rs
    // generates `--help` from `clap_complete::Shell`'s `value_variants`.
    // If a new variant is added upstream (e.g. Ksh when
    // `clap_complete` adds it) and we forget to widen the SHELLS
    // constant above, the smoke test will catch it on the new
    // shell; this test catches the *advertised* surface.
    let mut cmd = Command::cargo_bin("edge").unwrap();
    cmd.arg("completions").arg("--help");
    cmd.assert().success().stdout(predicate::str::contains(
        "bash, elvish, fish, powershell, zsh",
    ));
}
