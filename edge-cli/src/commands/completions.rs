//! `edge completions <shell>` — emit a shell completion script
//! (issue #506).
//!
//! Regenerates the clap tree at call time via `Cli::command()`,
//! so the generated script always matches the live command surface
//! without manual upkeep. Output goes to stdout so users pipe it to
//! their shell's completion directory (see `edge-cli/README.md` for
//! the per-shell install one-liners).
//!
//! Five shells are supported out of the box (all ship under
//! `clap_complete`'s default features): bash, zsh, fish, powershell,
//! elvish. The clap derive macro on the top-level `Command` enum in
//! `main.rs` (with `#[arg(value_enum)]`) drives the variant list —
//! no hand-maintained list of shell names here.

use anyhow::Result;
use clap::CommandFactory;
use clap_complete::{generate, Shell};

/// Generate the shell completion script for `shell` and write it to
/// stdout. Caller pipes to the install path for their shell.
///
/// `Cli::command()` rebuilds the full clap tree from the derive on
/// `main.rs::Cli` — adding a new subcommand, flag, or value-enum
/// variant upstream automatically widens the generated completion
/// on the next regeneration, with no edit to this file.
pub fn run(shell: Shell) -> Result<()> {
    let mut cmd = crate::Cli::command();
    let bin = cmd.get_name().to_string();
    generate(shell, &mut cmd, bin, &mut std::io::stdout());
    Ok(())
}
