//! WIT tree vendoring (issue #576).
//!
//! The canonical WIT lives at the repo-root `wit/` directory. The
//! `edge init` Rust starter needs that tree in `<project>/wit/` so
//! `src/lib.rs`'s `wit_bindgen::generate!({ path: "wit" })` resolves
//! it relative to the project's own `CARGO_MANIFEST_DIR` — without
//! vendoring, the scaffolded project only builds inside the host
//! monorepo (the historic `samples/hello` shape pointed at
//! `path: "../../wit"`).
//!
//! Commit 2 of the fix wires up `include_dir!("$CARGO_MANIFEST_DIR/../wit")`
//! and a `build.rs` rerun-if-changed hook, then replaces this stub
//! body with the real implementation. The stub keeps the
//! `scaffold_rust` call site compiling in commit 1.

use anyhow::Result;
use std::path::Path;

/// Materialize the vendored WIT tree under `<project_dir>/wit/`.
///
/// Idempotent: returns `Ok(())` without touching the filesystem if
/// `<project_dir>/wit/` already exists (so a developer who pre-wrote
/// their own `wit/` doesn't get clobbered by re-running `edge init`).
pub fn write_wit_tree(_project_dir: &Path) -> Result<()> {
    // Commit 2 replaces this body with `include_dir!`-driven materialization.
    // Commit 1 ships the template rewrite + call site; the actual embed
    // arrives next so each commit is independently reviewable.
    Ok(())
}
