//! E2E regression test for issue #592: the `EDGE_VERIFY_EMBED=1`
//! runtime check inside `scaffold_rust`.
//!
//! # Why this test exists
//!
//! The unit test in `edge-cli/src/scaffold/wit.rs::tests`
//! (`wit_embed_matches_canonical_wit_tree`) catches the merge-gate
//! story — "the binary I just built via `cargo test` carries a
//! fresh embed" — by virtue of cargo rebuilding before the test
//! runs. It cannot catch the developer-machine story — "a binary
//! I installed yesterday from a now-rolled-back commit still
//! embeds yesterday's WIT".
//!
//! `EDGE_VERIFY_EMBED=1` (issue #592) is the runtime check that
//! covers the second story: after `edge init --lang=rust`
//! materializes the scaffold's `wit/`, it walks the embed and the
//! fresh disk tree byte-for-byte and `bail!`s on mismatch.
//!
//! # What this test does
//!
//! 1. Skips if `cargo` isn't on PATH or the `edge` binary isn't
//!    built. Same gate shape as `scaffold_rust_builds.rs`.
//! 2. Builds the CLI.
//! 3. Runs `EDGE_VERIFY_EMBED=1 edge init my-app --lang=rust`
//!    against a fresh tempdir; asserts the exit status is success.
//! 4. Tamper test: corrupt the on-disk `<project>/wit/edge-cloud.wit`
//!    AFTER the scaffold writes it; remove the project; re-run
//!    with the flag. The scaffold's `write_wit_tree` is a no-op
//!    when `<project>/wit/` already exists (idempotency), but the
//!    verify check still walks it — which means the freshly-rewritten
//!    bytes (matching the embed) pass. To actually exercise the
//!    negative case, we'd have to feed the CLI a tampered CLI
//!    binary, which this test can't do. So this test asserts the
//!    positive side; the negative side is the unit test's job (it
//!    has direct access to a stale embed).

use std::process::Command;

fn cargo_bin() -> Option<String> {
    Command::new("cargo")
        .arg("--version")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|_| "cargo".to_string())
}

const APP_NAME: &str = "verify-embed-test";

fn locate_edge_bin() -> Option<std::path::PathBuf> {
    if let Some(p) = option_env!("CARGO_BIN_EXE_edge") {
        let path = std::path::PathBuf::from(p);
        if path.exists() {
            return Some(path);
        }
    }
    if let Some(home) = std::env::var_os("HOME") {
        let path = std::path::PathBuf::from(home)
            .join(".cache")
            .join("edgecloud-cargo")
            .join("debug")
            .join("edge");
        if path.exists() {
            return Some(path);
        }
        let path = path.with_file_name("release").join("edge");
        if path.exists() {
            return Some(path);
        }
    }
    None
}

#[test]
fn verify_embed_env_var_passes_on_fresh_scaffold() {
    let skip_reason = match (cargo_bin(), locate_edge_bin()) {
        (None, _) => Some("cargo not on PATH"),
        (_, None) => Some("built edge binary not found"),
        (Some(_), Some(_)) => None,
    };
    if let Some(reason) = skip_reason {
        eprintln!(
            "SKIPPED: {reason}. Run `cargo build -p edge-cli` to populate \
             the shared target dir before re-running."
        );
        return;
    }
    let edge = locate_edge_bin().expect("edge binary");
    let workspace = tempfile::tempdir().expect("tempdir");

    let init_out = Command::new(&edge)
        .args(["init", APP_NAME, "--lang=rust"])
        .env("EDGE_VERIFY_EMBED", "1")
        .current_dir(workspace.path())
        .output()
        .expect("spawn edge init");
    assert!(
        init_out.status.success(),
        "EDGE_VERIFY_EMBED=1 edge init exited non-zero on a fresh scaffold; \
         this means the just-built CLI's WIT_TREE embed doesn't match the \
         tree it just wrote — i.e. the embed is already stale. stderr:\n{}\n\
         stdout:\n{}",
        String::from_utf8_lossy(&init_out.stderr),
        String::from_utf8_lossy(&init_out.stdout),
    );

    // Sanity: the wit/ tree is on disk after the flag-gated run.
    assert!(
        workspace
            .path()
            .join(APP_NAME)
            .join("wit")
            .join("edge-cloud.wit")
            .exists(),
        "scaffold must include wit/edge-cloud.wit even with EDGE_VERIFY_EMBED set"
    );
}
