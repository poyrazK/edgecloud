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
//! Embed shape:
//!   - `include_dir!("$CARGO_MANIFEST_DIR/../wit")` captures the
//!     whole repo-root `wit/` directory at compile time and exposes
//!     it as a `Dir` value.
//!   - `build.rs` emits `cargo:rerun-if-changed=../wit` so any edit
//!     to a `.wit` file forces a rebuild of this crate (and refreshes
//!     the embedded `Dir`).
//!
//! Three copies of the WIT tree now exist in this repo:
//!   1. `wit/` (canonical, source of truth).
//!   2. `edge-control-plane/internal/service/wit/` (vendored into the
//!      Go control plane for in-process WIT parsing).
//!   3. The CLI binary's `WIT_TREE` static (this file).
//!
//! (1) ↔ (2) is guarded by `wit-drift-check` CI. (1) ↔ (3) is set at
//! compile time via the `build.rs` rerun hook — when `wit/` changes,
//! the CLI rebuilds and the new embed is picked up automatically.
//! The matching drift test lives at
//! `wit_embed_matches_canonical_wit_tree` below; it runs as part of
//! the existing `rust-test` job (no new CI step needed).
//!
//! Related memory: `wit-canonical-location`.

use anyhow::{Context, Result};
use include_dir::{include_dir, Dir, DirEntry};
use std::path::Path;

/// The vendored WIT tree, embedded at compile time from the canonical
/// `wit/` directory at the repo root. `$CARGO_MANIFEST_DIR` is the
/// directory cargo sets for the crate being built — for `edge-cli`,
/// that's `<repo-root>/edge-cli`, so `$CARGO_MANIFEST_DIR/../wit`
/// resolves to `<repo-root>/wit`, i.e. the canonical tree.
pub static WIT_TREE: Dir = include_dir!("$CARGO_MANIFEST_DIR/../wit");

/// Materialize the vendored WIT tree under `<project_dir>/wit/`.
///
/// Idempotent: returns `Ok(())` without touching the filesystem if
/// `<project_dir>/wit/` already exists (so a developer who pre-wrote
/// their own `wit/` — e.g. one based on a newer edge-runtime world —
/// doesn't get clobbered by re-running `edge init`).
pub fn write_wit_tree(project_dir: &Path) -> Result<()> {
    let target = project_dir.join("wit");
    if target.exists() {
        return Ok(());
    }
    write_dir(&target, &WIT_TREE)
        .with_context(|| format!("failed to write vendored WIT tree to {}", target.display()))?;
    Ok(())
}

/// Recursive mirror of a `Dir` onto the filesystem. Creates any
/// missing intermediate directories; existing files are overwritten
/// (the caller already checked `<project>/wit/` doesn't exist, so
/// individual file collisions shouldn't happen in normal flow).
///
/// Note: `include_dir`'s `DirEntry::Dir::path()` returns a path
/// relative to the embed ROOT (e.g. the `cli` subdirectory of
/// `wit/deps/` is reported as `"deps/cli"`, not `"cli"`), so for
/// directory entries we recurse with the bare subdir name rather
/// than re-joining the full embed-rooted path. For file entries the
/// `File::path()` is the same embed-rooted form, so we strip the
/// leading `dir.path()` prefix before joining it onto `target`.
fn write_dir(target: &Path, dir: &Dir) -> Result<()> {
    std::fs::create_dir_all(target)
        .with_context(|| format!("create_dir_all {}", target.display()))?;
    for entry in dir.entries() {
        match entry {
            DirEntry::Dir(sub) => {
                // `sub.path()` is embed-rooted; we want only the
                // leaf segment to walk down `target`.
                let name = sub.path().file_name().ok_or_else(|| {
                    anyhow::anyhow!("embed DirEntry::Dir has no leaf name: {:?}", sub.path())
                })?;
                write_dir(&target.join(name), sub)?;
            }
            DirEntry::File(f) => {
                // Strip the embed-rooted prefix (e.g. `deps/cli/...`)
                // back to the leaf relative to `dir` (e.g. `cli/...`)
                // before joining onto `target`. This prevents writing
                // `<target>/deps/cli/...` when we meant
                // `<target>/cli/...`.
                let rel = f
                    .path()
                    .strip_prefix(dir.path())
                    .unwrap_or(f.path())
                    .to_path_buf();
                let dest = target.join(&rel);
                if let Some(parent) = dest.parent() {
                    std::fs::create_dir_all(parent)
                        .with_context(|| format!("create_dir_all {}", parent.display()))?;
                }
                std::fs::write(&dest, f.contents())
                    .with_context(|| format!("write {}", dest.display()))?;
            }
        }
    }
    Ok(())
}

/// Compare every file in `WIT_TREE` against the on-disk tree rooted
/// at `disk_root`. Returns `Ok(())` if every file in the embed has a
/// matching on-disk copy with identical bytes. Returns
/// `Err(anyhow::Error)` with a drifted-path message and the rebuild
/// command if any file is missing from disk or has different bytes.
///
/// Mirrors `write_dir` directly (same embed-rooted-path gotcha: the
/// `Dir` arm recurses with the leaf-name, the `File` arm strips the
/// embed-rooted prefix to land on the leaf-relative relpath).
///
/// Issue #592: this lets the `EDGE_VERIFY_EMBED=1` runtime check in
/// `edge init --lang=rust` catch a stale CLI install on a developer
/// machine without rebuilding — a situation the `cargo test` unit
/// test cannot detect (the unit test always rebuilds before running).
pub(crate) fn verify_embed_matches_disk(disk_root: &Path) -> Result<()> {
    diff_against_disk(disk_root, &WIT_TREE)
}

/// Recursive helper used by `verify_embed_matches_disk`. Mirrors
/// `write_dir` (same embed-rooted-path gotcha: `DirEntry::Dir::path()`
/// is embed-rooted, so the `Dir` arm recurses with the leaf-name; the
/// `File` arm `strip_prefix`-es to the leaf-relative relpath). Returns
/// `Err(anyhow::Error)` on the first drifted file — the unit test
/// (`wit_embed_matches_canonical_wit_tree`) calls this and `.unwrap()`s
/// to keep the panic-shaped test failure mode.
fn diff_against_disk(disk_root: &Path, embed: &Dir) -> Result<()> {
    use include_dir::DirEntry;
    for entry in embed.entries() {
        match entry {
            DirEntry::Dir(sub) => {
                // `sub.path()` is embed-rooted; we want only the
                // leaf segment to walk down `disk_root`.
                let name = sub.path().file_name().ok_or_else(|| {
                    anyhow::anyhow!("embed DirEntry::Dir has no leaf name: {:?}", sub.path())
                })?;
                diff_against_disk(&disk_root.join(name), sub)?;
            }
            DirEntry::File(f) => {
                // Strip the embed-rooted prefix (e.g. `deps/cli/...`)
                // back to the leaf relative to `embed` (e.g.
                // `command.wit`) before joining onto `disk_root`.
                let rel = f
                    .path()
                    .strip_prefix(embed.path())
                    .unwrap_or(f.path())
                    .to_path_buf();
                let on_disk_path = disk_root.join(&rel);
                // `f.path()` is embed-rooted — the string a developer
                // would grep against `wit/` to find the file.
                let canonical_rel = f.path().to_string_lossy().into_owned();
                let rebuild_hint = "rebuild with `cargo install --path edge-cli --locked` \
                                    to refresh the embed";
                let on_disk = std::fs::read(&on_disk_path).map_err(|e| {
                    anyhow::anyhow!(
                        "WIT_TREE embed references {canonical_rel:?} (looked at {}) but \
                         the canonical tree has no matching file (read error: {e}). \
                         This usually means `wit/` was edited after the CLI was built; \
                         {rebuild_hint}.",
                        on_disk_path.display()
                    )
                })?;
                let embedded = f.contents();
                if on_disk.as_slice() != embedded {
                    return Err(anyhow::anyhow!(
                        "WIT_TREE embed of {canonical_rel:?} is stale — the canonical `wit/` \
                         tree at {} has different bytes than what the CLI binary embedded \
                         at compile time. {rebuild_hint}.",
                        on_disk_path.display()
                    ));
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wit_tree_contains_edge_cloud_wit() {
        // The canonical WIT's entrypoint file. If this ever moves or
        // is renamed, the `wit_bindgen::generate!` callers must update
        // alongside; this test guards the embed side.
        let root = WIT_TREE
            .get_file("edge-cloud.wit")
            .expect("WIT_TREE must contain the canonical edge-cloud.wit");
        let bytes = root.contents();
        assert!(
            bytes.starts_with(b"package edge:cloud"),
            "edge-cloud.wit must begin with `package edge:cloud`; got: {:?}",
            String::from_utf8_lossy(&bytes[..bytes.len().min(80)])
        );
    }

    #[test]
    fn wit_tree_contains_seven_deps_packages() {
        // `wit/deps/` is the WASI dependency packages. The exact set
        // varies as WASI Preview 2 evolves; the current canonical tree
        // (post-PR-#416) ships 7 packages — cli, clocks, filesystem,
        // http, io, random, sockets. Assert `>= 7` to catch a
        // regression that silently drops one, without breaking the
        // test every time a new package is added.
        let deps = WIT_TREE
            .get_dir("deps")
            .expect("WIT_TREE must contain a deps/ directory");
        let names: Vec<String> = deps
            .entries()
            .iter()
            .filter_map(|e| match e {
                DirEntry::Dir(d) => Some(d.path().to_string_lossy().into_owned()),
                _ => None,
            })
            .collect();
        assert!(
            names.len() >= 7,
            "expected at least 7 deps/ packages; got {} (names: {names:?}). \
             The canonical tree ships: cli, clocks, filesystem, http, io, \
             random, sockets — anything below 7 is a regression in the \
             embed.",
            names.len()
        );
        // Pin the names so a rename of one of the canonical packages
        // (which would also break every wit-bindgen consumer) shows up
        // as a clear diff here instead of as a downstream linker error.
        // `DirEntry::Dir::path()` is a relative path under the embed
        // root, so a `deps/cli` subdir is reported as `"deps/cli"`.
        for expected in [
            "cli",
            "clocks",
            "filesystem",
            "http",
            "io",
            "random",
            "sockets",
        ] {
            let needle = format!("deps/{expected}");
            assert!(
                names.iter().any(|n| n == &needle),
                "canonical WIT/deps/ is missing package {expected:?} (looked for {needle:?}); got: {names:?}"
            );
        }
    }

    #[test]
    fn write_wit_tree_round_trip_into_tempdir() {
        // End-to-end smoke: write into a fresh tempdir, then verify
        // (a) `wit/edge-cloud.wit` is on disk and matches the embed,
        // (b) the `deps/` directory is populated, (c) a second call
        // is a no-op (idempotency).
        let tmp = tempfile::tempdir().expect("tempdir");
        let project = tmp.path();

        write_wit_tree(project).expect("first write");

        let on_disk =
            std::fs::read(project.join("wit/edge-cloud.wit")).expect("edge-cloud.wit on disk");
        let embedded = WIT_TREE
            .get_file("edge-cloud.wit")
            .expect("embed")
            .contents();
        assert_eq!(
            on_disk, embedded,
            "disk contents must match embed byte-for-byte"
        );

        let deps_count = std::fs::read_dir(project.join("wit/deps"))
            .expect("deps/ on disk")
            .count();
        assert!(
            deps_count >= 7,
            "expected >= 7 deps packages on disk; got {deps_count}"
        );

        // Idempotency: a pre-existing `wit/` is left untouched.
        let sentinel = project.join("wit/edge-cloud.wit");
        // Mutate the on-disk copy to a known-bogus byte; if
        // write_wit_tree re-writes, the sentinel will flip back to
        // the canonical embed bytes. The tempdir is dropped after
        // this block so we don't bother restoring — the sentinel is
        // about to be deleted anyway.
        std::fs::write(&sentinel, b"BOGUS").expect("write sentinel");
        write_wit_tree(project).expect("second write");
        let after = std::fs::read(&sentinel).expect("read sentinel after");
        assert_eq!(
            after, b"BOGUS",
            "write_wit_tree must NOT overwrite a pre-existing <project>/wit/"
        );
    }

    /// Walk the canonical `wit/` tree on disk and verify every file's
    /// bytes match the corresponding entry in `WIT_TREE` byte-for-byte.
    ///
    /// This guards the third WIT copy (the CLI binary's `include_dir!`
    /// embed, see the module-level doc) against the same drift that's
    /// already guarded for the Go control plane's vendored copy by
    /// `wit-drift-check` CI. Without this test the binary on a
    /// developer's machine can silently carry yesterday's embed:
    /// `edge init --lang=rust` writes stale WIT into the scaffolded
    /// project, the user's build fails the wasmtime linker match
    /// (issue #576 follow-up #592), and the failure is far from the
    /// cause.
    ///
    /// The test runs against a test binary that cargo rebuilt after
    /// any `wit/` edit (via `build.rs`'s `cargo:rerun-if-changed=../wit`),
    /// so a passing assertion here confirms the most recent CLI build
    /// had a fresh embed. On a stale binary — one built before a `wit/`
    /// edit was committed — the assertion fails and points the operator
    /// at the rebuild command.
    #[test]
    fn wit_embed_matches_canonical_wit_tree() {
        let canonical_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../wit");
        // `.unwrap()` keeps the test's panic-shaped failure mode.
        // `diff_against_disk` returns `Result` so the
        // `EDGE_VERIFY_EMBED=1` runtime check (Commit 2) can surface
        // the same error message as `anyhow::Error` instead of a
        // panic.
        diff_against_disk(&canonical_root, &WIT_TREE)
            .expect("WIT_TREE embed matches canonical wit/");
    }
}
