//! Build script for `edge-cli`. The single job today is to tell cargo
//! to re-run this script (and therefore rebuild the CLI) when any
//! file under the canonical `wit/` tree changes, so the
//! `include_dir!("$CARGO_MANIFEST_DIR/../wit")` embed in
//! `src/scaffold/wit.rs` picks up WIT edits without a manual `cargo clean`.
//!
//! Issue #576: the embed is what lets `edge init --lang=rust` write a
//! project that builds offline outside the host monorepo. The canonical
//! WIT lives at repo-root `wit/`; the same tree is also vendored into
//! `edge-control-plane/internal/service/wit/` (guarded by `wit-drift-check`)
//! and into `samples/hello/wit/`. Bumping `edge-runtime`'s wasmtime
//! line usually requires a coordinated WIT bump — this hook ensures the
//! CLI binary refreshes automatically.

fn main() {
    // The whole `wit/` tree is the embed input — cargo's rerun
    // directive walks the directory recursively, so a single
    // declaration covers every `.wit` file under it. The two
    // followup lines below pin the two highest-churn subpaths
    // (the entrypoint `edge-cloud.wit` and `deps/`, which holds
    // the 7 WASI Preview 2 packages). On filesystems with poor
    // mtime resolution (HFS+, some NFS mounts) cargo's recursive
    // watcher can miss leaf-level edits — these explicit leaf
    // paths are a backstop so a tweak to `edge-cloud.wit` or a
    // WIT dep always invalidates the embed.
    println!("cargo:rerun-if-changed=../wit");
    println!("cargo:rerun-if-changed=../wit/edge-cloud.wit");
    println!("cargo:rerun-if-changed=../wit/deps");
}
