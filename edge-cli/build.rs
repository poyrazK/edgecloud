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
    println!("cargo:rerun-if-changed=../wit");
    // Belt-and-suspenders for the two highest-churn entry points.
    // cargo's recursive watcher only sees directory mtimes, so
    // touching a leaf .wit without these lines can fail to invalidate
    // the embed on some filesystems.
    println!("cargo:rerun-if-changed=../wit/edge-cloud.wit");
    println!("cargo:rerun-if-changed=../wit/deps");
}
