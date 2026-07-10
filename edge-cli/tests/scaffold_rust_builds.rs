//! End-to-end regression test for issue #576: `edge init --lang=rust`
//! must scaffold a project that builds offline and produces a
//! component the runtime can instantiate.
//!
//! # Why this test exists
//!
//! Before #576, `edge init my-app --lang=rust` wrote:
//!   * `src/main.rs` containing a plain `fn main()` (no FaaS shape);
//!   * `Cargo.toml` without `[workspace]`, `[lib]`, or `wit-bindgen`;
//!   * no `wit/` directory.
//!
//! The starter was non-functional: the deployed artifact printed one
//! line and exited without ever exposing an HTTP handler. The only
//! working Rust starter lived at `samples/hello/`, accessible only
//! to monorepo devs who knew where to look.
//!
//! # What this test does
//!
//! 1. Skips if `cargo`, `wasm-tools`, or the `wasm32-unknown-unknown`
//!    target aren't on PATH (same gate shape as
//!    `build_rust_wit_version.rs`).
//! 2. Builds the edge CLI binary to a tempdir scratch path so we
//!    exercise the post-Commit-2 version with the WIT embed wired
//!    up. (Building via `cargo run -p edge-cli` would also work but
//!    uses debug-mode artifacts; release is closer to what users
//!    install.)
//! 3. Invokes `edge init <name> --lang=rust` against a fresh tempdir
//!    and asserts the expected file set is produced: `edge.toml`,
//!    `Cargo.toml`, `src/lib.rs`, `wit/edge-cloud.wit`, and the
//!    seven `wit/deps/<pkg>/` packages.
//! 4. Runs `cargo build --target wasm32-unknown-unknown --release`
//!    inside the scaffold.
//! 5. Runs `wasm-tools component new` to wrap the core module.
//! 6. Asserts the wrapped component imports `wasi:http/types@0.2.1`
//!    (the wasmtime 45.0.3 contract) and exports
//!    `wasi:http/incoming-handler@0.2.1` (the FaaS entry point).
//!
//! If any of those steps regress — the scaffold forgets to write
//! `wit/`, the build switches back to `wasm32-wasip2`, or the pin
//! on `wit-bindgen = "0.45"` drifts — this test fails with a
//! targeted message naming the missing piece.

use std::path::PathBuf;
use std::process::Command;

fn cargo_bin() -> Option<String> {
    Command::new("cargo")
        .arg("--version")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|_| "cargo".to_string())
}

fn wasm_tools_bin() -> Option<String> {
    Command::new("wasm-tools")
        .arg("--version")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|_| "wasm-tools".to_string())
}

/// True iff the wasm32-unknown-unknown target is installed in the
/// active toolchain. The scaffold uses it to build the core module
/// (see #410/#414); a missing target means the e2e build will fail
/// for environment reasons unrelated to the scaffold itself.
fn wasm32_unknown_unknown_installed() -> bool {
    let out = Command::new("rustc")
        .args(["--print", "target-list"])
        .output()
        .ok()
        .filter(|o| o.status.success());
    match out {
        Some(o) => {
            let list = String::from_utf8_lossy(&o.stdout);
            list.lines()
                .any(|line| line.trim() == "wasm32-unknown-unknown")
        }
        None => false,
    }
}

/// Locate the built `edge` CLI binary. `cargo test` runs from the
/// workspace root, but the binary is written into the shared target
/// dir at `$HOME/.cache/edgecloud-cargo/debug/edge` (per
/// `.cargo/config.toml`). Fall back to `CARGO_BIN_EXE_edge` which
/// cargo sets for integration tests on the `edge` binary.
fn locate_edge_bin() -> Option<PathBuf> {
    if let Some(p) = option_env!("CARGO_BIN_EXE_edge") {
        let path = PathBuf::from(p);
        if path.exists() {
            return Some(path);
        }
    }
    // Fallback: shared target dir (matches `.cargo/config.toml`).
    if let Some(home) = std::env::var_os("HOME") {
        let path = PathBuf::from(home)
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

const APP_NAME: &str = "scaffold-rust-test";
/// Cargo replaces dashes with underscores when emitting the cdylib
/// artifact name — `name = "scaffold-rust-test"` in Cargo.toml yields
/// `target/.../scaffold_rust_test.wasm`. Centralize the conversion
/// here so the file-probe assertions stay in sync with the artifact
/// shape.
const ARTIFACT_NAME: &str = "scaffold_rust_test";
const EXPECTED_DEPS: &[&str] = &[
    "cli",
    "clocks",
    "filesystem",
    "http",
    "io",
    "random",
    "sockets",
];

#[test]
fn edge_init_rust_scaffolds_working_faas_project() {
    // Toolchain gate. Without cargo + wasm-tools + wasm32 target,
    // the end-to-end build can't run; skip rather than fail so CI
    // hosts without the toolchain still get compile-pass /
    // unit-test pass.
    let skip_reason = match (
        cargo_bin(),
        wasm_tools_bin(),
        wasm32_unknown_unknown_installed(),
    ) {
        (None, _, _) => Some("cargo not on PATH"),
        (_, None, _) => Some("wasm-tools not on PATH"),
        (_, _, false) => Some("wasm32-unknown-unknown target not installed"),
        (Some(_), Some(_), true) => None,
    };
    if let Some(reason) = skip_reason {
        eprintln!(
            "SKIPPED: {reason}. Install with `cargo install wasm-tools --locked` \
                   and `rustup target add wasm32-unknown-unknown` to run this end-to-end test."
        );
        return;
    }

    let edge = match locate_edge_bin() {
        Some(p) => p,
        None => {
            eprintln!(
                "SKIPPED: built `edge` binary not found at $CARGO_BIN_EXE_edge or \
                 ~/.cache/edgecloud-cargo/{{debug,release}}/edge. Run `cargo build -p edge-cli` first."
            );
            return;
        }
    };

    // ── 1. Scaffold ────────────────────────────────────────────────
    let workspace = tempfile::tempdir().expect("tempdir for scaffold");
    let project = workspace.path();

    let init_status = Command::new(&edge)
        .args(["init", APP_NAME, "--lang=rust"])
        .current_dir(project)
        .status()
        .expect("spawn edge init");
    assert!(
        init_status.success(),
        "edge init exited non-zero; see stderr above"
    );

    let app_dir = project.join(APP_NAME);
    assert!(
        app_dir.exists(),
        "scaffold directory {} not created",
        app_dir.display()
    );

    // ── 2. File-set assertions ─────────────────────────────────────
    for rel in ["edge.toml", "Cargo.toml", ".gitignore", "src/lib.rs"] {
        let p = app_dir.join(rel);
        assert!(
            p.exists(),
            "expected scaffold file missing: {}",
            p.display()
        );
    }
    assert!(
        app_dir.join("wit/edge-cloud.wit").exists(),
        "scaffold must include vendored wit/edge-cloud.wit"
    );
    for pkg in EXPECTED_DEPS {
        let p = app_dir.join("wit/deps").join(pkg);
        assert!(p.is_dir(), "scaffold must include vendored wit/deps/{pkg}/");
    }

    // ── 3. edge.toml shape ─────────────────────────────────────────
    let edge_toml = std::fs::read_to_string(app_dir.join("edge.toml")).expect("read edge.toml");
    assert!(
        edge_toml.contains("target = \"wasm32-unknown-unknown\""),
        "edge.toml must target wasm32-unknown-unknown (issues #410/#414); got:\n{edge_toml}"
    );
    assert!(
        edge_toml.contains("world = \"edge-runtime-handler\""),
        "edge.toml must declare FaaS world; got:\n{edge_toml}"
    );
    assert!(
        edge_toml.contains("language = \"rust\""),
        "edge.toml must declare language = \"rust\"; got:\n{edge_toml}"
    );

    // ── 4. Cargo.toml shape ────────────────────────────────────────
    let cargo_toml = std::fs::read_to_string(app_dir.join("Cargo.toml")).expect("read Cargo.toml");
    assert!(
        cargo_toml.contains("[workspace]"),
        "scaffold Cargo.toml must declare [workspace] (blocks host-monorepo walkup); got:\n{cargo_toml}"
    );
    assert!(
        cargo_toml.contains("crate-type = [\"cdylib\"]"),
        "scaffold Cargo.toml must use cdylib crate-type; got:\n{cargo_toml}"
    );
    assert!(
        cargo_toml.contains("wit-bindgen = \"0.45\""),
        "scaffold Cargo.toml must pin wit-bindgen = \"0.45\" (matches wasmtime 45.0.3); got:\n{cargo_toml}"
    );

    // ── 5. Offline build ───────────────────────────────────────────
    let target = app_dir.join("target");
    if target.exists() {
        std::fs::remove_dir_all(&target).expect("clean target");
    }
    let build_status = Command::new(cargo_bin().expect("cargo on PATH"))
        .args(["build", "--target", "wasm32-unknown-unknown", "--release"])
        .current_dir(&app_dir)
        .env("CARGO_TARGET_DIR", &target)
        .status()
        .expect("spawn cargo build");
    assert!(
        build_status.success(),
        "cargo build --target wasm32-unknown-unknown --release failed inside scaffold"
    );

    let core_release = target
        .join("wasm32-unknown-unknown")
        .join("release")
        .join(format!("{ARTIFACT_NAME}.wasm"));
    let core_deps = target
        .join("wasm32-unknown-unknown")
        .join("release")
        .join("deps")
        .join(format!("{ARTIFACT_NAME}.wasm"));
    // For [lib] crate-type = ["cdylib"] crates (this scaffold), cargo
    // emits the .wasm under release/deps/ rather than release/. The
    // release/ path also gets a copy, but it's the deps/ one that's
    // a real artifact; the release/ one is sometimes left behind by
    // the linker even when the actual component is elsewhere. Try
    // deps/ first; fall back to release/ for completeness.
    let core = if core_deps.exists() {
        core_deps
    } else if core_release.exists() {
        core_release
    } else {
        panic!(
            "core module not found at {} or {}; cargo build succeeded but the cdylib output is missing",
            core_deps.display(),
            core_release.display()
        );
    };

    // ── 6. Wrap + WASI contract ────────────────────────────────────
    let component = target.join("component.wasm");
    let wrap_status = Command::new(wasm_tools_bin().expect("wasm-tools on PATH"))
        .args([
            "component",
            "new",
            core.to_str().unwrap(),
            "-o",
            component.to_str().unwrap(),
        ])
        .status()
        .expect("spawn wasm-tools");
    assert!(
        wrap_status.success(),
        "wasm-tools component new failed for {}",
        core.display()
    );
    assert!(
        component.exists(),
        "wrapped component missing at {}",
        component.display()
    );

    let wit_out = Command::new(wasm_tools_bin().expect("wasm-tools on PATH"))
        .args(["component", "wit", component.to_str().unwrap()])
        .output()
        .expect("spawn wasm-tools component wit");
    assert!(
        wit_out.status.success(),
        "wasm-tools component wit failed: stderr={}",
        String::from_utf8_lossy(&wit_out.stderr)
    );
    let wit = String::from_utf8(wit_out.stdout).expect("wasm-tools output is utf-8");

    assert!(
        wit.contains("wasi:http/types@0.2.1"),
        "scaffolded component must import wasi:http/types@0.2.1 (wasmtime 45.0.3 contract); got:\n{wit}"
    );
    assert!(
        !wit.contains("wasi:http/types@0.2.4"),
        "scaffolded component must NOT import wasi:http/types@0.2.4 (the wasm32-wasip2 regression); got:\n{wit}"
    );
    assert!(
        wit.contains("wasi:http/incoming-handler@0.2.1"),
        "scaffolded component must export wasi:http/incoming-handler@0.2.1 (FaaS entry point); got:\n{wit}"
    );
}
