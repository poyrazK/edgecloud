//! Regression test for issue #410: `edge build` must produce a
//! component that the worker (wasmtime 45.0.3) can instantiate.
//!
//! # Background
//!
//! rustc 1.93.0's `wasm32-wasip2` target embeds `wit-component
//! 0.241.x` in the produced core module, which emits `wasi:http@0.2.4`
//! and `wasi:io@0.2.6`. Wasmtime 45.0.3's linker is built against the
//! WASI WIT files in `edge-runtime/src/wit/deps/`, which declare
//! `wasi:http@0.2.1` and `wasi:io@0.2.1`. The component-model
//! resolver rejects the version mismatch during `instantiate`, so
//! the worker's `linker.instantiate_pre(&component)` fails before
//! any guest code runs.
//!
//! The fix is to build with `wasm32-unknown-unknown` (which doesn't
//! embed the buggy `wit-component`) and wrap with `wasm-tools
//! component new` — the resulting component imports the
//! `wasi:http@0.2.1` interface the linker was built with.
//!
//! # What this test does
//!
//! 1. Skips if `cargo` or `wasm-tools` are not on PATH (CI hosts
//!    that lack the toolchain would still get compile-pass /
//!    unit-test pass, just no end-to-end build verification).
//! 2. Locates the `samples/hello/` project relative to the edge-cli
//!    crate root (the test runs from the workspace root).
//! 3. Runs `cargo build --target wasm32-unknown-unknown --release`
//!    inside the sample to produce the core module.
//! 4. Runs `wasm-tools component new <core> -o <component>` to
//!    wrap it.
//! 5. Runs `wasm-tools component wit <component>` to extract the
//!    world spec.
//! 6. Asserts the world imports `wasi:http/types@0.2.1` (not
//!    `@0.2.4`) and exports `wasi:http/incoming-handler@0.2.1`.
//!
//! This is the same shape as the unit test for `BuildMetadata.target`
//! — pin the contract that the build pipeline emits a component
//! wasmtime accepts. A future regression that drops the wrap step,
//! switches back to `wasm32-wasip2`, or pins the wrong `wit-bindgen`
//! version trips this test.

use std::path::{Path, PathBuf};
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

/// Locate the samples/hello project from the edge-cli crate root.
/// The test runs `cargo test -p edge-cli ...`, so `CARGO_MANIFEST_DIR`
/// is `edge-cli/`. The sample lives at `../samples/hello/`.
fn sample_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("CARGO_MANIFEST_DIR parent")
        .join("samples")
        .join("hello")
}

/// Build the sample from scratch and return the path to the
/// wrapped component. Cleans the sample's `target/` first so
/// re-runs don't get a stale `target/component.wasm`.
fn build_sample() -> PathBuf {
    let sample = sample_dir();
    let target = sample.join("target");
    if target.exists() {
        std::fs::remove_dir_all(&target).expect("clean sample target/");
    }

    // Step 1: cargo build --target wasm32-unknown-unknown --release
    let cargo_status = Command::new(cargo_bin().expect("cargo on PATH"))
        .args(["build", "--target", "wasm32-unknown-unknown", "--release"])
        .current_dir(&sample)
        .status()
        .expect("spawn cargo");
    assert!(
        cargo_status.success(),
        "cargo build failed in {}",
        sample.display()
    );

    let core = sample
        .join("target")
        .join("wasm32-unknown-unknown")
        .join("release")
        .join("hello.wasm");
    assert!(core.exists(), "core module not found at {}", core.display());

    // Step 2: wasm-tools component new <core> -o <component>
    let component = sample.join("target").join("component.wasm");
    if let Some(parent) = component.parent() {
        std::fs::create_dir_all(parent).expect("create target/");
    }
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
        "component not produced at {}",
        component.display()
    );
    component
}

/// Extract the WIT spec of a component (what `wasm-tools component
/// wit <component>` prints) and return it as a String. The output
/// is plain-text WIT, which is much easier to assert against than
/// the binary form.
fn extract_wit_spec(component: &Path) -> String {
    let out = Command::new(wasm_tools_bin().expect("wasm-tools on PATH"))
        .args(["component", "wit", component.to_str().unwrap()])
        .output()
        .expect("spawn wasm-tools component wit");
    assert!(
        out.status.success(),
        "wasm-tools component wit failed: stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("wasm-tools output is utf-8")
}

#[test]
fn build_produces_wasi_http_0_2_1_component() {
    // Skip on hosts without the toolchain. `cargo` and `wasm-tools`
    // are required for the end-to-end build; CI images have them.
    if cargo_bin().is_none() || wasm_tools_bin().is_none() {
        eprintln!(
            "SKIPPED: cargo and/or wasm-tools not on PATH — install with \
             `cargo install wasm-tools --locked` to run this end-to-end test."
        );
        return;
    }

    let component = build_sample();
    let wit = extract_wit_spec(&component);

    // The fix: wasi:http@0.2.1 (not 0.2.4 / 0.2.6). The runtime's
    // linker was built against 0.2.1; any other version is
    // rejected at instantiate time.
    assert!(
        wit.contains("wasi:http/types@0.2.1"),
        "expected wasi:http/types@0.2.1 in wrapped component, got:\n{wit}"
    );
    assert!(
        !wit.contains("wasi:http/types@0.2.4"),
        "wrapped component still imports wasi:http/types@0.2.4 — the \
         wasm32-wasip2 regression has come back. wit spec:\n{wit}"
    );

    // The component must export the FaaS entry point that
    // `HandlerDispatch::serve` (edge-worker/src/dispatch.rs)
    // calls. Without this export the supervisor's structural
    // detection (detect.rs) would route the app down the
    // long-running path and the guest's `_start` would never
    // run.
    assert!(
        wit.contains("wasi:http/incoming-handler@0.2.1"),
        "expected wasi:http/incoming-handler@0.2.1 export, got:\n{wit}"
    );
}
