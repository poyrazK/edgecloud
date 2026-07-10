//! Build script for `edge-js-runtime-long`.
//!
//! Mirrors `edge-js-runtime/build.rs:1-29`:
//!   - Reads `EDGE_JS_BUNDLE` env var (the esbuild output, set by
//!     `edge build --lang=js --world=edge-runtime`).
//!   - Writes the file contents to `$OUT_DIR/bundle.js`.
//!   - Declares `rerun-if-env-changed=EDGE_JS_BUNDLE` and
//!     `rerun-if-changed=<bundle path>` so cargo rebuilds when the
//!     upstream bundle changes.
//!
//! The cdylib then `include_str!`s the bundle (see `src/lib.rs`), so
//! the JS runs in-process with no fs dependency at runtime.

use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    let bundle_path = env::var("EDGE_JS_BUNDLE").unwrap_or_else(|_| {
        // Fallback: look for bundle.js next to Cargo.toml (for dev/test).
        "bundle.js".to_string()
    });

    let js_source = fs::read_to_string(&bundle_path).unwrap_or_else(|_| {
        // If no bundle exists, embed a minimal placeholder that runs
        // an empty `start()` (returns immediately) so the runtime
        // crate can compile even without user JS (for CI/testing).
        r#"globalThis.start = function({wsPort}) { /* no JS bundle embedded */ };"#.to_string()
    });

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR set by cargo"));
    fs::write(out_dir.join("bundle.js"), &js_source).expect("write bundle.js");

    println!("cargo:rerun-if-env-changed=EDGE_JS_BUNDLE");
    println!("cargo:rerun-if-changed={}", bundle_path);
}
