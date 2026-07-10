//! Build script for `hello-js-ws`.
//!
//! Mirrors `edge-js-runtime/build.rs:1-29`:
//!   - Reads `EDGE_JS_BUNDLE` env var (the esbuild output).
//!   - Copies the file contents to `$OUT_DIR/bundle.js`.
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
    println!("cargo:rerun-if-env-changed=EDGE_JS_BUNDLE");

    let bundle_path = env::var("EDGE_JS_BUNDLE").unwrap_or_else(|_| "bundle.js".to_string());
    let bundle_path = PathBuf::from(&bundle_path);

    println!("cargo:rerun-if-changed={}", bundle_path.display());

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR set by cargo"));
    let out_file = out_dir.join("bundle.js");

    let bytes = fs::read(&bundle_path).unwrap_or_else(|e| {
        panic!(
            "hello-js-ws: failed to read bundle at {}: {e}. \
             Build via `edge build` (which runs esbuild and sets \
             EDGE_JS_BUNDLE=<.edge/bundle.js>), or pass \
             EDGE_JS_BUNDLE=/abs/path/to/bundle.js",
            bundle_path.display()
        )
    });
    fs::write(&out_file, bytes).expect("write $OUT_DIR/bundle.js");
}
