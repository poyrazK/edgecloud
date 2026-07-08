use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    // EDGE_JS_BUNDLE is set by `edge build` to the absolute
    // path of the esbuild-bundled JS file.
    let bundle_path = env::var("EDGE_JS_BUNDLE").unwrap_or_else(|_| {
        // Fallback: look for bundle.js next to Cargo.toml (for dev/test)
        "bundle.js".to_string()
    });

    let js_source = fs::read_to_string(&bundle_path).unwrap_or_else(|_| {
        // If no bundle exists, embed a minimal placeholder that will
        // produce a 501 Not Implemented response. This allows the
        // runtime crate to compile even without user JS (for CI/testing).
        r#"globalThis.handleRequest = function(req) {
            return { status: 501, body: "no JS bundle embedded" };
        };"#
        .to_string()
    });

    fs::write(out_dir.join("bundle.js"), &js_source).unwrap();

    println!("cargo:rerun-if-env-changed=EDGE_JS_BUNDLE");
    println!("cargo:rerun-if-changed={}", bundle_path);
}
