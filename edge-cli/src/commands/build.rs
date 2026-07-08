//! `edge build` — compile the project to WebAssembly.

use anyhow::Result;
use std::path::Path;
use std::process::Command;

use crate::config::EdgeToml;

pub fn run(path: &Path) -> Result<()> {
    let edge_toml = EdgeToml::from_path(path)?;
    match edge_toml.project.language.as_str() {
        "js" | "javascript" => build_js(path, &edge_toml),
        _ => build_rust(path, &edge_toml),
    }
}

/// Rust build — existing logic, unchanged.
fn build_rust(path: &Path, edge_toml: &EdgeToml) -> Result<()> {
    let project_name = &edge_toml.project.name;
    println!("Building '{}' (Rust, target: {})...", project_name, edge_toml.project.target);

    let status = Command::new("cargo")
        .args(["build", "--target", "wasm32-wasip2", "--release"])
        .current_dir(path)
        .spawn()?
        .wait()?;

    if !status.success() {
        anyhow::bail!("cargo build failed");
    }

    let artifact = path
        .join("target/wasm32-wasip2/release")
        .join(format!("{}.wasm", project_name));

    if !artifact.exists() {
        anyhow::bail!("artifact not found at {}", artifact.display());
    }

    println!("✓ Built successfully");
    println!("  Artifact: {}", artifact.display());
    Ok(())
}

/// JavaScript build pipeline:
///   1. npm install (if node_modules missing)
///   2. esbuild bundle
///   3. cargo build edge-js-runtime (with EDGE_JS_BUNDLE env)
///   4. wasm-tools component new
fn build_js(path: &Path, edge_toml: &EdgeToml) -> Result<()> {
    let project_name = &edge_toml.project.name;
    println!("Building '{}' (JavaScript)...", project_name);

    let edge_dir = path.join(".edge");
    std::fs::create_dir_all(&edge_dir)?;

    // 1. npm install if needed
    if !path.join("node_modules").exists() {
        println!("  Installing npm dependencies...");
        let status = Command::new("npm")
            .args(["install"])
            .current_dir(path)
            .spawn()?
            .wait()?;
        if !status.success() {
            anyhow::bail!("npm install failed");
        }
    }

    // 2. Bundle with esbuild
    let bundle_path = edge_dir.join("bundle.js");
    let entry = path.join("src/handler.js");
    if !entry.exists() {
        anyhow::bail!("entry point not found: src/handler.js");
    }

    println!("  Bundling JS...");
    let status = Command::new("npx")
        .args([
            "esbuild",
            &entry.to_string_lossy(),
            "--bundle",
            "--format=iife",
            "--platform=neutral",
            &format!("--outfile={}", bundle_path.display()),
        ])
        .current_dir(path)
        .spawn()?
        .wait()?;

    if !status.success() {
        anyhow::bail!("esbuild bundling failed");
    }

    // 3. Build the JS runtime crate with the bundled JS embedded.
    let runtime_dir = resolve_runtime_dir()?;

    println!("  Compiling JS runtime...");
    let status = Command::new("cargo")
        .args(["build", "--target", "wasm32-wasip1", "--release"])
        .current_dir(&runtime_dir)
        .env("EDGE_JS_BUNDLE", bundle_path.canonicalize()?)
        .spawn()?
        .wait()?;

    if !status.success() {
        anyhow::bail!("JS runtime compilation failed");
    }

    // 4. Componentize with wasm-tools
    let core_wasm = runtime_dir
        .join("target/wasm32-wasip1/release/edge_js_runtime.wasm");
    let adapter = runtime_dir.join("wasi_snapshot_preview1.reactor.wasm");

    let output_dir = path.join("target/wasm32-wasip2/release");
    std::fs::create_dir_all(&output_dir)?;
    let output = output_dir.join(format!("{}.wasm", project_name));

    println!("  Creating component...");
    let status = Command::new("wasm-tools")
        .args([
            "component", "new",
            &core_wasm.to_string_lossy(),
            "--adapt", &adapter.to_string_lossy(),
            "-o", &output.to_string_lossy(),
        ])
        .spawn()?
        .wait()?;

    if !status.success() {
        anyhow::bail!("wasm-tools component new failed");
    }

    println!("✓ Built successfully");
    println!("  Artifact: {}", output.display());
    Ok(())
}

/// Resolve the edge-js-runtime crate directory.
/// Looks for EDGE_JS_RUNTIME_DIR env var first, then
/// falls back to a path relative to the project's monorepo root.
fn resolve_runtime_dir() -> Result<std::path::PathBuf> {
    if let Ok(dir) = std::env::var("EDGE_JS_RUNTIME_DIR") {
        return Ok(std::path::PathBuf::from(dir));
    }

    // Walk up from CWD looking for edge-js-runtime/
    let mut dir = std::env::current_dir()?;
    for _ in 0..5 {
        let candidate = dir.join("edge-js-runtime");
        if candidate.join("Cargo.toml").exists() {
            return Ok(candidate);
        }
        if !dir.pop() {
            break;
        }
    }

    anyhow::bail!(
        "Cannot find edge-js-runtime/ crate. Set EDGE_JS_RUNTIME_DIR \
         or run from within the edgecloud monorepo."
    )
}
