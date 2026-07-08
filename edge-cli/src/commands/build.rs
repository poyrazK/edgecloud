//! `edge build` — compile the project to WebAssembly.
//!
//! The dispatch on source language lives in [`run`]. The current
//! supported languages are `rust` (cargo build --target wasm32-wasip2)
//! and `js` (QuickJS custom runtime). Each language writes its artifact to a
//! language-namespaced path under `<project>/target/` so multiple
//! languages can coexist in the same checkout.

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::config::EdgeToml;
use crate::LangArg;

/// Compile the project to WebAssembly.
///
/// `lang` is the optional source language override. When `None`,
/// reads `[project] language` from `edge.toml` (falling back to
/// `"rust"` for legacy projects). When `Some(l)`, cross-checks
/// against the toml and rejects mismatches.
pub fn run(path: &Path, lang: Option<LangArg>) -> Result<()> {
    let edge_toml = EdgeToml::from_path(path)?;
    let project_name = &edge_toml.project.name;
    let toml_lang = edge_toml.project.language_or_default();

    // Resolve effective language: flag wins if provided, otherwise toml.
    let effective = match lang {
        Some(flag) => {
            // Cross-check the CLI `--lang` against `edge.toml`'s
            // `[project] language`. Mismatches are rejected here.
            if flag.as_str() != toml_lang {
                anyhow::bail!(
                    "`--lang {flag}` does not match `[project] language = {toml:?}` in edge.toml. \
                     Re-run with `--lang {toml}` (or remove the `language` line from edge.toml) so \
                     build and deploy stay in sync.",
                    flag = flag.as_str(),
                    toml = toml_lang,
                );
            }
            flag
        }
        None => {
            // Parse the toml language string into a LangArg.
            match toml_lang {
                "rust" => LangArg::Rust,
                "js" => LangArg::Js,
                other => anyhow::bail!(
                    "unsupported language {other:?} in `[project] language` in edge.toml. \
                     Supported values: `rust`, `js`."
                ),
            }
        }
    };

    println!(
        "Building '{}' (target: {}, language: {})...",
        project_name,
        edge_toml.project.target,
        effective.as_str(),
    );

    match effective {
        LangArg::Rust => build_rust(path, project_name),
        LangArg::Js => build_js(path, project_name),
    }
}

/// Resolve the on-disk artifact path for a project. Single source of
/// truth used by both `build.rs` (which writes the file) and
/// `deploy.rs` (which reads it). Exposed `pub(crate)` so the deploy
/// command can call it without duplicating the path layout.
///
/// Layout:
/// - `rust` → `target/wasm32-wasip2/release/<name>.wasm` (cargo output)
/// - `js`   → `target/javy/<name>.wasm`                  (javy/QuickJS component output)
pub(crate) fn path_for(project_root: &Path, name: &str, lang: &str) -> Result<PathBuf> {
    let artifact = match lang {
        "rust" => project_root
            .join("target")
            .join("wasm32-wasip2")
            .join("release")
            .join(format!("{}.wasm", name)),
        "js" => project_root
            .join("target")
            .join("javy")
            .join(format!("{}.wasm", name)),
        other => {
            anyhow::bail!(
                "unsupported language {other:?}: supported values are `rust` or `js`. \
                 Fix `[project] language` in edge.toml (or remove it to fall back to `rust`)."
            );
        }
    };
    Ok(artifact)
}

/// Build via `cargo build --target wasm32-wasip2 --release`.
fn build_rust(path: &Path, project_name: &str) -> Result<()> {
    let status = Command::new("cargo")
        .args(["build", "--target", "wasm32-wasip2", "--release"])
        .current_dir(path)
        .spawn()?
        .wait()?;

    if !status.success() {
        anyhow::bail!("cargo build failed");
    }

    let artifact = path_for(path, project_name, "rust").context("resolving rust artifact path")?;
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
fn build_js(path: &Path, project_name: &str) -> Result<()> {
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
    let core_wasm = runtime_dir.join("target/wasm32-wasip1/release/edge_js_runtime.wasm");
    let adapter = runtime_dir.join("wasi_snapshot_preview1.reactor.wasm");

    let artifact = path_for(path, project_name, "js").context("resolving JS artifact path")?;
    if let Some(parent) = artifact.parent() {
        std::fs::create_dir_all(parent)?;
    }

    println!("  Creating component...");
    let status = Command::new("wasm-tools")
        .args([
            "component",
            "new",
            &core_wasm.to_string_lossy(),
            "--adapt",
            &adapter.to_string_lossy(),
            "-o",
            &artifact.to_string_lossy(),
        ])
        .spawn()?
        .wait()?;

    if !status.success() {
        anyhow::bail!("wasm-tools component new failed");
    }

    println!("✓ Built successfully");
    println!("  Artifact: {}", artifact.display());
    Ok(())
}

/// Resolve the edge-js-runtime crate directory.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_for_returns_rust_target_dir() {
        let root = Path::new("/proj");
        let got = path_for(root, "myapp", "rust").expect("rust is a supported language");
        assert_eq!(
            got,
            PathBuf::from("/proj/target/wasm32-wasip2/release/myapp.wasm")
        );
    }

    #[test]
    fn path_for_returns_javy_target_dir() {
        let root = Path::new("/proj");
        let got = path_for(root, "myapp", "js").expect("js is a supported language");
        assert_eq!(got, PathBuf::from("/proj/target/javy/myapp.wasm"));
    }

    #[test]
    fn path_for_rejects_unknown_language_with_clear_error() {
        let root = Path::new("/proj");
        let err = path_for(root, "myapp", "ruby")
            .expect_err("unknown language must error, not fall back");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("unsupported language") && msg.contains("\"ruby\""),
            "expected unsupported-language error mentioning \"ruby\", got: {msg}"
        );
    }

    #[test]
    fn path_for_rejects_empty_string_with_clear_error() {
        let root = Path::new("/proj");
        let err =
            path_for(root, "myapp", "").expect_err("empty language must error, not fall back");
        assert!(
            format!("{err:#}").contains("unsupported language"),
            "expected unsupported-language error, got: {err:#}"
        );
    }
}
