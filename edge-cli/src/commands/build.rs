//! `edge build` — compile the project to WebAssembly.
//!
//! The dispatch on source language lives in [`run`]. The current
//! supported languages are `rust` (cargo build --target wasm32-wasip2)
//! and `js` (javy compile). Each language writes its artifact to a
//! language-namespaced path under `<project>/target/<lang>/` so multiple
//! languages can coexist in the same checkout (e.g. a workspace that
//! runs an integration fixture in JS against a Rust handler).

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::config::EdgeToml;
use crate::LangArg;

/// Compile the project to WebAssembly.
pub fn run(path: &Path, lang: LangArg) -> Result<()> {
    let edge_toml = EdgeToml::from_path(path)?;
    let project_name = &edge_toml.project.name;

    // Cross-check the CLI `--lang` against `edge.toml`'s
    // `[project] language`. Mismatches used to surface as a confusing
    // missing-artifact error at deploy time (finding 2 of the
    // PR #221 review); rejecting here is the user-friendly fix.
    // The toml wins when `--lang` is omitted because the toml is the
    // authoritative record of what the project was scaffolded as.
    let toml_lang = edge_toml.project.language_or_default();
    if toml_lang != lang.as_str() {
        anyhow::bail!(
            "`--lang {flag}` does not match `[project] language = {toml:?}` in edge.toml. \
             Re-run with `--lang {toml}` (or remove the `language` line from edge.toml) so \
             build and deploy stay in sync.",
            flag = lang.as_str(),
            toml = toml_lang,
        );
    }

    println!(
        "Building '{}' (target: {}, language: {})...",
        project_name,
        edge_toml.project.target,
        lang.as_str(),
    );

    match lang {
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
/// - `js`   → `target/javy/<name>.wasm`                  (javy output)
/// - any other value defaults to the rust path; callers should
///   reject unknown values before reaching here.
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

/// Locate the `javy` binary on PATH. Testable seam: pass any
/// `which_fn` (e.g. `which::which`) and assert the discovered path
/// or `None`. Mirrors the `Preprocessor::discover_with` pattern at
/// `edge-migrate/edge-migrate-lib/src/preprocessor.rs:98-114`.
///
/// `which_fn` is invoked with the literal string `"javy"`. We do NOT
/// consult a `$JAVY_PATH` env var (Javy has no standard install
/// convention analogous to `$WASI_SDK_PATH`); users put javy on PATH.
pub(crate) fn probe_javy_with<F>(which_fn: F) -> Option<PathBuf>
where
    F: Fn(&str) -> Option<PathBuf>,
{
    which_fn("javy")
}

/// Build via `cargo build --target wasm32-wasip2 --release`. The
/// pre-language-dispatch original logic; kept verbatim so existing
/// Rust projects get byte-identical output.
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

/// Build via `javy compile -o <artifact> <source>`. Requires Javy
/// v3.x on PATH; surfaces a friendly error with install URL if not.
/// Captures Javy's stderr and surfaces it on non-zero exit (sharper
/// than the Rust path's silent cargo-output).
fn build_js(path: &Path, project_name: &str) -> Result<()> {
    let javy = probe_javy_with(|name| which::which(name).ok()).ok_or_else(|| {
        anyhow::anyhow!(
            "`javy` was not found on PATH.\n  \
             Install from https://github.com/bytecodealliance/javy/releases \
             (v3.x recommended)\n  \
             and ensure it is on your PATH before running `edge build --lang=js`."
        )
    })?;

    let entry = path.join("index.js");
    if !entry.is_file() {
        anyhow::bail!(
            "`index.js` not found in {} — create it at the project root \
             (matching the `edge init --lang=js` starter).",
            path.display(),
        );
    }

    // Create the language-namespaced target dir idempotently.
    let target_dir = path.join("target").join("javy");
    std::fs::create_dir_all(&target_dir)?;

    let artifact = target_dir.join(format!("{}.wasm", project_name));

    // Use `Command::output()` (not `spawn/wait`) so Javy's stderr
    // reaches the terminal on non-zero exit. Javy compile is fast
    // enough that blocking on it is fine; we don't need streaming.
    let output = Command::new(&javy)
        .arg("compile")
        .arg("-o")
        .arg(&artifact)
        .arg(&entry)
        .current_dir(path)
        .output()?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        crate::output::error(&format!("javy compile failed:\n{stderr}"));
        anyhow::bail!("javy compile failed (see error above)");
    }

    if !artifact.exists() {
        anyhow::bail!(
            "javy exited successfully but artifact is missing at {}",
            artifact.display(),
        );
    }

    println!("✓ Built successfully");
    println!("  Artifact: {}", artifact.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_javy_with_returns_none_when_javy_missing() {
        let result = probe_javy_with(|_| None);
        assert!(result.is_none(), "expected None, got {result:?}");
    }

    #[test]
    fn probe_javy_with_returns_some_when_javy_on_path() {
        let expected = PathBuf::from("/usr/local/bin/javy");
        let result = probe_javy_with(|name| {
            assert_eq!(name, "javy", "probe_javy must look up exactly 'javy'");
            Some(expected.clone())
        });
        assert_eq!(result, Some(expected));
    }

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
        // The old `_` arm silently routed unknown languages to the
        // rust path, which made typos (e.g. `language = "ruby"`) surface
        // as a confusing missing-file error at deploy time instead of
        // a friendly unknown-language error. The helper is now the one
        // place that knows the supported set.
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
        // Combined with `Project::language_or_default`, an empty
        // string in the toml now resolves to "rust" before reaching
        // here. But callers that bypass the default (e.g. a future
        // direct API) should still get a clear error, not a
        // silent fall-through.
        let root = Path::new("/proj");
        let err =
            path_for(root, "myapp", "").expect_err("empty language must error, not fall back");
        assert!(
            format!("{err:#}").contains("unsupported language"),
            "expected unsupported-language error, got: {err:#}"
        );
    }
}
