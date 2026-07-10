//! `edge build` — compile the project to WebAssembly.
//!
//! The dispatch on source language lives in [`run`]. The current
//! supported languages are `rust` (cargo build --target <target> +
//! `wasm-tools component new` wrap) and `js` (QuickJS custom runtime).
//! Each language writes its artifact to a language-namespaced path
//! under `<project>/target/` so multiple languages can coexist in the
//! same checkout.

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::SystemTime;

use crate::config::EdgeToml;
use crate::state::BuildMetadata;
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
        "Building '{}' (target: {}, world: {}, language: {})...",
        project_name,
        edge_toml.project.target,
        edge_toml.project.world,
        effective.as_str(),
    );

    match effective {
        LangArg::Rust => build_rust(
            path,
            project_name,
            &edge_toml.project.target,
            &edge_toml.project.world,
        ),
        LangArg::Js => build_js(path, project_name),
    }
}

/// Resolve the on-disk artifact path for a project. Single source of
/// truth used by both `build.rs` (which writes the file) and
/// `deploy.rs` (which reads it). Exposed `pub(crate)` so the deploy
/// command can call it without duplicating the path layout.
///
/// Layout (issue #410):
/// - `rust` → `target/component.wasm` (the wasm-tools-wrapped component
///   the worker loads). The intermediate cargo output
///   `target/<target>/release/<name>.wasm` is the core module; it
///   stays on disk for debugging but is NOT what `edge deploy` reads.
/// - `js`   → `target/javy/<name>.wasm`  (javy/QuickJS component output)
pub(crate) fn path_for(project_root: &Path, name: &str, lang: &str) -> Result<PathBuf> {
    let artifact = match lang {
        "rust" => project_root.join("target").join("component.wasm"),
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

/// Resolve the intermediate cargo output path for a Rust project.
/// Exposed `pub(crate)` so the regression test (and any future
/// `edge build --keep-core` style flag) can locate the file. Lives
/// next to `path_for` so the two layout choices stay in sync.
pub(crate) fn core_path_for(project_root: &Path, name: &str, target: &str) -> PathBuf {
    project_root
        .join("target")
        .join(target)
        .join("release")
        .join(format!("{}.wasm", name))
}

/// Build a Rust project: cargo → core module, then `wasm-tools
/// component new` → wrapped component at `target/component.wasm`.
///
/// `target` is the cargo target triple (defaults to
/// `wasm32-unknown-unknown`); `world` is the WIT world name (e.g.
/// `edge-runtime-handler`). Both come from `edge.toml`; see
/// [`config::edgetoml::Project`] for the defaults / required fields.
fn build_rust(path: &Path, project_name: &str, target: &str, world: &str) -> Result<()> {
    let started_iso = iso8601_now();

    // Step 1: cargo build --target <target> --release. Produces the
    // core wasm module at target/<target>/release/<name>.wasm.
    // Issue #410: <target> defaults to wasm32-unknown-unknown
    // (NOT wasm32-wasip2) because the latter's bundled
    // wit-component 0.241.x emits wasi:http@0.2.4, which wasmtime
    // 45.0.3's linker rejects. Building the core module with
    // wasm32-unknown-unknown leaves the wrapping to wasm-tools,
    // which produces a wasi:http@0.2.1 component.
    let status = Command::new("cargo")
        .args(["build", "--target", target, "--release"])
        .current_dir(path)
        .spawn()
        .context("failed to spawn `cargo build`")?
        .wait()
        .context("failed to wait for `cargo build`")?;

    if !status.success() {
        anyhow::bail!("cargo build failed (target: {target})");
    }

    let core = core_path_for(path, project_name, target);
    if !core.exists() {
        anyhow::bail!(
            "cargo output not found at {} — did `cargo build --target {target}` produce a \
             cdylib artifact named `{project_name}.wasm`? Check the crate's `[lib] name`.",
            core.display()
        );
    }

    // Step 2: wasm-tools component new <core> -o target/component.wasm.
    // Wraps the core module into a component the worker can load.
    //
    // The world name + WIT interface definitions are read from
    // `wit-component-encoding` custom sections that `wit-bindgen`
    // embedded in the core module at compile time — `wasm-tools
    // component new` (1.252.0+) does NOT need `--world` or
    // `--wit-dir` flags. The `world` parameter is still threaded
    // through to `build_rust` so it appears in the print banner (a
    // self-documenting sanity check) and so future enhancements
    // (e.g. cross-check the declared world against the produced
    // component's actual world) have it on hand.
    let _ = world; // intentionally unused — see comment above.
    let artifact = path_for(path, project_name, "rust").context("resolving rust artifact path")?;
    if let Some(parent) = artifact.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating artifact parent directory {}", parent.display()))?;
    }

    println!("  Wrapping with wasm-tools...");
    let status = Command::new("wasm-tools")
        .args([
            "component",
            "new",
            &core.to_string_lossy(),
            "-o",
            &artifact.to_string_lossy(),
        ])
        .spawn()
        .context(
            "failed to spawn `wasm-tools component new`. Install with: \
             `cargo install wasm-tools --locked`",
        )?
        .wait()
        .context("failed to wait for `wasm-tools component new`")?;

    if !status.success() {
        anyhow::bail!(
            "wasm-tools component new failed — the cargo output is at {} if you want to debug. \
             Common causes: a stale `target/` (try `cargo clean` first), or a `wit-bindgen` \
             version that emits a wasi:http version wasmtime 45.0.3 doesn't accept.",
            core.display()
        );
    }

    if !artifact.exists() {
        anyhow::bail!(
            "artifact not found at {} after a reportedly successful wasm-tools run",
            artifact.display()
        );
    }

    // Issue #307 PR2 — capture build-time metadata into
    // .edge/build_metadata.json so the deploy path can upload
    // it as the multipart `build_metadata` form field. The
    // control plane uses these fields to populate the SLSA L1
    // envelope's `predicate.buildTools[]` and
    // `predicate.invocation.parameters` entries.
    //
    // `target` here is the cargo-side target triple (what the
    // user wrote in edge.toml). The control plane treats this as
    // an opaque string — the SLSA envelope's
    // `predicate.invocation.parameters.target` field is just a
    // record of what was used, not a validation point.
    let build_metadata = BuildMetadata {
        toolchain_rustc: capture_tool_version("rustc", &["--version"]),
        toolchain_cargo: capture_tool_version("cargo", &["--version"]),
        toolchain_clang: String::new(),
        toolchain_rustup: capture_tool_version("rustup", &["show", "active-toolchain"]),
        target: target.to_string(),
        profile: "release".to_string(),
        source_digest: compute_source_digest(path).unwrap_or_default(),
        build_started_on: started_iso,
    };
    if let Err(e) = build_metadata.save(path) {
        // Non-fatal — the deploy path treats missing build
        // metadata as best-effort. Log and continue.
        eprintln!("warning: failed to write build_metadata.json: {e}");
    }

    println!("✓ Built successfully");
    println!("  Artifact: {}", artifact.display());
    println!("  Core:     {}", core.display());
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

    // The runtime crate (`edge-js-runtime/Cargo.toml`) declares its own
    // `[workspace]` root, so the parent monorepo's `.cargo/config.toml`
    // — which pins `build.target-dir` to a shared location — is NOT
    // inherited by this cargo invocation. Pin `CARGO_TARGET_DIR`
    // explicitly so the inner build writes to the same location the
    // resolver below probes first (`$CARGO_TARGET_DIR/...`, then the
    // conventional `~/.cache/edgecloud-cargo/...` fallback, then the
    // per-crate default). Without this, the build succeeds but the
    // artifact lands at `edge-js-runtime/target/...` and the resolver
    // can't find it.
    let target_dir = std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| {
        std::env::var("HOME")
            .map(|h| format!("{h}/.cache/edgecloud-cargo"))
            .unwrap_or_else(|_| "target".to_string())
    });

    println!("  Compiling JS runtime...");
    let status = Command::new("cargo")
        .args(["build", "--target", "wasm32-wasip2", "--release"])
        .current_dir(&runtime_dir)
        .env("EDGE_JS_BUNDLE", bundle_path.canonicalize()?)
        .env("CARGO_TARGET_DIR", &target_dir)
        .spawn()?
        .wait()?;

    if !status.success() {
        anyhow::bail!("JS runtime compilation failed");
    }

    // 4. Locate the built artifact.
    //
    // `cargo build --target wasm32-wasip2 --release` emits a complete
    // component directly (NOT a core module — the wasip2 target bundles
    // the component-model adapter natively). The runtime's CARGO_TARGET_DIR
    // is set explicitly above to `$HOME/.cache/edgecloud-cargo` when the
    // env is unset (the conventional shared cache location, also pinned by
    // the parent monorepo's `.cargo/config.toml` for its own workspace
    // members; the runtime crate is its own `[workspace]` root so the
    // config is not inherited). The resolver below probes that path
    // first; without the explicit `CARGO_TARGET_DIR` on the inner
    // invocation the artifact would land at `edge-js-runtime/target/...`
    // and the resolver would have to fall back to the per-crate default.
    //
    // Note on wasi:http version drift: rustc 1.93's bundled
    // `wit-component` emits `wasi:http/types@0.2.4` (and
    // `wasi:cli/environment@0.2.6`, etc.) in the component's imports.
    // wasmtime 45.0.3's linker matches imports by interface identity +
    // major.minor, so 0.2.1 (the linker) and 0.2.4 (the guest) resolve
    // cleanly. Verified by `edge-runtime/tests/handler_fixture_load.rs`
    // running against a wasip2-built component.
    let core_wasm = resolve_runtime_core_wasm(&runtime_dir)?;

    let artifact = path_for(path, project_name, "js").context("resolving JS artifact path")?;
    if let Some(parent) = artifact.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Step 5: copy the built component into `target/javy/<name>.wasm`.
    // No `wasm-tools component new` wrap needed — the wasip2 cargo
    // output IS already a complete component. The copy preserves the
    // canonical artifact layout under `target/javy/` that `edge deploy`
    // reads (see `path_for`'s JS branch above).
    println!("  Staging component...");
    std::fs::copy(&core_wasm, &artifact)
        .with_context(|| format!("copying {} to {}", core_wasm.display(), artifact.display()))?;

    println!("✓ Built successfully");
    println!("  Artifact: {}", artifact.display());
    Ok(())
}

/// Resolve the on-disk path of the wasip2 component that
/// `cargo build` (above) produced. Probes three locations in order:
///
/// 1. `$CARGO_TARGET_DIR/wasm32-wasip2/release/deps/...` — when set
///    explicitly in the environment (overrides everything).
/// 2. `$HOME/.cache/edgecloud-cargo/wasm32-wasip2/release/deps/...` —
///    the conventional shared cache location. The inner cargo build
///    in step 3 explicitly pins `CARGO_TARGET_DIR` to this path when
///    unset, so this is the common-case probe on a developer box
///    and on CI runners.
/// 3. `<runtime_dir>/target/wasm32-wasip2/release/deps/...` — the
///    per-crate default when no `CARGO_TARGET_DIR` is set anywhere.
///
/// Cargo emits cdylib artifacts under `<triple>/release/deps/`
/// (not `<triple>/release/` directly) when the crate's `[lib]`
/// declares `crate-type = ["cdylib", "rlib"]` — the `release/`
/// directory holds the rlib and incremental metadata. Probing
/// `release/deps/` matches cargo's actual layout.
///
/// The repo's `.cargo/config.toml` is *not* read by the child
/// process directly — `build.target-dir` is a cargo-internal
/// setting that's applied only when cargo runs. The shared target
/// dir is the common case for the dev loop, so we probe it
/// explicitly.
fn resolve_runtime_core_wasm(runtime_dir: &std::path::Path) -> Result<std::path::PathBuf> {
    let name = "edge_js_runtime.wasm";
    let rel = |base: std::path::PathBuf| {
        base.join("wasm32-wasip2")
            .join("release")
            .join("deps")
            .join(name)
    };

    let mut tried: Vec<std::path::PathBuf> = Vec::new();

    if let Ok(t) = std::env::var("CARGO_TARGET_DIR") {
        let candidate = rel(std::path::PathBuf::from(t));
        if candidate.exists() {
            return Ok(candidate);
        }
        tried.push(candidate);
    }

    if let Ok(home) = std::env::var("HOME") {
        let candidate = rel(std::path::PathBuf::from(format!(
            "{home}/.cache/edgecloud-cargo"
        )));
        if candidate.exists() {
            return Ok(candidate);
        }
        tried.push(candidate);
    }

    let default = rel(runtime_dir.join("target"));
    if default.exists() {
        return Ok(default);
    }
    tried.push(default);

    anyhow::bail!(
        "expected core wasm at one of:\n  - {}\n  - <CARGO_TARGET_DIR>/wasm32-wasip2/release/deps/{name}\n\
         Checked (in order):\n{}",
        tried.first().map(|p| p.display().to_string()).unwrap_or_default(),
        tried
            .iter()
            .map(|p| format!("  - {}", p.display()))
            .collect::<Vec<_>>()
            .join("\n")
    )
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
    fn path_for_returns_rust_component_wasm() {
        // Issue #410: the `rust` artifact path is now the wrapped
        // component at `target/component.wasm`, not the raw cargo
        // output. The deploy path reads this file; the cargo output
        // (`core_path_for`) is an intermediate.
        let root = Path::new("/proj");
        let got = path_for(root, "myapp", "rust").expect("rust is a supported language");
        assert_eq!(got, PathBuf::from("/proj/target/component.wasm"));
    }

    #[test]
    fn path_for_returns_javy_target_dir() {
        let root = Path::new("/proj");
        let got = path_for(root, "myapp", "js").expect("js is a supported language");
        assert_eq!(got, PathBuf::from("/proj/target/javy/myapp.wasm"));
    }

    #[test]
    fn core_path_for_uses_target_subdir() {
        // The cargo output is at `target/<target>/release/<name>.wasm`,
        // matching cargo's own layout. Test pinning the target string
        // so a future refactor that drops the target prefix trips the
        // test (the deploy path would silently point at a stale file).
        let root = Path::new("/proj");
        let got = core_path_for(root, "myapp", "wasm32-unknown-unknown");
        assert_eq!(
            got,
            PathBuf::from("/proj/target/wasm32-unknown-unknown/release/myapp.wasm")
        );
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

/// Invoke `tool args…` and return stdout trimmed. Returns an
/// empty string when the tool isn't on PATH (so the SLSA
/// envelope's `buildTools[]` list simply omits unavailable
/// tools — the upload is still well-formed, just with a
/// partial toolchain picture).
fn capture_tool_version(tool: &str, args: &[&str]) -> String {
    match Command::new(tool).args(args).output() {
        Ok(out) if out.status.success() => String::from_utf8_lossy(&out.stdout).trim().to_string(),
        _ => String::new(),
    }
}

/// Compute a deterministic SHA-256 over the project's Rust
/// source bytes (src/**/*.rs). The result is hex-encoded and
/// fits the SLSA `materials[].digest.sha256` slot on the
/// server-side envelope. We deliberately use Cargo's source
/// layout (`src/**/*.rs`), not just the entrypoint — the
/// SLSA spec asks for "all sources that contributed to the
/// build", and `src/` is the canonical Rust layout here.
///
/// Returns `Err` only on IO errors; an empty project (no
/// `src/` dir) returns the SHA-256 of zero bytes, which the
/// downstream audit pipeline sees as "no source manifest was
/// available for this build".
fn compute_source_digest(project_dir: &Path) -> Result<String> {
    use sha2::{Digest, Sha256};

    let mut entries: Vec<PathBuf> = Vec::new();
    let src_dir = project_dir.join("src");
    if src_dir.is_dir() {
        collect_rs_files(&src_dir, &mut entries)?;
    }

    // Sort for deterministic hash on different filesystems /
    // walkdir orders. SLSA wants the materials in a stable
    // order so the envelope signature doesn't churn every
    // build.
    entries.sort();

    let mut hasher = Sha256::new();
    for entry in &entries {
        // Append a length-prefixed path + file contents. The
        // path is included so two projects with identical
        // files but different layouts get distinct digests.
        let relpath = entry
            .strip_prefix(project_dir)
            .unwrap_or(entry)
            .to_string_lossy();
        hasher.update(relpath.as_bytes());
        hasher.update([0u8]); // separator (NUL is unlikely in paths)
        let contents = std::fs::read(entry)
            .with_context(|| format!("reading source file {}", entry.display()))?;
        hasher.update(&contents);
        hasher.update([0u8]);
    }

    let digest = hasher.finalize();
    Ok(hex::encode(digest))
}

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in
        std::fs::read_dir(dir).with_context(|| format!("reading directory {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_rs_files(&path, out)?;
        } else if path.extension().and_then(|s| s.to_str()) == Some("rs") {
            out.push(path);
        }
    }
    Ok(())
}

/// RFC3339 / ISO 8601 timestamp with seconds precision,
/// in UTC. Used as `build_started_on` on the SLSA envelope.
fn iso8601_now() -> String {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    // Avoid bringing in chrono — split the Unix epoch into
    // y/m/d/h/m/s inline. Accurate through year 2100.
    let (year, month, day, hour, min, sec) = epoch_to_civil(secs);
    format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z",
        year = year,
        month = month,
        day = day,
        hour = hour,
        min = min,
        sec = sec,
    )
}

/// Convert seconds-since-Unix-epoch to UTC (Y, M, D, h, m, s).
/// Floored at 1970-01-01 00:00:00 for negative inputs (clamp).
fn epoch_to_civil(secs: u64) -> (i32, u32, u32, u32, u32, u32) {
    let days = (secs / 86400) as i64;
    let time_of_day = secs % 86400;
    let hour = (time_of_day / 3600) as u32;
    let min = ((time_of_day % 3600) / 60) as u32;
    let sec = (time_of_day % 60) as u32;

    // Algorithm from Howard Hinnant's date algorithms.
    // http://howardhinnant.github.io/date_algorithms.html
    let z = days + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y_final = if m <= 2 { y + 1 } else { y };
    (y_final as i32, m, d, hour, min, sec)
}

#[cfg(test)]
mod metadata_tests {
    use super::*;

    #[test]
    fn capture_tool_version_returns_empty_when_missing() {
        // `definitely-not-a-real-tool-xyz` shouldn't be on
        // any test host's PATH. Empty string is the
        // documented "tool unavailable" sentinel.
        let got = capture_tool_version("definitely-not-a-real-tool-xyz", &["--version"]);
        assert!(got.is_empty());
    }

    #[test]
    fn iso8601_now_is_iso_format() {
        let s = iso8601_now();
        // e.g. "2026-07-08T14:21:00Z" — 20 chars exactly.
        assert_eq!(s.len(), 20);
        assert!(s.ends_with('Z'));
        assert_eq!(s.chars().nth(4), Some('-'));
        assert_eq!(s.chars().nth(7), Some('-'));
        assert_eq!(s.chars().nth(10), Some('T'));
        assert_eq!(s.chars().nth(13), Some(':'));
        assert_eq!(s.chars().nth(16), Some(':'));
    }

    #[test]
    fn compute_source_digest_is_stable_across_calls() {
        let dir = tempfile::tempdir().unwrap();
        let src_dir = dir.path().join("src");
        std::fs::create_dir(&src_dir).unwrap();
        std::fs::write(src_dir.join("a.rs"), "fn a() {}").unwrap();
        std::fs::write(src_dir.join("b.rs"), "fn b() {}").unwrap();

        let d1 = compute_source_digest(dir.path()).unwrap();
        let d2 = compute_source_digest(dir.path()).unwrap();
        assert_eq!(d1, d2);
        assert_eq!(d1.len(), 64);
    }

    #[test]
    fn compute_source_digest_changes_when_source_changes() {
        let dir = tempfile::tempdir().unwrap();
        let src_dir = dir.path().join("src");
        std::fs::create_dir(&src_dir).unwrap();
        std::fs::write(src_dir.join("a.rs"), "fn a() {}").unwrap();

        let d1 = compute_source_digest(dir.path()).unwrap();
        std::fs::write(src_dir.join("a.rs"), "fn a() { 1 }").unwrap();
        let d2 = compute_source_digest(dir.path()).unwrap();
        assert_ne!(d1, d2);
    }

    #[test]
    fn compute_source_digest_no_src_dir_returns_zero_hash() {
        let dir = tempfile::tempdir().unwrap();
        let d = compute_source_digest(dir.path()).unwrap();
        // SHA-256 of zero bytes is well-known.
        assert_eq!(
            d,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }
}
