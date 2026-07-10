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
    // explicitly to `<runtime_dir>/target` so the inner build writes
    // to a deterministic location the resolver below probes first
    // (`$CARGO_TARGET_DIR/...`, then the shared-target-cache fallback,
    // then the legacy `~/.cache/edgecloud-cargo` cache, then the
    // per-crate default). Without this pin the artifact lands in
    // different places across Cargo versions on different platforms
    // and the resolver probes become order-dependent on toolchain
    // behavior — issue #423's "Cannot find core wasm" failure mode.
    let target_dir = std::env::var("CARGO_TARGET_DIR")
        .unwrap_or_else(|_| runtime_dir.join("target").to_string_lossy().into_owned());

    println!("  Compiling JS runtime...");
    let status = Command::new("cargo")
        .args(["build", "--target", "wasm32-wasip1", "--release"])
        .current_dir(&runtime_dir)
        // Pin the target-dir to <runtime_dir>/target so the artifact
        // is in a known place. The repo's `.cargo/config.toml:37`
        // would otherwise set `../target-cache/edgecloud` (relative
        // to the config file's dir), but `edge-js-runtime` declares
        // `[workspace]` (a separate workspace root), so cargo's
        // inheritance of the parent's `.cargo/config.toml` is
        // ambiguous across Cargo versions and platforms. Pinning
        // here removes the ambiguity; the artifact lands at
        // `<runtime_dir>/target/wasm32-wasip1/release/edge_js_runtime.wasm`,
        // which is exactly probe (4) in `resolve_runtime_core_wasm`.
        .env("CARGO_TARGET_DIR", runtime_dir.join("target"))
        .env("EDGE_JS_BUNDLE", bundle_path.canonicalize()?)
        .env("CARGO_TARGET_DIR", &target_dir)
        .spawn()?
        .wait()?;

    if !status.success() {
        anyhow::bail!("JS runtime compilation failed");
    }

    // 4. Componentize with wasm-tools
    //
    // The runtime's `cargo build --target wasm32-wasip1 --release`
    // above explicitly pins `CARGO_TARGET_DIR` to
    // `<runtime_dir>/target` so the artifact is at a known location
    // (probe (4) in `resolve_runtime_core_wasm`). The shared
    // `build.target-dir = "../target-cache/edgecloud"` in
    // `.cargo/config.toml:37` is overridden by the explicit env var.
    let (core_wasm, adapter) = resolve_js_build_artifacts(&runtime_dir)?;

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
        anyhow::bail!(
            "wasm-tools component new failed (exit {exit}). \
             If the error mentions a missing `wasi_snapshot_preview1.reactor.wasm`, \
             run `sha256sum -c edge-cli/adapters/SHA256SUMS` to verify the vendored adapter is intact, \
             or set $EDGE_JS_WASI_ADAPTER to a custom adapter path. \
             See resolve_wasi_adapter for the lookup path.",
            exit = status
                .code()
                .map(|c| c.to_string())
                .unwrap_or_else(|| "<signal>".to_string()),
        );
    }

    println!("✓ Built successfully");
    println!("  Artifact: {}", artifact.display());
    Ok(())
}

/// Combined resolve of both the JS runtime core wasm and the WASI
/// Preview 1 reactor adapter in one call. Used by `build_js` (which
/// always needs both); the integration test in
/// `resolve_js_build_artifacts_returns_both_paths_from_temp_workspace`
/// exercises the combined path so a regression that fixes one
/// resolver but breaks the other surfaces here.
///
/// Extracted from `build_js` so the integration test doesn't have to
/// construct a fake `Command::new("cargo")` invocation just to call
/// the resolvers.
fn resolve_js_build_artifacts(
    runtime_dir: &std::path::Path,
) -> Result<(std::path::PathBuf, std::path::PathBuf)> {
    let core_wasm = resolve_runtime_core_wasm(runtime_dir)?;
    let adapter = resolve_wasi_adapter()?;
    Ok((core_wasm, adapter))
}

/// Resolve the on-disk path of the wasm32-wasip1 core module that
/// `cargo build` (above) produced. Probes four locations in order:
///
/// 1. `$CARGO_TARGET_DIR/wasm32-wasip1/release/...` — set explicitly
///    by the parent (rare for a CLI invocation; wins over everything).
/// 2. `<workspace>/target-cache/edgecloud/wasm32-wasip1/release/...`
///    — the `build.target-dir = "../target-cache/edgecloud"` setting
///    from `.cargo/config.toml:37`, resolved relative to `runtime_dir`'s
///    parent (cargo was invoked from inside `runtime_dir`, but
///    `build.target-dir` is relative to the CWD at cargo-invocation
///    time, which is `runtime_dir`, so the actual target lands at
///    `<runtime_dir>/../target-cache/edgecloud/...` = `<workspace>/target-cache/edgecloud/...`).
///    Issue #423: the prior probe `$HOME/.cache/edgecloud-cargo/...`
///    missed this directory on fresh clones where the committed
///    `.cargo/config.toml` is in effect.
/// 3. `$HOME/.cache/edgecloud-cargo/wasm32-wasip1/release/...` —
///    legacy local convention kept for dev machines with an older
///    unsynced config; ONE `exists()` call, costs nothing.
/// 4. `<runtime_dir>/target/wasm32-wasip1/release/...` — cargo's
///    own default with no `CARGO_TARGET_DIR` (e.g. a CI run that
///    doesn't pick up `.cargo/config.toml`).
///
/// The repo's `.cargo/config.toml` is *not* read by this CLI
/// process directly — `build.target-dir` is a cargo-internal
/// setting that's applied only when cargo runs. We probe each
/// of the plausible on-disk layouts.
fn resolve_runtime_core_wasm(runtime_dir: &std::path::Path) -> Result<std::path::PathBuf> {
    let name = "edge_js_runtime.wasm";
    let rel = |base: std::path::PathBuf| base.join("wasm32-wasip1").join("release").join(name);

    let mut tried: Vec<std::path::PathBuf> = Vec::new();

    // (1) $CARGO_TARGET_DIR — explicit override wins.
    if let Ok(t) = std::env::var("CARGO_TARGET_DIR") {
        let candidate = rel(std::path::PathBuf::from(t));
        if candidate.exists() {
            return Ok(candidate);
        }
        tried.push(candidate);
    }

    // (2) <workspace>/target-cache/edgecloud/... — matches the committed
    // `.cargo/config.toml` build.target-dir, resolved relative to the
    // workspace root (which is `runtime_dir`'s parent).
    if let Some(parent) = runtime_dir.parent() {
        let candidate = rel(parent.join("target-cache").join("edgecloud"));
        if candidate.exists() {
            return Ok(candidate);
        }
        tried.push(candidate);
    }

    // (3) Legacy $HOME/.cache/edgecloud-cargo — kept for dev machines
    // with an older unsynced config.
    if let Ok(home) = std::env::var("HOME") {
        let candidate = rel(std::path::PathBuf::from(format!(
            "{home}/.cache/edgecloud-cargo"
        )));
        if candidate.exists() {
            return Ok(candidate);
        }
        tried.push(candidate);
    }

    // (4) <runtime_dir>/target/... — cargo's own default.
    let default = rel(runtime_dir.join("target"));
    if default.exists() {
        return Ok(default);
    }
    tried.push(default);

    anyhow::bail!(
        "expected core wasm at one of:\n  - $CARGO_TARGET_DIR/wasm32-wasip1/release/{name}\n\
         \n\
         Checked (in order):\n{}",
        tried
            .iter()
            .map(|p| format!("  - {} (missing)", p.display()))
            .collect::<Vec<_>>()
            .join("\n")
    )
}

/// Walk `<base>/wasm32-wasip2/release/**/<name>` and return the
/// first match. `read_dir` is recursive enough for cargo's output
/// layout (`release/` → `deps/`, `examples/`, `build/`, etc.).
///
/// Kept for the future wasip2 path (currently the `edge-js-runtime`
/// target emits `wasm32-wasip1` + `wasm-tools component new` instead).
/// Issue #423 ships against the wasip1 path; the wasip2 path will
/// pick this helper back up when the migration lands.
#[allow(dead_code)]
fn find_runtime_wasm(base: &std::path::Path) -> Option<std::path::PathBuf> {
    let release = base.join("wasm32-wasip2").join("release");
    let name = "edge_js_runtime.wasm";
    let mut stack = vec![release];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.file_name().map(|f| f == name).unwrap_or(false) {
                return Some(path);
            }
        }
    }
    None
}

/// Combined resolve of both the JS runtime core wasm and the WASI
/// Preview 1 reactor adapter in one call. Used by `build_js` (which
/// always needs both); the integration test in
/// `resolve_js_build_artifacts_returns_both_paths_from_temp_workspace`
/// exercises the combined path so a regression that fixes one
/// resolver but breaks the other surfaces here.
///
/// Extracted from `build_js` so the integration test doesn't have to
/// construct a fake `Command::new("cargo")` invocation just to call
/// the resolvers.
fn resolve_js_build_artifacts(
    runtime_dir: &std::path::Path,
) -> Result<(std::path::PathBuf, std::path::PathBuf)> {
    let core_wasm = resolve_runtime_core_wasm(runtime_dir)?;
    let adapter = resolve_wasi_adapter()?;
    Ok((core_wasm, adapter))
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

/// On-disk path of the vendored WASI Preview 1 reactor adapter.
///
/// The adapter is committed at `<repo>/edge-cli/adapters/wasi_snapshot_preview1.reactor.wasm`
/// (tracked past the root `.gitignore`'s `*.wasm` rule via an explicit
/// exception there) and pinned via the SHA256SUMS sidecar. The bytes
/// are byte-identical to the `v45.0.3` wasmtime release asset — see
/// `edge-cli/adapters/SHA256SUMS` and the `rust-js-build` CI job in
/// `.github/workflows/ci.yml` for the CI-side verification.
///
/// Why vendored, not pulled from the cargo registry: the
/// `wasi-preview1-component-adapter-provider` crate is not a declared
/// dependency anywhere in the workspace, so on a fresh clone the cargo
/// registry cache doesn't contain its artefacts and the prior
/// `resolve_wasi_adapter` glob returned nothing. Issue #423.
///
/// Updating this constant (or moving the vendored file) requires a
/// corresponding `edge build` semantic bump — the wasm-component-tools
/// `--adapt` step will refuse a future-format adapter against an
/// older core module. Track both via the wasmtime pin in
/// `edge-runtime/Cargo.toml`.
fn vendored_wasi_adapter_path() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("adapters")
        .join("wasi_snapshot_preview1.reactor.wasm")
}

/// Locate the `wasi_snapshot_preview1.reactor.wasm` adapter that
/// `wasm-tools component new --adapt <path>` needs to wrap a
/// `wasm32-wasip1` core module as a WASI Preview 2 component. Probes
/// three sources in priority order:
///
/// 1. `EDGE_JS_WASI_ADAPTER` env override (existing; wins over all).
/// 2. **Vendored** adapter at `edge-cli/adapters/wasi_snapshot_preview1.reactor.wasm`
///    relative to this crate's manifest dir. This is the canonical
///    source on a fresh clone; the SHA-256 is checked by the
///    `rust-js-build` CI job.
/// 3. Cargo registry cache: `$CARGO_HOME/registry/src/*/wasi-preview1-component-adapter-provider-*/artefacts/wasi_snapshot_preview1.reactor.wasm`.
///    Kept as a fallback for developers who happen to have the crate
///    cached locally from another project (no network is attempted).
fn resolve_wasi_adapter() -> Result<std::path::PathBuf> {
    resolve_wasi_adapter_with_vendored(&vendored_wasi_adapter_path())
}

/// Testable inner resolver. `vendored` is the absolute path to probe
/// in priority 2; tests pass a `tempdir` path instead of relying on
/// the in-tree vendored file existing.
fn resolve_wasi_adapter_with_vendored(vendored: &std::path::Path) -> Result<std::path::PathBuf> {
    // (1) Env override.
    if let Ok(p) = std::env::var("EDGE_JS_WASI_ADAPTER") {
        let path = std::path::PathBuf::from(&p);
        if path.exists() {
            return Ok(path);
        }
        anyhow::bail!("EDGE_JS_WASI_ADAPTER points at {p}, but that file does not exist");
    }

    // (2) Vendored adapter (canonical, fresh-clone-safe).
    if vendored.exists() {
        return Ok(vendored.to_path_buf());
    }

    // (3) Cargo registry cache fallback.
    //
    // `CARGO_HOME` if set, else `$HOME/.cargo` (the conventional
    // default; `cargo` itself uses this when the env var is unset).
    let cargo_home = match std::env::var("CARGO_HOME") {
        Ok(s) => s,
        Err(_) => match std::env::var("HOME") {
            Ok(h) => format!("{h}/.cargo"),
            Err(_) => anyhow::bail!(
                "neither CARGO_HOME nor HOME is set, and no vendored adapter at {}",
                vendored.display()
            ),
        },
    };
    let registry = std::path::Path::new(&cargo_home)
        .join("registry")
        .join("src");

    // The index subdir name varies by host (e.g.
    // `index.crates.io-1949cf8c6b5b557f`); walk one level deep,
    // then look for the `wasi-preview1-component-adapter-provider-*`
    // crate (matches all semver patch versions) and check for the
    // artefact. Missing registry dir is non-fatal — the vendored
    // adapter is the canonical source; this is just a dev-machine
    // shortcut.
    if let Ok(entries) = std::fs::read_dir(&registry) {
        for entry in entries {
            let Ok(entry) = entry else { continue };
            let Ok(subs) = std::fs::read_dir(entry.path()) else {
                continue;
            };
            for sub in subs {
                let Ok(sub) = sub else { continue };
                let name = sub.file_name();
                let name = name.to_string_lossy();
                if name.starts_with("wasi-preview1-component-adapter-provider-") {
                    let candidate = sub
                        .path()
                        .join("artefacts")
                        .join("wasi_snapshot_preview1.reactor.wasm");
                    if candidate.exists() {
                        return Ok(candidate);
                    }
                }
            }
        }
    }

    anyhow::bail!(
        "Cannot find the wasi-preview1 adapter. Checked (in priority order):\n  \
         1. $EDGE_JS_WASI_ADAPTER (not set)\n  \
         2. vendored at {vendored} (missing)\n  \
         3. cargo registry at {registry} (no wasi-preview1-component-adapter-provider-* crate cached)\n\n\
         The vendored adapter should ship with this repo. If it's missing, \
         run `cd edge-cli/adapters && sha256sum -c SHA256SUMS` to verify the file is intact. \
         To use a custom adapter, set EDGE_JS_WASI_ADAPTER to its absolute path.\n\
         Hint: each source above is annotated `(not set)` / `(missing)` / `(no ... cached)` — \
         that tells you whether the override was empty, the file was deleted, or the registry \
         hasn't been populated by a `cargo install wasm-tools` or `cargo fetch` step.",
        vendored = vendored.display(),
        registry = registry.display(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Mutex serializing the adapter-resolver tests so env-var
    /// mutations (`EDGE_JS_WASI_ADAPTER`, `CARGO_HOME`, `HOME`) don't
    /// race across parallel test threads.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

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

    // ---- Issue #423: vendored-adapter resolver tests ----
    //
    // These tests pin the priority order documented on
    // `resolve_wasi_adapter_with_vendored` (1: env, 2: vendored,
    // 3: registry cache). The shared `ENV_LOCK` mutex serializes
    // them so env-var mutations don't race across parallel test
    // threads.

    /// RAII guard that snapshots process env on construction and
    /// restores every captured variable on drop. Used by the
    /// `resolve_wasi_adapter_with_vendored` tests below so we don't
    /// leak env mutations to other tests in the same process.
    struct EnvGuard {
        snapshot: Vec<(String, Option<String>)>,
    }
    impl EnvGuard {
        fn new(keys: &[&str]) -> Self {
            let snapshot = keys
                .iter()
                .map(|k| (k.to_string(), std::env::var(k).ok()))
                .collect();
            EnvGuard { snapshot }
        }
    }
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (k, v) in self.snapshot.drain(..) {
                match v {
                    Some(s) => std::env::set_var(k, s),
                    None => std::env::remove_var(k),
                }
            }
        }
    }

    #[test]
    fn resolve_wasi_adapter_prefers_env_override_over_vendored() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = EnvGuard::new(&["EDGE_JS_WASI_ADAPTER", "CARGO_HOME", "HOME"]);

        let dir = tempfile::tempdir().unwrap();
        let env_path = dir.path().join("custom-adapter.wasm");
        let vendored = dir.path().join("vendored.wasm");
        std::fs::write(&env_path, b"\0asm\x01\0\0\0env").unwrap();
        std::fs::write(&vendored, b"\0asm\x01\0\0\0vdr").unwrap();

        std::env::set_var("EDGE_JS_WASI_ADAPTER", &env_path);

        let got = resolve_wasi_adapter_with_vendored(&vendored).expect("env override should win");
        assert_eq!(got, env_path, "env override must beat vendored adapter");
    }

    #[test]
    fn resolve_wasi_adapter_prefers_vendored_over_registry() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = EnvGuard::new(&["EDGE_JS_WASI_ADAPTER", "CARGO_HOME", "HOME"]);

        // No env override → vendored wins over registry.
        std::env::remove_var("EDGE_JS_WASI_ADAPTER");

        let dir = tempfile::tempdir().unwrap();
        let vendored = dir.path().join("vendored.wasm");
        std::fs::write(&vendored, b"\0asm\x01\0\0\0vdr").unwrap();

        // Point CARGO_HOME at a directory that does NOT contain the
        // registry glob target, so the only way the resolver can
        // succeed is via the vendored path.
        let cargo_home = dir.path().join("cargo-home");
        std::fs::create_dir_all(&cargo_home).unwrap();
        std::env::set_var("CARGO_HOME", &cargo_home);
        std::env::set_var("HOME", dir.path());

        let got = resolve_wasi_adapter_with_vendored(&vendored)
            .expect("vendored adapter should be returned when registry is empty");
        assert_eq!(
            got, vendored,
            "vendored adapter must beat registry fallback"
        );
    }

    #[test]
    fn resolve_wasi_adapter_falls_back_to_registry_when_vendored_missing() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = EnvGuard::new(&["EDGE_JS_WASI_ADAPTER", "CARGO_HOME", "HOME"]);

        std::env::remove_var("EDGE_JS_WASI_ADAPTER");

        let dir = tempfile::tempdir().unwrap();
        let vendored = dir.path().join("does-not-exist.wasm"); // absent
        assert!(!vendored.exists());

        // Stage a fake registry containing the crate +
        // `wasi_snapshot_preview1.reactor.wasm` artefact.
        let cargo_home = dir.path().join("cargo-home");
        let registry_index = cargo_home
            .join("registry")
            .join("src")
            .join("index.crates.io-abc");
        let crate_dir = registry_index.join("wasi-preview1-component-adapter-provider-45.0.3.0");
        let artefacts = crate_dir.join("artefacts");
        std::fs::create_dir_all(&artefacts).unwrap();
        let adapter = artefacts.join("wasi_snapshot_preview1.reactor.wasm");
        std::fs::write(&adapter, b"\0asm\x01\0\0\0reg").unwrap();
        std::env::set_var("CARGO_HOME", &cargo_home);
        std::env::set_var("HOME", dir.path());

        let got = resolve_wasi_adapter_with_vendored(&vendored)
            .expect("registry cache must be used as fallback when vendored is absent");
        assert_eq!(
            got, adapter,
            "registry adapter must be returned when vendored is absent"
        );
    }

    #[test]
    fn resolve_wasi_adapter_reports_vendored_path_when_all_probes_miss() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = EnvGuard::new(&["EDGE_JS_WASI_ADAPTER", "CARGO_HOME", "HOME"]);

        std::env::remove_var("EDGE_JS_WASI_ADAPTER");

        let dir = tempfile::tempdir().unwrap();
        let vendored = dir.path().join("missing-vendored.wasm");

        let cargo_home = dir.path().join("cargo-home");
        std::fs::create_dir_all(&cargo_home).unwrap();
        std::env::set_var("CARGO_HOME", &cargo_home);
        std::env::set_var("HOME", dir.path());

        let err = resolve_wasi_adapter_with_vendored(&vendored)
            .expect_err("all probes miss should error");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("Cannot find the wasi-preview1 adapter"),
            "error should announce the failure mode, got: {msg}"
        );
        assert!(
            msg.contains(&vendored.display().to_string()),
            "error should mention the vendored path the resolver looked at, got: {msg}"
        );
    }

    #[test]
    fn resolve_runtime_core_wasm_probes_configured_target_cache() {
        // Build a fake workspace layout:
        //   <temp>/edge-js-runtime/                       (runtime_dir)
        //   <temp>/target-cache/edgecloud/wasm32-wasip1/release/edge_js_runtime.wasm
        // and assert the configured target-cache path wins over the
        // legacy $HOME probe (which is empty here) and the
        // per-crate default (which is also empty).
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = EnvGuard::new(&["CARGO_TARGET_DIR", "HOME"]);

        std::env::remove_var("CARGO_TARGET_DIR");

        let dir = tempfile::tempdir().unwrap();
        let runtime_dir = dir.path().join("edge-js-runtime");
        let cache = dir
            .path()
            .join("target-cache")
            .join("edgecloud")
            .join("wasm32-wasip1")
            .join("release");
        std::fs::create_dir_all(&cache).unwrap();
        let core = cache.join("edge_js_runtime.wasm");
        std::fs::write(&core, b"\0asm\x01\0\0\0core").unwrap();

        let got = resolve_runtime_core_wasm(&runtime_dir)
            .expect("configured target-cache path should win");
        assert_eq!(
            got, core,
            "expected the configured target-cache probe to succeed"
        );
    }

    #[test]
    fn resolve_runtime_core_wasm_marks_probed_paths_missing_on_bail() {
        // F3 review follow-up: each probed path in the bail message
        // is annotated with `(missing)` so operators can distinguish
        // "the file got deleted" from "the file is the wrong shape".
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = EnvGuard::new(&["CARGO_TARGET_DIR", "HOME"]);

        std::env::remove_var("CARGO_TARGET_DIR");

        let dir = tempfile::tempdir().unwrap();
        let runtime_dir = dir.path().join("edge-js-runtime");
        std::fs::create_dir_all(&runtime_dir).unwrap();

        let err = resolve_runtime_core_wasm(&runtime_dir).expect_err("no probe hits should error");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("(missing)"),
            "error should annotate each probed path with `(missing)`, got: {msg}"
        );
        // Sanity check: bail message should still list the configured
        // target-cache path (probe (2)) so an operator can see whether
        // the cargo config got picked up.
        let expected_cache = dir.path().join("target-cache").join("edgecloud");
        assert!(
            msg.contains(&expected_cache.display().to_string()),
            "error should mention the configured target-cache probe path, got: {msg}"
        );
    }

    #[test]
    fn resolve_js_build_artifacts_returns_both_paths_from_temp_workspace() {
        // F2 review follow-up: integration test that exercises the
        // combined resolve path `build_js` uses. Stages a fake
        // workspace with BOTH (a) the runtime core wasm at the
        // configured target-cache path AND (b) the vendored adapter
        // at `<temp>/edge-cli/adapters/...`, then asserts both come
        // back from the combined resolver. Catches the class of
        // regression where one resolver's priority order is fixed
        // but the other is re-broken by a later refactor.
        //
        // We override `CARGO_MANIFEST_DIR`-style resolution by
        // constructing a vendored path inside the tempdir and then
        // patching env so `resolve_wasi_adapter_with_vendored` finds
        // it; the simpler version of the same trick is to rely on
        // `resolve_wasi_adapter_with_vendored` directly via a temp
        // helper, but `build_js` calls `resolve_wasi_adapter` (the
        // manifest-dir-relative one). For this test we stage the
        // vendored file at the *real* manifest path and let the
        // resolver find it; the integration under test is therefore
        // "both resolvers return non-error on a coherent workspace
        // layout", which is the actual regression mode we care
        // about.
        let _lock = ENV_LOCK.lock().unwrap();
        let _env = EnvGuard::new(&["CARGO_TARGET_DIR", "HOME"]);

        std::env::remove_var("CARGO_TARGET_DIR");

        // Stage a fake workspace layout: `<temp>/edge-js-runtime/`
        // + `<temp>/target-cache/edgecloud/wasm32-wasip1/release/edge_js_runtime.wasm`.
        let dir = tempfile::tempdir().unwrap();
        let runtime_dir = dir.path().join("edge-js-runtime");
        std::fs::create_dir_all(&runtime_dir).unwrap();
        let cache = dir
            .path()
            .join("target-cache")
            .join("edgecloud")
            .join("wasm32-wasip1")
            .join("release");
        std::fs::create_dir_all(&cache).unwrap();
        let core = cache.join("edge_js_runtime.wasm");
        std::fs::write(&core, b"\0asm\x01\0\0\0core").unwrap();

        // The combined resolver needs `resolve_wasi_adapter` to
        // find the vendored file at the manifest-relative path —
        // which is exactly the file the in-tree vendoring committed
        // (sha256 49fafb…5bea). So a coherent workspace always
        // finds both pieces via the *real* vendored file. Assert
        // the core-wasm resolves to the staged tempdir file and
        // the adapter resolves to the in-tree vendored file.
        let (resolved_core, resolved_adapter) =
            resolve_js_build_artifacts(&runtime_dir).expect("combined resolve should succeed");
        assert_eq!(
            resolved_core, core,
            "core wasm should resolve to the staged target-cache file"
        );
        assert_eq!(
            resolved_adapter,
            vendored_wasi_adapter_path(),
            "adapter should resolve to the in-tree vendored file"
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
