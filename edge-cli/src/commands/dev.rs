//! `edge dev` — local dev server with hot-reload.
//!
//! Builds the wasm artifact with the language-appropriate toolchain
//! (`cargo build --target wasm32-wasip2` for Rust, `javy compile` for
//! JavaScript), serves it locally via `wasmtime serve`, and
//! automatically rebuilds + restarts on file changes.

use anyhow::{Context, Result};
use notify::{Config, RecommendedWatcher, RecursiveMode, Watcher};
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::commands::build;
use crate::config::EdgeToml;
use crate::output;
use crate::LangArg;

/// Local development server with hot-reload.
///
/// Starts `wasmtime serve` after a successful build, then watches the
/// project directory for changes. On modification or creation events,
/// the child process is killed, the project is rebuilt, and a new
/// `wasmtime serve` is spawned.
///
/// `lang` selects the build pipeline (Rust vs JS). The artifact path
/// is resolved through [`build::path_for`] so the lookup stays
/// language-aware — JS projects land at `target/javy/<name>.wasm`,
/// Rust at `target/wasm32-wasip2/release/<name>.wasm`. Pre-fix this
/// command hardcoded cargo + the Rust path, so `edge dev` after
/// `edge init --lang=js myapp` failed with a misleading
/// "Could not find Cargo.toml" (finding 3 of the PR #221 review).
pub fn run(path: &Path, lang: LangArg) -> Result<()> {
    let edge_toml = EdgeToml::from_path(path)?;
    let project_name = edge_toml.project.name.clone();
    let port = 8080;

    println!("Starting dev server for '{}'...", project_name);

    // Initial build — delegate to the same dispatch the user sees
    // from `edge build`, so JS projects invoke javy, not cargo.
    build_project(path, lang)?;

    // Resolve the artifact through `build::path_for` so the path
    // layout stays in one place. We don't use the returned `Result`
    // shape (the path is computed, not read), so we unwrap the
    // `Result` knowing the value comes from the user's `--lang`
    // flag which has already been validated by clap.
    let artifact = build::path_for(path, &project_name, lang.as_str())
        .context("resolving dev artifact path")?;

    if !artifact.exists() {
        anyhow::bail!(
            "built artifact not found at {} — check that the binary name matches the project name",
            artifact.display()
        );
    }

    // Spawn wasmtime serve
    let child = Command::new("wasmtime")
        .args([
            "serve",
            &artifact.to_string_lossy(),
            "--port",
            &port.to_string(),
        ])
        .spawn()
        .with_context(|| {
            "failed to start `wasmtime serve` — install the wasmtime CLI first (https://github.com/bytecodealliance/wasmtime)"
        })?;

    // Track the child in an Arc<Mutex> so the signal handler and
    // rebuild loop can both access it.
    let child: Arc<std::sync::Mutex<std::process::Child>> = Arc::new(std::sync::Mutex::new(child));

    // Signal handling
    let running = Arc::new(AtomicBool::new(true));
    let sig_running = running.clone();
    let sig_child = Arc::clone(&child);
    ctrlc::set_handler(move || {
        if let Ok(mut c) = sig_child.lock() {
            let _ = c.kill();
            let _ = c.wait();
        }
        sig_running.store(false, Ordering::SeqCst);
    })?;

    // File watcher
    let (tx, rx) = std::sync::mpsc::channel();
    let mut watcher = RecommendedWatcher::new(
        move |res: Result<notify::Event, _>| {
            if let Ok(e) = res {
                let _ = tx.send(e);
            }
        },
        Config::default().with_poll_interval(Duration::from_secs(1)),
    )?;

    watcher.watch(path, RecursiveMode::Recursive)?;

    println!("Local:  http://localhost:{}", port);
    println!("Watch:  {}", path.display());
    println!("Ctrl+C to stop\n");

    // Event loop — poll with timeout so we can also check the
    // running flag on Ctrl+C.
    loop {
        match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(event) if event.kind.is_modify() || event.kind.is_create() => {
                println!("\n--- Change detected, rebuilding ---");

                // Kill the running wasmtime process.
                if let Ok(mut c) = child.lock() {
                    let _ = c.kill();
                    let _ = c.wait();
                }

                // Rebuild.
                if let Err(e) = build_project(path, lang) {
                    output::warn(&format!("Build failed: {e}"));
                    continue;
                }

                // Restart.
                match Command::new("wasmtime")
                    .args([
                        "serve",
                        &artifact.to_string_lossy(),
                        "--port",
                        &port.to_string(),
                    ])
                    .spawn()
                {
                    Ok(c) => {
                        if let Ok(mut guard) = child.lock() {
                            *guard = c;
                        }
                    }
                    Err(e) => output::warn(&format!("Restart failed: {e}")),
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Ok(_) => {} // ignore non-modify/create events (e.g. remove, access)
            Err(_) => break,
        }

        if !running.load(Ordering::SeqCst) {
            // Ctrl+C was pressed — cleanup already happened in the
            // signal handler. Exit the loop.
            break;
        }
    }

    Ok(())
}

/// Run the language-appropriate build. Dispatches on `lang`:
/// - `rust` → `cargo build --target wasm32-wasip2` (debug profile;
///   release is overkill for a local dev loop).
/// - `js`   → `javy compile -o <artifact> index.js`.
///
/// Re-invokes the toolchain directly instead of delegating to
/// `commands::build::run` because the dev loop wants the debug
/// profile and the JS entry to be the project-root `index.js`
/// (matching the `edge init --lang=js` starter), whereas `build::run`
/// hardcodes `--release` and uses cargo's release layout. The
/// per-language commands still come from the same set as `edge build`
/// so the toolchain surface stays consistent.
fn build_project(path: &Path, lang: LangArg) -> Result<()> {
    match lang {
        LangArg::Rust => {
            let status = Command::new("cargo")
                .args(["build", "--target", "wasm32-wasip2"])
                .current_dir(path)
                .spawn()?
                .wait()?;
            if !status.success() {
                anyhow::bail!("cargo build failed");
            }
            println!("✓ Built");
        }
        LangArg::Js => {
            let javy = build::probe_javy_with(|name| which::which(name).ok()).ok_or_else(|| {
                anyhow::anyhow!(
                    "`javy` was not found on PATH.\n  \
                     Install from https://github.com/bytecodealliance/javy/releases \
                     (v3.x recommended)\n  \
                     and ensure it is on your PATH before running `edge dev --lang=js`."
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
            let target_dir = path.join("target").join("javy");
            std::fs::create_dir_all(&target_dir)?;
            // The exact filename is `<project_name>.wasm`; resolve
            // it via the same path_for helper the initial build used
            // so we don't duplicate the layout.
            // Read `edge.toml` once to grab the project name — if
            // it's missing the watcher should have caught it earlier.
            let edge_toml = EdgeToml::from_path(path)?;
            let artifact = build::path_for(path, &edge_toml.project.name, "js")
                .context("resolving dev JS artifact path")?;
            let output = Command::new(&javy)
                .arg("compile")
                .arg("-o")
                .arg(&artifact)
                .arg(&entry)
                .current_dir(path)
                .output()?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                output::error(&format!("javy compile failed:\n{stderr}"));
                anyhow::bail!("javy compile failed (see error above)");
            }
            println!("✓ Built");
        }
    }
    Ok(())
}
