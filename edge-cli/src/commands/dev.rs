//! `edge dev` — local dev server with hot-reload.
//!
//! Builds the wasm artifact with `cargo build --target wasm32-wasip2`,
//! serves it locally via `wasmtime serve`, and automatically rebuilds
//! + restarts on file changes.

use anyhow::{Context, Result};
use notify::{Config, RecommendedWatcher, RecursiveMode, Watcher};
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::config::EdgeToml;
use crate::output;

/// Local development server with hot-reload.
///
/// Starts `wasmtime serve` after a successful build, then watches the
/// project directory for changes. On modification or creation events,
/// the child process is killed, the project is rebuilt, and a new
/// `wasmtime serve` is spawned.
pub fn run(path: &Path) -> Result<()> {
    let edge_toml = EdgeToml::from_path(path)?;
    let project_name = edge_toml.project.name.clone();
    let port = 8080;

    println!("Starting dev server for '{}'...", project_name);

    // Initial build
    build_project(path, &project_name)?;

    let artifact = if edge_toml.project.language == "js" {
        path.join("target")
            .join("wasm32-wasip2")
            .join("release")
            .join(format!("{}.wasm", project_name))
    } else {
        path.join("target")
            .join("wasm32-wasip2")
            .join("debug")
            .join(format!("{}.wasm", project_name))
    };

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
            "--addr",
            &format!("127.0.0.1:{}", port),
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
                // Ignore changes to target/, .edge/, and node_modules/
                let should_ignore = event.paths.iter().any(|p| {
                    let s = p.to_string_lossy();
                    s.contains("/target/") || s.contains("/.edge/") || s.contains("/node_modules/")
                });
                if should_ignore {
                    continue;
                }

                println!("\n--- Change detected, rebuilding ---");

                // Kill the running wasmtime process.
                if let Ok(mut c) = child.lock() {
                    let _ = c.kill();
                    let _ = c.wait();
                }

                // Rebuild.
                if let Err(e) = build_project(path, &project_name) {
                    output::warn(&format!("Build failed: {e}"));
                    continue;
                }

                // Restart.
                match Command::new("wasmtime")
                    .args([
                        "serve",
                        &artifact.to_string_lossy(),
                        "--addr",
                        &format!("127.0.0.1:{}", port),
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

fn build_project(path: &Path, name: &str) -> Result<()> {
    let edge_toml = EdgeToml::from_path(path)?;
    if edge_toml.project.language == "js" || edge_toml.project.language == "javascript" {
        return crate::commands::build::run(path);
    }

    let status = Command::new("cargo")
        .args(["build", "--target", "wasm32-wasip2"])
        .current_dir(path)
        .spawn()?
        .wait()?;

    if !status.success() {
        anyhow::bail!("build failed");
    }

    let artifact = path
        .join("target")
        .join("wasm32-wasip2")
        .join("debug")
        .join(format!("{}.wasm", name));

    if artifact.exists() {
        println!("✓ Built: {}", artifact.display());
    }
    Ok(())
}
