//! `edge dev` — local dev server with hot-reload.

use anyhow::Result;
use notify::{Config, RecommendedWatcher, RecursiveMode, Watcher};
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use crate::config::EdgeToml;
use crate::output;

/// Local development server with hot-reload.
pub fn run(path: &Path) -> Result<()> {
    let edge_toml = EdgeToml::from_path(path)?;
    let project_name = &edge_toml.project.name;

    println!("Starting dev server for '{}'...", project_name);
    println!("Watching for changes (Ctrl+C to stop)\n");

    // Initial build
    build_project(path, project_name)?;

    // Watch for changes
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
    println!("Watching {} for changes...", path.display());

    for event in rx {
        if event.kind.is_modify() || event.kind.is_create() {
            println!("\n--- Change detected, rebuilding ---");
            if let Err(e) = build_project(path, project_name) {
                output::warn(&format!("Build failed: {}", e));
            }
        }
    }

    Ok(())
}

fn build_project(path: &Path, name: &str) -> Result<()> {
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
