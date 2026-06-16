//! `edge init` — scaffold a new project.

use crate::output;
use anyhow::Result;

const EDGE_TOML_TEMPLATE: &str = r#"[project]
name = "{name}"
version = "0.1.0"
target = "wasm32-wasip2"

[deployment]
api = "https://api.edgecloud.dev"
"#;

const CARGO_TOML_TEMPLATE: &str = r#"[package]
name = "{name}"
version = "0.1.0"
edition = "2021"

[dependencies]
"#;

const MAIN_RS_TEMPLATE: &str = r#"//! {name} — built with edgeCloud.

use std::io::{self, Write};

fn main() {
    // WASI Preview 2 component entry point
    writeln!(io::stdout(), "Hello from edgeCloud!").unwrap();
}
"#;

const GITIGNORE: &str = r#"/target/
/.wasm/
/.cargo/
.edge/
*.wasm
.DS_Store
"#;

/// Scaffold a new edgeCloud project.
pub fn run(name: &str) -> Result<()> {
    let dir = std::path::Path::new(name);

    if dir.exists() {
        anyhow::bail!("directory '{}' already exists", name);
    }

    std::fs::create_dir_all(dir)?;

    // edge.toml
    let edge_toml = EDGE_TOML_TEMPLATE.replace("{name}", name);
    std::fs::write(dir.join("edge.toml"), edge_toml)?;

    // Cargo.toml
    let cargo_toml = CARGO_TOML_TEMPLATE.replace("{name}", name);
    std::fs::write(dir.join("Cargo.toml"), cargo_toml)?;

    // src/main.rs
    let main_rs = MAIN_RS_TEMPLATE.replace("{name}", name);
    std::fs::create_dir_all(dir.join("src"))?;
    std::fs::write(dir.join("src").join("main.rs"), main_rs)?;

    // .gitignore
    std::fs::write(dir.join(".gitignore"), GITIGNORE)?;

    output::success(&format!("Project '{}' created", name));
    println!("  cd {} && edge build", name);
    Ok(())
}
