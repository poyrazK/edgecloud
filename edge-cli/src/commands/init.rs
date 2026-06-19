//! `edge init` — scaffold a new project.

use crate::output;
use anyhow::Result;

const EDGE_TOML_HEADER: &str = r#"[project]
name = "{name}"
version = "0.1.0"
target = "wasm32-wasip2"

[deployment]
"#;

/// `edge.toml` body when `--api <URL>` was supplied. The URL is
/// substituted at write time. When `--api` is omitted, the
/// `[deployment]` section is left empty so the runtime falls back to
/// `EDGE_API_URL` → `~/.config/edgecloud/config.toml` → the default
/// production URL at deploy time.
const EDGE_TOML_DEPLOYMENT_WITH_API: &str = r#"api = "{api}"
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
///
/// `api` is the optional control-plane URL written into `[deployment]`.
/// When `None`, the `[deployment]` section is left empty so the
/// runtime falls back to `EDGE_API_URL` → `~/.config/edgecloud/config.toml`
/// → `https://api.edgecloud.dev`.
pub fn run(name: &str, api: Option<&str>) -> Result<()> {
    let dir = std::path::Path::new(name);

    if dir.exists() {
        anyhow::bail!("directory '{}' already exists", name);
    }

    std::fs::create_dir_all(dir)?;

    // edge.toml — header + optional api line.
    let mut edge_toml = EDGE_TOML_HEADER.replace("{name}", name);
    if let Some(url) = api {
        edge_toml.push_str(&EDGE_TOML_DEPLOYMENT_WITH_API.replace("{api}", url));
    }
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
    output::hint("Next: edge auth signup  (or `edge auth login` if you already have an API key)");
    if api.is_none() {
        output::hint(
            "no --api given; edge.toml will fall back to EDGE_API_URL or \
             ~/.config/edgecloud/config.toml at deploy time",
        );
    }
    Ok(())
}
