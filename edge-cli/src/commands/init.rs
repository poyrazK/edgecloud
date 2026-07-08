//! `edge init` — scaffold a new project.

use crate::output;
use anyhow::Result;

const EDGE_TOML_HEADER: &str = r#"[project]
name = "{name}"
version = "0.1.0"
target = "wasm32-wasip2"

[deployment]
"#;

const EDGE_TOML_HEADER_JS: &str = r#"[project]
name = "{name}"
version = "0.1.0"
target = "wasm32-wasip2"
language = "js"

[deployment]
"#;

/// `edge.toml` body when `--api <URL>` was supplied.
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

const PACKAGE_JSON_TEMPLATE: &str = r#"{
  "name": "{name}",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "scripts": {
    "build": "edge build"
  },
  "dependencies": {
    "@edgecloud/sdk": "{sdk_path}"
  },
  "devDependencies": {
    "esbuild": "^0.25.0"
  }
}
"#;

const JS_HANDLER_TEMPLATE: &str = r#"import { kv, time } from '@edgecloud/sdk';

/**
 * Handle an incoming HTTP request.
 * @param {{ method: string, path: string, headers: object, body: string }} req
 * @returns {{ status: number, body: string, contentType?: string }}
 */
function handleRequest(req) {
  const now = time.now();
  return {
    status: 200,
    body: JSON.stringify({
      hello: "world",
      path: req.path,
      now: Number(now),
    }),
    contentType: "application/json",
  };
}

// Export to globalThis so the runtime can call it.
globalThis.handleRequest = handleRequest;
"#;

const GITIGNORE: &str = r#"target/
.edge/
node_modules/
*.wasm
.DS_Store
"#;

/// Scaffold a new edgeCloud project.
pub fn run(name: &str, api: Option<&str>, lang: Option<&str>) -> Result<()> {
    let dir = std::path::Path::new(name);

    if dir.exists() {
        anyhow::bail!("directory '{}' already exists", name);
    }

    std::fs::create_dir_all(dir)?;

    let language = lang.unwrap_or("rust");

    if language == "js" || language == "javascript" {
        // edge.toml (JS)
        let mut edge_toml = EDGE_TOML_HEADER_JS.replace("{name}", name);
        if let Some(url) = api {
            edge_toml.push_str(&EDGE_TOML_DEPLOYMENT_WITH_API.replace("{api}", url));
        }
        std::fs::write(dir.join("edge.toml"), edge_toml)?;

        // package.json
        let sdk_path = resolve_sdk_path();
        let package_json = PACKAGE_JSON_TEMPLATE
            .replace("{name}", name)
            .replace("{sdk_path}", &sdk_path);
        std::fs::write(dir.join("package.json"), package_json)?;

        // src/handler.js
        std::fs::create_dir_all(dir.join("src"))?;
        std::fs::write(dir.join("src").join("handler.js"), JS_HANDLER_TEMPLATE)?;
    } else {
        // edge.toml (Rust)
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
    }

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

fn resolve_sdk_path() -> String {
    if let Ok(cwd) = std::env::current_dir() {
        let mut dir = cwd;
        for _ in 0..5 {
            let sdk_dir = dir.join("edge-js-sdk");
            if sdk_dir.join("package.json").exists() {
                if let Ok(abs_sdk) = sdk_dir.canonicalize() {
                    return format!("file:{}", abs_sdk.to_string_lossy());
                }
            }
            if !dir.pop() {
                break;
            }
        }
    }
    "file:../edge-js-sdk".to_string()
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_edge_toml_header_substitution() {
        let result = super::EDGE_TOML_HEADER.replace("{name}", "myapp");
        assert!(result.contains(r#"name = "myapp""#));
        assert!(result.contains("version = \"0.1.0\""));
        assert!(result.contains("wasm32-wasip2"));
    }

    #[test]
    fn test_edge_toml_header_valid_toml() {
        let result = super::EDGE_TOML_HEADER.replace("{name}", "myapp");
        let parsed: toml::Value = toml::from_str(&result).expect("invalid TOML");
        assert_eq!(parsed["project"]["name"].as_str(), Some("myapp"));
    }
}
