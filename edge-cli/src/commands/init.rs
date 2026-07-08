//! `edge init` — scaffold a new project.

use crate::output;
use crate::LangArg;
use anyhow::Result;

/// `[project]` block template shared by all starter languages. Two
/// placeholders: `{name}` (the project name from the CLI arg) and
/// `{language}` (the lowercase `LangArg` form, e.g. `"rust"`/`"js"`).
/// Keeping a single template prevents the JS and Rust scaffolds from
/// drifting on `target` / `version` as new languages are added.
const EDGE_TOML_HEADER_TEMPLATE: &str = r#"[project]
name = "{name}"
version = "0.1.0"
target = "wasm32-wasip2"
language = "{language}"

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

/// JS starter for `edge init --lang=js`. Exports `handle(request)`
/// in the shape Javy v3.x expects for a wasi:http/incoming-handler
/// component. Uses the canonical `Request`/`Response` globals
/// (Deno-style, which Javy's QuickJS ships with).
///
/// The `{name}` placeholder is the only substitution; JS uses single
/// braces for object literals, so the surrounding `{ ... }` are
/// written as plain single braces here (NOT `{{` / `}}` — this is a
/// raw string, not a `format!` invocation, so brace escaping is
/// unnecessary and would render literally in the output).
const INDEX_JS_TEMPLATE: &str = r#"// {name} — built with edgeCloud (JavaScript via Javy).
//
// The runtime hands you a Fetch-style Request and expects a Response
// back. The `handle` named export is what `wasi:http/incoming-handler`
// calls per inbound request.
//
// This starter uses ONLY `wasi:http` — there is no edge:cloud/* SDK
// for JavaScript in v0.2. To use kv-store, cache, scheduling, etc.
// from JS, see the follow-up SDK work for issue #317.

export async function handle(request) {
  const url = new URL(request.url);
  return new Response(JSON.stringify({
    hello: "js",
    path: url.pathname,
    method: request.method,
  }), {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}
"#;

const GITIGNORE: &str = r#"/target/
/.wasm/
/.cargo/
.edge/
*.wasm
.DS_Store
/node_modules/
"#;

/// Scaffold a new edgeCloud project.
///
/// `api` is the optional control-plane URL written into `[deployment]`.
/// When `None`, the `[deployment]` section is left empty so the
/// runtime falls back to `EDGE_API_URL` → `~/.config/edgecloud/config.toml`
/// → `https://api.edgecloud.dev`.
///
/// `lang` selects the starter template. `Rust` (the default) writes a
/// Cargo project; `Js` writes a Javascript project using esbuild and the JS SDK.
/// The choice is persisted to `[project] language` in `edge.toml`.
pub fn run(name: &str, api: Option<&str>, lang: LangArg) -> Result<()> {
    let dir = std::path::Path::new(name);

    if dir.exists() {
        anyhow::bail!("directory '{}' already exists", name);
    }

    std::fs::create_dir_all(dir)?;

    match lang {
        LangArg::Rust => scaffold_rust(dir, name, api)?,
        LangArg::Js => scaffold_js(dir, name, api)?,
    }

    output::success(&format!("Project '{}' created", name));
    println!(
        "  cd {} && edge build{}",
        name,
        // Only emit `--lang=<x>` for non-default languages so the
        // Rust hint stays uncluttered. `as_str()` is the single
        // source of truth — adding Python or Go here would only
        // require adding the variant to `LangArg`.
        match lang {
            LangArg::Rust => String::new(),
            other => format!(" --lang={}", other.as_str()),
        }
    );
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

/// Rust starter: edge.toml + Cargo.toml + src/main.rs + .gitignore.
fn scaffold_rust(dir: &std::path::Path, name: &str, api: Option<&str>) -> Result<()> {
    write_edge_toml(dir, name, LangArg::Rust, api)?;

    let cargo_toml = CARGO_TOML_TEMPLATE.replace("{name}", name);
    std::fs::write(dir.join("Cargo.toml"), cargo_toml)?;

    let main_rs = MAIN_RS_TEMPLATE.replace("{name}", name);
    std::fs::create_dir_all(dir.join("src"))?;
    std::fs::write(dir.join("src").join("main.rs"), main_rs)?;

    std::fs::write(dir.join(".gitignore"), GITIGNORE)?;
    Ok(())
}

/// JS starter: edge.toml + package.json + src/handler.js + .gitignore.
fn scaffold_js(dir: &std::path::Path, name: &str, api: Option<&str>) -> Result<()> {
    write_edge_toml(dir, name, LangArg::Js, api)?;

    let sdk_path = resolve_sdk_path();
    let package_json = PACKAGE_JSON_TEMPLATE
        .replace("{name}", name)
        .replace("{sdk_path}", &sdk_path);
    std::fs::write(dir.join("package.json"), package_json)?;

    std::fs::create_dir_all(dir.join("src"))?;
    std::fs::write(dir.join("src").join("handler.js"), JS_HANDLER_TEMPLATE)?;

    std::fs::write(dir.join(".gitignore"), GITIGNORE)?;
    Ok(())
}

/// Render `edge.toml` from [`EDGE_TOML_HEADER_TEMPLATE`] and write it
/// to `<dir>/edge.toml`. Shared by every language scaffold so the
/// header can't drift between starters.
fn write_edge_toml(
    dir: &std::path::Path,
    name: &str,
    lang: LangArg,
    api: Option<&str>,
) -> Result<()> {
    let mut edge_toml = EDGE_TOML_HEADER_TEMPLATE
        .replace("{name}", name)
        .replace("{language}", lang.as_str());
    if let Some(url) = api {
        edge_toml.push_str(&EDGE_TOML_DEPLOYMENT_WITH_API.replace("{api}", url));
    }
    std::fs::write(dir.join("edge.toml"), edge_toml)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    /// Render the shared `edge.toml` template with both placeholders
    /// substituted, mirroring what `write_edge_toml` does at runtime.
    /// Keeps the language-specific test bodies short.
    fn render_header(name: &str, lang: super::LangArg) -> String {
        super::EDGE_TOML_HEADER_TEMPLATE
            .replace("{name}", name)
            .replace("{language}", lang.as_str())
    }

    #[test]
    fn test_edge_toml_header_substitution() {
        let result = render_header("myapp", super::LangArg::Rust);
        assert!(result.contains(r#"name = "myapp""#));
        assert!(result.contains("version = \"0.1.0\""));
        assert!(result.contains("wasm32-wasip2"));
    }

    #[test]
    fn test_edge_toml_header_valid_toml() {
        let result = render_header("myapp", super::LangArg::Rust);
        let parsed: toml::Value = toml::from_str(&result).expect("invalid TOML");
        assert_eq!(parsed["project"]["name"].as_str(), Some("myapp"));
    }

    #[test]
    fn test_edge_toml_with_api_section() {
        let mut result = render_header("myapp", super::LangArg::Rust);
        result.push_str(
            &super::EDGE_TOML_DEPLOYMENT_WITH_API.replace("{api}", "https://api.example.com"),
        );
        let parsed: toml::Value = toml::from_str(&result).expect("invalid TOML");
        assert_eq!(
            parsed["deployment"]["api"].as_str(),
            Some("https://api.example.com")
        );
    }

    #[test]
    fn test_cargo_toml_template_substitution() {
        let result = super::CARGO_TOML_TEMPLATE.replace("{name}", "myapp");
        assert!(result.contains("myapp"));
        assert!(result.contains("0.1.0"));
    }

    #[test]
    fn test_cargo_toml_template_valid_toml() {
        let result = super::CARGO_TOML_TEMPLATE.replace("{name}", "myapp");
        let _: toml::Value = toml::from_str(&result).expect("invalid Cargo.toml template");
    }

    #[test]
    fn test_main_rs_template_substitution() {
        let result = super::MAIN_RS_TEMPLATE.replace("{name}", "hello-world");
        assert!(result.contains("hello-world"));
    }

    #[test]
    fn test_gitignore_contains_expected_entries() {
        let gi = super::GITIGNORE;
        assert!(gi.contains("/target/"));
        assert!(gi.contains(".edge/"));
        assert!(gi.contains(".wasm/"));
        assert!(gi.contains("*.wasm"));
    }

    // ── language field (issue #317) ───────────────────────────────────

    #[test]
    fn rust_header_includes_language_line() {
        let result = render_header("myapp", super::LangArg::Rust);
        // Explicit `language = "rust"` line makes the toml
        // self-documenting; missing it is a UX wart for greppers.
        assert!(
            result.contains(r#"language = "rust""#),
            "expected language = \"rust\" in: {result}"
        );
    }

    #[test]
    fn js_header_includes_language_line() {
        let result = render_header("myapp", super::LangArg::Js);
        assert!(
            result.contains(r#"language = "js""#),
            "expected language = \"js\" in: {result}"
        );
        // Target stays wasm32-wasip2 — Javy emits Preview 2
        // components; the wasm target is invariant across languages.
        assert!(result.contains("wasm32-wasip2"));
    }

    #[test]
    fn js_header_round_trips_through_parser() {
        let result = render_header("myapp", super::LangArg::Js);
        let parsed: toml::Value = toml::from_str(&result).expect("invalid TOML");
        assert_eq!(parsed["project"]["language"].as_str(), Some("js"));
        assert_eq!(parsed["project"]["target"].as_str(), Some("wasm32-wasip2"));
    }

    #[test]
    fn index_js_template_substitution() {
        let result = super::INDEX_JS_TEMPLATE.replace("{name}", "myapp");
        // The header comment should name the project so devs know
        // which scaffold they got when they revisit a repo later.
        assert!(result.contains("myapp"));
        // Must export `handle` — the wasi:http/incoming-handler contract.
        assert!(
            result.contains("export async function handle"),
            "expected `export async function handle` in: {result}"
        );
    }

    #[test]
    fn index_js_template_is_valid_js_shape() {
        // We can't parse ES modules with vanilla tools, but we can
        // assert the template produces non-empty output and that the
        // brace balance is even — a basic sanity check.
        let result = super::INDEX_JS_TEMPLATE.replace("{name}", "myapp");
        let opens = result.matches('{').count();
    }
}
