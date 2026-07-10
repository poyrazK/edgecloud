//! `edge init` — scaffold a new project.
//!
//! The JS starter (`scaffold_js`) writes a `package.json` that
//! pulls `@edgecloud/sdk` from the npm registry (issue #424).
//! Earlier versions walked up from CWD looking for an in-tree
//! `edge-js-sdk/` and referenced it via `file:...` — that path
//! only worked for monorepo devs and produced a broken
//! `package.json` for everyone else. See `PACKAGE_JSON_TEMPLATE`
//! for the npm contract.

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

#[allow(dead_code)]
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

// `package.json` for the JS starter. The SDK is pulled from npm
// (issue #424) — `edge-js-sdk/package.json` already declares
// `"name": "@edgecloud/sdk"` and `"version": "0.2.0"`, so the
// `^0.2.0` range below is the contract. The single `{name}`
// placeholder is the project name; the SDK version is fixed.
// Bumping the SDK to 0.3.0 requires a coordinated CLI template
// change + a new SDK release — see `edge-js-sdk/README.md`.
const PACKAGE_JSON_TEMPLATE: &str = r#"{
  "name": "{name}",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "scripts": {
    "build": "edge build"
  },
  "dependencies": {
    "@edgecloud/sdk": "^0.2.0"
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

// Active JS starter is `JS_HANDLER_TEMPLATE` above. The
// previous Javy-era `index.js` scaffold (Fetch-style
// `export async function handle(request) → Response`) was removed
// in the QuickJS pilot migration; the runtime contract is now
// `globalThis.handleRequest(req) → {status, body, contentType}`.

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

    // The `package.json` references `@edgecloud/sdk` from npm;
    // see the doc comment on `PACKAGE_JSON_TEMPLATE`. Monorepo
    // devs who want to test a local SDK change should use
    // `npm link edge-js-sdk/` after the scaffold — that's the
    // standard npm workflow and is not encoded here.
    let package_json = PACKAGE_JSON_TEMPLATE.replace("{name}", name);
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

    // ── package.json scaffold (issue #424) ─────────────────────────
    //
    // Three pin tests on `PACKAGE_JSON_TEMPLATE`:
    //
    // 1. The npm version is `^0.2.0` and the literal `"file:"` is
    //    NOT anywhere in the template. Catches a future regression
    //    that re-introduces the path-walk fallback.
    // 2. The substituted template parses as valid JSON and the
    //    parsed `dependencies` map contains the npm reference.
    // 3. The active `JS_HANDLER_TEMPLATE` does NOT mention
    //    `wasi:http` or `Javy` — the dead `INDEX_JS_TEMPLATE`
    //    did, and any future template that re-introduces those
    //    would be a Javy-era regression.

    #[test]
    fn package_json_template_pins_npm_version_and_no_file_path() {
        // The template must reference the SDK via a semver range, not
        // a `file:...` path (the `file:` form is what broke for
        // outside-monorepo users — see issue #424).
        assert!(
            super::PACKAGE_JSON_TEMPLATE.contains(r#""@edgecloud/sdk": "^0.2.0""#),
            "expected pinned npm version ^0.2.0 in PACKAGE_JSON_TEMPLATE; got: {}",
            super::PACKAGE_JSON_TEMPLATE
        );
        assert!(
            !super::PACKAGE_JSON_TEMPLATE.contains("file:"),
            "PACKAGE_JSON_TEMPLATE must not reference the SDK via a file: path (issue #424); got: {}",
            super::PACKAGE_JSON_TEMPLATE
        );
    }

    #[test]
    fn package_json_template_round_trips_through_parser() {
        let result = super::PACKAGE_JSON_TEMPLATE.replace("{name}", "myapp");
        let parsed: serde_json::Value =
            serde_json::from_str(&result).expect("PACKAGE_JSON_TEMPLATE must be valid JSON");
        assert_eq!(parsed["name"], "myapp");
        assert_eq!(
            parsed["dependencies"]["@edgecloud/sdk"], "^0.2.0",
            "expected npm version range in parsed dependencies"
        );
        // `serde_json` is already in scope via other CLI code paths;
        // the test surface keeps the dependency visible to anyone
        // touching this template.
    }

    #[test]
    fn js_handler_template_does_not_import_javy_runtime() {
        // The active scaffold is the QuickJS contract
        // (`globalThis.handleRequest(req) → {status, body, contentType}`).
        // It must NOT mention `wasi:http` directly (that's the
        // host-side binding the runtime injects), and it must NOT
        // mention Javy (the previous runtime the JS pilot replaced).
        assert!(
            !super::JS_HANDLER_TEMPLATE.contains("wasi:http"),
            "JS_HANDLER_TEMPLATE must not reference wasi:http directly; got: {}",
            super::JS_HANDLER_TEMPLATE
        );
        assert!(
            !super::JS_HANDLER_TEMPLATE.contains("Javy"),
            "JS_HANDLER_TEMPLATE must not mention Javy; got: {}",
            super::JS_HANDLER_TEMPLATE
        );
    }
}
