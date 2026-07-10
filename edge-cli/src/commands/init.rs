//! `edge init` — scaffold a new project.
//!
//! The JS starter (`scaffold_js`) writes a `package.json` that
//! pulls `@edgecloud/sdk` from the npm registry (issue #424).
//! Earlier versions walked up from CWD looking for an in-tree
//! `edge-js-sdk/` and referenced it via `file:...` — that path
//! only worked for monorepo devs and produced a broken
//! `package.json` for everyone else. See `PACKAGE_JSON_TEMPLATE`
//! for the npm contract.
//!
//! The Rust starter (`scaffold_rust` — issue #576) writes a
//! FaaS-shaped `src/lib.rs` + `Cargo.toml` modeled on
//! `samples/hello/`, plus a vendored `wit/` tree so the project
//! builds offline outside the monorepo. The WIT tree is embedded
//! at compile time via `include_dir!`; see `crate::scaffold::wit`.
//!
//! The literal template strings the scaffold writes live in
//! `crate::scaffold::templates` (split out per the PR #589
//! review); this module owns the procedural "which file do we
//! write where" wiring.

use crate::output;
use crate::scaffold::templates::{
    CARGO_TOML_TEMPLATE, EDGE_TOML_DEPLOYMENT_WITH_API, EDGE_TOML_JS_TAIL, EDGE_TOML_PREAMBLE,
    EDGE_TOML_RUST_TAIL, GITIGNORE, JS_HANDLER_TEMPLATE, LIB_RS_TEMPLATE, PACKAGE_JSON_TEMPLATE,
};
use crate::LangArg;
use anyhow::{Context, Result};

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
///
/// Environment variables:
/// - `EDGE_VERIFY_EMBED=1` — after writing the scaffolded `wit/`
///   (Rust starter only), byte-compare the freshly-written tree
///   against the CLI's compiled-in `WIT_TREE` and `bail!` on drift.
///   Catches a stale CLI install (one built before a recent
///   `wit/` edit). Off by default; the CI merge-gate guard is the
///   `wit_embed_matches_canonical_wit_tree` unit test, see #592.
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

/// Rust starter: edge.toml + Cargo.toml + src/lib.rs + wit/ + .gitignore.
///
/// `wit/` is populated by `crate::scaffold::wit::write_wit_tree` (commit 2).
/// The `src/lib.rs` body wires `wit_bindgen::generate!({ path: "wit" })`
/// against the vendored tree, so the project builds offline outside the
/// monorepo. `samples/hello/` is the reference for every line here.
///
/// Setting `EDGE_VERIFY_EMBED=1` after `write_wit_tree` runs a
/// byte-for-byte check between `WIT_TREE` and the freshly-written
/// `wit/` on disk — surfaces a stale CLI install (one built before
/// the most recent `wit/` edit) loudly instead of silently shipping
/// outdated WIT into the scaffolded project. Off by default so the
/// normal path doesn't pay the recursive-walk cost; CI doesn't set
/// it (the unit test in `scaffold::wit::tests` already runs in
/// `rust-test` and provides the merge-gate guard — see #592).
fn scaffold_rust(dir: &std::path::Path, name: &str, api: Option<&str>) -> Result<()> {
    write_edge_toml(dir, name, LangArg::Rust, api)?;

    let cargo_toml = CARGO_TOML_TEMPLATE.replace("{name}", name);
    std::fs::write(dir.join("Cargo.toml"), cargo_toml)?;

    let lib_rs = LIB_RS_TEMPLATE.replace("{name}", name);
    std::fs::create_dir_all(dir.join("src"))?;
    std::fs::write(dir.join("src").join("lib.rs"), lib_rs)?;

    // WIT vendoring (commit 2). Kept as a no-op stub in commit 1 so
    // the template rewrite lands independently — the e2e test (commit 3)
    // asserts the `wit/` files actually appear after this returns.
    crate::scaffold::wit::write_wit_tree(dir)?;

    if std::env::var_os("EDGE_VERIFY_EMBED").is_some() {
        let wit_dir = dir.join("wit");
        crate::scaffold::wit::verify_embed_matches_disk(&wit_dir).context(
            "EDGE_VERIFY_EMBED=1: the embedded WIT_TREE doesn't match what the CLI just wrote",
        )?;
    }

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

/// Render `edge.toml` from [`EDGE_TOML_PREAMBLE`] + per-language tail
/// and write it to `<dir>/edge.toml`. Each language needs its own
/// `target` / `world` / `language` triple:
///
/// - Rust: `target = "wasm32-unknown-unknown"` (issues #410/#414),
///   `world = "edge-runtime-handler"` (FaaS world; required by the
///   `Project` schema at `config/edgetoml.rs:38`).
/// - JS: `target = "wasm32-wasip2"` (correct after PR #584 — the JS
///   runtime now builds directly for wasip2 and emits a complete
///   component, no adapter wrap needed). Same FaaS world as Rust.
///
/// Splitting into preamble + tail keeps the shared keys (`name`,
/// `version`) in one place while letting the language-specific
/// settings live with the language they describe.
fn write_edge_toml(
    dir: &std::path::Path,
    name: &str,
    lang: LangArg,
    api: Option<&str>,
) -> Result<()> {
    let mut edge_toml = EDGE_TOML_PREAMBLE.replace("{name}", name);
    let tail = match lang {
        LangArg::Rust => EDGE_TOML_RUST_TAIL,
        LangArg::Js => EDGE_TOML_JS_TAIL,
    };
    edge_toml.push_str(tail);
    if let Some(url) = api {
        edge_toml.push_str(&EDGE_TOML_DEPLOYMENT_WITH_API.replace("{api}", url));
    }
    std::fs::write(dir.join("edge.toml"), edge_toml)?;
    Ok(())
}
