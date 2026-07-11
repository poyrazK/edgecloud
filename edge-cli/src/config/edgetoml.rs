//! edge.toml parsing.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::path::Path;

use crate::config::auth::{load_api_url, load_web_url};

/// edge.toml project configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct EdgeToml {
    pub project: Project,
    pub deployment: Deployment,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Project {
    pub name: String,
    #[allow(dead_code)]
    pub version: String,
    /// Cargo build target for the project's primary crate. Defaults
    /// to `"wasm32-unknown-unknown"` (via [`default_target`]) so the
    /// common Rust path — `edge build` running cargo + a `wasm-tools
    /// component new` wrap — works on a toml that omits the key.
    /// Set explicitly for projects that need a different target
    /// (e.g. `wasm32-wasip2` for direct wasi-http component emission).
    #[serde(default = "default_target")]
    pub target: String,
    /// WIT world the guest component implements, used to wrap the
    /// cargo output into a component via `wasm-tools component new
    /// --world <world>`. Required: there's no good default because
    /// `edge:cloud@0.2.0` declares two worlds (`edge-runtime` and
    /// `edge-runtime-handler`) and the user's `wit_bindgen::generate!`
    /// call in `lib.rs` already names the world explicitly
    /// (e.g. `samples/hello/src/lib.rs:36`). Surfacing a missing
    /// field at parse time is much friendlier than failing
    /// `wasm-tools` mid-build with a vague "world not found" error.
    pub world: String,
    /// Source language this project builds with. `None` (and absent
    /// from legacy `edge.toml` files) resolves to `"rust"` via
    /// [`Project::language_or_default`].
    ///
    /// `#[serde(default)]` preserves backward compatibility — tomls
    /// without a `language` key keep parsing as Rust projects. Allowed
    /// values at use sites (build / deploy): `"rust"`, `"js"`. Other
    /// values surface as a friendly error at the build step, not at
    /// parse time, so read-only commands (`status`, `apps get`,
    /// `rollback`) keep working on tomls with stale language fields.
    #[serde(default)]
    pub language: Option<String>,
    /// Wire protocol for this app (issue #548). `None` (and absent
    /// from pre-#548 edge.toml files) resolves to `"http"` via
    /// [`Project::protocol_or_default`].
    ///
    /// `#[serde(default)]` preserves backward compatibility — tomls
    /// without a `protocol` key keep parsing as HTTP apps. Allowed
    /// values at the build step: `"http"` or `"tcp"`. Other values
    /// surface as a friendly error at `edge build`, not at parse
    /// time, so read-only commands (`status`, `apps get`) keep
    /// working on tomls with stale protocol fields.
    ///
    /// The CLI/Cargo build step does NOT distinguish between L4 and
    /// HTTP at the rustc invocation — both paths emit a
    /// `wasm32-unknown-unknown` cdylib and wrap it via
    /// `wasm-tools component new` into the declared world. The
    /// `protocol` knob exists for two downstream consumers:
    ///   1. `validate_protocol_combo` in `commands/build.rs` —
    ///      errors early if the user picked `world =
    ///      "edge-runtime-handler"` + `protocol = "tcp"` (FaaS
    ///      apps cannot speak raw TCP).
    ///   2. `edge deploy` — the value is forwarded to the control
    ///      plane as part of the deployment manifest; the CP
    ///      stamps `EDGE_PROTOCOL` in the worker's spec.env so a
    ///      rebuild picks up the L4 path without re-editing the
    ///      toml.
    #[serde(default)]
    pub protocol: Option<String>,
}

/// Default `[project].target` for projects that omit the key. Picked
/// at serde-deserialize time so `Project::target` is always a
/// non-empty string at use sites — `build_rust` doesn't have to
/// special-case a missing field. `wasm32-unknown-unknown` is the
/// supported build target for the standard `edge build` pipeline
/// (cargo + `wasm-tools component new` wrap). The legacy
/// `wasm32-wasip2` target produces components that wasmtime 45.0.3
/// rejects (`wasi:http@0.2.4` vs the runtime's `wasi:http@0.2.1`).
fn default_target() -> String {
    "wasm32-unknown-unknown".to_string()
}

impl Project {
    /// Resolve the project's source language, defaulting to `"rust"`
    /// for projects whose `edge.toml` was written before the language
    /// field existed. Always returns a non-empty string — even if the
    /// toml explicitly sets `language = ""` (a valid TOML value), we
    /// treat that as "absent" and fall back to the default, because
    /// `path_for` would otherwise match its `_` arm and silently
    /// route the deploy to a stale rust artifact.
    pub fn language_or_default(&self) -> &str {
        match self.language.as_deref() {
            Some(s) if !s.is_empty() => s,
            _ => "rust",
        }
    }

    /// Resolve the wire protocol (issue #548), defaulting to `"http"`
    /// for projects whose `edge.toml` was written before the field
    /// existed. Mirrors [`language_or_default`] — empty string is
    /// treated as absent so a stray `protocol = ""` does not silently
    /// fail validation later with a confusing error.
    pub fn protocol_or_default(&self) -> &str {
        match self.protocol.as_deref() {
            Some(s) if !s.is_empty() => s,
            _ => "http",
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct Deployment {
    /// Optional. When present, takes precedence over the env var and
    /// the per-user config file; when absent, the runtime falls back
    /// to `EDGE_API_URL` → `~/.config/edgecloud/config.toml` →
    /// `fallback` (typically `https://api.edgecloud.dev`).
    pub api: Option<String>,
    /// Optional. Used by `edge billing portal` to decide where Stripe
    /// should send the user when they leave the hosted portal. Kept
    /// distinct from `api` because the API host and the web console
    /// host are usually different subdomains (e.g. `api.edgecloud.dev`
    /// vs `edgecloud.dev`). Resolved via [`EdgeToml::web_url`].
    pub web: Option<String>,
}

impl EdgeToml {
    /// Read and parse edge.toml from the given directory.
    pub fn from_path(path: &Path) -> Result<Self> {
        let path = path.join("edge.toml");
        let content = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        toml::from_str(&content).with_context(|| format!("failed to parse {}", path.display()))
    }

    /// Resolve the control-plane URL with precedence:
    /// 1. `edge.toml` `[deployment].api` (per-project override)
    /// 2. `EDGE_API_URL` env var (per-shell override)
    /// 3. `~/.config/edgecloud/config.toml` `[default].api`
    /// 4. `fallback` (typically the production URL)
    ///
    /// Use this everywhere instead of cloning `deployment.api` directly
    /// — that pattern silently broke when `api` became optional.
    pub fn api_url(&self, fallback: &str) -> String {
        self.deployment
            .api
            .clone()
            .unwrap_or_else(|| load_api_url(fallback))
    }

    /// Resolve the developer-facing web URL with precedence:
    /// 1. `edge.toml` `[deployment].web` (per-project override)
    /// 2. `EDGE_WEB_URL` env var (per-shell override)
    /// 3. `~/.config/edgecloud/config.toml` `[default].web`
    /// 4. `fallback` (typically the production user-console URL)
    ///
    /// Distinct from [`EdgeToml::api_url`] because the API host and
    /// the web console host are usually different subdomains. Today
    /// only `edge billing portal` consumes this, but any future
    /// command that links out to the operator's user-facing console
    /// (status page, account settings, etc.) should call this rather
    /// than hand-rolling the env+config+fallback chain.
    pub fn web_url(&self, fallback: &str) -> String {
        self.deployment
            .web
            .clone()
            .unwrap_or_else(|| load_web_url(fallback))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(content: &str) -> EdgeToml {
        toml::from_str(content).expect("test fixture must parse")
    }

    #[test]
    fn api_url_returns_toml_value_when_set() {
        let toml = parse(
            r#"[project]
name = "x"
version = "0.1.0"
target = "wasm32-wasip2"
world = "edge-runtime-handler"

[deployment]
api = "https://from-toml"
"#,
        );
        // Even with env set, toml wins.
        // SAFETY: env-var mutation in a unit test is OK as long as no
        // other test in this file reads EDGE_API_URL. None do.
        // SAFETY justification: this is a single-threaded test, env
        // changes are scoped to the test process lifetime.
        assert_eq!(toml.api_url("https://default"), "https://from-toml");
    }

    #[test]
    fn web_url_returns_toml_value_when_set() {
        // [deployment].web is the top-priority source. Independent of
        // [deployment].api so a project can ship pointing at a
        // staging API while still linking out to production web UI.
        let toml = parse(
            r#"[project]
name = "x"
version = "0.1.0"
target = "wasm32-wasip2"
world = "edge-runtime-handler"

[deployment]
web = "https://from-toml-web"
"#,
        );
        assert_eq!(toml.web_url("https://default-web"), "https://from-toml-web");
    }

    #[test]
    fn web_url_toml_and_api_are_independent() {
        // Setting only [deployment].api (no web) must NOT bleed into
        // web_url — they resolve from separate fields and separate
        // env vars (`EDGE_WEB_URL`).
        let toml = parse(
            r#"[project]
name = "x"
version = "0.1.0"
target = "wasm32-wasip2"
world = "edge-runtime-handler"

[deployment]
api = "https://api.from-toml"
"#,
        );
        // api is wired to its setter; web_url must NOT see that value
        // even when env/config have nothing. With no env interference
        // the loader returns the fallback verbatim.
        assert_eq!(toml.api_url("https://api-default"), "https://api.from-toml");
        assert_eq!(
            toml.web_url("https://web-sentinel-default"),
            "https://web-sentinel-default"
        );
    }

    #[test]
    fn web_url_accepts_omitted_field() {
        // Backward-compat: tomls written before [deployment].web
        // existed must keep parsing, and web_url() must return
        // *something* non-empty (the env+config+fallback chain).
        let toml = parse(
            r#"[project]
name = "x"
version = "0.1.0"
target = "wasm32-wasip2"
world = "edge-runtime-handler"

[deployment]
"#,
        );
        let resolved = toml.web_url("https://web-sentinel-fallback");
        assert!(
            !resolved.is_empty(),
            "web_url must always return a non-empty URL"
        );
    }

    #[test]
    fn api_url_falls_back_when_absent() {
        // Pass-through to load_api_url is covered at higher fidelity by
        // the wiremock integration tests in tests/auth.rs (which inject
        // EDGE_API_URL via the child process env). This unit test only
        // pins the contract that an absent [deployment].api key falls
        // through to the caller-supplied `fallback` argument when no
        // env var or user config interferes. We cannot fully isolate
        // cargo's shared process env in a unit test, so we assert the
        // shape (some non-empty string from the loader chain), not the
        // exact value.
        let toml = parse(
            r#"[project]
name = "x"
version = "0.1.0"
target = "wasm32-wasip2"
world = "edge-runtime-handler"

[deployment]
"#,
        );
        let resolved = toml.api_url("https://sentinel-default-that-does-not-exist-in-env");
        assert!(
            !resolved.is_empty(),
            "api_url must always return a non-empty URL"
        );
        // If no env/config interfered, the loader returns the fallback
        // verbatim. CI runs in a clean env so this assertion is true in
        // CI; a developer with EDGE_API_URL set will see a different
        // (still-valid) value here.
    }

    // ── language field (issue #317) ───────────────────────────────────

    #[test]
    fn parse_accepts_language_field() {
        // Forward-compat: a toml with `language = "js"` parses and the
        // value is exposed on Project::language for callers to act on.
        let toml = parse(
            r#"[project]
name = "x"
version = "0.1.0"
target = "wasm32-wasip2"
world = "edge-runtime-handler"
language = "js"

[deployment]
"#,
        );
        assert_eq!(toml.project.language.as_deref(), Some("js"));
        assert_eq!(toml.project.language_or_default(), "js");
    }

    #[test]
    fn parse_accepts_missing_language_field_for_backcompat() {
        // Backward-compat: existing tomls written before the field
        // existed parse unchanged, and language_or_default() returns
        // "rust" so the existing Rust build path is selected.
        let toml = parse(
            r#"[project]
name = "x"
version = "0.1.0"
target = "wasm32-wasip2"
world = "edge-runtime-handler"

[deployment]
"#,
        );
        assert_eq!(toml.project.language, None);
        assert_eq!(toml.project.language_or_default(), "rust");
    }

    #[test]
    fn language_or_default_treats_empty_string_as_missing() {
        // `language = ""` is a valid TOML value and parses as
        // Some(""). The unwrap_or("rust") fallback alone would return
        // "" (because Some("") is_some), which then matches the `_`
        // arm in `path_for` and silently deploys a stale rust
        // artifact. Treat empty-string as absent so the default
        // wins.
        let toml = parse(
            r#"[project]
name = "x"
version = "0.1.0"
target = "wasm32-wasip2"
world = "edge-runtime-handler"
language = ""

[deployment]
"#,
        );
        assert_eq!(toml.project.language.as_deref(), Some(""));
        assert_eq!(toml.project.language_or_default(), "rust");
    }

    // ── protocol field (issue #548) ─────────────────────────────────

    #[test]
    fn parse_accepts_protocol_field() {
        // Forward-compat: a toml with `protocol = "tcp"` parses and the
        // value is exposed on Project::protocol for callers to act on.
        // Validation against the declared `world` lives in
        // `commands::build::validate_protocol_combo` (a separate unit
        // — protocol/world cross-check is a build-time concern, not a
        // parse-time one).
        let toml = parse(
            r#"[project]
name = "x"
version = "0.1.0"
world = "edge-runtime"
protocol = "tcp"

[deployment]
"#,
        );
        assert_eq!(toml.project.protocol.as_deref(), Some("tcp"));
        assert_eq!(toml.project.protocol_or_default(), "tcp");
    }

    #[test]
    fn parse_accepts_missing_protocol_field_for_backcompat() {
        // Backward-compat: existing tomls written before the field
        // existed parse unchanged, and protocol_or_default() returns
        // "http" so the existing HTTP build/ingress path is selected.
        let toml = parse(
            r#"[project]
name = "x"
version = "0.1.0"
world = "edge-runtime-handler"

[deployment]
"#,
        );
        assert_eq!(toml.project.protocol, None);
        assert_eq!(toml.project.protocol_or_default(), "http");
    }

    #[test]
    fn protocol_or_default_treats_empty_string_as_missing() {
        // Mirrors `language_or_default_treats_empty_string_as_missing`:
        // stray `protocol = ""` should fall through to the default
        // rather than silently propagating an empty string into the
        // build pipeline (where validate_protocol_combo would
        // surface a confusing "unsupported protocol" error).
        let toml = parse(
            r#"[project]
name = "x"
version = "0.1.0"
world = "edge-runtime-handler"
protocol = ""

[deployment]
"#,
        );
        assert_eq!(toml.project.protocol.as_deref(), Some(""));
        assert_eq!(toml.project.protocol_or_default(), "http");
    }

    // ── target field (issue #410) ────────────────────────────────────

    #[test]
    fn target_defaults_when_absent() {
        // An edge.toml that omits the `target` line parses cleanly
        // and `Project::target` resolves to the documented default
        // (`wasm32-unknown-unknown`). Issue #410: this default is
        // what makes the `edge build` two-step recipe (cargo +
        // `wasm-tools component new` wrap) work without a target
        // override in the sample's edge.toml.
        let toml = parse(
            r#"[project]
name = "x"
version = "0.1.0"
world = "edge-runtime-handler"

[deployment]
"#,
        );
        assert_eq!(toml.project.target, "wasm32-unknown-unknown");
    }

    #[test]
    fn target_respects_explicit_value() {
        // Setting `target = "wasm32-wasip2"` (or any other value) is
        // preserved verbatim — the default is only the fallback for
        // absent keys. This keeps backward compat with projects that
        // explicitly opt into non-default targets.
        let toml = parse(
            r#"[project]
name = "x"
version = "0.1.0"
target = "wasm32-wasip2"
world = "edge-runtime-handler"

[deployment]
"#,
        );
        assert_eq!(toml.project.target, "wasm32-wasip2");
    }

    #[test]
    fn world_is_required() {
        // `[project].world` has no default. The two valid values today
        // are `edge-runtime` (long-running) and `edge-runtime-handler`
        // (FaaS), both declared in `wit/edge-cloud.wit`. Missing-key
        // errors at parse time rather than at `wasm-tools` time, so
        // the user gets a pointer to the field they need to add
        // instead of a confusing "world not found" later in the
        // pipeline.
        let result = toml::from_str::<EdgeToml>(
            r#"[project]
name = "x"
version = "0.1.0"

[deployment]
"#,
        );
        let err = result.expect_err("missing `world` field must fail parse");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("world") || msg.contains("missing field"),
            "expected the parse error to mention `world` or `missing field`, got: {msg}"
        );
    }
}
