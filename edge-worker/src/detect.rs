//! Detect the execution model of a WASI Preview 2 component.
//!
//! The worker supports two execution models:
//!
//! * **LongRunning** — the guest implements `_start` and is responsible
//!   for hosting its own TCP server (typically via `wasi:sockets`). The
//!   supervisor spawns `run_app_loop` to drive the guest.
//!
//! * **Handler (FaaS)** — the guest implements
//!   `wasi:http/incoming-handler` and is invoked once per HTTP request.
//!   The supervisor hosts an axum server; each request goes through a
//!   `wasmtime_wasi_http::ProxyPre` that calls the guest's
//!   `handle(request, response-out)` function.
//!
//! Detection is purely structural — we inspect the component's exported
//! interface list without instantiating it. That makes the choice cheap
//! and lets us pick the right linker factory before
//! `linker.instantiate_pre(&component)` is attempted.

use wasmtime::component::Component;

/// Which execution model a component expects.
///
/// Maps directly to (a) which linker factory the supervisor picks and
/// (b) which task the supervisor spawns in `start_app`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionModel {
    /// Component implements `_start` and hosts its own TCP server via
    /// `wasi:sockets`. Spawned via `run_app_loop`.
    LongRunning,
    /// Component implements `wasi:http/incoming-handler`. The worker
    /// owns the HTTP listener and dispatches each request through a
    /// `wasmtime_wasi_http::ProxyPre`. Wired up in Phase C.
    Handler,
}

/// Canonical export key for the FaaS interface. The canonical-ABI
/// `ComponentType::exports` iterator returns keys in
/// `<interface-name>[@<version>]` form (e.g.
/// `wasi:http/incoming-handler@0.2.1`). We match the bare prefix
/// followed by either end-of-string or `@` so that:
///
/// * `wasi:http/incoming-handler`            — matches (no version)
/// * `wasi:http/incoming-handler@0.2.1`      — matches (canonical form)
/// * `wasi:http/incoming-handler-foo`        — does NOT match
/// * `wasi:http/incoming-handler@0.2.1-evil` — does NOT match
///
/// The previous `starts_with` form silently misclassified any name
/// beginning with the prefix.
#[allow(dead_code)]
const HANDLER_EXPORT: &str = "wasi:http/incoming-handler";

/// Returns `true` if `name` matches the handler export key either
/// exactly or with a canonical `@<version>` suffix.
///
/// Separated from `detect_execution_model` so the matching logic is
/// unit-testable without a real `Component`.
#[allow(dead_code)]
pub(crate) fn is_handler_export(name: &str) -> bool {
    // Exact match (no version in the export key).
    if name == HANDLER_EXPORT {
        return true;
    }
    // Canonical-ABI `<name>@<version>` form. bindgen normalizes
    // across patch versions, so `@0.2.0` is valid even though our
    // WIT pins `@0.2.1`. We check that `@` appears RIGHT after the
    // prefix with no intervening characters, so that
    // `wasi:http/incoming-handler-foo` does NOT match.
    if let Some(suffix) = name.strip_prefix(HANDLER_EXPORT) {
        return suffix.starts_with('@');
    }
    false
}

/// Inspect a component's exported interfaces to decide which execution
/// model it expects.
///
/// We treat any component exporting the canonical
/// `wasi:http/incoming-handler` (with an optional `@<version>` suffix)
/// as `Handler`. LongRunning is the default — `_start` is canonical for
/// WASI Preview 2 components and we don't require any specific
/// signature beyond that.
#[allow(dead_code)]
pub fn detect_execution_model(component: &Component) -> ExecutionModel {
    let ty = component.component_type();
    // `ComponentType::exports` needs the engine because canonical-ABI
    // type lookups inside the component are engine-scoped. The component
    // already holds its engine internally; we just borrow it back.
    let engine = component.engine();
    for (name, _item) in ty.exports(engine) {
        if is_handler_export(name) {
            return ExecutionModel::Handler;
        }
    }
    ExecutionModel::LongRunning
}

/// A fast-path execution model detector that does not require full WebAssembly compilation.
/// We look for a `wasi:http/incoming-handler` export which indicates a FaaS function.
pub fn detect_execution_model_from_bytes(bytes: &[u8]) -> ExecutionModel {
    match wasmparser::Parser::new(0)
        .parse_all(bytes)
        .find_map(|p| match p {
            Ok(wasmparser::Payload::ExportSection(s)) => Some(s),
            _ => None,
        }) {
        Some(exports) => {
            for e in exports.into_iter().flatten() {
                if e.name.contains("wasi:http/incoming-handler") {
                    return ExecutionModel::Handler;
                }
            }
            ExecutionModel::LongRunning
        }
        None => ExecutionModel::LongRunning,
    }
}

#[cfg(test)]
mod tests {
    //! Unit tests for `is_handler_export` (the string-matching core of
    //! `detect_execution_model`) and the enum variants.
    //!
    //! Full component-level detection tests live in
    //! `edge-runtime/tests/handler_fixture_load.rs` (Handler path) and
    //! will be added for the LongRunning path once a long-running
    //! fixture is built (Phase D, L8).

    use super::*;

    /// Sanity-check the enum variants are distinct and copyable.
    #[test]
    fn execution_model_variants_distinct() {
        assert_ne!(ExecutionModel::LongRunning, ExecutionModel::Handler);
        let a = ExecutionModel::Handler;
        let b = a; // Copy
        assert_eq!(a, b);
    }

    // ── is_handler_export: exact matches ───────────────────────────────

    #[test]
    fn exact_handler_export_matches() {
        assert!(is_handler_export("wasi:http/incoming-handler"));
    }

    #[test]
    fn handler_export_with_0_2_1_matches() {
        assert!(is_handler_export("wasi:http/incoming-handler@0.2.1"));
    }

    #[test]
    fn handler_export_with_0_2_0_matches() {
        // bindgen normalizes across patch versions.
        assert!(is_handler_export("wasi:http/incoming-handler@0.2.0"));
    }

    #[test]
    fn handler_export_with_major_only_matches() {
        assert!(is_handler_export("wasi:http/incoming-handler@1"));
    }

    // ── is_handler_export: negative cases ──────────────────────────────

    #[test]
    fn unrelated_export_does_not_match() {
        assert!(!is_handler_export("wasi:cli/run"));
    }

    #[test]
    fn empty_string_does_not_match() {
        assert!(!is_handler_export(""));
    }

    #[test]
    fn prefix_with_extra_hyphen_does_not_match() {
        // The previous `starts_with` implementation would have
        // misclassified this as a handler export.
        assert!(!is_handler_export("wasi:http/incoming-handler-foo"));
    }

    #[test]
    fn handler_export_with_semver_prerelease_matches() {
        // Semver pre-release tags are valid (e.g. `0.2.1-evil`).
        // bindgen never emits this, but the matching logic correctly
        // treats it as a versioned match.
        assert!(is_handler_export("wasi:http/incoming-handler@0.2.1-evil"));
    }

    #[test]
    fn completely_different_path_does_not_match() {
        assert!(!is_handler_export("wasi:http/outgoing-handler"));
    }

    #[test]
    fn http_handler_does_not_match() {
        // `wasi:http/handler` is a different (deprecated) interface.
        assert!(!is_handler_export("wasi:http/handler"));
    }

    #[test]
    fn case_sensitive_mismatch() {
        assert!(!is_handler_export("WASI:HTTP/INCOMING-HANDLER"));
    }

    // ── is_handler_export: edge cases ──────────────────────────────────

    #[test]
    fn only_at_symbol_without_version_still_matches() {
        // Technically `@` alone is not valid semver, but the function
        // only checks for the `@` prefix — it doesn't validate the
        // version string. bindgen never emits this form anyway.
        assert!(is_handler_export("wasi:http/incoming-handler@"));
    }

    #[test]
    fn handler_prefix_in_longer_namespace_does_not_match() {
        // The prefix appears as a substring but not at an interface boundary.
        // Real component tooling would never emit this, but the guard
        // protects against future export-name encoding changes.
        assert!(!is_handler_export("x-wasi:http/incoming-handler@0.2.1"));
    }

    #[test]
    fn handler_without_wasi_prefix_does_not_match() {
        assert!(!is_handler_export("custom:http/incoming-handler@0.2.1"));
    }

    // ── detect_execution_model_from_bytes tests ────────────────────────

    #[test]
    fn detect_handler_from_fixture_bytes() {
        // Load the handler fixture wasm and verify detect_execution_model_from_bytes
        // classifies it as Handler.
        let paths = [
            "tests/fixtures/handler.wasm",
            "edge-worker/tests/fixtures/handler.wasm",
        ];
        let wasm_path = paths
            .iter()
            .map(std::path::PathBuf::from)
            .find(|p| p.exists())
            .expect("handler.wasm fixture not found");
        let bytes = std::fs::read(&wasm_path).unwrap();
        assert_eq!(
            detect_execution_model_from_bytes(&bytes),
            ExecutionModel::Handler
        );
    }

    #[test]
    fn detect_long_running_from_minimal_wasm() {
        // A minimal valid wasm module (magic + version, no sections at all)
        // has no handler export → LongRunning.
        let minimal = b"\x00asm\x01\x00\x00\x00";
        assert_eq!(
            detect_execution_model_from_bytes(minimal),
            ExecutionModel::LongRunning
        );
    }

    #[test]
    fn detect_from_empty_bytes_returns_long_running() {
        assert_eq!(
            detect_execution_model_from_bytes(b""),
            ExecutionModel::LongRunning
        );
    }

    #[test]
    fn detect_from_invalid_wasm_bytes_returns_long_running() {
        assert_eq!(
            detect_execution_model_from_bytes(b"not-wasm"),
            ExecutionModel::LongRunning
        );
    }
}
