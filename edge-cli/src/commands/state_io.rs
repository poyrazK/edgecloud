//! Shared helpers for reading and writing `.edge/state.json`.
//!
//! Extracted from `commands/activate.rs`, `commands/rollback.rs`,
//! `commands/deploy.rs`, and `commands/logs.rs`, which each had a
//! byte-identical copy of `load_state_optional` and
//! `resolve_app_name`. All five callers want the same semantics — a
//! missing state.json is OK if the user passed an explicit app name
//! (resolve_app_name will then error with a clear message), but a
//! corrupt state.json must surface a real diagnostic.

use anyhow::{anyhow, Result};
use std::path::Path;

use crate::state::State;

/// Load `.edge/state.json` if it exists. Suppress only `NotFound`;
/// surface parse/IO errors so the user gets a real diagnostic instead
/// of a generic "requires an app name" message.
pub(crate) fn load_state_optional(path: &Path) -> Result<Option<State>> {
    match State::load(path) {
        Ok(s) => Ok(Some(s)),
        Err(e) => {
            let is_not_found = e.chain().any(|c| {
                c.downcast_ref::<std::io::Error>()
                    .is_some_and(|io| io.kind() == std::io::ErrorKind::NotFound)
            });
            if is_not_found {
                Ok(None)
            } else {
                Err(e)
            }
        }
    }
}

/// Resolve the app name for a command that takes one optionally.
///
/// Precedence:
///
/// 1. Non-empty positional `app` wins.
/// 2. Otherwise, the app name in `state.json` (only if also
///    non-empty — an empty `state.app_name` is treated as missing).
/// 3. Otherwise, error with a message naming `cmd` so the user knows
///    which command failed.
///
/// `cmd` is the user-facing command name (e.g. `"edge rollback"`,
/// `"edge logs"`) and appears in the error message verbatim.
/// Centralized here because the precedence rule had drifted across
/// three near-identical copies (see PR #138 review finding #4).
pub(crate) fn resolve_app_name(cmd: &str, app: &str, state: Option<&State>) -> Result<String> {
    if !app.is_empty() {
        return Ok(app.to_string());
    }
    match state {
        Some(s) if !s.app_name.is_empty() => Ok(s.app_name.clone()),
        _ => Err(anyhow!(
            "{cmd} requires an app name; pass it positionally \
             or run from a directory with .edge/state.json"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state_with(name: &str) -> State {
        State {
            deployment_id: "d_test".to_string(),
            app_name: name.to_string(),
            live_url: "https://example.test".to_string(),
            regions: vec![],
            desired_replicas: 0,
        }
    }

    #[test]
    fn load_state_optional_returns_none_when_missing() {
        // A directory with no .edge/state.json at all.
        let dir = tempfile::tempdir().unwrap();
        let got = load_state_optional(dir.path()).unwrap();
        assert!(got.is_none());
    }

    #[test]
    fn load_state_optional_surfaces_parse_error() {
        // A .edge/state.json that exists but is not valid JSON — must
        // surface the error rather than silently treating it as
        // "no state".
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(".edge")).unwrap();
        std::fs::write(
            dir.path().join(".edge").join("state.json"),
            "{not valid json",
        )
        .unwrap();
        let err = load_state_optional(dir.path()).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("failed to parse") || msg.contains("parse"),
            "expected a parse error, got: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // resolve_app_name — single source of truth for the precedence rule.
    // Adding a new caller? Point it here and add a test for any new edge.
    // -----------------------------------------------------------------------

    #[test]
    fn resolve_positional_wins_over_empty() {
        let got = resolve_app_name("edge logs", "myapp", None).unwrap();
        assert_eq!(got, "myapp");
    }

    #[test]
    fn resolve_falls_back_to_state_when_positional_empty() {
        let s = state_with("from-state");
        let got = resolve_app_name("edge logs", "", Some(&s)).unwrap();
        assert_eq!(got, "from-state");
    }

    #[test]
    fn resolve_positional_wins_over_state() {
        let s = state_with("from-state");
        let got = resolve_app_name("edge rollback", "positional", Some(&s)).unwrap();
        assert_eq!(got, "positional");
    }

    #[test]
    fn resolve_errors_when_no_inputs() {
        let err = resolve_app_name("edge logs", "", None).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("edge logs requires an app name"),
            "expected 'edge logs requires an app name' in error, got: {msg}"
        );
    }

    #[test]
    fn resolve_treats_empty_state_as_missing() {
        let s = state_with("");
        let err = resolve_app_name("edge logs", "", Some(&s)).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("edge logs requires an app name"),
            "expected 'edge logs requires an app name' in error, got: {msg}"
        );
    }
}
