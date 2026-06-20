//! Shared helpers for reading and writing `.edge/state.json`.
//!
//! Extracted from `commands/activate.rs`, `commands/rollback.rs`, and
//! `commands/deploy.rs`, which each had a byte-identical copy of
//! `load_state_optional`. The three callers all want the same
//! "suppress only `NotFound`; surface parse/IO errors" semantics — a
//! missing state.json is OK if the user passed an explicit app name
//! (resolve_app_name will then error with a clear message), but a
//! corrupt state.json must surface a real diagnostic.

use anyhow::Result;
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
