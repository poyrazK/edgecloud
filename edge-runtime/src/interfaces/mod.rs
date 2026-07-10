//! Host function implementations for edge:* WIT interfaces.

/// Returns `true` iff `s` is safe to use as a single directory component
/// on disk. Rejects empty strings, `.`, `..`, path separators, null
/// bytes, colons, and Windows reserved device names (CON, PRN, AUX,
/// NUL, COM1-9, LPT1-9).
///
/// Used by:
///   * `is_safe_tenant_id` — guards `<EDGE_KV_STORE_PATH>/<tenant_id>/`
///     and the per-app preopen tenant half (`edge-runtime/src/runtime.rs`).
///   * `is_safe_app_name` — guards the per-app preopen subdirectory
///     (`edge-runtime/src/runtime.rs`, issue #558).
///
/// Defense in depth: the worker upstream also validates `tenant_id` and
/// `app_name` (see `edge-worker/src/downloader.rs`), but the runtime
/// does not trust upstream and refuses to mount an unsafe name.
pub fn is_safe_path_component(s: &str) -> bool {
    if s.is_empty() || s == "." || s == ".." {
        return false;
    }
    if s.contains('/') || s.contains('\\') || s.contains('\0') || s.contains(':') {
        return false;
    }
    !is_windows_reserved_name(s)
}

/// Returns `true` iff `s` (case-insensitive) is one of the Windows
/// reserved device names — CON, PRN, AUX, NUL, COM1-9, LPT1-9.
/// On Windows these resolve to the device namespace regardless of
/// directory; on POSIX they're legal filenames but rejecting them
/// here keeps the runtime's path-construction behavior portable.
fn is_windows_reserved_name(s: &str) -> bool {
    matches!(
        s.to_ascii_uppercase().as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    )
}

/// Returns `true` iff `id` is safe to use as a single directory component
/// for a tenant id. Alias of [`is_safe_path_component`].
pub fn is_safe_tenant_id(id: &str) -> bool {
    is_safe_path_component(id)
}

/// Returns `true` iff `name` is safe to use as a single directory component
/// for the `EDGE_FS_PATH` per-app preopen subdirectory (issue #558).
/// Alias of [`is_safe_path_component`].
pub fn is_safe_app_name(name: &str) -> bool {
    is_safe_path_component(name)
}

// The http_client / http_server / networking / dns modules were dropped in
// v0.2 — components needing HTTP go through `wasi:http`, sockets through
// `wasi:sockets`, and DNS through `wasi:sockets/ip-name-lookup`.
// The async, host-provided `edge:cloud/*` interfaces retained here.
pub mod cache;
pub mod kv_store;
pub mod observe;
pub mod process;
pub mod scheduling;
pub mod time;
pub mod websocket;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_string() {
        assert!(!is_safe_tenant_id(""));
    }

    #[test]
    fn rejects_dot_paths() {
        assert!(!is_safe_tenant_id("."));
        assert!(!is_safe_tenant_id(".."));
    }

    #[test]
    fn rejects_path_separators() {
        assert!(!is_safe_tenant_id("foo/bar"));
        assert!(!is_safe_tenant_id("foo\\bar"));
    }

    #[test]
    fn rejects_null_byte() {
        assert!(!is_safe_tenant_id("foo\0bar"));
    }

    #[test]
    fn rejects_colon() {
        assert!(!is_safe_tenant_id("foo:bar"));
    }

    #[test]
    fn rejects_windows_reserved_names() {
        let reserved = [
            "CON", "con", "Con", "PRN", "prn", "AUX", "aux", "NUL", "nul", "COM1", "COM2", "COM3",
            "COM4", "COM5", "COM6", "COM7", "COM8", "COM9", "com1", "com9", "LPT1", "LPT2", "LPT3",
            "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9", "lpt1", "lpt9",
        ];
        for name in &reserved {
            assert!(
                !is_safe_tenant_id(name),
                "reserved name '{name}' must be rejected"
            );
        }
    }

    #[test]
    fn accepts_valid_tenant_ids() {
        assert!(is_safe_tenant_id("abc"));
        assert!(is_safe_tenant_id("my-tenant_42"));
        assert!(is_safe_tenant_id("a"));
        assert!(is_safe_tenant_id("valid-tenant-name"));
        assert!(is_safe_tenant_id("tenant_with_underscores"));
        assert!(is_safe_tenant_id("a1b2c3"));
    }

    #[test]
    fn accepts_windows_like_not_reserved() {
        // Near-matches of reserved names that should NOT be rejected.
        assert!(is_safe_tenant_id("CON1"));
        assert!(is_safe_tenant_id("LPTO"));
        assert!(is_safe_tenant_id("COM10"));
        assert!(is_safe_tenant_id("LPT10"));
        assert!(is_safe_tenant_id("con0"));
        assert!(is_safe_tenant_id("nulx"));
    }

    // ── `is_safe_app_name` — mirrors `is_safe_tenant_id` for issue #558.
    //    Same rejection set so the per-app preopen subdirectory gets the
    //    same filesystem-safety guarantees as the per-tenant preopen
    //    already gets via `is_safe_tenant_id`.

    #[test]
    fn app_name_rejects_empty_string() {
        assert!(!is_safe_app_name(""));
    }

    #[test]
    fn app_name_rejects_dot_paths() {
        assert!(!is_safe_app_name("."));
        assert!(!is_safe_app_name(".."));
    }

    #[test]
    fn app_name_rejects_path_separators() {
        assert!(!is_safe_app_name("foo/bar"));
        assert!(!is_safe_app_name("foo\\bar"));
    }

    #[test]
    fn app_name_rejects_null_byte() {
        assert!(!is_safe_app_name("foo\0bar"));
    }

    #[test]
    fn app_name_rejects_colon() {
        assert!(!is_safe_app_name("foo:bar"));
    }

    #[test]
    fn app_name_rejects_windows_reserved_names() {
        let reserved = [
            "CON", "con", "Con", "PRN", "prn", "AUX", "aux", "NUL", "nul", "COM1", "COM2", "COM3",
            "COM4", "COM5", "COM6", "COM7", "COM8", "COM9", "com1", "com9", "LPT1", "LPT2", "LPT3",
            "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9", "lpt1", "lpt9",
        ];
        for name in &reserved {
            assert!(
                !is_safe_app_name(name),
                "reserved name '{name}' must be rejected"
            );
        }
    }

    #[test]
    fn app_name_accepts_valid_names() {
        assert!(is_safe_app_name("abc"));
        assert!(is_safe_app_name("my-app_42"));
        assert!(is_safe_app_name("a"));
        assert!(is_safe_app_name("valid-app-name"));
        assert!(is_safe_app_name("app_with_underscores"));
        assert!(is_safe_app_name("a1b2c3"));
    }

    #[test]
    fn app_name_accepts_windows_like_not_reserved() {
        // Near-matches of reserved names that should NOT be rejected.
        assert!(is_safe_app_name("CON1"));
        assert!(is_safe_app_name("LPTO"));
        assert!(is_safe_app_name("COM10"));
        assert!(is_safe_app_name("LPT10"));
        assert!(is_safe_app_name("con0"));
        assert!(is_safe_app_name("nulx"));
    }

    // ── `is_safe_path_component` — the underlying helper. Both
    //    `is_safe_tenant_id` and `is_safe_app_name` are aliases of this
    //    function (review follow-up on PR #599). Test the helper
    //    directly so the contract is named in one place.

    #[test]
    fn path_component_aliases_match_helper() {
        // Spot-check that the aliases produce the same answer as the
        // underlying helper on a representative sample. Comprehensive
        // coverage lives in the per-aliased tests above.
        for s in [
            "", ".", "..", "foo/bar", "foo\\bar", "foo\0bar", "foo:bar", "NUL", "abc", "a-b_c",
        ] {
            assert_eq!(
                is_safe_path_component(s),
                is_safe_tenant_id(s),
                "tenant-id alias diverged on {s:?}"
            );
            assert_eq!(
                is_safe_path_component(s),
                is_safe_app_name(s),
                "app-name alias diverged on {s:?}"
            );
        }
    }

    #[test]
    fn windows_reserved_name_helper_matches_public_predicate() {
        // `is_windows_reserved_name` is private; test it through the
        // public predicate — for any reserved name, `is_safe_path_component`
        // must return false. This catches a refactor that drops the
        // private helper from the public one.
        for name in ["CON", "PRN", "AUX", "NUL", "COM1", "LPT1", "con", "nul"] {
            assert!(
                !is_safe_path_component(name),
                "reserved name '{name}' must be rejected"
            );
        }
    }
}
