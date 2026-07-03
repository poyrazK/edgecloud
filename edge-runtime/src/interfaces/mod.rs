//! Host function implementations for edge:* WIT interfaces.

/// Returns `true` iff `id` is safe to use as a single directory component.
/// Rejects empty strings, path separators, `.`, `..`, null bytes, colons,
/// and Windows reserved device names (CON, NUL, PRN, AUX, COM1-9, LPT1-9).
pub fn is_safe_tenant_id(id: &str) -> bool {
    if id.is_empty() || id == "." || id == ".." {
        return false;
    }
    if id.contains('/') || id.contains('\\') || id.contains('\0') || id.contains(':') {
        return false;
    }
    let upper = id.to_ascii_uppercase();
    if matches!(
        upper.as_str(),
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
    ) {
        return false;
    }
    true
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
}
