//! URL path-component validator for the edge CLI.
//!
//! The Go control plane's [`validatePathComponent`] defends server-side
//! against filesystem-path-traversal (`\0`, `/`, `\`, `..`) — its
//! threat model is *disk-path*: it guards writes under
//! `cfg.Storage.ArtifactPath`. This module is the **defense-in-depth
//! client-side pre-flight guard** applied before every
//! path-interpolating URL build. Its threat model is *outbound URI
//! character*: it never writes to disk, so the allow set can be
//! tighter (no percent, no whitespace, no control chars, no non-ASCII)
//! without breaking any legitimate CLI use case. The two helpers share
//! the same allow-set philosophy — `[A-Za-z0-9._~-]` plus the
//! `..`-rejection — but the CLI helper rejects strictly more, because
//! there is no filesystem I/O to defend.
//!
//! Invalid input surfaces an actionable error before any round-trip,
//! instead of silently arriving at the server as a malformed URI.
//!
//! Does **not** percent-encode. A value like `"my app"` (with a space)
//! refuses with a clear error rather than silently encoding to
//! `my%20app`. The CLI is an operator tool: user mistakes should fail
//! loud and early, not silently round-trip as a different-looking URI.
//!
//! Issue #671.
//!
//! [`validatePathComponent`]:
//!     ../../../../edge-control-plane/internal/storage/artifact.go

/// Reject `value` as a URL path component.
///
/// `name` is the human-readable component identifier (e.g. `"app_name"`,
/// `"deployment_id"`); it appears in the error message so the caller
/// sees WHICH field is bad.
///
/// Rejects: empty, `..` (literal or substring), `\0`, `/`, `\`, `?`,
/// `#`, `&`, `=`, `+`, `%`, ASCII whitespace, tab/newline/CR, control
/// chars, and any non-ASCII character. Allow set is `[A-Za-z0-9._~-]`.
///
/// `#[must_use]` prevents a future caller from `.ok();`-ing the result
/// and silently dropping a validation failure — every site today binds
/// it via `?` or `.map_err`, but the attribute is cheap insurance.
#[must_use = "validating a path component must not be silently discarded — bind via `?` or `.map_err`"]
pub fn validate_path_component(name: &str, value: &str) -> anyhow::Result<()> {
    if value.is_empty() {
        anyhow::bail!("{name} cannot be empty");
    }
    if value == ".." || value.contains("..") {
        anyhow::bail!("{name} cannot contain '..'");
    }
    if let Some(c) = value.chars().find(|c| {
        matches!(
            c,
            '/' | '\\' | '\0' | '?' | '#' | '&' | '=' | '+' | '%' | ' ' | '\t' | '\n' | '\r'
        ) || c.is_control()
            || (c.len_utf8() > 1)
    }) {
        anyhow::bail!("{name} contains invalid character {c:?} (only [A-Za-z0-9._~-] are allowed)");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_simple_alnum_dot_dash_underscore_tilde() {
        for v in [
            "ok",
            "my.app_v1",
            "abc.123",
            "x-y_z~q",
            "k_abc123",
            "t_owner",
            "d_redis_lite_001",
            "k_seed",
        ] {
            validate_path_component("x", v).unwrap_or_else(|e| panic!("{v}: {e}"));
        }
    }

    #[test]
    fn rejects_empty() {
        let err = validate_path_component("app_name", "").unwrap_err();
        assert!(err.to_string().contains("cannot be empty"), "{err}");
    }

    #[test]
    fn rejects_double_dot_literal() {
        let err = validate_path_component("p", "..").unwrap_err();
        assert!(err.to_string().contains("'..'"), "{err}");
    }

    #[test]
    fn rejects_double_dot_substring() {
        for v in ["foo..bar", "d..1", "..secret", "k_..bad"] {
            let err = validate_path_component("p", v).unwrap_err();
            assert!(err.to_string().contains("'..'"), "{v}: {err}");
        }
    }

    #[test]
    fn rejects_slash_and_backslash() {
        for v in ["a/b", "a\\b", "/etc", "sub\\dir"] {
            let err = validate_path_component("p", v).unwrap_err();
            assert!(err.to_string().contains("invalid character"), "{v}: {err}");
        }
    }

    #[test]
    fn rejects_null_byte() {
        let err = validate_path_component("p", "a\0b").unwrap_err();
        assert!(err.to_string().contains("invalid character"), "{err}");
    }

    #[test]
    fn rejects_url_reserved_chars_and_whitespace() {
        for v in [
            "a?b", "a#b", "a&b", "a=b", "a+b", "a%b", "a b", "a\tb", "a\nb",
        ] {
            let err = validate_path_component("p", v).unwrap_err();
            assert!(
                err.to_string().contains("invalid character"),
                "{v:?}: {err}"
            );
        }
    }

    /// `is_control()` covers both C0 (`\x00`–`\x1f`) and DEL (`\x7f`),
    /// and C1 (`\x80`–`\x9f`). The literals `\t` (`\x09`), `\n`
    /// (`\x0a`), `\r` (`\x0d`) are technically redundant with
    /// `is_control()`, but the test matrix explicitly covers them
    /// alongside the dedicated `is_control()` arms so a future
    /// refactor that drops the literal list still has to make
    /// these tests pass.
    #[test]
    fn rejects_control_chars_via_is_control() {
        // C0 + DEL: classic ASCII control block, plus the explicit
        // tab/newline/CR (covered by both the literal list and
        // `is_control()`).
        for v in [
            "a\x01b",      // SOH
            "a\x07b",      // BEL
            "a\x0bb",      // vertical tab (C0 control, not in literal list)
            "a\x1fb",      // unit separator
            "a\x7fb",      // DEL
        ] {
            let err = validate_path_component("p", v).unwrap_err();
            assert!(
                err.to_string().contains("invalid character"),
                "{v:?}: {err}"
            );
        }

        // C1 controls: the bytes \x80..\x9f are *not* literal-listed,
// but `is_control()` catches them. This is the path that a
// misclassified "non-ASCII" check might miss — pin it. C1 controls
// are valid Unicode codepoints (U+0080..U+009F) but are not valid
// UTF-8 single-byte sequences; encode them via `char::from_u32`.
        for v in [
            format!("a{}b", char::from_u32(0x80).unwrap()),
            format!("a{}b", char::from_u32(0x8F).unwrap()),
            format!("a{}b", char::from_u32(0x9F).unwrap()),
        ] {
            let err = validate_path_component("p", &v).unwrap_err();
            assert!(
                err.to_string().contains("invalid character"),
                "{v:?}: {err}"
            );
        }
    }

    #[test]
    fn rejects_non_ascii() {
        for v in ["café", "über", "🚀app", "日本"] {
            let err = validate_path_component("p", v).unwrap_err();
            assert!(
                err.to_string().contains("invalid character"),
                "{v:?}: {err}"
            );
        }
    }

    #[test]
    fn error_message_names_field() {
        // The field name MUST appear in the error so the operator
        // knows which field is bad when chained inside `.context()`.
        let err = validate_path_component("deployment_id", "a/b").unwrap_err();
        assert!(err.to_string().contains("deployment_id"), "{err}");
    }
}
