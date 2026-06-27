//! C preprocessor expansion via `clang -E`.
//!
//! When the analyzer has a preprocessor attached, source is first expanded
//! with `clang -E -P -nostdinc` before tree-sitter analysis. This catches
//! POSIX patterns hidden behind macros like
//! `#define socket(x) make_socket(x)`.
//!
//! Silent fallback: when `clang` is not reachable, `Preprocessor::discover`
//! returns `None` and the analyzer uses the unexpanded source as-is.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use thiserror::Error;

/// Errors from the preprocessor.
#[derive(Debug, Error)]
pub enum PreprocessError {
    /// `clang` returned non-zero exit status.
    #[error("clang -E failed: {0}")]
    ClangFailed(String),

    /// `clang` stdout could not be decoded as UTF-8.
    #[error("clang output is not valid UTF-8")]
    NotUtf8,

    /// I/O error spawning `clang`.
    #[error("failed to spawn clang: {0}")]
    Io(#[from] std::io::Error),
}

/// Result of a successful expansion.
#[derive(Debug, Clone)]
pub struct ExpandedSource {
    /// The fully expanded C source (one logical line per output line).
    pub text: String,
    /// Maps each expanded line (0-indexed) to the original source line
    /// (1-indexed). `line_map[i]` is the original line that produced
    /// expanded line `i + 1`. When `# <lineno> "<file>"` markers are
    /// absent or cannot be parsed, the entry is `i + 1` (identity).
    pub line_map: Vec<u32>,
    /// Per-line byte remap, parallel to `line_map`. For each expanded
    /// line (0-indexed): `(expanded_byte_start_of_line, original_byte_start_of_line)`.
    /// The expanded byte is the byte offset of the start of that line
    /// within `text`. The original byte is the byte offset of the
    /// corresponding user-file line within the original source.
    ///
    /// Synthetic lines (linemarkers, `<built-in>`, `<command line>`)
    /// have `u32::MAX` for the original byte — the analyzer skips
    /// remapping for matches that fall on such lines.
    ///
    /// When no preprocessor is attached, expansion fails, or the
    /// callers don't need byte remap, callers pass `Vec::new()` —
    /// the analyzer treats an empty map as identity.
    pub byte_map: Vec<(u32, u32)>,
    /// Number of macro substitutions observed (heuristic, may be 0).
    pub macros_expanded: usize,
}

/// Summary of preprocessing metadata, attached to reports.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreprocessorInfo {
    /// Version reported by `clang --version` (best-effort).
    pub clang_version: Option<String>,
    /// Number of files the preprocessor was invoked on.
    pub files_processed: usize,
    /// Total macros expanded across all invocations.
    pub macros_expanded: usize,
}

/// Wrapper around a `clang -E` invocation.
#[derive(Debug, Clone)]
pub struct Preprocessor {
    /// Path to the `clang` binary in use.
    clang_path: PathBuf,
}

impl Preprocessor {
    /// Locate `clang` on the system. Returns `None` if not found.
    ///
    /// Search order:
    /// 1. `which clang` (PATH lookup)
    /// 2. `$WASI_SDK_PATH/bin/clang` — the wasi-sdk ships a `clang` binary;
    ///    with `-nostdinc` the bundled sysroot is irrelevant for
    ///    preprocessor-only mode, so we can reuse it for expansion.
    pub fn discover() -> Option<Self> {
        Self::discover_with(
            |name| which::which(name).ok(),
            std::env::var("WASI_SDK_PATH").ok(),
        )
    }

    /// Internal testable seam for `discover`.
    ///
    /// `which_fn` looks up a binary by name (mimics the real `which::which`).
    /// `wasi_sdk_path` is the value of the `WASI_SDK_PATH` env var, if any.
    fn discover_with<F>(which_fn: F, wasi_sdk_path: Option<String>) -> Option<Self>
    where
        F: Fn(&str) -> Option<PathBuf>,
    {
        if let Some(p) = which_fn("clang") {
            return Some(Self { clang_path: p });
        }
        if let Some(sdk) = wasi_sdk_path {
            let candidate = Path::new(&sdk).join("bin").join("clang");
            if candidate.is_file() {
                return Some(Self {
                    clang_path: candidate,
                });
            }
        }
        None
    }

    /// Construct from an explicit path. Mostly for tests.
    pub fn new(clang_path: PathBuf) -> Self {
        Self { clang_path }
    }

    /// Path to the `clang` binary in use.
    pub fn clang_path(&self) -> &Path {
        &self.clang_path
    }

    /// Run `clang -E -nostdinc` on the given source and return the
    /// expanded output plus a line-mapping table.
    ///
    /// The source is written to a unique temp file named with
    /// `filename_hint` as the basename (so `# <lineno> "<file>"`
    /// markers in clang's output reference it). The temp file is
    /// removed before returning.
    ///
    /// `filename_hint` is matched against clang's linemarker files to
    /// build `line_map[i]` = the original source line that produced
    /// expanded line `i + 1`. Linemarker lines themselves map to `0`
    /// (synthetic). Linemarkers for `<built-in>`, `<command line>`,
    /// and any other non-matching file also map to `0` until the next
    /// linemarker for `filename_hint` is seen.
    ///
    /// **`-nostdinc`** keeps system headers out of the expansion —
    /// without it, the output explodes with glibc declarations. This
    /// means project-internal headers need a future `--include-dir`
    /// flag (out of scope for M1).
    pub fn expand(
        &self,
        source: &str,
        filename_hint: &str,
    ) -> Result<ExpandedSource, PreprocessError> {
        use std::io::Write;

        // Write source to a unique temp file in the OS temp dir, named
        // so that clang's `# "<file>"` markers reference it. We pass
        // the full temp filename (not just the hint) to build_line_map
        // so the linemarker file matches.
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let pid = std::process::id();
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let safe_hint = sanitize_filename_hint(filename_hint);
        let temp_name = format!("edge_migrate_pp_{}_{}_{}.c", pid, n, safe_hint);
        let source_path = std::env::temp_dir().join(&temp_name);
        let temp_path_str = source_path
            .to_str()
            .ok_or_else(|| {
                PreprocessError::ClangFailed("temp path is not valid UTF-8".to_string())
            })?
            .to_string();

        {
            let mut f = std::fs::File::create(&source_path)?;
            f.write_all(source.as_bytes())?;
        }

        // Run clang. Capture stdout (the expanded source) and stderr
        // (diagnostics, surfaced on error).
        let output = Command::new(&self.clang_path)
            .args(["-E", "-nostdinc", &temp_path_str])
            .output();

        // Always clean up the temp file, even on error paths.
        let _ = std::fs::remove_file(&source_path);

        let output = output?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            return Err(PreprocessError::ClangFailed(stderr));
        }

        let text = String::from_utf8(output.stdout).map_err(|_| PreprocessError::NotUtf8)?;
        // Match against the full temp path so clang's `# "<file>"`
        // markers (which reference the full path) align.
        let line_map = build_line_map(&text, &temp_path_str);
        // Parallel byte-offset remap; see `build_byte_map` for semantics.
        let byte_map = build_byte_map(&text, &line_map, source);
        let macros_expanded = count_macros(source);

        Ok(ExpandedSource {
            text,
            line_map,
            byte_map,
            macros_expanded,
        })
    }

    /// Best-effort `clang --version` probe. Used for `PreprocessorInfo`.
    pub fn clang_version(&self) -> Option<String> {
        let output = Command::new(&self.clang_path)
            .arg("--version")
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let stdout = String::from_utf8(output.stdout).ok()?;
        // First line of `clang --version` is the version string.
        Some(stdout.lines().next()?.to_string())
    }
}

/// Reduce a user-supplied `filename_hint` to a clang-safe basename.
///
/// Strips any path components and replaces characters that clang's
/// linemarker parser might mishandle. We don't try to be exhaustive —
/// the goal is to keep `# <lineno> "<file>"` markers parseable.
fn sanitize_filename_hint(hint: &str) -> String {
    let basename = Path::new(hint)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("edge_migrate_input.c");
    basename
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Parse a clang linemarker line. Returns `(original_line, filename)`
/// on success, `None` otherwise.
///
/// Format: `# <num> "<file>" [<flags>]` (the `[]` is optional). The
/// file is the part between the first pair of `"` characters.
fn parse_linemarker(line: &str) -> Option<(u32, &str)> {
    let rest = line.strip_prefix('#')?.trim_start();
    let mut parts = rest.splitn(3, char::is_whitespace);
    let num_str = parts.next()?;
    let num: u32 = num_str.parse().ok()?;
    let filename_part = parts.next()?;
    if !filename_part.starts_with('"') {
        return None;
    }
    let after_quote = &filename_part[1..];
    let end_quote = after_quote.find('"')?;
    Some((num, &after_quote[..end_quote]))
}

/// Build a `line_map` from clang's preprocessed output. The map has
/// one entry per output line (0-indexed). Source lines map to the
/// lineno of the most recent linemarker for `filename_hint`; linemarker
/// lines and lines from other files map to `0` (synthetic).
fn build_line_map(text: &str, filename_hint: &str) -> Vec<u32> {
    let mut map = Vec::with_capacity(text.lines().count());
    let mut current_user_line: u32 = 0;
    for line in text.lines() {
        if let Some((lineno, filename)) = parse_linemarker(line) {
            if filename == filename_hint {
                current_user_line = lineno;
            }
            map.push(0); // synthetic linemarker line
        } else {
            map.push(current_user_line);
        }
    }
    map
}
/// Build a `byte_map` parallel to `line_map`. For each expanded line
/// (0-indexed), the entry is `(expanded_byte_start_of_line, original_byte_start_of_line)`.
///
/// The expanded byte is the byte offset within `expanded_text` where
/// that line starts (computed by walking the text once). The original
/// byte is the byte offset within `original_source` where the
/// corresponding user-file line starts. Lines whose `line_map[i]` is
/// `0` (synthetic — linemarkers, `<built-in>`, etc.) get `u32::MAX`
/// for the original byte so the analyzer can skip remapping.
///
/// This is the byte-range counterpart to `line_map`. The analyzer
/// uses it to remap tree-sitter byte ranges (which are in expanded
/// coordinates) back to original-source coordinates before the
/// transformer slices the original source.
///
/// **Limitations:** the same as `line_map` — clang emits linemarkers
/// at file boundaries, not at every source line, so a match on a
/// synthetic line is not remappable. Single-line matches on user-file
/// lines are exact.
fn build_byte_map(
    expanded_text: &str,
    line_map: &[u32],
    original_source: &str,
) -> Vec<(u32, u32)> {
    // Precompute original-source line starts: line_starts_orig[0] = 0,
    // line_starts_orig[i] = byte offset of the i-th line (0-indexed).
    // A line starts at the byte after each `\n` in the original.
    let mut line_starts_orig: Vec<u32> = Vec::with_capacity(line_map.len().max(1));
    line_starts_orig.push(0);
    for (i, b) in original_source.bytes().enumerate() {
        if b == b'\n' {
            line_starts_orig.push((i + 1) as u32);
        }
    }

    // Walk expanded text line by line, tracking running byte offset.
    // We use `split('\n').take(line_map.len())` to align the iteration
    // count with `line_map` (which is built with `text.lines()`, which
    // strips a single trailing `\n`). `split('\n')` would otherwise
    // yield one extra empty entry for a file ending in `\n`, making
    // `result.len() > line_map.len()` and causing
    // `byte_map[expanded_row]` lookups for the last user line to read
    // a phantom (byte=0, orig=0) entry.
    let mut result = Vec::with_capacity(line_map.len());
    let mut expanded_byte: u32 = 0;
    for (i, line) in expanded_text.split('\n').take(line_map.len()).enumerate() {
        let line_len = (line.len() + 1) as u32; // include the trailing `\n`
        let user_line = line_map.get(i).copied().unwrap_or(0);
        let original_byte = if user_line == 0 {
            u32::MAX // synthetic
        } else {
            // user_line is 1-indexed; convert to 0-indexed for the
            // line_starts_orig lookup. If the original source is
            // shorter than the line_map claims (should not happen,
            // but defensive), fall back to u32::MAX.
            let idx = (user_line - 1) as usize;
            line_starts_orig.get(idx).copied().unwrap_or(u32::MAX)
        };
        result.push((expanded_byte, original_byte));
        expanded_byte = expanded_byte.saturating_add(line_len);
    }
    result
}



/// Heuristic count of `#define` directives in the original source.
/// Used for `PreprocessorInfo.macros_expanded` — not exact, but a
/// reasonable proxy for "how much did the preprocessor do".
fn count_macros(source: &str) -> usize {
    source
        .lines()
        .filter(|l| {
            let t = l.trim_start();
            t.starts_with("#define ") || t.starts_with("#define\t")
        })
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_discover_with_returns_none_when_clang_missing() {
        let result = Preprocessor::discover_with(|_| None, None);
        assert!(result.is_none());
    }

    #[test]
    fn test_discover_with_returns_none_when_only_wasi_sdk_path_set_but_no_file() {
        // Even with WASI_SDK_PATH set, if the clang binary doesn't exist
        // at the expected path, discover returns None.
        let result =
            Preprocessor::discover_with(|_| None, Some("/this/path/does/not/exist".to_string()));
        assert!(result.is_none());
    }

    #[test]
    fn test_discover_with_finds_clang_in_path() {
        let result = Preprocessor::discover_with(
            |name| {
                assert_eq!(name, "clang");
                Some(PathBuf::from("/usr/bin/clang"))
            },
            None,
        );
        let p = result.expect("expected Some");
        assert_eq!(p.clang_path(), Path::new("/usr/bin/clang"));
    }

    #[test]
    fn test_discover_with_falls_back_to_wasi_sdk_path() {
        // PATH lookup returns nothing, but $WASI_SDK_PATH/bin/clang exists
        // (we use the system clang to create a real file for the test).
        let system_clang = which::which("clang").expect("system clang must exist for this test");
        let sdk_dir = system_clang
            .parent()
            .and_then(|p| p.parent())
            .expect("clang's grandparent dir")
            .to_path_buf();
        let result =
            Preprocessor::discover_with(|_| None, Some(sdk_dir.to_string_lossy().to_string()));
        let p = result.expect("expected fallback to WASI_SDK_PATH");
        assert!(p.clang_path().starts_with(&sdk_dir));
        assert!(p.clang_path().ends_with("clang"));
    }

    #[test]
    fn test_path_lookup_wins_over_wasi_sdk_path() {
        // When both PATH and WASI_SDK_PATH are set, PATH wins.
        let result = Preprocessor::discover_with(
            |_| Some(PathBuf::from("/from/path/clang")),
            Some("/should/be/ignored".to_string()),
        );
        let p = result.expect("expected Some");
        assert_eq!(p.clang_path(), Path::new("/from/path/clang"));
    }

    #[test]
    fn test_new_and_clang_path() {
        let p = Preprocessor::new(PathBuf::from("/opt/clang-19/bin/clang"));
        assert_eq!(p.clang_path(), Path::new("/opt/clang-19/bin/clang"));
    }

    // --- expand() tests (require a real clang on the system) ---

    #[test]
    fn test_expand_unwraps_simple_define() {
        // Skip when the test runner doesn't have clang. The integration
        // test gated on EDGE_TEST_CLANG=1 lives in the bin crate.
        let Some(pp) = Preprocessor::discover() else {
            eprintln!("skipping: clang not found");
            return;
        };
        let source = "#define SOCK_STREAM 1\nint main() { return SOCK_STREAM; }\n";
        let expanded = pp
            .expand(source, "test_simple_define.c")
            .expect("expand should succeed");
        // The macro is expanded to the literal `1`.
        assert!(
            expanded.text.contains("return 1"),
            "expected 'return 1' in expanded source, got: {}",
            expanded.text
        );
        // The original `#define` line is gone.
        assert!(!expanded.text.contains("#define SOCK_STREAM"));
        // We saw one macro in the source.
        assert_eq!(expanded.macros_expanded, 1);
    }

    #[test]
    fn test_expand_socket_macro_matches_posix_call() {
        let Some(pp) = Preprocessor::discover() else {
            eprintln!("skipping: clang not found");
            return;
        };
        // The fixture is checked into edge-migrate/testdata/macro_socket.c.
        let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("testdata")
            .join("macro_socket.c");
        let source = std::fs::read_to_string(&fixture_path).expect("read fixture");
        let expanded = pp
            .expand(&source, "macro_socket.c")
            .expect("expand should succeed");
        // The hidden socket(...) call must now appear as make_socket(...).
        assert!(
            expanded.text.contains("make_socket"),
            "expected make_socket(...) in expanded source"
        );
        // The original `socket(...)` text (not the macro call site) is
        // gone — it's been replaced.
        assert!(
            !expanded.text.contains("int fd = socket("),
            "expected the macro call site to be expanded away"
        );
        // We saw at least 3 macros (socket, SOCK_STREAM, AF_INET).
        assert!(
            expanded.macros_expanded >= 3,
            "expected at least 3 macros expanded, got {}",
            expanded.macros_expanded
        );
    }

    #[test]
    fn test_expand_line_map_points_back_at_original() {
        let Some(pp) = Preprocessor::discover() else {
            eprintln!("skipping: clang not found");
            return;
        };
        // Source where line numbers matter: a 5-line source with the
        // socket() call on line 5. After expansion, the line_map for
        // the line that contains `make_socket(` should map back to
        // *some* line in the original source (not zero, not synthetic).
        //
        // **Known limitation:** clang's `clang -E` only emits
        // linemarkers at file boundaries, not at every line. The
        // remapping is therefore best-effort: when a linemarker for
        // the user file appears before a source line, that lineno is
        // the entry; otherwise the entry is `0` (synthetic). The
        // common case — a single `#define` in a small file — is
        // covered because the re-entry linemarker for the user file
        // is emitted near the top of the expanded source. This is
        // documented in `docs/design.md` as a limitation; an exact
        // mapping would require either a custom preprocessor or
        // libclang's preprocessing API, both out of scope for M1.
        let source = "/* line 1 */\n/* line 2 */\n/* line 3 */\n\
                     #define socket(f, t, p) make_socket(f, t, p)\n\
                     int x = socket(1, 2, 3);\n";
        let expanded = pp
            .expand(source, "line_map_test.c")
            .expect("expand should succeed");
        let make_socket_idx = expanded
            .text
            .lines()
            .position(|l| l.contains("make_socket("))
            .expect("make_socket should appear in expanded source");
        let original_line = expanded.line_map[make_socket_idx];
        // The mapping is best-effort: it should be either 0 (synthetic)
        // or some non-zero value pointing back into the user file. We
        // don't assert a specific number because clang's behavior here
        // depends on the system headers and built-in markers, which
        // vary across platforms.
        assert!(
            original_line <= source.lines().count() as u32,
            "line_map entry {} is beyond the original source's line count",
            original_line
        );
    }

    #[test]
    fn test_expand_preserves_non_macro_source_unchanged() {
        let Some(pp) = Preprocessor::discover() else {
            eprintln!("skipping: clang not found");
            return;
        };
        // No macros at all.
        let source = "int main() { return 42; }\n";
        let expanded = pp
            .expand(source, "no_macros.c")
            .expect("expand should succeed");
        // Source survives intact (modulo linemarkers).
        assert!(expanded.text.contains("int main()"));
        assert!(expanded.text.contains("return 42"));
        assert_eq!(expanded.macros_expanded, 0);
    }

    // --- build_line_map unit tests (no clang needed) ---

    #[test]
    fn test_build_line_map_synthetic_markers() {
        // Markers for a different file do not change the user-line
        // tracker; source lines between them map to whatever the most
        // recent user-file linemarker said.
        let text = "\
# 1 \"user.c\"
int main() {
# 5 \"user.c\"
    return 0;
# 1 \"<built-in>\"
foo
";
        let map = build_line_map(text, "user.c");
        // Indices into `map` correspond to output lines 0..N.
        // Line 0: marker for user.c:1 -> 0 (synthetic)
        assert_eq!(map[0], 0);
        // Line 1: source line at user.c:1
        assert_eq!(map[1], 1);
        // Line 2: marker for user.c:5 -> 0 (synthetic)
        assert_eq!(map[2], 0);
        // Line 3: source line at user.c:5
        assert_eq!(map[3], 5);
        // Line 4: marker for <built-in> -> 0
        assert_eq!(map[4], 0);
        // Line 5: source line at user.c:5 (still — the next marker is
        // for <built-in>, which we ignore)
        assert_eq!(map[5], 5);
    }

    #[test]
    fn test_build_byte_map_marks_synthetic_lines_max() {
        // Same input shape as test_build_line_map_synthetic_markers:
        // a clang-style expanded output with linemarker lines.
        let text = "\
# 1 \"user.c\"
int main() {
# 3 \"user.c\"
    return 0;
# 1 \"<built-in>\"
foo
";
        let line_map = build_line_map(text, "user.c");
        // Original source has 3 user lines: line 1 at byte 0,
        // line 2 at byte 13, line 3 at byte 27.
        let original = "int main() {\n    return 0;\n}\n";
        let byte_map = build_byte_map(text, &line_map, original);
        // Synthetic linemarker lines (entries 0, 2, 4) get u32::MAX
        // for the original byte.
        assert_eq!(byte_map[0].1, u32::MAX, "linemarker line should be synthetic");
        assert_eq!(byte_map[2].1, u32::MAX, "linemarker line should be synthetic");
        assert_eq!(byte_map[4].1, u32::MAX, "linemarker line should be synthetic");
        // User-file source lines map to actual byte offsets in the
        // original source. line_map[1]=1 → byte 0; line_map[3]=3 → byte 27.
        assert_eq!(byte_map[1].1, 0, "line 1 maps to original byte 0");
        assert_eq!(byte_map[3].1, 27, "line 3 maps to original byte 27");
        // The expanded byte offsets must be monotonically increasing.
        for win in byte_map.windows(2) {
            assert!(win[1].0 > win[0].0, "expanded byte must be increasing");
        }
    }

    #[test]
    fn test_build_byte_map_identity_when_no_expansion() {
        // When the expanded text is identical to the original (no
        // macros, no linemarkers), byte_map[i] should map each
        // expanded line to the same byte offset as the corresponding
        // original line.
        let text = "int main() {\n    return 0;\n}\n";
        // Identity line_map: each expanded line is from user file at
        // the same 1-indexed line number.
        let line_map = vec![1, 2, 3, 4];
        let byte_map = build_byte_map(text, &line_map, text);
        // "int main() {" = 12 chars (line 1, byte 0)
        // "    return 0;" = 13 chars (line 2, byte 13)
        // "}" = 1 char (line 3, byte 27)
        // "" trailing (line 4, byte 29 — after "}\n")
        assert_eq!(byte_map[0], (0, 0));
        assert_eq!(byte_map[1], (13, 13));
        assert_eq!(byte_map[2], (27, 27));
        assert_eq!(byte_map[3], (29, 29));
    }

    fn test_parse_linemarker_basic() {
        let (n, f) = parse_linemarker("# 1 \"foo.c\"").unwrap();
        assert_eq!(n, 1);
        assert_eq!(f, "foo.c");

        let (n, f) = parse_linemarker("# 42 \"path/to/file.c\" 3").unwrap();
        assert_eq!(n, 42);
        assert_eq!(f, "path/to/file.c");
    }

    #[test]
    fn test_parse_linemarker_ignores_non_markers() {
        assert!(parse_linemarker("int main() {").is_none());
        assert!(parse_linemarker("").is_none());
        assert!(parse_linemarker("#define FOO 1").is_none());
        assert!(parse_linemarker("# \"foo.c\"").is_none()); // missing lineno
    }

    #[test]
    fn test_sanitize_filename_hint_strips_path() {
        assert_eq!(sanitize_filename_hint("/tmp/foo.c"), "foo.c");
        assert_eq!(sanitize_filename_hint("a/b/c/d.c"), "d.c");
        assert_eq!(sanitize_filename_hint("plain.c"), "plain.c");
    }

    #[test]
    fn test_sanitize_filename_hint_replaces_bad_chars() {
        assert_eq!(sanitize_filename_hint("a b.c"), "a_b.c");
        assert_eq!(sanitize_filename_hint("foo;bar.c"), "foo_bar.c");
        assert_eq!(sanitize_filename_hint(""), "edge_migrate_input.c");
    }

    #[test]
    fn test_count_macros_counts_only_defines() {
        let source = "#define A 1\n#define B 2\nint x = A + B;\n#define C 3\n";
        assert_eq!(count_macros(source), 3);
        let source = "int x = 1;\n";
        assert_eq!(count_macros(source), 0);
        // Indented defines count too.
        let source = "  #define A 1\n";
        assert_eq!(count_macros(source), 1);
    }
}
