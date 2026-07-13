//! Directory walking + per-file tree transformation.
//!
//! [`walk_tree`] scans a directory for `.c`/`.h` files, skipping build
//! directories (`build/`, `target/`, `node_modules/`, …), and loads
//! each match into a [`FileEntry`] sorted lexicographically. Callers
//! feed the entries into [`transform_tree`] to produce a
//! [`TreeTransformResult`] with one [`FileReport`] per file.
//!
//! M3 adds [`walk_tree_for_language`] and
//! [`transform_tree_for_language`], which dispatch on a [`Language`]
//! value. The original `walk_tree` and `transform_tree` functions are
//! preserved as thin aliases for `Language::C`, so existing callers
//! (the M2 CLI bin, the Go control plane) are unchanged.

use crate::analyzer::{pre_pass_deny_c, CAnalyzer};
use crate::patterns::Language;
use crate::preprocessor::Preprocessor;
use crate::report::{FileReport, MigrationReport, TreeMigrationReport};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use thiserror::Error;
use walkdir::WalkDir;

/// Directory names (any segment) whose contents are skipped during
/// the walk. These match the conventions of the major build systems
/// (cargo `target/`, npm `node_modules/`, CMake `build/`, etc.).
const SKIP_DIRS: &[&str] = &[
    "target",
    "build",
    "node_modules",
    ".git",
    "__pycache__",
    ".cache",
    "dist",
    "out",
];

/// True when any segment of `e.path()` matches a [`SKIP_DIRS`] entry.
/// Used as the `filter_entry` callback for [`walk_tree_for_language`]
/// so walkdir prunes the whole subtree (e.g. `target/`) without
/// descending into it.
fn is_in_skip_dir(e: &walkdir::DirEntry) -> bool {
    e.path()
        .components()
        .any(|c| SKIP_DIRS.contains(&c.as_os_str().to_string_lossy().as_ref()))
}

/// Case-insensitive set of file extensions the walker accepts per
/// language. M3 added `Language::Rust → ["rs"]`; the C entry is the
/// original `["c", "h"]` (header files are included so the downstream
/// clang invocation can resolve `#include "header.h"`).
fn allowed_exts_for(language: Language) -> &'static [&'static str] {
    match language {
        Language::C => &["c", "h"],
        Language::Rust => &["rs"],
    }
}

/// A single source file discovered during a tree walk.
///
/// `path` is forward-slash-relative to the walk root (so
/// `src/util.c`, never `./src/util.c`). `absolute_path` and `source`
/// are marked `#[serde(skip)]` because they're consumed locally by
/// the CLI / server and would just bloat the wire format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    /// Forward-slash path relative to the walk root.
    pub path: String,
    /// Absolute path on disk. Used by the CLI to read the file and
    /// by the server to write the transformed file into the temp dir.
    #[serde(skip)]
    pub absolute_path: PathBuf,
    /// File contents eagerly loaded at walk time so callers can hand
    /// the source directly to the analyzer without re-reading.
    #[serde(skip)]
    pub source: String,
}

/// Errors that can occur during a tree walk.
#[derive(Debug, Error)]
pub enum WalkError {
    /// The walk root exists but is not a directory.
    #[error("path is not a directory: {0}")]
    NotADirectory(PathBuf),
    /// An I/O error occurred while reading a file or listing the root.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Walk a directory recursively, returning every source file (filtered
/// by [`Language`]) as a [`FileEntry`]. Entries are sorted
/// lexicographically by `path` so the output is deterministic across
/// runs.
///
/// Symlinks are followed by default (walkdir's `follow_links(true)`).
/// This is a known limitation — a future hardening pass should reject
/// symlinks that escape the root. Tracked as a follow-up issue.
pub fn walk_tree_for_language(
    root: &Path,
    language: Language,
) -> Result<Vec<FileEntry>, WalkError> {
    let meta = std::fs::metadata(root)?;
    if !meta.is_dir() {
        return Err(WalkError::NotADirectory(root.to_path_buf()));
    }

    let allowed_exts = allowed_exts_for(language);

    // Use `filter_entry` to prune entire subtrees rooted in `SKIP_DIRS`
    // before walkdir descends into them. The previous implementation
    // visited every entry under `target/` / `node_modules` / etc. and
    // filtered after the fact, which is a real perf regression for
    // large Rust crates (10 k+ files in `target/`). The filter
    // callback returns `false` for any entry whose path contains a
    // skip-dir segment; walkdir treats that as "do not yield this
    // entry **or any of its descendants**".
    let mut entries: Vec<FileEntry> = Vec::new();
    let walker = WalkDir::new(root)
        .follow_links(true)
        .into_iter()
        .filter_entry(|e| !is_in_skip_dir(e));

    for entry in walker {
        let entry = match entry {
            Ok(e) => e,
            Err(err) => {
                // walkdir surfaces errors per-entry (e.g. permission denied);
                // surface as an I/O error so callers can fail or skip.
                return Err(WalkError::Io(std::io::Error::new(
                    err.io_error()
                        .map(|e| e.kind())
                        .unwrap_or(std::io::ErrorKind::Other),
                    err.to_string(),
                )));
            }
        };

        // Skip directories (we only emit file entries).
        if entry.file_type().is_dir() {
            continue;
        }

        // Filter to language-specific extensions (case-insensitive on the extension).
        let ext = entry
            .path()
            .extension()
            .and_then(|e| e.to_str())
            .map(|s| s.to_ascii_lowercase());
        let ext = match ext {
            Some(e) => e,
            None => continue,
        };
        if !allowed_exts.contains(&ext.as_str()) {
            continue;
        }

        let abs = entry.path().to_path_buf();
        let source = std::fs::read_to_string(&abs)?;

        // Forward-slash path relative to root.
        let rel = entry
            .path()
            .strip_prefix(root)
            .unwrap_or(entry.path())
            .to_string_lossy()
            .replace('\\', "/");

        entries.push(FileEntry {
            path: rel,
            absolute_path: abs,
            source,
        });
    }

    // Stable, deterministic order — important so that the user's first
    // file (`main.c`) is processed before downstream files and so that
    // the manifest JSON round-trip is stable across runs.
    entries.sort_by(|a, b| a.path.cmp(&b.path));

    Ok(entries)
}

/// Walk a directory for **C** sources (`.c`/`.h`). Equivalent to
/// [`walk_tree_for_language`] with `Language::C`. Preserved as a
/// non-breaking alias so existing callers (the M2 CLI bin, the Go
/// control plane) don't need to pass a language argument.
pub fn walk_tree(root: &Path) -> Result<Vec<FileEntry>, WalkError> {
    walk_tree_for_language(root, Language::C)
}

/// Result of [`transform_tree`]: the input entries plus one
/// [`FileReport`] per file, aggregated into a [`TreeMigrationReport`].
#[derive(Debug)]
pub struct TreeTransformResult {
    /// The input entries, in the order they were processed.
    pub entries: Vec<FileEntry>,
    /// Per-file reports, in the same order as `entries`.
    pub file_reports: Vec<FileReport>,
    /// Tree-level aggregate report (status, totals, etc.).
    pub tree_report: TreeMigrationReport,
}

/// Run the analyzer + transformer for a specific language over a list
/// of [`FileEntry`]s and produce per-file + tree-level reports.
///
/// **Language dispatch:**
/// - `Language::C`: builds one [`Preprocessor`] (via
///   [`Preprocessor::discover`]) and one
///   [`CAnalyzer::with_preprocessor`] reused across files. `clang`
///   is still invoked once per file (no batch mode yet; tracked as
///   a follow-up).
/// - `Language::Rust` (requires `rust` feature): builds one
///   [`RustAnalyzer`] reused across files. No preprocessor.
///
/// On a per-file parse / transform failure, the file produces a
/// `FileReport` with `status: Failed` and an entry in `errors`. Tree
/// processing continues — one bad file does not abort the rest of
/// the tree.
pub fn transform_tree_for_language(
    entries: Vec<FileEntry>,
    language: Language,
) -> TreeTransformResult {
    let app_name = String::new();
    transform_tree_for_language_with_app_name(entries, &app_name, language)
}

/// Like [`transform_tree_for_language`] but uses the provided
/// `app_name` when building each per-file `MigrationReport`. The CLI
/// passes the developer-supplied app name; the server uses a fixed
/// string.
pub fn transform_tree_for_language_with_app_name(
    entries: Vec<FileEntry>,
    app_name: &str,
    language: Language,
) -> TreeTransformResult {
    let file_reports = match language {
        Language::C => transform_tree_c(entries.iter().collect(), app_name),
        Language::Rust => {
            #[cfg(feature = "rust")]
            {
                transform_tree_rust(entries.iter().collect(), app_name)
            }
            #[cfg(not(feature = "rust"))]
            {
                // Without the rust feature, we can't analyze Rust
                // sources. Surface the situation as a single Failed
                // report so callers see the language mismatch
                // explicitly rather than a silent empty result.
                let _ = app_name;
                entries
                    .iter()
                    .map(|e| {
                        use crate::report::{ErrorInfo, MigrationStatus};
                        FileReport {
                            path: e.path.clone(),
                            sha256: if e.source.is_empty() {
                                String::new()
                            } else {
                                crate::report::sha256_hex(e.source.as_bytes())
                            },
                            status: MigrationStatus::Failed,
                            patterns_detected: Vec::new(),
                            transformations: Vec::new(),
                            manual_review: Vec::new(),
                            errors: vec![ErrorInfo {
                                line: 0,
                                message: "edge-migrate-lib was built without the `rust` feature; \
                                          Rust language requested but unavailable"
                                    .to_string(),
                                code: None,
                            }],
                            preprocessor: None,
                        }
                    })
                    .collect()
            }
        }
    };

    let tree_report = TreeMigrationReport::from_files(app_name.to_string(), file_reports.clone());

    TreeTransformResult {
        entries,
        file_reports,
        tree_report,
    }
}

/// C-only path of `transform_tree_for_language_with_app_name`. Builds
/// the preprocessor + CAnalyzer once and reuses them across files.
fn transform_tree_c(entries: Vec<&FileEntry>, app_name: &str) -> Vec<FileReport> {
    let pre = Preprocessor::discover();
    let mut analyzer = match &pre {
        Some(p) => CAnalyzer::with_preprocessor(p.clone()),
        None => CAnalyzer::new(),
    };

    let mut file_reports: Vec<FileReport> = Vec::with_capacity(entries.len());
    for entry in &entries {
        // Issue #622 commit 2 — short-circuit on host-reach
        // #include/#embed BEFORE the analyzer runs. Per-file
        // rejections surface as a `FileReport` with `Status: Failed`
        // and `code: SECURITY_DENY:C_INCLUDE` entries; the Go
        // short-circuit guard (parallel to
        // `MigrationService.Migrate`'s commit-1 guard) prevents the
        // per-file `clang` invocation from running on a denied file.
        // Aggregate tree status naturally lands on `Failed` via the
        // existing `aggregate_status` rule (`failed` if any file is
        // `failed`).
        if let Some(denied_report) = c_deny_tree_report(app_name, &entry.source) {
            file_reports.push(FileReport::from_report(
                entry.path.clone(),
                denied_report,
                &entry.source,
            ));
            continue;
        }
        let (matches, pp_info) = analyzer.analyze_with_preprocessor_info(&entry.source);
        let report = match pp_info {
            Some(info) => {
                MigrationReport::from_pattern_matches_with_preprocessor(app_name, matches, info)
            }
            None => MigrationReport::from_pattern_matches(app_name, matches),
        };
        file_reports.push(FileReport::from_report(
            entry.path.clone(),
            report,
            &entry.source,
        ));
    }
    file_reports
}

/// Issue #622 commit 2: per-file C deny-list pre-pass for the
/// tree-mode path. Mirrors `c_deny_report` in `edge-migrate-bin`
/// but lives here in `edge-migrate-lib` because
/// `transform_tree_for_language_with_app_name` (which the Go
/// control plane drives via the lib, not the bin) is the canonical
/// entry point for the CP's `MigrateTree` flow.
fn c_deny_tree_report(app_name: &str, source: &str) -> Option<MigrationReport> {
    let errs = pre_pass_deny_c(source);
    if errs.is_empty() {
        return None;
    }
    let report = MigrationReport {
        status: crate::report::MigrationStatus::Failed,
        wasm_stored: false,
        deployment_id: None,
        app_name: app_name.to_string(),
        patterns_detected: Vec::new(),
        patterns_transformed: Vec::new(),
        patterns_manual_review: Vec::new(),
        errors: errs,
        preprocessor: None,
    };
    Some(report)
}

/// Rust path of `transform_tree_for_language_with_app_name`. Builds
/// one `RustAnalyzer` reused across files; no preprocessor.
#[cfg(feature = "rust")]
fn transform_tree_rust(entries: Vec<&FileEntry>, app_name: &str) -> Vec<FileReport> {
    use crate::rust_analyzer::RustAnalyzer;

    let mut analyzer = RustAnalyzer::new();
    let mut file_reports: Vec<FileReport> = Vec::with_capacity(entries.len());
    for entry in &entries {
        let matches = analyzer.analyze(&entry.source);
        let report = MigrationReport::from_pattern_matches(app_name, matches);
        file_reports.push(FileReport::from_report(
            entry.path.clone(),
            report,
            &entry.source,
        ));
    }
    file_reports
}

/// Run the C analyzer + transformer over a list of [`FileEntry`]s and
/// produce per-file + tree-level reports. Equivalent to
/// [`transform_tree_for_language`] with `Language::C`. Preserved as
/// a non-breaking alias for M2 callers.
pub fn transform_tree(entries: Vec<FileEntry>) -> TreeTransformResult {
    transform_tree_for_language(entries, Language::C)
}

/// Like [`transform_tree`] but uses the provided `app_name` when
/// building each per-file `MigrationReport`. The CLI passes the
/// developer-supplied app name; the server uses a fixed string.
pub fn transform_tree_with_app_name(
    entries: Vec<FileEntry>,
    app_name: &str,
) -> TreeTransformResult {
    transform_tree_for_language_with_app_name(entries, app_name, Language::C)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// A minimal tempdir helper that doesn't depend on the `tempfile`
    /// crate. Each test gets a unique subdir under the system temp dir,
    /// removed on drop.
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(label: &str) -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let id = COUNTER.fetch_add(1, Ordering::SeqCst);
            let pid = std::process::id();
            let path =
                std::env::temp_dir().join(format!("edge_migrate_test_{}_{}_{}", label, pid, id));
            fs::create_dir_all(&path).expect("create tempdir");
            Self { path }
        }
        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    /// Create a small project layout:
    ///
    /// ```text
    /// root/
    /// ├── main.c
    /// ├── helper.c
    /// ├── helper.h
    /// ├── z_late.c
    /// ├── Makefile
    /// ├── src/
    /// │   └── util.c
    /// ├── build/
    /// │   ├── skip_me.c
    /// │   └── deep/
    /// │       └── nested.c
    /// └── target/
    ///     └── debug/
    ///         └── skip_target.c
    /// ```
    ///
    /// The deeply-nested `.c` files inside `build/` and `target/` are
    /// intentionally placed with the **right** extension so the only
    /// thing keeping them out of the result is the directory-pruning
    /// logic. If `filter_entry` regresses, these would appear in the
    /// output and the test would fail.
    fn make_project() -> TempDir {
        let dir = TempDir::new("walk");
        fs::write(dir.path().join("main.c"), "int main(){return 0;}\n").unwrap();
        fs::write(dir.path().join("helper.c"), "// helper\n").unwrap();
        fs::write(dir.path().join("helper.h"), "// header\n").unwrap();
        fs::write(dir.path().join("z_late.c"), "// z\n").unwrap();
        fs::write(dir.path().join("Makefile"), "# not a C file\n").unwrap();
        fs::create_dir_all(dir.path().join("src")).unwrap();
        fs::write(dir.path().join("src/util.c"), "// util\n").unwrap();
        fs::create_dir_all(dir.path().join("build/deep")).unwrap();
        fs::write(dir.path().join("build/skip_me.c"), "// skip\n").unwrap();
        fs::write(dir.path().join("build/deep/nested.c"), "// nested\n").unwrap();
        fs::create_dir_all(dir.path().join("target/debug")).unwrap();
        fs::write(dir.path().join("target/debug/skip_target.c"), "// skip\n").unwrap();
        dir
    }

    #[test]
    fn test_walk_filters_to_c_and_h() {
        let dir = make_project();
        let entries = walk_tree(dir.path()).expect("walk");
        let paths: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
        // .c and .h included, Makefile excluded.
        assert!(paths.contains(&"main.c"));
        assert!(paths.contains(&"helper.c"));
        assert!(paths.contains(&"helper.h"));
        assert!(paths.contains(&"z_late.c"));
        assert!(paths.contains(&"src/util.c"));
        assert!(!paths.iter().any(|p| p.ends_with("Makefile")));
    }

    #[test]
    fn test_walk_sorts_lexicographically() {
        let dir = make_project();
        let entries = walk_tree(dir.path()).expect("walk");
        let paths: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
        let mut sorted = paths.clone();
        sorted.sort();
        assert_eq!(paths, sorted, "walk_tree must produce sorted output");
    }

    #[test]
    fn test_walk_errors_on_nonexistent_root() {
        let bogus = std::path::PathBuf::from("/this/path/does/not/exist/at/all");
        let err = walk_tree(&bogus).unwrap_err();
        // IO error variant (not_found, etc.)
        assert!(matches!(err, WalkError::Io(_)));
    }

    #[test]
    fn test_walk_errors_on_file_not_directory() {
        let dir = make_project();
        let file_path = dir.path().join("main.c");
        let err = walk_tree(&file_path).unwrap_err();
        assert!(matches!(err, WalkError::NotADirectory(_)));
    }

    #[test]
    fn test_walk_skips_nested_build() {
        let dir = make_project();
        let entries = walk_tree(dir.path()).expect("walk");
        let paths: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
        // No path under build/ (including deeply nested .c files)
        // may appear. This is the property `filter_entry` provides
        // over the older "filter after the fact" approach.
        assert!(
            !paths.iter().any(|p| p.contains("build")),
            "build/ contents must be skipped, got: {:?}",
            paths
        );
        assert!(
            !paths.iter().any(|p| p.contains("deep/nested")),
            "build/deep/nested.c must be skipped (proves subtree pruning), got: {:?}",
            paths
        );
    }

    #[test]
    fn test_walk_filter_entry_skips_target_subtree() {
        // Parallel to `test_walk_skips_nested_build` for the other
        // high-volume skip dir: `target/`. cargo writes thousands of
        // files into `target/debug/...` and we must not enumerate any
        // of them.
        let dir = make_project();
        let entries = walk_tree(dir.path()).expect("walk");
        let paths: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
        assert!(
            !paths.iter().any(|p| p.contains("target")),
            "target/ contents must be skipped, got: {:?}",
            paths
        );
        assert!(
            !paths.iter().any(|p| p.contains("skip_target")),
            "target/debug/skip_target.c must be skipped, got: {:?}",
            paths
        );
    }

    #[test]
    fn test_walk_source_is_loaded() {
        let dir = make_project();
        let entries = walk_tree(dir.path()).expect("walk");
        let main = entries.iter().find(|e| e.path == "main.c").unwrap();
        assert!(main.source.contains("int main"));
    }

    // ─────────────────────────────────────────────────────────────────
    // transform_tree tests (M2.C4)
    // ─────────────────────────────────────────────────────────────────

    /// Make a 2-file project with a TCP server (main) and a helper
    /// that has BOTH a transformable pattern (socket) and a
    /// non-transformable one (poll) so the helper file's status is
    /// Partial (some transformable + some not).
    fn make_tree_with_poll() -> TempDir {
        let dir = TempDir::new("tt");
        fs::write(
            dir.path().join("main.c"),
            "int main(){int fd = socket(2, 1, 0); (void)fd; return 0;}\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("helper.c"),
            "int use_poll(void){int fd = socket(2, 1, 0); struct pollfd fds[1]; poll(fds, 1, 0); (void)fd; return 0;}\n",
        )
        .unwrap();
        dir
    }

    #[test]
    fn test_tree_transform_produces_one_report_per_entry() {
        let dir = make_project();
        let entries = walk_tree(dir.path()).expect("walk");
        let n = entries.len();
        let result = transform_tree(entries);
        assert_eq!(result.file_reports.len(), n);
        assert_eq!(result.tree_report.files_total, n);
        // Each file report must carry its path.
        for (entry, fr) in result.entries.iter().zip(result.file_reports.iter()) {
            assert_eq!(entry.path, fr.path);
        }
    }

    #[test]
    fn test_tree_transform_reports_partial_when_one_file_has_manual_review() {
        use crate::report::MigrationStatus;
        let dir = make_tree_with_poll();
        let entries = walk_tree(dir.path()).expect("walk");
        let result = transform_tree(entries);
        // main.c = Success (socket is auto-transformable), helper.c =
        // Partial (poll is non-transformable). Aggregate → Partial.
        assert!(matches!(
            result.tree_report.status,
            MigrationStatus::Partial
        ));
        let main = result
            .file_reports
            .iter()
            .find(|f| f.path == "main.c")
            .unwrap();
        let helper = result
            .file_reports
            .iter()
            .find(|f| f.path == "helper.c")
            .unwrap();
        assert!(matches!(main.status, MigrationStatus::Success));
        assert!(matches!(helper.status, MigrationStatus::Partial));
        assert!(!result.tree_report.is_migratable());
    }

    #[test]
    fn test_tree_transform_continues_after_one_file_parse_error() {
        // A file that won't parse should produce a Failed FileReport
        // with an error message, while other files still produce
        // reports. (Tree-sitter is permissive, so we construct a
        // scenario that triggers the fallback: an empty source.)
        let dir = TempDir::new("tt_err");
        fs::write(dir.path().join("a.c"), "int main(){return 0;}\n").unwrap();
        fs::write(dir.path().join("broken.c"), "").unwrap();
        let entries = walk_tree(dir.path()).expect("walk");
        let result = transform_tree(entries);
        // Both files produce a report (no panics, no aborts).
        assert_eq!(result.file_reports.len(), 2);
        // Empty source produces zero matches → status is Success (no
        // patterns means no manual review). The important property is
        // that transform_tree did not panic and continued.
        let _ = result.tree_report.clone();
    }

    // ─────────────────────────────────────────────────────────────────
    // M3.C5 — walk_tree_for_language / transform_tree_for_language
    // ─────────────────────────────────────────────────────────────────

    /// Make a small mixed Rust project: one `.rs` file, one `.c` file,
    /// one `Makefile` (no extension match).
    fn make_rust_project() -> TempDir {
        let dir = TempDir::new("rust_proj");
        fs::write(
            dir.path().join("main.rs"),
            "fn main() {\n    let _ = std::net::TcpListener::bind(\"127.0.0.1:80\");\n}\n",
        )
        .unwrap();
        fs::write(
            dir.path().join("ignored.c"),
            "// C file we should not see\n",
        )
        .unwrap();
        fs::write(dir.path().join("Makefile"), "# not Rust\n").unwrap();
        dir
    }

    #[test]
    fn test_walk_tree_for_language_rust_filters_to_rs() {
        let dir = make_rust_project();
        let entries = walk_tree_for_language(dir.path(), Language::Rust).expect("walk");
        let paths: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
        assert_eq!(
            paths,
            vec!["main.rs"],
            "Rust walk should pick only .rs, got {:?}",
            paths
        );
    }

    #[test]
    fn test_walk_tree_for_language_c_includes_h() {
        // C walk must still include .h (regression check for the
        // C path after the extension list moved into a function).
        let dir = make_project();
        let entries = walk_tree_for_language(dir.path(), Language::C).expect("walk");
        let paths: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
        assert!(paths.contains(&"main.c"));
        assert!(paths.contains(&"helper.h"));
        assert!(!paths.iter().any(|p| p.ends_with(".rs")));
    }

    #[test]
    fn test_transform_tree_default_alias_unchanged_for_c() {
        // The M2 `transform_tree` alias must keep returning C reports
        // for C inputs (regression check after introducing the
        // language-aware variant).
        let dir = make_tree_with_poll();
        let entries = walk_tree(dir.path()).expect("walk");
        let result = transform_tree(entries);
        // Two C files with one socket + one poll → Partial at the
        // tree level (matches the M2.C4 test).
        use crate::report::MigrationStatus;
        assert!(matches!(
            result.tree_report.status,
            MigrationStatus::Partial
        ));
        assert_eq!(result.file_reports.len(), 2);
    }

    #[cfg(feature = "rust")]
    #[test]
    fn test_transform_tree_for_language_rust_dispatches_to_rust_analyzer() {
        let dir = make_rust_project();
        let entries = walk_tree_for_language(dir.path(), Language::Rust).expect("walk");
        let result = transform_tree_for_language(entries, Language::Rust);
        assert_eq!(result.file_reports.len(), 1);
        let main = &result.file_reports[0];
        assert_eq!(main.path, "main.rs");
        // The Rust analyzer should detect the TcpBind match.
        // PatternInfo.pattern renders via Debug today, so the
        // string form is "Rust(TcpBind)" — see report.rs. M3.C5
        // keeps the existing Debug-format rendering for
        // back-compat with C reports.
        assert!(
            main.patterns_detected
                .iter()
                .any(|p| p.pattern.contains("TcpBind")),
            "expected a TcpBind detection in Rust report, got {:?}",
            main.patterns_detected
        );
        // M3 wires the Rust analyzer through `analyze()` but the
        // binary transformation still happens server-side via the
        // `edge-migrate --transform` subprocess. The lib's local
        // tree path reports the analysis only, mirroring the C path
        // where the per-file transformed source is also produced by
        // the subprocess. Confirm the manual_review list is empty
        // (TcpBind is AutoTransformable, not BestEffort).
        assert!(main.manual_review.is_empty());
        // And no preprocessor info (Rust has none in v1).
        assert!(main.preprocessor.is_none());
    }

    #[cfg(not(feature = "rust"))]
    #[test]
    fn test_transform_tree_for_language_rust_without_feature_records_failed_reports() {
        // When the lib is built without the `rust` feature, asking
        // for Rust produces explicit Failed reports (not silent empty
        // results). This is the defense-in-depth contract for
        // downstream consumers.
        let dir = make_rust_project();
        let entries = walk_tree_for_language(dir.path(), Language::Rust).expect("walk");
        let result = transform_tree_for_language(entries, Language::Rust);
        assert_eq!(result.file_reports.len(), 1);
        let main = &result.file_reports[0];
        use crate::report::MigrationStatus;
        assert!(matches!(main.status, MigrationStatus::Failed));
        assert!(
            main.errors[0].message.contains("rust"),
            "error message must mention the missing feature, got: {}",
            main.errors[0].message
        );
    }
}
