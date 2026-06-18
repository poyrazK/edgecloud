//! Migration report types.
//!
//! Defines the structured report format returned by the migration pipeline.

use crate::patterns::PatternMatch;
use crate::preprocessor::PreprocessorInfo;
use serde::{Deserialize, Serialize};

/// Overall migration status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MigrationStatus {
    /// Migration succeeded — all patterns auto-transformed.
    Success,
    /// Migration partially succeeded — some patterns require manual review.
    Partial,
    /// Migration failed — untransformable patterns detected.
    Failed,
}

/// A single pattern detected in the source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternInfo {
    /// 1-based line number.
    pub line: usize,
    /// The type of pattern.
    pub pattern: String,
    /// The original source code snippet.
    pub snippet: String,
    /// WASI equivalent description.
    pub wasi_equivalent: String,
    /// Transformability classification.
    pub transformability: String,
}

/// An error encountered during migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorInfo {
    /// 1-based line number.
    pub line: usize,
    /// Error message.
    pub message: String,
}

/// The migration report returned to the developer and used by the CLI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationReport {
    /// Migration status.
    pub status: MigrationStatus,
    /// Whether the wasm binary was stored on edgeCloud.
    pub wasm_stored: bool,
    /// The deployment ID assigned (if wasm was stored).
    pub deployment_id: Option<String>,
    /// The app name derived from the filename.
    pub app_name: String,
    /// All patterns detected in the source.
    pub patterns_detected: Vec<PatternInfo>,
    /// Patterns that were auto-transformed.
    pub patterns_transformed: Vec<PatternInfo>,
    /// Patterns that require manual review.
    pub patterns_manual_review: Vec<PatternInfo>,
    /// Errors encountered.
    pub errors: Vec<ErrorInfo>,
    /// Preprocessor metadata, when a preprocessor was used during
    /// analysis. `None` when no preprocessor was attached, when the
    /// preprocessor was not discovered, or when the analyzer fell
    /// back to the unexpanded source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preprocessor: Option<PreprocessorInfo>,
}

impl MigrationReport {
    /// Determine migratability: does the source have any NotTransformable patterns?
    pub fn is_migratable(&self) -> bool {
        self.patterns_manual_review.is_empty()
    }

    /// Create a report from a list of pattern matches.
    pub fn from_pattern_matches(
        app_name: &str,
        matches: Vec<PatternMatch>,
    ) -> Self {
        let patterns_detected: Vec<PatternInfo> = matches
            .iter()
            .map(|m| PatternInfo {
                line: m.line,
                pattern: format!("{:?}", m.pattern),
                snippet: m.snippet.clone(),
                wasi_equivalent: m.pattern.wasi_equivalent().to_string(),
                transformability: format!("{:?}", m.transformability),
            })
            .collect();

        let patterns_transformed: Vec<PatternInfo> = patterns_detected
            .iter()
            .filter(|p| p.transformability != "NotTransformable")
            .cloned()
            .collect();

        let patterns_manual_review: Vec<PatternInfo> = patterns_detected
            .iter()
            .filter(|p| p.transformability == "NotTransformable")
            .cloned()
            .collect();

        let status = if patterns_manual_review.is_empty() {
            MigrationStatus::Success
        } else if patterns_transformed.is_empty() {
            MigrationStatus::Failed
        } else {
            MigrationStatus::Partial
        };

        Self {
            status,
            wasm_stored: false,
            deployment_id: None,
            app_name: app_name.to_string(),
            patterns_detected,
            patterns_transformed,
            patterns_manual_review,
            errors: Vec::new(),
            preprocessor: None,
        }
    }

    /// Create a report with preprocessor metadata attached.
    pub fn from_pattern_matches_with_preprocessor(
        app_name: &str,
        matches: Vec<PatternMatch>,
        preprocessor: PreprocessorInfo,
    ) -> Self {
        let mut report = Self::from_pattern_matches(app_name, matches);
        report.preprocessor = Some(preprocessor);
        report
    }
}

/// Per-file migration report, embedded inside a
/// [`TreeMigrationReport`]. Mirrors the subset of [`MigrationReport`]
/// that is meaningful at the file level.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileReport {
    /// Forward-slash path relative to the walk root.
    pub path: String,
    /// Per-file status.
    pub status: MigrationStatus,
    /// All patterns detected in this file.
    pub patterns_detected: Vec<PatternInfo>,
    /// Patterns that were auto-transformed.
    pub transformations: Vec<PatternInfo>,
    /// Patterns that require manual review.
    pub manual_review: Vec<PatternInfo>,
    /// Per-file errors (parse failure, transform failure). Empty on success.
    #[serde(default)]
    pub errors: Vec<ErrorInfo>,
    /// Preprocessor metadata for this file, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preprocessor: Option<PreprocessorInfo>,
}

/// Tree-level migration report returned to the developer / CLI.
///
/// Aggregates per-file [`FileReport`]s plus tree-level metadata
/// (deployment ID, aggregate status, totals). Used by
/// `POST /api/migrate-tree`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreeMigrationReport {
    /// Aggregate tree-level status. Derived from per-file statuses:
    /// `Success` if every file is `Success`; `Failed` if any file
    /// is `Failed`; else `Partial`.
    pub status: MigrationStatus,
    /// Whether the wasm binary was stored on edgeCloud.
    pub wasm_stored: bool,
    /// The deployment ID assigned (if wasm was stored).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployment_id: Option<String>,
    /// The app name supplied by the developer.
    pub app_name: String,
    /// Per-file reports, sorted by `path` (matches the walk order).
    pub files: Vec<FileReport>,
    /// Tree-level errors (clang invocation, wasm size, db write).
    /// Distinct from per-file errors.
    #[serde(default)]
    pub errors: Vec<ErrorInfo>,
    /// Number of `.c`/`.h` files in the tree.
    pub files_total: usize,
    /// Number of files with at least one auto-transformed pattern.
    pub files_transformed: usize,
    /// Number of files with at least one manual-review pattern.
    pub files_manual_review: usize,
}

impl FileReport {
    /// Build a `FileReport` from a path + the per-file `MigrationReport`
    /// produced by `transform_tree`. The path is recorded; everything
    /// else is borrowed from the input report (per-file preprocessor
    /// info, if present, is moved over).
    pub fn from_report(path: String, r: MigrationReport) -> Self {
        Self {
            path,
            status: r.status,
            patterns_detected: r.patterns_detected,
            transformations: r.patterns_transformed,
            manual_review: r.patterns_manual_review,
            errors: r.errors,
            preprocessor: r.preprocessor,
        }
    }

    /// Build a `FileReport` representing a per-file parse / transform
    /// failure. The status is `Failed` and the provided message is
    /// recorded in `errors`.
    pub fn from_error(path: String, line: usize, message: String) -> Self {
        Self {
            path,
            status: MigrationStatus::Failed,
            patterns_detected: Vec::new(),
            transformations: Vec::new(),
            manual_review: Vec::new(),
            errors: vec![ErrorInfo { line, message }],
            preprocessor: None,
        }
    }
}

impl TreeMigrationReport {
    /// A tree is migratable iff every file is fully transformable:
    /// no per-file failures AND no manual-review patterns. Used by
    /// the CLI to decide whether to upload without `--force`.
    pub fn is_migratable(&self) -> bool {
        self.files.iter().all(|f| {
            !matches!(f.status, MigrationStatus::Failed) && f.manual_review.is_empty()
        })
    }

    /// Build a `TreeMigrationReport` from per-file reports. Computes
    /// the aggregate status, counts, and tree-level error list.
    pub fn from_files(app_name: String, files: Vec<FileReport>) -> Self {
        let files_total = files.len();
        let files_transformed = files
            .iter()
            .filter(|f| !f.transformations.is_empty())
            .count();
        let files_manual_review = files
            .iter()
            .filter(|f| !f.manual_review.is_empty())
            .count();
        let status = aggregate_status(&files);

        Self {
            status,
            wasm_stored: false,
            deployment_id: None,
            app_name,
            files,
            errors: Vec::new(),
            files_total,
            files_transformed,
            files_manual_review,
        }
    }
}

/// Aggregate status from per-file statuses:
/// `Success` if every file is `Success`; `Failed` if any file is
/// `Failed`; else `Partial`. An empty file list is `Success` (a tree
/// with no `.c` files trivially "migrates" to an empty wasm).
fn aggregate_status(files: &[FileReport]) -> MigrationStatus {
    if files.is_empty() {
        return MigrationStatus::Success;
    }
    let mut any_failed = false;
    let mut any_partial = false;
    for f in files {
        match f.status {
            MigrationStatus::Failed => any_failed = true,
            MigrationStatus::Partial => any_partial = true,
            MigrationStatus::Success => {}
        }
    }
    if any_failed {
        MigrationStatus::Failed
    } else if any_partial {
        MigrationStatus::Partial
    } else {
        MigrationStatus::Success
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::patterns::{PatternKind, PosixPattern, Transformability};

    #[test]
    fn test_is_migratable_all_auto() {
        let matches = vec![
            PatternMatch {
                line: 1,
                column: None,
                start_byte: 0,
                end_byte: 0,
                pattern: PatternKind::Posix(PosixPattern::SocketTcp),
                snippet: "socket(AF_INET, SOCK_STREAM, 0)".to_string(),
                arg_nodes: vec!["AF_INET".to_string(), "SOCK_STREAM".to_string(), "0".to_string()],
                transformability: Transformability::AutoTransformable,
            },
            PatternMatch {
                line: 2,
                column: None,
                start_byte: 0,
                end_byte: 0,
                pattern: PatternKind::Posix(PosixPattern::Connect),
                snippet: "connect(fd, ...)".to_string(),
                arg_nodes: vec!["fd".to_string(), "...".to_string()],
                transformability: Transformability::AutoTransformable,
            },
        ];
        let report = MigrationReport::from_pattern_matches("hello_world", matches);
        assert!(report.is_migratable());
        assert!(matches!(report.status, MigrationStatus::Success));
    }

    #[test]
    fn test_is_migratable_with_manual_review() {
        let matches = vec![
            PatternMatch {
                line: 1,
                column: None,
                start_byte: 0,
                end_byte: 0,
                pattern: PatternKind::Posix(PosixPattern::SocketTcp),
                snippet: "socket(AF_INET, SOCK_STREAM, 0)".to_string(),
                arg_nodes: vec!["AF_INET".to_string(), "SOCK_STREAM".to_string(), "0".to_string()],
                transformability: Transformability::AutoTransformable,
            },
            PatternMatch {
                line: 2,
                column: None,
                start_byte: 0,
                end_byte: 0,
                pattern: PatternKind::Posix(PosixPattern::Poll),
                snippet: "poll(fds, 2, timeout)".to_string(),
                arg_nodes: vec!["fds".to_string(), "2".to_string(), "timeout".to_string()],
                transformability: Transformability::NotTransformable,
            },
        ];
        let report = MigrationReport::from_pattern_matches("hello_world", matches);
        assert!(!report.is_migratable());
        assert!(matches!(report.status, MigrationStatus::Partial));
        assert_eq!(report.patterns_manual_review.len(), 1);
    }

    #[test]
    fn test_is_migratable_all_not_transformable() {
        let matches = vec![PatternMatch {
            line: 1,
            column: None,
            start_byte: 0,
            end_byte: 0,
            pattern: PatternKind::Posix(PosixPattern::Fork),
            snippet: "fork()".to_string(),
            arg_nodes: vec![],
            transformability: Transformability::NotTransformable,
        }];
        let report = MigrationReport::from_pattern_matches("hello_world", matches);
        assert!(!report.is_migratable());
        assert!(matches!(report.status, MigrationStatus::Failed));
    }

    #[test]
    fn test_report_with_preprocessor_attaches_info() {
        let matches = vec![PatternMatch {
            line: 1,
            column: None,
            start_byte: 0,
            end_byte: 0,
            pattern: PatternKind::Posix(PosixPattern::SocketTcp),
            snippet: "socket(AF_INET, SOCK_STREAM, 0)".to_string(),
            arg_nodes: vec!["AF_INET".to_string(), "SOCK_STREAM".to_string(), "0".to_string()],
            transformability: Transformability::AutoTransformable,
        }];
        let pp = PreprocessorInfo {
            clang_version: Some("clang version 17.0.0".to_string()),
            files_processed: 1,
            macros_expanded: 3,
        };
        let report = MigrationReport::from_pattern_matches_with_preprocessor(
            "hello_world",
            matches,
            pp,
        );
        let attached = report.preprocessor.expect("preprocessor info should be set");
        assert_eq!(attached.files_processed, 1);
        assert_eq!(attached.macros_expanded, 3);
        assert_eq!(attached.clang_version.as_deref(), Some("clang version 17.0.0"));
    }

    #[test]
    fn test_report_default_has_no_preprocessor() {
        let matches = vec![];
        let report = MigrationReport::from_pattern_matches("hello_world", matches);
        assert!(report.preprocessor.is_none());
    }

    // ─────────────────────────────────────────────────────────────────────
    // Tree / per-file report tests (M2.C2)
    // ─────────────────────────────────────────────────────────────────────

    fn make_pattern_info(line: usize, transformability: &str) -> PatternInfo {
        PatternInfo {
            line,
            pattern: "SocketTcp".to_string(),
            snippet: "socket(AF_INET, SOCK_STREAM, 0)".to_string(),
            wasi_equivalent: "create-tcp-socket(ipv4)".to_string(),
            transformability: transformability.to_string(),
        }
    }

    #[test]
    fn test_file_report_from_report_renames_fields() {
        // `from_report` populates `transformations` from the per-file
        // report's `patterns_transformed` field (the per-file report
        // uses `transformations`, the single-file report uses
        // `patterns_transformed` — the conversion must rename).
        let r = MigrationReport {
            status: MigrationStatus::Success,
            wasm_stored: false,
            deployment_id: None,
            app_name: "hello".to_string(),
            patterns_detected: vec![make_pattern_info(1, "AutoTransformable")],
            patterns_transformed: vec![make_pattern_info(1, "AutoTransformable")],
            patterns_manual_review: vec![],
            errors: vec![],
            preprocessor: None,
        };
        let fr = FileReport::from_report("src/main.c".to_string(), r);
        assert_eq!(fr.path, "src/main.c");
        assert!(matches!(fr.status, MigrationStatus::Success));
        assert_eq!(fr.transformations.len(), 1);
        assert!(fr.manual_review.is_empty());
        assert!(fr.preprocessor.is_none());
    }

    #[test]
    fn test_file_report_from_error_marks_failed() {
        let fr = FileReport::from_error("broken.c".to_string(), 0, "parse error".to_string());
        assert!(matches!(fr.status, MigrationStatus::Failed));
        assert_eq!(fr.errors.len(), 1);
        assert_eq!(fr.errors[0].message, "parse error");
    }

    #[test]
    fn test_tree_report_status_all_success() {
        let r = MigrationReport {
            status: MigrationStatus::Success,
            wasm_stored: false,
            deployment_id: None,
            app_name: "x".to_string(),
            patterns_detected: vec![],
            patterns_transformed: vec![],
            patterns_manual_review: vec![],
            errors: vec![],
            preprocessor: None,
        };
        let files = vec![
            FileReport::from_report("a.c".to_string(), r.clone()),
            FileReport::from_report("b.c".to_string(), r.clone()),
        ];
        let tree = TreeMigrationReport::from_files("hello".to_string(), files);
        assert!(matches!(tree.status, MigrationStatus::Success));
        assert_eq!(tree.files_total, 2);
        assert_eq!(tree.files_transformed, 0);
        assert_eq!(tree.files_manual_review, 0);
        assert!(tree.is_migratable());
    }

    #[test]
    fn test_tree_report_status_partial_when_one_file_partial() {
        let success = MigrationReport {
            status: MigrationStatus::Success,
            wasm_stored: false,
            deployment_id: None,
            app_name: "x".to_string(),
            patterns_detected: vec![make_pattern_info(1, "AutoTransformable")],
            patterns_transformed: vec![make_pattern_info(1, "AutoTransformable")],
            patterns_manual_review: vec![],
            errors: vec![],
            preprocessor: None,
        };
        let partial = MigrationReport {
            status: MigrationStatus::Partial,
            wasm_stored: false,
            deployment_id: None,
            app_name: "x".to_string(),
            patterns_detected: vec![
                make_pattern_info(1, "AutoTransformable"),
                make_pattern_info(2, "NotTransformable"),
            ],
            patterns_transformed: vec![make_pattern_info(1, "AutoTransformable")],
            patterns_manual_review: vec![make_pattern_info(2, "NotTransformable")],
            errors: vec![],
            preprocessor: None,
        };
        let files = vec![
            FileReport::from_report("a.c".to_string(), success),
            FileReport::from_report("b.c".to_string(), partial),
        ];
        let tree = TreeMigrationReport::from_files("hello".to_string(), files);
        assert!(matches!(tree.status, MigrationStatus::Partial));
        assert_eq!(tree.files_total, 2);
        assert_eq!(tree.files_manual_review, 1);
        assert!(!tree.is_migratable());
    }

    #[test]
    fn test_tree_report_status_failed_when_any_file_failed() {
        let success = MigrationReport {
            status: MigrationStatus::Success,
            wasm_stored: false,
            deployment_id: None,
            app_name: "x".to_string(),
            patterns_detected: vec![],
            patterns_transformed: vec![],
            patterns_manual_review: vec![],
            errors: vec![],
            preprocessor: None,
        };
        let failed = FileReport::from_error("broken.c".to_string(), 0, "boom".to_string());
        let files = vec![
            FileReport::from_report("a.c".to_string(), success),
            failed,
        ];
        let tree = TreeMigrationReport::from_files("hello".to_string(), files);
        assert!(matches!(tree.status, MigrationStatus::Failed));
        assert!(!tree.is_migratable());
    }

    #[test]
    fn test_tree_report_empty_input_is_success() {
        let tree = TreeMigrationReport::from_files("hello".to_string(), vec![]);
        assert!(matches!(tree.status, MigrationStatus::Success));
        assert_eq!(tree.files_total, 0);
        assert!(tree.is_migratable());
    }

    #[test]
    fn test_tree_report_serializes_with_optional_fields_absent() {
        // Round-trip with wasm_stored=false, deployment_id=None, and
        // a per-file preprocessor=None must produce a JSON object that
        // omits the optional fields.
        let fr = FileReport::from_report(
            "a.c".to_string(),
            MigrationReport {
                status: MigrationStatus::Success,
                wasm_stored: false,
                deployment_id: None,
                app_name: "x".to_string(),
                patterns_detected: vec![],
                patterns_transformed: vec![],
                patterns_manual_review: vec![],
                errors: vec![],
                preprocessor: None,
            },
        );
        let tree = TreeMigrationReport::from_files("hello".to_string(), vec![fr]);
        let json = serde_json::to_string(&tree).expect("serialize");
        // Optional fields must NOT appear in the JSON.
        assert!(!json.contains("deployment_id"), "json: {}", json);
        assert!(!json.contains("preprocessor"), "json: {}", json);
        // Required fields ARE present.
        assert!(json.contains("\"status\""));
        assert!(json.contains("\"files\""));
        assert!(json.contains("\"files_total\""));

        // Round-trip back into a TreeMigrationReport.
        let parsed: TreeMigrationReport = serde_json::from_str(&json).expect("parse");
        assert!(matches!(parsed.status, MigrationStatus::Success));
        assert_eq!(parsed.files.len(), 1);
        assert!(parsed.deployment_id.is_none());
    }

    #[test]
    fn test_tree_report_files_transformed_count() {
        let with_trans = MigrationReport {
            status: MigrationStatus::Success,
            wasm_stored: false,
            deployment_id: None,
            app_name: "x".to_string(),
            patterns_detected: vec![make_pattern_info(1, "AutoTransformable")],
            patterns_transformed: vec![make_pattern_info(1, "AutoTransformable")],
            patterns_manual_review: vec![],
            errors: vec![],
            preprocessor: None,
        };
        let without_trans = MigrationReport {
            status: MigrationStatus::Success,
            wasm_stored: false,
            deployment_id: None,
            app_name: "x".to_string(),
            patterns_detected: vec![],
            patterns_transformed: vec![],
            patterns_manual_review: vec![],
            errors: vec![],
            preprocessor: None,
        };
        let files = vec![
            FileReport::from_report("a.c".to_string(), with_trans.clone()),
            FileReport::from_report("b.c".to_string(), without_trans.clone()),
            FileReport::from_report("c.c".to_string(), with_trans),
        ];
        let tree = TreeMigrationReport::from_files("hello".to_string(), files);
        assert_eq!(tree.files_transformed, 2);
        assert_eq!(tree.files_total, 3);
    }
}
