//! Migration report types.
//!
//! Defines the structured report format returned by the migration pipeline.

use crate::patterns::PatternMatch;
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
}

impl MigrationReport {
    /// Determine migratability: does the source have any NotTransformable patterns?
    pub fn is_migratable(&self) -> bool {
        self.patterns_manual_review.is_empty()
    }

    /// Create a report from a list of pattern matches.
    pub fn from_pattern_matches(app_name: &str, matches: Vec<PatternMatch>) -> Self {
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::patterns::{PosixPattern, Transformability};

    #[test]
    fn test_is_migratable_all_auto() {
        let matches = vec![
            PatternMatch {
                line: 1,
                start_byte: 0,
                end_byte: 0,
                pattern: PosixPattern::SocketTcp,
                snippet: "socket(AF_INET, SOCK_STREAM, 0)".to_string(),
                arg_nodes: vec![
                    "AF_INET".to_string(),
                    "SOCK_STREAM".to_string(),
                    "0".to_string(),
                ],
                transformability: Transformability::AutoTransformable,
            },
            PatternMatch {
                line: 2,
                start_byte: 0,
                end_byte: 0,
                pattern: PosixPattern::Connect,
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
                start_byte: 0,
                end_byte: 0,
                pattern: PosixPattern::SocketTcp,
                snippet: "socket(AF_INET, SOCK_STREAM, 0)".to_string(),
                arg_nodes: vec![
                    "AF_INET".to_string(),
                    "SOCK_STREAM".to_string(),
                    "0".to_string(),
                ],
                transformability: Transformability::AutoTransformable,
            },
            PatternMatch {
                line: 2,
                start_byte: 0,
                end_byte: 0,
                pattern: PosixPattern::Poll,
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
            start_byte: 0,
            end_byte: 0,
            pattern: PosixPattern::Fork,
            snippet: "fork()".to_string(),
            arg_nodes: vec![],
            transformability: Transformability::NotTransformable,
        }];
        let report = MigrationReport::from_pattern_matches("hello_world", matches);
        assert!(!report.is_migratable());
        assert!(matches!(report.status, MigrationStatus::Failed));
    }
}
