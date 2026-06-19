//! edge-migrate-lib: Core analysis + transformation library for edge-migrate.
//!
//! Provides C AST analysis via tree-sitter, POSIX pattern detection,
//! POSIX → WASI transformation, and structured migration reports.

pub mod analyzer;
pub mod patterns;
pub mod report;
pub mod transformer;

pub use analyzer::CAnalyzer;
pub use patterns::{PatternMatch, PosixPattern, Transformability};
pub use report::{MigrationReport, TransformOutput};
pub use transformer::{TransformResult, Transformer};
