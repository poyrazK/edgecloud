//! edge-migrate-lib: Core analysis + transformation library for edge-migrate.
//!
//! Provides C AST analysis via tree-sitter, POSIX pattern detection,
//! POSIX → WASI transformation, and structured migration reports.
//!
//! M3 adds an optional `rust` feature flag that pulls in `tree-sitter-rust`
//! and exposes the `rust_analyzer` / `rust_transformer` modules. The
//! default build is C-only — no Rust toolchain dependency is added for
//! users who don't enable the feature.

pub mod analyzer;
pub mod patterns;
pub mod preprocessor;
pub mod report;
pub mod transformer;
pub mod tree;

#[cfg(feature = "rust")]
pub mod rust_analyzer;
#[cfg(feature = "rust")]
pub mod rust_transformer;

pub use analyzer::CAnalyzer;
pub use patterns::{is_valid_deployment_app_name, Language, PatternKind, PatternMatch, PosixPattern, RustPattern, Transformability};
pub use preprocessor::{ExpandedSource, PreprocessError, Preprocessor, PreprocessorInfo};
pub use report::{FileReport, MigrationReport, TreeMigrationReport};
pub use transformer::{TransformResult, Transformer};
pub use tree::{
    transform_tree, transform_tree_for_language, transform_tree_for_language_with_app_name,
    transform_tree_with_app_name, walk_tree, walk_tree_for_language, FileEntry,
    TreeTransformResult, WalkError,
};

#[cfg(feature = "rust")]
pub use rust_analyzer::RustAnalyzer;
#[cfg(feature = "rust")]
pub use rust_transformer::RustTransformer;
