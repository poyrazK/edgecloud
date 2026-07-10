//! Project scaffolding helpers — `edge init` and friends.
//!
//! The `wit` submodule materializes the canonical WIT tree into
//! a freshly scaffolded project so the project builds offline
//! outside the monorepo (issue #576). The `templates` submodule
//! holds the literal strings the scaffold writes — split out per
//! the PR #589 review so the procedural code in
//! `crate::commands::init` stays focused on the filesystem moves.

pub mod templates;
pub mod wit;
