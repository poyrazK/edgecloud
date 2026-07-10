//! Project scaffolding helpers — `edge init` and friends.
//!
//! The `wit` submodule materializes the canonical WIT tree into
//! a freshly scaffolded project so the project builds offline
//! outside the monorepo (issue #576). The actual `include_dir!`
//! embed + the `build.rs` rerun-if-changed wiring land in commit 2;
//! the stub here lets commit 1's template rewrite compile cleanly.

pub mod wit;
