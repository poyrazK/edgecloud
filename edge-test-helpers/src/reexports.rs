//! Re-exports of `edge-worker` types so consumers can write
//! `use edge_test_helpers::{Config, Supervisor};` in a single line.
//!
//! Keeping these in a dedicated module (vs. inline in `lib.rs`) means
//! future additions to the re-export set don't churn `lib.rs` and they
//! sit in one readable place.

pub use edge_worker::config::Config;
pub use edge_worker::supervisor::Supervisor;
