//! Library surface for `edge-ingress-sidecar` (issue #665, PR E).
//!
//! The crate is **primarily a binary** (`edge-ingress-sidecar`) but
//! also exposes a small library target so the integration test
//! (`tests/integration_test.rs`) can drive the full pipeline from a
//! separate compilation unit:
//!
//!   NatsPublisher::publish_delta → NATS subject → push consumer
//!   (`nats_sub::spawn_consumer` + freshness gate) → Aggregator::tick
//!   → Snapshot::per_replica_cap.
//!
//! The production binary at `src/main.rs` uses these same modules
//! identically — `src/main.rs` does NOT redeclare them; it just adds
//! the `fn main` wiring. The `pub mod` declarations live here so the
//! integration test sees them via the library's path.
//!
//! **Module visibility.** All modules are `pub` so the integration
//! test can reach them. The only consumer of these `pub` paths
//! outside the binary is `tests/integration_test.rs`, gated behind
//! `RUN_INTEGRATION_TESTS`.
//!
//! **Why a lib target.** Rust integration tests under `tests/*.rs`
//! compile as **separate test binaries** that link against the crate
//! they're testing. Without a `[lib]` block, the crate has no library
//! surface to link against, and the integration test's
//! `use edge_ingress_sidecar::*` fails to resolve. Adding the lib
//! target — with the same module tree as `main.rs` would declare —
//! is the standard hatch (also used by `edge-js-runtime` /
//! `edge-js-runtime-long`, see CLAUDE.md "edge-js-runtime long-running
//! sibling crate").

pub mod aggregate;
pub mod caddy_metrics;
pub mod config;
pub mod expose;
pub mod nats_pub;
pub mod nats_sub;
