//! Caddy admin `/metrics` scraper. Filled in by PR B (issue #665).
//!
//! Diffs the `caddy_http_requests_total` Prometheus counter against the
//! previous tick's value and returns the per-second RPS delta. PR B will
//! add the HTTP GET loop, error/backoff handling, and the unit tests
//! that pin the "missed second decays gracefully" invariant.

#![allow(dead_code)]
