//! Rust source analyzer (M3).
//!
//! Detects `std::net`, `std::fs`, and `std::process` patterns via
//! tree-sitter-rust. The full implementation lands in M3.C3; this stub
//! exists so the `rust` feature flag and module wiring (M3.C2) compile
//! cleanly on their own.

use crate::patterns::PatternMatch;

/// Detects `std::net::*` / `std::fs::*` / `std::process::*` patterns
/// in Rust source. Mirrors the `CAnalyzer` constructor shape so the
/// M3.C5 dispatcher can hold either analyzer in the same slot.
///
/// **Stub** — M3.C3 replaces this with a real `tree_sitter_rust::Parser`
/// walker. For now `analyze` returns an empty `Vec` so the feature flag
/// builds in isolation.
pub struct RustAnalyzer;

impl RustAnalyzer {
    /// Build a new analyzer with a fresh tree-sitter-rust parser.
    pub fn new() -> Self {
        Self
    }

    /// Parse the given Rust source and return all detected patterns.
    ///
    /// **Stub** — M3.C3 lands the real implementation.
    pub fn analyze(&mut self, _source: &str) -> Vec<PatternMatch> {
        Vec::new()
    }
}

impl Default for RustAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stub_returns_empty() {
        let mut a = RustAnalyzer::new();
        let matches = a.analyze("fn main() { let _ = std::net::TcpListener::bind(\"127.0.0.1:80\"); }");
        assert!(matches.is_empty(), "stub should return no matches");
    }
}
