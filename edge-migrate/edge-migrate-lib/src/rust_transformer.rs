//! Rust source transformer (M3).
//!
//! Rewrites `std::net::*` / `std::fs::*` calls into their
//! `wasi::socket::*` / `wasi::filesystem::*` equivalents via the
//! same byte-range descending-order pattern used by the C transformer.
//!
//! The full implementation lands in M3.C4; this stub exists so the
//! `rust` feature flag and module wiring (M3.C2) compile cleanly on
//! their own.

use crate::patterns::PatternMatch;
use crate::transformer::TransformResult;

/// Rewrites detected Rust patterns into `wasi::socket::*` /
///
/// `wasi::filesystem::*` calls.
///
/// **Stub** — M3.C4 replaces this with a real byte-range rewriter.
pub struct RustTransformer;

impl RustTransformer {
    /// Build a new transformer. Stateless; constructor exists only to
    /// mirror the C `Transformer` shape and give the bin a place to
    /// hang per-instance config later.
    pub fn new() -> Self {
        Self
    }

    /// Rewrite the source by applying each match's replacement in
    /// descending byte-range order.
    ///
    /// **Stub** — M3.C4 lands the real implementation. For now the
    /// source is passed through unchanged and no transformations are
    /// reported.
    pub fn transform(&self, source: &str, _matches: Vec<PatternMatch>) -> TransformResult {
        TransformResult {
            transformed_source: source.to_string(),
            transformations_applied: Vec::new(),
            manual_review: Vec::new(),
            errors: Vec::new(),
            preprocessor: None,
        }
    }
}

impl Default for RustTransformer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stub_passes_source_through() {
        let src = "fn main() { let _ = std::net::TcpListener::bind(\"127.0.0.1:80\"); }";
        let t = RustTransformer::new();
        let r = t.transform(src, Vec::new());
        assert_eq!(r.transformed_source, src);
        assert!(r.transformations_applied.is_empty());
        assert!(r.manual_review.is_empty());
        assert!(r.errors.is_empty());
        assert!(r.preprocessor.is_none());
    }
}
