//! Rust source transformer (M3).
//!
//! Rewrites `std::net::*` / `std::fs::*` / `std::process::*` calls into
//! their `wasi::socket::*` / `wasi::filesystem::*` equivalents using
//! the same byte-range descending-order rewriter pattern as the C
//! `Transformer`.
//!
//! **Scope:** only `std` patterns (mirrors `RustAnalyzer`). `tokio::net`,
//! `async-std`, `#![no_std]`, and `macro_rules!` are out of scope for
//! v1 — see `rust_analyzer.rs` for the matching scope statement.
//!
//! **Preprocessor:** none. The transformer emits `preprocessor: None`
//! on its `TransformResult`.
//!
//! ## Output shape
//!
//! The transformed source is laid out as:
//!
//! ```text
//! // <auto-prelude: use wasi::*>
//! // <original source from byte 0 up to the first match's start>
//! <match-1 replacement>
//! // <gap content between match-1 and match-2 in original coords>
//! <match-2 replacement>
//! ...
//! // <trailing original content from last match's start to EOF>
//! ```
//!
//! Matches are processed from the **end of the file backward** so the
//! byte offsets of matches earlier in the file remain valid as we go.
//! `NotTransformable` matches are excluded from rewriting and instead
//! surface as `manual_review` entries on the result.

use crate::patterns::{PatternKind, PatternMatch, RustPattern, Transformability};
use crate::transformer::{TransformError, TransformResult, Transformation};

/// WASI Rust prelude prepended to the transformed source.
///
/// Mirrors the C `WASI_INCLUDES` block in `transformer.rs`. The
/// `wasi::filesystem` and `wasi::socket` re-exports are conservative
/// — only the symbols the transformer actually emits. Adding symbols
/// here makes them visible across the transformed file without a
/// per-replacement `use` statement.
const WASI_RUST_PRELUDE: &str = "\
use wasi::socket::tcp::TcpSocket;
use wasi::socket::udp::UdpSocket;
use wasi::socket::AddressFamily;
use wasi::filesystem;

";

/// Rewrites detected Rust patterns into `wasi::socket::*` /
/// `wasi::filesystem::*` calls.
///
/// Stateless; the constructor exists only to mirror the C
/// `Transformer` shape and to give the bin a place to hang per-instance
/// config later.
pub struct RustTransformer;

impl RustTransformer {
    /// Build a new transformer.
    pub fn new() -> Self {
        Self
    }

    /// Rewrite the source by applying each match's replacement in
    /// descending byte-range order. `NotTransformable` matches are
    /// collected into `manual_review` on the result.
    ///
    /// Takes `&self` to mirror `RustAnalyzer::analyze`; the struct is
    /// stateless today but future config (custom prelude, dialect
    /// selection) will hang off the instance.
    pub fn transform(&self, source: &str, matches: Vec<PatternMatch>) -> TransformResult {
        let mut transformations_applied = Vec::new();
        let mut manual_review = Vec::new();
        let mut errors = Vec::new();

        // Partition into transformable, not-transformable, and Posix-leak
        // errors. The Rust transformer never sees a C match in
        // production; if it does (a caller bug), we want a loud error
        // rather than silent dropping into `manual_review` (which
        // would lie about what kind of pattern it is).
        let mut transformable: Vec<PatternMatch> = Vec::new();
        let mut posix_leaks: Vec<PatternMatch> = Vec::new();
        for m in matches {
            match (m.transformability, m.pattern) {
                (Transformability::NotTransformable, _) => manual_review.push(m),
                (_, PatternKind::Posix(_)) => posix_leaks.push(m),
                (_, PatternKind::Rust(_)) => transformable.push(m),
            }
        }
        for m in posix_leaks {
            errors.push(TransformError {
                line: m.line,
                pattern: m.pattern,
                message: format!(
                    "RustTransformer received a POSIX match ({}); this is a caller bug",
                    m.pattern.name()
                ),
            });
        }

        // Sort by start_byte **ascending** so we can emit the output in
        // source order: prelude + prefix + replacement + gap +
        // replacement + suffix. The descending-order variant that
        // the C `Transformer` uses produces a scrambled output where
        // the prefix ends up at the end; that bug only shows up
        // when callers inspect the byte ordering, but it's still
        // wrong and the Rust path fixes it.
        let mut sorted = transformable;
        sorted.sort_by_key(|m| m.start_byte);

        let source_bytes = source.as_bytes();

        // Build output: WASI Rust prelude + (gap + WASI replacement)
        // for each match, then trailing original content.
        let mut output = WASI_RUST_PRELUDE.as_bytes().to_vec();

        // prev_end tracks the END of the previous match in ORIGINAL
        // coordinates. Start at 0 (beginning of file).
        let mut prev_end: usize = 0;

        for m in &sorted {
            let wasi_code = match generate_wasi_code(m) {
                Ok(s) => s,
                Err(e) => {
                    errors.push(TransformError {
                        line: m.line,
                        pattern: m.pattern,
                        message: e,
                    });
                    continue;
                }
            };
            if wasi_code.is_empty() {
                continue;
            }

            let orig_start = m.start_byte;
            let orig_end = m.end_byte;

            // Copy the gap content (between the previous match's end
            // and this match's start in original coordinates).
            output.extend_from_slice(&source_bytes[prev_end..orig_start]);

            // Append the WASI replacement.
            output.extend_from_slice(wasi_code.as_bytes());

            prev_end = orig_end;

            let pattern_name = m.pattern.name();
            transformations_applied.push(Transformation {
                line: m.line,
                pattern: m.pattern,
                description: format!(
                    "Transformed {pattern_name} → {}",
                    wasi_code.split_whitespace().next().unwrap_or(pattern_name)
                ),
            });
        }

        // Trailing original content from the last match's end to EOF.
        if prev_end < source_bytes.len() {
            output.extend_from_slice(&source_bytes[prev_end..]);
        }

        let transformed_source =
            String::from_utf8(output).expect("Transformed Rust source is not valid UTF-8");

        TransformResult {
            transformed_source,
            transformations_applied,
            manual_review,
            errors,
            // Rust has no preprocessor in v1.
            preprocessor: None,
        }
    }
}

impl Default for RustTransformer {
    fn default() -> Self {
        Self::new()
    }
}

/// Render the WASI replacement for a single Rust match. Returns
/// `Err(String)` when the match carries no usable argument (e.g. a
/// `TcpBind` without an address). Caller records this as a
/// `TransformError` and skips the replacement.
fn generate_wasi_code(m: &PatternMatch) -> Result<String, String> {
    let pattern = match m.pattern {
        PatternKind::Rust(p) => p,
        // C matches that leaked through the partition: report a
        // helpful error instead of producing wrong-looking Rust code.
        PatternKind::Posix(_) => {
            return Err(format!(
                "RustTransformer received a POSIX match ({}); this is a bug",
                m.pattern.name()
            ));
        }
    };
    let arg = |i: usize| -> Result<&str, String> {
        m.arg_nodes
            .get(i)
            .map(|s| s.as_str())
            .ok_or_else(|| format!("missing argument index {} for {:?}", i, pattern))
    };
    Ok(match pattern {
        RustPattern::TcpBind => format!(
            "TcpSocket::new(AddressFamily::Ipv4)?.start_bind({})?.finish_bind()?.start_listen()?.finish_listen()",
            arg(0)?
        ),
        RustPattern::TcpConnect => format!(
            "TcpSocket::new(AddressFamily::Ipv4)?.start_connect({})?.finish_connect()",
            arg(0)?
        ),
        RustPattern::UdpBind => format!(
            "UdpSocket::new(AddressFamily::Ipv4)?.start_bind({})?.finish_bind()",
            arg(0)?
        ),
        // NotTransformable variants should have been partitioned out
        // by the caller. If we reach them here, emit an empty string
        // (no replacement) and let the caller record nothing.
        //
        // #128: TcpAccept was BestEffort (busy-spin poll loop) and is
        // now NotTransformable — the busy-spin was a placeholder for
        // a real wasi:io pollable subscription that doesn't exist in
        // MVP. We retain it in this catch-all so a partition bug
        // surfaces as "no emit" rather than the broken busy-spin
        // emit. The transformability flip in `patterns.rs` keeps the
        // match out of `sorted` entirely.
        RustPattern::TcpAccept | RustPattern::UdpConnect | RustPattern::ProcessExit => String::new(),
        RustPattern::FsOpen => format!("filesystem::open({})", arg(0)?),
        RustPattern::FsRead => format!("filesystem::read({})", arg(0)?),
        RustPattern::FsWrite => format!("filesystem::write({}, {})", arg(0)?, arg(1)?),
        RustPattern::FsClose => "drop(self)".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::patterns::{PatternKind, RustPattern, Transformability};

    fn match_at(
        source: &str,
        needle: &str,
        pattern: RustPattern,
        transformability: Transformability,
        arg_nodes: Vec<String>,
    ) -> PatternMatch {
        let start = source.find(needle).expect("needle must exist in source");
        let end = start + needle.len();
        PatternMatch {
            line: 1,
            column: Some(0),
            start_byte: start,
            end_byte: end,
            original_start_byte: start,
            original_end_byte: end,
            pattern: PatternKind::Rust(pattern),
            snippet: needle.to_string(),
            arg_nodes,
            transformability,
        }
    }

    #[test]
    fn test_transform_tcp_listener_to_wasi_socket() {
        let src = "fn main() {\n    let _ = std::net::TcpListener::bind(\"127.0.0.1:80\");\n}\n";
        let m = match_at(
            src,
            "std::net::TcpListener::bind(\"127.0.0.1:80\")",
            RustPattern::TcpBind,
            Transformability::AutoTransformable,
            vec!["\"127.0.0.1:80\"".to_string()],
        );
        let r = RustTransformer::new().transform(src, vec![m]);
        assert_eq!(r.manual_review.len(), 0);
        assert_eq!(r.transformations_applied.len(), 1);
        assert!(r.transformed_source.contains("TcpSocket::new"));
        assert!(r.transformed_source.contains("start_bind"));
        assert!(r.transformed_source.contains("finish_bind"));
        assert!(r.transformed_source.contains("start_listen"));
        assert!(r.transformed_source.contains("finish_listen"));
        assert!(r.transformed_source.contains("\"127.0.0.1:80\""));
    }

    #[test]
    fn test_transform_tcp_stream_connect() {
        let src = "fn main() {\n    let _ = std::net::TcpStream::connect(\"127.0.0.1:9000\");\n}\n";
        let m = match_at(
            src,
            "std::net::TcpStream::connect(\"127.0.0.1:9000\")",
            RustPattern::TcpConnect,
            Transformability::AutoTransformable,
            vec!["\"127.0.0.1:9000\"".to_string()],
        );
        let r = RustTransformer::new().transform(src, vec![m]);
        assert!(r.transformed_source.contains("TcpSocket::new"));
        assert!(r.transformed_source.contains("start_connect"));
        assert!(r.transformed_source.contains("finish_connect"));
    }

    #[test]
    fn test_transform_accept_not_transformable_in_mvp() {
        // #128: TcpAccept was BestEffort (busy-spin poll loop) and is
        // now NotTransformable. The previous emit referenced
        // `std::thread::yield_now` as a placeholder for a real
        // wasi:io pollable subscription that doesn't exist in MVP.
        // The MVP fix: leave the original `listener.accept()` in the
        // source verbatim and report it as manual_review.
        let src = "fn main() {\n    let _ = listener.accept();\n}\n";
        let m = match_at(
            src,
            "listener.accept()",
            RustPattern::TcpAccept,
            Transformability::NotTransformable,
            vec![],
        );
        let r = RustTransformer::new().transform(src, vec![m]);
        // 0 transformations applied — the match was routed to
        // manual_review by the partition.
        assert_eq!(r.transformations_applied.len(), 0);
        assert_eq!(r.manual_review.len(), 1);
        assert_eq!(
            r.manual_review[0].pattern,
            PatternKind::Rust(RustPattern::TcpAccept)
        );
        // Original POSIX call preserved verbatim.
        assert!(
            r.transformed_source.contains("listener.accept()"),
            "original accept() should be preserved; got:\n{}",
            r.transformed_source
        );
        // Broken busy-spin emit is GONE.
        assert!(
            !r.transformed_source.contains("std::thread::yield_now"),
            "the broken busy-spin emit should NOT appear"
        );
        assert!(
            !r.transformed_source.contains("TODO"),
            "the TODO placeholder should NOT appear in the output"
        );
    }

    #[test]
    fn test_transform_udp_bind() {
        let src = "fn main() {\n    let _ = std::net::UdpSocket::bind(\"0.0.0.0:5353\");\n}\n";
        let m = match_at(
            src,
            "std::net::UdpSocket::bind(\"0.0.0.0:5353\")",
            RustPattern::UdpBind,
            Transformability::AutoTransformable,
            vec!["\"0.0.0.0:5353\"".to_string()],
        );
        let r = RustTransformer::new().transform(src, vec![m]);
        assert!(r.transformed_source.contains("UdpSocket::new"));
        assert!(r.transformed_source.contains("start_bind"));
        assert!(r.transformed_source.contains("finish_bind"));
    }

    #[test]
    fn test_transform_udp_connect_not_transformable() {
        let src = "fn main() {\n    let _ = sock.connect(\"127.0.0.1:9000\");\n}\n";
        let m = match_at(
            src,
            "sock.connect(\"127.0.0.1:9000\")",
            RustPattern::UdpConnect,
            Transformability::NotTransformable,
            vec!["\"127.0.0.1:9000\"".to_string()],
        );
        let r = RustTransformer::new().transform(src, vec![m]);
        assert_eq!(r.transformations_applied.len(), 0);
        assert_eq!(r.manual_review.len(), 1);
        // Original source preserved verbatim because nothing was
        // replaced.
        assert!(r.transformed_source.contains("sock.connect"));
    }

    #[test]
    fn test_transform_std_process_exit_not_transformable() {
        let src = "fn main() {\n    std::process::exit(0);\n}\n";
        let m = match_at(
            src,
            "std::process::exit(0)",
            RustPattern::ProcessExit,
            Transformability::NotTransformable,
            vec!["0".to_string()],
        );
        let r = RustTransformer::new().transform(src, vec![m]);
        assert_eq!(r.transformations_applied.len(), 0);
        assert_eq!(r.manual_review.len(), 1);
        assert!(r.transformed_source.contains("std::process::exit"));
    }

    #[test]
    fn test_transform_fs_open_and_write() {
        let src = "fn main() {\n    let _ = std::fs::File::open(\"a.txt\");\n    let _ = std::fs::write(\"b.txt\", b\"x\");\n}\n";
        let open_m = match_at(
            src,
            "std::fs::File::open(\"a.txt\")",
            RustPattern::FsOpen,
            Transformability::AutoTransformable,
            vec!["\"a.txt\"".to_string()],
        );
        let write_m = match_at(
            src,
            "std::fs::write(\"b.txt\", b\"x\")",
            RustPattern::FsWrite,
            Transformability::AutoTransformable,
            vec!["\"b.txt\"".to_string(), "b\"x\"".to_string()],
        );
        let r = RustTransformer::new().transform(src, vec![open_m, write_m]);
        assert_eq!(r.transformations_applied.len(), 2);
        assert!(r.transformed_source.contains("filesystem::open(\"a.txt\")"));
        assert!(r
            .transformed_source
            .contains("filesystem::write(\"b.txt\", b\"x\")"));
    }

    #[test]
    fn test_transform_prepends_wasi_use_statements() {
        let src = "fn main() {\n    let _ = std::net::TcpListener::bind(\"127.0.0.1:80\");\n}\n";
        let m = match_at(
            src,
            "std::net::TcpListener::bind(\"127.0.0.1:80\")",
            RustPattern::TcpBind,
            Transformability::AutoTransformable,
            vec!["\"127.0.0.1:80\"".to_string()],
        );
        let r = RustTransformer::new().transform(src, vec![m]);
        // The prelude must come before any original content.
        let prelude_end = r.transformed_source.find("fn main()").unwrap();
        assert!(r.transformed_source[..prelude_end].contains("use wasi::socket::tcp::TcpSocket;"));
        assert!(r.transformed_source[..prelude_end].contains("use wasi::filesystem;"));
    }

    #[test]
    fn test_transform_emits_use_and_original_content_in_correct_order() {
        // Regression test for the byte-range rewriter: when the
        // match is at offset N in the source, the prelude + the
        // original prefix (bytes 0..N) + the replacement + the
        // original suffix (bytes N+len..EOF) must all appear in that
        // exact order in the output.
        let src = "fn main() {\n    let _ = std::net::TcpListener::bind(\"127.0.0.1:80\");\n    done();\n}\n";
        let m = match_at(
            src,
            "std::net::TcpListener::bind(\"127.0.0.1:80\")",
            RustPattern::TcpBind,
            Transformability::AutoTransformable,
            vec!["\"127.0.0.1:80\"".to_string()],
        );
        let r = RustTransformer::new().transform(src, vec![m]);
        let prelude_idx = r.transformed_source.find("use wasi::").unwrap();
        let prefix_idx = r.transformed_source.find("fn main()").unwrap();
        let replacement_idx = r.transformed_source.find("TcpSocket::new").unwrap();
        let suffix_idx = r.transformed_source.find("done();").unwrap();
        assert!(
            prelude_idx < prefix_idx,
            "prelude {prelude_idx} must come before original prefix {prefix_idx}"
        );
        assert!(
            prefix_idx < replacement_idx,
            "original prefix {prefix_idx} must come before replacement {replacement_idx}"
        );
        assert!(
            replacement_idx < suffix_idx,
            "replacement {replacement_idx} must come before original suffix {suffix_idx}"
        );
    }

    #[test]
    fn test_transform_multiple_matches_byte_range_preserves_order() {
        // Two matches in source order (bind at byte 14, connect at
        // byte 60). The transformer sorts by start_byte descending
        // and must produce a result that, when read left-to-right,
        // has both replacements in their source order (because
        // descending processing flips them back into ascending
        // emission).
        let src = "\
fn main() {
    let _ = std::net::TcpListener::bind(\"a\");
    let _ = std::net::TcpStream::connect(\"b\");
}
";
        let bind_m = match_at(
            src,
            "std::net::TcpListener::bind(\"a\")",
            RustPattern::TcpBind,
            Transformability::AutoTransformable,
            vec!["\"a\"".to_string()],
        );
        let connect_m = match_at(
            src,
            "std::net::TcpStream::connect(\"b\")",
            RustPattern::TcpConnect,
            Transformability::AutoTransformable,
            vec!["\"b\"".to_string()],
        );
        // Feed them in reverse source order to confirm the descending
        // sort handles either input ordering.
        let r = RustTransformer::new().transform(src, vec![connect_m, bind_m]);
        assert_eq!(r.transformations_applied.len(), 2);
        let bind_idx = r.transformed_source.find("start_bind").unwrap();
        let connect_idx = r.transformed_source.find("start_connect").unwrap();
        assert!(
            bind_idx < connect_idx,
            "bind {bind_idx} must precede connect {connect_idx} in output"
        );
    }

    #[test]
    fn test_transform_missing_arg_records_error() {
        // TcpBind without an arg_nodes entry — the transformer should
        // record an error and skip the replacement rather than
        // producing an empty placeholder that won't compile.
        let src = "fn main() { let _ = std::net::TcpListener::bind(); }\n";
        let m = match_at(
            src,
            "std::net::TcpListener::bind()",
            RustPattern::TcpBind,
            Transformability::AutoTransformable,
            vec![], // no args!
        );
        let r = RustTransformer::new().transform(src, vec![m]);
        assert_eq!(r.errors.len(), 1);
        assert_eq!(r.transformations_applied.len(), 0);
        assert!(r.errors[0].message.contains("missing argument"));
    }

    #[test]
    fn test_transform_posix_match_recorded_as_error() {
        // A PosixPattern leaked into the Rust transformer (caller bug).
        // The transformer should record it as an error rather than
        // silently dropping or producing wrong-looking Rust code.
        let src = "fn main() { /* posix leak */ }\n";
        let m = PatternMatch {
            line: 1,
            column: Some(0),
            start_byte: 0,
            end_byte: src.len(),
            original_start_byte: 0,
            original_end_byte: src.len(),
            pattern: PatternKind::Posix(crate::patterns::PosixPattern::Bind),
            snippet: "bind(...)".to_string(),
            arg_nodes: vec!["fd".to_string()],
            transformability: Transformability::AutoTransformable,
        };
        let r = RustTransformer::new().transform(src, vec![m]);
        assert_eq!(r.errors.len(), 1);
        assert!(r.errors[0].message.contains("POSIX"));
    }

    #[test]
    fn test_transform_no_matches_passes_through_with_prelude() {
        let src = "fn main() { println!(\"hi\"); }\n";
        let r = RustTransformer::new().transform(src, vec![]);
        assert_eq!(r.transformations_applied.len(), 0);
        assert_eq!(r.manual_review.len(), 0);
        assert_eq!(r.errors.len(), 0);
        // Prelude is prepended even with no matches.
        assert!(r
            .transformed_source
            .contains("use wasi::socket::tcp::TcpSocket;"));
        // Original content is preserved verbatim.
        assert!(r
            .transformed_source
            .contains("fn main() { println!(\"hi\"); }"));
    }

    #[test]
    fn test_transform_preprocessor_is_none() {
        // Rust has no preprocessor in v1; the field must always be
        // None so the wire format stays clean.
        let src = "fn main() { let _ = std::net::TcpListener::bind(\"a\"); }\n";
        let m = match_at(
            src,
            "std::net::TcpListener::bind(\"a\")",
            RustPattern::TcpBind,
            Transformability::AutoTransformable,
            vec!["\"a\"".to_string()],
        );
        let r = RustTransformer::new().transform(src, vec![m]);
        assert!(r.preprocessor.is_none());
    }
}
