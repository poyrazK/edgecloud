//! Rust source transformer (M3).
//!
//! Rewrites `std::net::*` / `std::fs::*` / `std::process::*` calls into
//! their wit-bindgen-0.45-shaped `crate::wasi::socket::*` /
//! `crate::wasi::filesystem::*` equivalents.
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
//! // <auto-prelude: crate::wasi::* imports + parse_addr_v4 helper>
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
//!
//! ## API target
//!
//! Emitted shapes track the bindings `wit-bindgen = "0.45"` generates
//! against this repo's canonical WIT (see `wit/deps/sockets/*.wit` and
//! `wit/deps/filesystem/*.wit`). Cross-references:
//!
//! - Socket factory functions `create_tcp_socket` / `create_udp_socket`
//!   live in their own interfaces (`tcp-create-socket` /
//!   `udp-create-socket`) and bindgen maps them to submodules under
//!   `crate::wasi::sockets::*`.
//! - `IpAddressFamily` lives in `wasi:network`, **not** `wasi:tcp`.
//! - `start_bind` / `start_connect` take `(network, ip-socket-address)`
//!   rather than a single address string — we feed them through
//!   `parse_addr_v4` (declared in the prelude below) to convert the
//!   `std::net::TcpListener::bind("host:port")` syntax.
//! - Filesystem I/O is always through a `Descriptor` you hold (typically
//!   a preopen at `crate::wasi::filesystem::preopens::get_directories`).

use crate::patterns::{PatternKind, PatternMatch, RustPattern, Transformability};
use crate::transformer::{TransformError, TransformResult, Transformation};

/// WASI Rust prelude prepended to the transformed source.
///
/// Emits `crate::wasi::*` paths directly — the canonical import shape
/// for `wit-bindgen = "0.45"` (verified against
/// `edge-worker/tests/fixtures/handler/src/lib.rs:47-60`). The
/// `parse_addr_v4` helper translates the `std::net::...bind("host:port")`
/// string syntax into the `IpSocketAddress` struct that
/// `start_bind`/`start_connect` require.
///
/// `preopens` is imported as a module (not a specific symbol) so the
/// emit at `Descriptor::open_at` can call `preopens::get_directories()`
/// without an extra `use` per replacement.
const WASI_RUST_PRELUDE: &str = "\
use crate::wasi::sockets::tcp_create_socket::create_tcp_socket;
use crate::wasi::sockets::udp_create_socket::create_udp_socket;
use crate::wasi::sockets::instance_network::instance_network;
use crate::wasi::sockets::network::{
    IpAddressFamily, IpSocketAddress, Ipv4SocketAddress,
};
use crate::wasi::filesystem::types::{
    Descriptor, DescriptorFlags, OpenFlags, PathFlags,
};
use crate::wasi::filesystem::preopens;

fn parse_addr_v4(s: &str) -> IpSocketAddress {
    let (host, port) = s.split_once(':').expect(\"invalid host:port\");
    let mut oct = host.split('.');
    let a: u8 = oct.next().unwrap().parse().unwrap();
    let b: u8 = oct.next().unwrap().parse().unwrap();
    let c: u8 = oct.next().unwrap().parse().unwrap();
    let d: u8 = oct.next().unwrap().parse().unwrap();
    IpSocketAddress::Ipv4(Ipv4SocketAddress {
        port: port.parse().unwrap(),
        address: (a, b, c, d),
    })
}

";

/// Rewrites detected Rust patterns into `crate::wasi::socket::*` /
/// `crate::wasi::filesystem::*` call sites (wit-bindgen 0.45 shape).
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
        // wit-bindgen 0.45 emits free functions (create_tcp_socket,
        // create_udp_socket) under the per-interface submodules
        // `tcp-create-socket` / `udp-create-socket`. The Network
        // handle comes from `instance_network`; the address is
        // converted from a `std::net::...bind("host:port")` literal
        // by the `parse_addr_v4` helper in the prelude.
        //
        // Each emit is a multi-line block of statements (let bindings
        // + ?-chained wasi calls). The transformer's byte-range loop
        // concatenates `generate_wasi_code`'s `String` return into the
        // output buffer verbatim, so multi-line emits work for free.
        RustPattern::TcpBind => format!(
            "let _s = create_tcp_socket(IpAddressFamily::Ipv4)?;\n\
             let _n = instance_network();\n\
             _s.start_bind(&_n, parse_addr_v4({}))?.finish_bind()?.start_listen()?.finish_listen()?;",
            arg(0)?
        ),
        // `finish_connect` returns `(InputStream, OutputStream)` per
        // `wit/deps/sockets/tcp.wit`. Both halves are bound — the rx
        // is needed for downstream reads, the tx for writes; without
        // a binding the values are dropped at the end of the
        // statement and the resource is closed immediately.
        RustPattern::TcpConnect => format!(
            "let _s = create_tcp_socket(IpAddressFamily::Ipv4)?;\n\
             let _n = instance_network();\n\
             let (_rx, _tx) = _s.start_connect(&_n, parse_addr_v4({}))?.finish_connect()?;",
            arg(0)?
        ),
        RustPattern::UdpBind => format!(
            "let _s = create_udp_socket(IpAddressFamily::Ipv4)?;\n\
             let _n = instance_network();\n\
             _s.start_bind(&_n, parse_addr_v4({}))?.finish_bind()?;",
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
        // Filesystem — there is no free `filesystem::open` /
        // `filesystem::read` / `filesystem::write` in wit-bindgen
        // 0.45. I/O always goes through a `Descriptor` you hold
        // (typically a preopen). The transformer acquires preopen 0
        // and calls `Descriptor::open_at(...)` against it.
        //
        // Issue #417 scope: typecheck + load-only. Runtime calls
        // against an empty preopen set fail at runtime; wiring a
        // preopen into the synthesized cargo project (written by
        // MigrationService.compileRustAsComponent) is a follow-up.
        RustPattern::FsOpen => format!(
            "let _preopens = preopens::get_directories();\n\
             let _base = _preopens.get(0).expect(\"no preopens\").0.clone();\n\
             let _d = _base.open_at(PathFlags::empty(), {}, OpenFlags::empty(), DescriptorFlags::READ)?;",
            arg(0)?
        ),
        // `Descriptor::read(length, offset)` takes a length, which
        // the POSIX `std::fs::read(path)` syntax doesn't carry. Emit
        // `.read(0, 0)` as a documented placeholder — a length=0
        // typecheck-clean call. The user lifts the length into the
        // source for a load-bearing call (analogous to how TcpAccept
        // surfaces as `manual_review`).
        RustPattern::FsRead => format!(
            "let _preopens = preopens::get_directories();\n\
             let _base = _preopens.get(0).expect(\"no preopens\").0.clone();\n\
             let _d = _base.open_at(PathFlags::empty(), {}, OpenFlags::empty(), DescriptorFlags::READ)?;\n\
             let (_bytes, _eof) = _d.read(0, 0)?;",
            arg(0)?
        ),
        // `Descriptor::write(buffer, offset)` returns `filesize` and
        // takes a `list<u8>` (i.e. `Vec<u8>`) by value. The
        // `OpenFlags::CREATE | OpenFlags::TRUNCATE` bitflags pattern
        // is bindgen-emitted; `OpenFlags::empty()` is the constant
        // constructor for the empty flag set.
        RustPattern::FsWrite => format!(
            "let _preopens = preopens::get_directories();\n\
             let _base = _preopens.get(0).expect(\"no preopens\").0.clone();\n\
             let _d = _base.open_at(PathFlags::empty(), {}, OpenFlags::CREATE | OpenFlags::TRUNCATE, DescriptorFlags::WRITE)?;\n\
             let _n: u64 = _d.write({}.to_vec(), 0)?;",
            arg(0)?, arg(1)?
        ),
        // `Descriptor` has a bindgen-generated `Drop` impl; plain
        // `drop(self)` works as before.
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
            bound_var: None,
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
        // wit-bindgen 0.45 emits a free `create_tcp_socket` factory
        // instead of the adapter's `TcpSocket::new` constructor.
        assert!(r.transformed_source.contains("create_tcp_socket"));
        assert!(r.transformed_source.contains("IpAddressFamily::Ipv4"));
        assert!(r.transformed_source.contains("instance_network"));
        // Address goes through parse_addr_v4 (prelude helper) and the
        // literal is preserved in the call site.
        assert!(r.transformed_source.contains("parse_addr_v4"));
        assert!(r.transformed_source.contains("\"127.0.0.1:80\""));
        // start_bind/finish_bind/start_listen/finish_listen chain
        // still appears — bindgen 0.45 names them verbatim.
        assert!(r.transformed_source.contains("start_bind"));
        assert!(r.transformed_source.contains("finish_bind"));
        assert!(r.transformed_source.contains("start_listen"));
        assert!(r.transformed_source.contains("finish_listen"));
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
        assert!(r.transformed_source.contains("create_tcp_socket"));
        assert!(r.transformed_source.contains("start_connect"));
        assert!(r.transformed_source.contains("finish_connect"));
        // finish_connect returns (InputStream, OutputStream); both must
        // be bound to keep the resource alive.
        assert!(r.transformed_source.contains("let (_rx, _tx) ="));
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
        // Free create_udp_socket factory — replaces the adapter's
        // UdpSocket::new.
        assert!(r.transformed_source.contains("create_udp_socket"));
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
        // wit-bindgen 0.45: open through preopen[0] via
        // Descriptor::open_at(...). The exact `filesystem::open(...)`
        // substring is GONE — assert against the new emit instead.
        assert!(r.transformed_source.contains("preopens::get_directories"));
        assert!(r
            .transformed_source
            .contains("open_at(PathFlags::empty(), \"a.txt\""));
        // FsWrite needs the CREATE|TRUNCATE flag combo and the
        // DescriptorFlags::WRITE arg — confirm the bitflags pattern
        // (bindgen-emitted `|`) appears.
        assert!(r
            .transformed_source
            .contains("OpenFlags::CREATE | OpenFlags::TRUNCATE"));
        assert!(r.transformed_source.contains("DescriptorFlags::WRITE"));
        assert!(r.transformed_source.contains(".write(b\"x\".to_vec(), 0)"));
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
        // The prelude must come before any original content. The new
        // prelude emits `crate::wasi::*` paths (matching the
        // wit-bindgen 0.45 binding tree) plus the parse_addr_v4
        // helper.
        let prelude_end = r.transformed_source.find("fn main()").unwrap();
        assert!(r.transformed_source[..prelude_end]
            .contains("use crate::wasi::sockets::tcp_create_socket::create_tcp_socket;"));
        assert!(
            r.transformed_source[..prelude_end].contains("use crate::wasi::filesystem::types::")
        );
        assert!(r.transformed_source[..prelude_end].contains("fn parse_addr_v4"));
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
        let prelude_idx = r.transformed_source.find("use crate::wasi::").unwrap();
        let prefix_idx = r.transformed_source.find("fn main()").unwrap();
        // The replacement's first line is `let _s = create_tcp_socket(...)`
        // — pick that landmark so we anchor on the emit, not the
        // prelude's import line (which mentions create_tcp_socket
        // too but appears earlier).
        let replacement_idx = r
            .transformed_source
            .find("let _s = create_tcp_socket")
            .unwrap();
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
        // byte 60). The transformer sorts by start_byte ascending
        // and emits in source order — the multi-line emit blocks
        // for bind/connect must surface in their source order
        // regardless of input ordering.
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
        // Feed them in reverse source order to confirm the sort
        // handles either input ordering.
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
            bound_var: None,
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
        // Prelude is prepended even with no matches. The path
        // shape changed (`use crate::wasi::*` instead of
        // `use wasi::*`) — assert on the new shape.
        assert!(r
            .transformed_source
            .contains("use crate::wasi::sockets::tcp_create_socket::create_tcp_socket;"));
        // Original content is preserved verbatim.
        assert!(r
            .transformed_source
            .contains("fn main() { println!(\"hi\"); }"));
    }

    #[test]
    fn test_transform_prelude_exposes_parse_addr_v4() {
        // The transformer emits the parse_addr_v4 helper inline in
        // the prelude so that emit blocks for socket patterns can
        // route `"host:port"` literals through it (without adding
        // a serde or net parse dep). Pin that the helper is in the
        // output even when no matches apply — it's part of the
        // prelude, which always renders.
        let src = "fn main() { let _ = 1 + 2; }\n";
        let r = RustTransformer::new().transform(src, vec![]);
        assert_eq!(r.transformations_applied.len(), 0);
        assert!(
            r.transformed_source.contains("fn parse_addr_v4"),
            "prelude helper missing from:\n{}",
            r.transformed_source
        );
        assert!(r.transformed_source.contains("IpSocketAddress::Ipv4"));
        assert!(r.transformed_source.contains("Ipv4SocketAddress"));
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
