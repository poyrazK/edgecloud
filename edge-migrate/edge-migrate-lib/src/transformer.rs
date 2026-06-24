//! POSIX → WASI transformation.
//!
//! Transforms detected POSIX patterns to WASI equivalents,
//! generating transformed source code and a transformation report.

use crate::patterns::{PatternKind, PatternMatch, PosixPattern, Transformability};
use crate::preprocessor::PreprocessorInfo;
use serde::{Deserialize, Serialize};

/// A single transformation applied to the source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transformation {
    /// 1-based line number where the transformation was applied.
    pub line: usize,
    /// The pattern that was transformed (M3: `PatternKind`, was `PosixPattern`).
    pub pattern: PatternKind,
    /// A description of what was changed.
    pub description: String,
}

/// An error that occurred during transformation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformError {
    /// 1-based line number where the error occurred.
    pub line: usize,
    /// The pattern that failed (M3: `PatternKind`, was `PosixPattern`).
    pub pattern: PatternKind,
    /// Human-readable error message.
    pub message: String,
}

/// The result of a transformation operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformResult {
    /// The WASI-compatible C source code.
    pub transformed_source: String,
    /// All transformations that were applied.
    pub transformations_applied: Vec<Transformation>,
    /// Patterns that require manual review.
    pub manual_review: Vec<PatternMatch>,
    /// Errors that occurred during transformation.
    pub errors: Vec<TransformError>,
    /// Preprocessor metadata, when a preprocessor was used during
    /// analysis. `None` when no preprocessor was attached, when the
    /// preprocessor was not discovered, or when the analyzer fell
    /// back to the unexpanded source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preprocessor: Option<PreprocessorInfo>,
}

/// WASI header includes to prepend to the transformed source.
const WASI_INCLUDES: &str = r#"#include <wasi/sockets.h>
#include <wasi/io/streams.h>
#include <wasi/filesystem.h>

"#;

/// Transforms POSIX C source to WASI-compatible C source.
pub struct Transformer;

impl Transformer {
    /// Transform the given C source based on detected pattern matches.
    ///
    /// Processes matches from lowest to highest byte position (start of
    /// file forward), building the output by appending for each match
    /// (original content in the gap, WASI replacement). The gap is the
    /// content BETWEEN the previous match's end and the current match's
    /// start in original source coordinates. After all matches, append
    /// any remaining original content from the last match's end to EOF.
    ///
    /// `preprocessor_info` is attached to the result so callers (the CLI
    /// bin, the control plane) can report "Preprocessor: expanded N macros"
    /// back to the user. Pass `None` when no preprocessor was used.
    pub fn transform(
        source: &str,
        matches: Vec<PatternMatch>,
        preprocessor_info: Option<PreprocessorInfo>,
    ) -> TransformResult {
        let mut transformations_applied = Vec::new();
        let mut manual_review = Vec::new();

        // Partition into transformable and not-transformable
        let (transformable, not_transformable): (Vec<_>, Vec<_>) = matches
            .into_iter()
            .partition(|m| m.transformability != Transformability::NotTransformable);

        manual_review.extend(not_transformable);

        // Sort by ORIGINAL start_byte ASCENDING — process from start of
        // file forward. We use `original_start_byte` (not `start_byte`)
        // because `start_byte` is in expanded-source coordinates when a
        // preprocessor is attached; slicing the ORIGINAL source with
        // expanded bytes overflows the source length and panics.
        // `original_start_byte` equals `start_byte` when no preprocessor
        // is attached (identity mapping), so the no-preprocessor path is
        // unchanged.
        let mut sorted = transformable;
        sorted.sort_by_key(|m| m.original_start_byte);

        let source_bytes = source.as_bytes();

        // Build output: WASI header + for each match (gap content + WASI replacement)
        let mut output = WASI_INCLUDES.as_bytes().to_vec();

        // prev_end tracks the END of the previous match in ORIGINAL
        // coordinates. Start at 0 (beginning of file in original).
        let mut prev_end: usize = 0;

        for m in &sorted {
            let wasi_code = Self::generate_wasi_code(m);
            if wasi_code.is_empty() {
                continue;
            }

            // For socket calls bound to a declared variable
            // (`int fd = socket(...)`), the analyzer captured the
            // surrounding declaration's byte range so we can rewrite
            // the whole `int fd = socket(...)` line — replacing the
            // stale `int` type with `wasi_socket_tcp_t *` to match
            // the WASI create() return type. Otherwise replace just
            // the call itself. All byte ranges here are in ORIGINAL
            // source coordinates (post-remap through the preprocessor).
            let (orig_start, orig_end) = match &m.bound_var {
                Some(bv)
                    if matches!(
                        m.pattern,
                        PatternKind::Posix(PosixPattern::SocketTcp | PosixPattern::SocketUdp)
                    ) =>
                {
                    (bv.original_decl_start_byte, bv.original_decl_end_byte)
                }
                _ => (m.original_start_byte, m.original_end_byte),
            };

            // Sanity guard for synthetic-line matches (byte_map entry
            // was `u32::MAX`, couldn't be remapped). Such matches are
            // best-effort and should not be sliced — route to
            // manual_review instead. Without this guard, orig_end
            // would be `usize::MAX` (overflow) or some out-of-range
            // value, and the `extend_from_slice` below would panic.
            if orig_end > source_bytes.len()
                || orig_start > orig_end
                || orig_start == usize::MAX
                || orig_end == usize::MAX
            {
                tracing::warn!(
                    "skipping match on synthetic line (orig_start={}, orig_end={}, source_len={}); routing to manual_review",
                    orig_start,
                    orig_end,
                    source_bytes.len()
                );
                manual_review.push(m.clone());
                continue;
            }

            // Copy original content from prev_end to orig_start — the
            // gap between the previous match's end and this match's
            // start in original coordinates.
            output.extend_from_slice(&source_bytes[prev_end..orig_start]);

            // Append WASI replacement
            output.extend_from_slice(wasi_code.as_bytes());

            // Update: this match's end becomes the boundary for the next iteration.
            prev_end = orig_end;

            transformations_applied.push(Transformation {
                line: m.line,
                pattern: m.pattern,
                description: format!(
                    "Transformed {} → {}",
                    m.snippet.split('(').next().unwrap_or(&m.snippet),
                    m.pattern.wasi_equivalent()
                ),
            });
        }

        // After all matches: append remaining original content from prev_end to EOF.
        if prev_end < source_bytes.len() {
            output.extend_from_slice(&source_bytes[prev_end..]);
        }

        let transformed_source =
            String::from_utf8(output).expect("Transformed source is not valid UTF-8");

        TransformResult {
            transformed_source,
            transformations_applied,
            manual_review,
            errors: Vec::new(),
            preprocessor: preprocessor_info,
        }
    }

    /// Generate WASI C code for a pattern match.
    ///
    /// M3 added `PatternKind` (a sum type with `Posix(...)` and
    /// `Rust(...)` variants). The C `Transformer` only handles the
    /// `Posix` arm; `Rust` matches are caught by the `_ => String::new()`
    /// fallback (they have no place in C source). M3.C4 introduces
    /// `RustTransformer` for the Rust path.
    fn generate_wasi_code(m: &PatternMatch) -> String {
        match &m.pattern {
            PatternKind::Posix(PosixPattern::SocketTcp) => {
                // When the call was bound to a declared variable
                // (`int fd = socket(...)`), emit a full declaration
                // with the correct WASI return type so the variable
                // stays in scope for the bind/listen/close lines.
                // For bare-expression socket calls (e.g.
                // `socket(AF_INET, SOCK_STREAM, 0);` as a standalone
                // statement) keep the existing bare-expression form.
                if let Some(bv) = &m.bound_var {
                    format!(
                        "wasi_socket_tcp_t *{} = wasi_socket_tcp_create(IP_ADDRESS_FAMILY_IPV4);",
                        bv.name
                    )
                } else {
                    "wasi_socket_tcp_create(IP_ADDRESS_FAMILY_IPV4)".to_string()
                }
            }
            PatternKind::Posix(PosixPattern::SocketUdp) => {
                if let Some(bv) = &m.bound_var {
                    format!(
                        "wasi_socket_udp_t *{} = wasi_socket_udp_create(IP_ADDRESS_FAMILY_IPV4);",
                        bv.name
                    )
                } else {
                    "wasi_socket_udp_create(IP_ADDRESS_FAMILY_IPV4)".to_string()
                }
            }
            PatternKind::Posix(PosixPattern::Bind) => {
                // Two-phase: start-bind + finish-bind.
                // `wasi_socket_tcp_finish_bind` takes ONLY the socket fd
                // (no address, no length). The previous version passed
                // `extract_third_arg(m)` here — which for
                // `bind(fd, addr, len)` is `len` (e.g. `sizeof(addr)`) —
                // producing a call like
                // `wasi_socket_tcp_finish_bind(sizeof(addr))` that the
                // clang syntax check rejects.
                format!(
                    "// WASI: two-phase bind\n{{\n  wasi_socket_tcp_start_bind({}, {});\n  wasi_socket_tcp_finish_bind({});\n}}",
                    Self::extract_first_arg(m),
                    Self::extract_second_arg(m),
                    Self::extract_first_arg(m)
                )
            }
            PatternKind::Posix(PosixPattern::Listen) => {
                // Two-phase: start-listen + finish-listen
                // listen(fd, backlog) — arg0=fd(socket), arg1=backlog
                format!(
                    "// WASI: two-phase listen\n{{\n  wasi_socket_tcp_start_listen({}, {});\n  wasi_socket_tcp_finish_listen({});\n}}",
                    Self::extract_first_arg(m),  // socket fd
                    Self::extract_second_arg(m), // backlog
                    Self::extract_first_arg(m)   // socket fd again
                )
            }
            PatternKind::Posix(PosixPattern::Connect) => {
                // Two-phase: start-connect + finish-connect.
                // `wasi_socket_tcp_finish_connect` takes ONLY the socket
                // fd, not the address. Same bug as Bind — fix passes
                // the first arg again instead of the third.
                format!(
                    "// WASI: two-phase connect\n{{\n  wasi_socket_tcp_start_connect({}, {});\n  wasi_socket_tcp_finish_connect({});\n}}",
                    Self::extract_first_arg(m),
                    Self::extract_second_arg(m),
                    Self::extract_first_arg(m)
                )
            }
            PatternKind::Posix(PosixPattern::Accept) => {
                // #128: downgraded to NotTransformable. The previous
                // poll-loop wrapper referenced an undeclared
                // `pollable` (no wasi-sdk subscription API in MVP) and
                // produced a brace-wrapped block that didn't fit the
                // `int client = accept(...)` shape. Accept is left in
                // the source verbatim and routed to manual_review by
                // the partition in `transform()` (line 84-86). Nothing
                // to emit here — the partition catches it before this
                // match is iterated.
                String::new()
            }
            PatternKind::Posix(PosixPattern::Recv) => {
                format!(
                    "wasi_input_stream_read({}, {}, {})",
                    Self::extract_first_arg(m),
                    Self::extract_second_arg(m),
                    Self::extract_third_arg(m)
                )
            }
            PatternKind::Posix(PosixPattern::Send) => {
                format!(
                    "wasi_output_stream_write({}, {}, {})",
                    Self::extract_first_arg(m),
                    Self::extract_second_arg(m),
                    Self::extract_third_arg(m)
                )
            }
            PatternKind::Posix(PosixPattern::GetHostByName) => {
                // G3: downgraded to NotTransformable. The previous
                // emit produced `wasi_ip_name_lookup_resolve(host)`
                // but the runtime's `edge:cloud/networking.resolve`
                // returns `list<string>`, not the
                // `wasi:ip-name-lookup.resolve-address` resource
                // stream shape. Keep the call verbatim in source;
                // partition in `transform()` routes it to
                // manual_review before this arm is reached.
                String::new()
            }
            PatternKind::Posix(PosixPattern::Close) => {
                format!("wasi_socket_close({})", Self::extract_first_arg(m))
            }
            PatternKind::Posix(PosixPattern::Fopen) => {
                format!(
                    "wasi_filesystem_open({}, {})",
                    Self::extract_first_arg(m),
                    Self::extract_second_arg(m)
                )
            }
            PatternKind::Posix(PosixPattern::Fread) => {
                format!(
                    "wasi_filesystem_read({}, {}, {})",
                    Self::extract_first_arg(m),
                    Self::extract_second_arg(m),
                    Self::extract_third_arg(m)
                )
            }
            PatternKind::Posix(PosixPattern::Fwrite) => {
                format!(
                    "wasi_filesystem_write({}, {}, {})",
                    Self::extract_first_arg(m),
                    Self::extract_second_arg(m),
                    Self::extract_third_arg(m)
                )
            }
            PatternKind::Posix(PosixPattern::Fclose) => {
                format!("wasi_filesystem_close({})", Self::extract_first_arg(m))
            }
            // Rust variants (handled by RustTransformer in M3.C4) and
            // NotTransformable Posix patterns (should not reach here)
            // are caught by the catch-all and emit an empty string.
            _ => String::new(),
        }
    }

    /// Extract the first argument from a call via arg_nodes (avoids comma-re-parsing bugs).
    fn extract_first_arg(m: &PatternMatch) -> String {
        m.arg_nodes
            .first()
            .cloned()
            .unwrap_or_else(|| "/* unknown */".to_string())
    }

    /// Extract the second argument from a call via arg_nodes.
    fn extract_second_arg(m: &PatternMatch) -> String {
        m.arg_nodes
            .get(1)
            .cloned()
            .unwrap_or_else(|| "/* unknown */".to_string())
    }

    /// Extract the third argument from a call via arg_nodes.
    fn extract_third_arg(m: &PatternMatch) -> String {
        m.arg_nodes
            .get(2)
            .cloned()
            .unwrap_or_else(|| "/* unknown */".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transform_tcp_socket() {
        let source = r#"
int main() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    return 0;
}
"#;
        let call_start = source.find("socket(").unwrap();
        let call_end = source.find("0);").unwrap() + 3; // include "0);"
        let matches = vec![PatternMatch {
            line: 3,
            column: None,
            start_byte: call_start,
            end_byte: call_end,
            original_start_byte: call_start,
            original_end_byte: call_end,
            pattern: PatternKind::Posix(PosixPattern::SocketTcp),
            snippet: "socket(AF_INET, SOCK_STREAM, 0)".to_string(),
            arg_nodes: vec![
                "AF_INET".to_string(),
                "SOCK_STREAM".to_string(),
                "0".to_string(),
            ],
            transformability: Transformability::AutoTransformable,
            bound_var: None,
        }];
        let result = Transformer::transform(source, matches, None);
        assert_eq!(result.transformations_applied.len(), 1);
        assert_eq!(result.manual_review.len(), 0);
        assert!(result.transformed_source.contains("wasi_socket_tcp_create"));
    }

    #[test]
    fn test_transform_poll_not_transformable() {
        let source = r#"
int main() {
    poll(fds, 2, timeout);
    return 0;
}
"#;
        let call_start = source.find("poll(").unwrap();
        let call_end = source.find("timeout);").unwrap() + 9; // include "timeout);"
        let matches = vec![PatternMatch {
            line: 3,
            column: None,
            start_byte: call_start,
            end_byte: call_end,
            original_start_byte: call_start,
            original_end_byte: call_end,
            pattern: PatternKind::Posix(PosixPattern::Poll),
            snippet: "poll(fds, 2, timeout)".to_string(),
            arg_nodes: vec!["fds".to_string(), "2".to_string(), "timeout".to_string()],
            transformability: Transformability::NotTransformable,
            bound_var: None,
        }];
        let result = Transformer::transform(source, matches, None);
        assert_eq!(result.transformations_applied.len(), 0);
        assert_eq!(result.manual_review.len(), 1);
    }

    #[test]
    fn test_transform_bind_two_phase() {
        let source = r#"
int main() {
    bind(fd, (struct sockaddr*)&addr, sizeof(addr));
    return 0;
}
"#;
        let call_start = source.find("bind(").unwrap();
        let call_end = source.find("addr));").unwrap() + 5; // include "addr));"
        let matches = vec![PatternMatch {
            line: 3,
            column: None,
            start_byte: call_start,
            end_byte: call_end,
            original_start_byte: call_start,
            original_end_byte: call_end,
            pattern: PatternKind::Posix(PosixPattern::Bind),
            snippet: "bind(fd, (struct sockaddr*)&addr, sizeof(addr))".to_string(),
            arg_nodes: vec![
                "fd".to_string(),
                "(struct sockaddr*)&addr".to_string(),
                "sizeof(addr)".to_string(),
            ],
            transformability: Transformability::AutoTransformable,
            bound_var: None,
        }];
        let result = Transformer::transform(source, matches, None);
        assert_eq!(result.transformations_applied.len(), 1);
        assert!(result
            .transformed_source
            .contains("wasi_socket_tcp_start_bind"));
        assert!(result
            .transformed_source
            .contains("wasi_socket_tcp_finish_bind"));
    }

    #[test]
    fn test_transform_connect_two_phase() {
        let source = r#"
int main() {
    connect(fd, (struct sockaddr*)&addr, sizeof(addr));
    return 0;
}
"#;
        let call_start = source.find("connect(").unwrap();
        let call_end = source.find("addr));").unwrap() + 5; // include "addr));"
        let matches = vec![PatternMatch {
            line: 3,
            column: None,
            start_byte: call_start,
            end_byte: call_end,
            original_start_byte: call_start,
            original_end_byte: call_end,
            pattern: PatternKind::Posix(PosixPattern::Connect),
            snippet: "connect(fd, (struct sockaddr*)&addr, sizeof(addr))".to_string(),
            arg_nodes: vec![
                "fd".to_string(),
                "(struct sockaddr*)&addr".to_string(),
                "sizeof(addr)".to_string(),
            ],
            transformability: Transformability::AutoTransformable,
            bound_var: None,
        }];
        let result = Transformer::transform(source, matches, None);
        assert_eq!(result.transformations_applied.len(), 1);
        assert!(result
            .transformed_source
            .contains("wasi_socket_tcp_start_connect"));
        assert!(result
            .transformed_source
            .contains("wasi_socket_tcp_finish_connect"));
    }

    #[test]
    fn test_transform_recv() {
        let source = r#"
int main() {
    recv(fd, buf, len, 0);
    return 0;
}
"#;
        let call_start = source.find("recv(").unwrap();
        let call_end = source.find("0);").unwrap() + 3; // include "0);"
        let matches = vec![PatternMatch {
            line: 3,
            column: None,
            start_byte: call_start,
            end_byte: call_end,
            original_start_byte: call_start,
            original_end_byte: call_end,
            pattern: PatternKind::Posix(PosixPattern::Recv),
            snippet: "recv(fd, buf, len, 0)".to_string(),
            arg_nodes: vec![
                "fd".to_string(),
                "buf".to_string(),
                "len".to_string(),
                "0".to_string(),
            ],
            transformability: Transformability::AutoTransformable,
            bound_var: None,
        }];
        let result = Transformer::transform(source, matches, None);
        assert_eq!(result.transformations_applied.len(), 1);
        assert!(result.transformed_source.contains("wasi_input_stream_read"));
    }

    #[test]
    fn test_transform_send() {
        let source = r#"
int main() {
    send(fd, buf, len, 0);
    return 0;
}
"#;
        let call_start = source.find("send(").unwrap();
        let call_end = source.find("0);").unwrap() + 3; // include "0);"
        let matches = vec![PatternMatch {
            line: 3,
            column: None,
            start_byte: call_start,
            end_byte: call_end,
            original_start_byte: call_start,
            original_end_byte: call_end,
            pattern: PatternKind::Posix(PosixPattern::Send),
            snippet: "send(fd, buf, len, 0)".to_string(),
            arg_nodes: vec![
                "fd".to_string(),
                "buf".to_string(),
                "len".to_string(),
                "0".to_string(),
            ],
            transformability: Transformability::AutoTransformable,
            bound_var: None,
        }];
        let result = Transformer::transform(source, matches, None);
        assert_eq!(result.transformations_applied.len(), 1);
        assert!(result
            .transformed_source
            .contains("wasi_output_stream_write"));
    }

    #[test]
    fn test_transform_accept_not_transformable_in_mvp() {
        // #128: Accept was downgraded from BestEffort to
        // NotTransformable. The previous emit wrapped the call in a
        // `wasi_poll_pollable_block(pollable)` loop that referenced
        // an undeclared `pollable` (no wasi-sdk subscription API in
        // MVP) and produced a brace-wrapped block that didn't fit
        // the `int client = accept(...)` shape. The MVP fix: leave
        // the original accept() in the source verbatim and report
        // it as manual_review so the developer sees the call needs
        // attention, rather than getting silently broken code.
        let source = r#"
int main() {
    accept(fd, NULL, NULL);
    return 0;
}
"#;
        let call_start = source.find("accept(").unwrap();
        let call_end = source.find("NULL);").unwrap() + 6; // include "NULL);"
        let matches = vec![PatternMatch {
            line: 3,
            column: None,
            start_byte: call_start,
            end_byte: call_end,
            original_start_byte: call_start,
            original_end_byte: call_end,
            pattern: PatternKind::Posix(PosixPattern::Accept),
            snippet: "accept(fd, NULL, NULL)".to_string(),
            arg_nodes: vec!["fd".to_string(), "NULL".to_string(), "NULL".to_string()],
            transformability: Transformability::NotTransformable,
            bound_var: None,
        }];
        let result = Transformer::transform(source, matches, None);
        // 0 transformations applied — the match was routed to
        // manual_review by the partition in `transform()`.
        assert_eq!(result.transformations_applied.len(), 0);
        // 1 manual_review entry — the developer sees the call.
        assert_eq!(result.manual_review.len(), 1);
        assert_eq!(
            result.manual_review[0].pattern,
            PatternKind::Posix(PosixPattern::Accept)
        );
        // The original POSIX accept() is preserved verbatim — no
        // broken emit, no undeclared `pollable` reference, no
        // brace-wrapped block.
        assert!(
            result.transformed_source.contains("accept(fd, NULL, NULL)"),
            "original accept() should be preserved verbatim; got:\n{}",
            result.transformed_source
        );
        // And the broken-poll-loop emit is GONE.
        assert!(
            !result
                .transformed_source
                .contains("wasi_poll_pollable_block"),
            "the broken poll-loop wrapper should NOT appear in the output"
        );
        assert!(
            !result.transformed_source.contains("wasi_socket_tcp_accept"),
            "no wasi_socket_tcp_accept emit should be present (call is not transformable in MVP)"
        );
    }

    #[test]
    fn test_extract_first_arg() {
        let m = PatternMatch {
            line: 0,
            column: None,
            start_byte: 0,
            end_byte: 0,
            original_start_byte: 0,
            original_end_byte: 0,
            pattern: PatternKind::Posix(PosixPattern::SocketTcp),
            snippet: "socket(AF_INET, SOCK_STREAM, 0)".to_string(),
            arg_nodes: vec![
                "AF_INET".to_string(),
                "SOCK_STREAM".to_string(),
                "0".to_string(),
            ],
            transformability: Transformability::AutoTransformable,
            bound_var: None,
        };
        assert_eq!(Transformer::extract_first_arg(&m), "AF_INET");

        let m = PatternMatch {
            line: 0,
            column: None,
            start_byte: 0,
            end_byte: 0,
            original_start_byte: 0,
            original_end_byte: 0,
            pattern: PatternKind::Posix(PosixPattern::Bind),
            snippet: "bind(fd, &addr, len)".to_string(),
            arg_nodes: vec!["fd".to_string(), "&addr".to_string(), "len".to_string()],
            transformability: Transformability::AutoTransformable,
            bound_var: None,
        };
        assert_eq!(Transformer::extract_first_arg(&m), "fd");
    }

    #[test]
    fn test_extract_second_arg() {
        let m = PatternMatch {
            line: 0,
            column: None,
            start_byte: 0,
            end_byte: 0,
            original_start_byte: 0,
            original_end_byte: 0,
            pattern: PatternKind::Posix(PosixPattern::Bind),
            snippet: "bind(fd, &addr, len)".to_string(),
            arg_nodes: vec!["fd".to_string(), "&addr".to_string(), "len".to_string()],
            transformability: Transformability::AutoTransformable,
            bound_var: None,
        };
        assert_eq!(Transformer::extract_second_arg(&m), "&addr");

        let m = PatternMatch {
            line: 0,
            column: None,
            start_byte: 0,
            end_byte: 0,
            original_start_byte: 0,
            original_end_byte: 0,
            pattern: PatternKind::Posix(PosixPattern::Listen),
            snippet: "listen(fd, 128)".to_string(),
            arg_nodes: vec!["fd".to_string(), "128".to_string()],
            transformability: Transformability::AutoTransformable,
            bound_var: None,
        };
        assert_eq!(Transformer::extract_second_arg(&m), "128");
    }

    /// Verifies that string literals with commas are NOT split on the comma.
    #[test]
    fn test_extract_arg_with_comma_in_string_literal() {
        let m = PatternMatch {
            line: 0,
            column: None,
            start_byte: 0,
            end_byte: 0,
            original_start_byte: 0,
            original_end_byte: 0,
            pattern: PatternKind::Posix(PosixPattern::Fopen),
            snippet: r#"fopen("foo,bar", "r")"#.to_string(),
            arg_nodes: vec![r#"foo,bar"#.to_string(), r#""r""#.to_string()],
            transformability: Transformability::AutoTransformable,
            bound_var: None,
        };
        // extract_first_arg must return "foo,bar" (with the comma inside the string)
        assert_eq!(Transformer::extract_first_arg(&m), r#"foo,bar"#);
        assert_eq!(Transformer::extract_second_arg(&m), r#""r""#);
    }

    /// Regression test for #129: when `socket(...)` is the
    /// initializer of a C `declaration` (e.g. `int fd =
    /// socket(...)`), the transformer must rewrite the WHOLE
    /// declaration — replacing the stale `int` type with the
    /// correct WASI return type (`wasi_socket_tcp_t *`) — so the
    /// `fd` references in downstream bind/listen/close calls stay
    /// valid C.
    #[test]
    fn test_transform_socket_preserves_declared_fd_binding() {
        let source = "\
int main() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    bind(fd, &addr, sizeof(addr));
    listen(fd, 128);
    return 0;
}
";
        let mut analyzer = crate::analyzer::CAnalyzer::new();
        let matches = analyzer.analyze(source);
        let result = Transformer::transform(source, matches, None);

        // The whole `int fd = socket(...)` line is rewritten to
        // `wasi_socket_tcp_t *fd = wasi_socket_tcp_create(...)` —
        // both the type AND the trailing `;` are replaced. This is
        // the fix for #129.
        assert!(
            result.transformed_source.contains(
                "wasi_socket_tcp_t *fd = wasi_socket_tcp_create(IP_ADDRESS_FAMILY_IPV4);"
            ),
            "expected full declaration rewrite; got:\n{}",
            result.transformed_source
        );
        // The stale `int fd = socket(...)` line is gone.
        assert!(
            !result.transformed_source.contains("int fd = socket"),
            "stale int fd = socket declaration still present"
        );
        // Downstream `fd` references still resolve to the same
        // variable name (we didn't rename it).
        assert!(result
            .transformed_source
            .contains("wasi_socket_tcp_start_bind(fd,"));
        assert!(result
            .transformed_source
            .contains("wasi_socket_tcp_start_listen(fd, 128)"));
        // No manual_review entry — this case is in scope for the MVP fix.
        assert_eq!(
            result.manual_review.len(),
            0,
            "expected 0 manual_review entries, got {}",
            result.manual_review.len()
        );
    }

    /// When `socket(...)` appears as a bare expression statement
    /// (not the initializer of a declaration), there's no `fd`
    /// binding to preserve. The transformer emits the WASI call
    /// alone, without the bare expression's leading whitespace or
    /// trailing `;` (those are spliced from the original gap).
    #[test]
    fn test_transform_socket_bare_expression() {
        let source = "int main() { socket(AF_INET, SOCK_STREAM, 0); return 0; }\n";
        let mut analyzer = crate::analyzer::CAnalyzer::new();
        let matches = analyzer.analyze(source);
        let result = Transformer::transform(source, matches, None);

        // Bare-expression emit: just the call, no `wasi_socket_tcp_t *` prefix.
        assert!(result
            .transformed_source
            .contains("wasi_socket_tcp_create(IP_ADDRESS_FAMILY_IPV4)"));
        // The original bare-expression's `socket(...)` is replaced.
        assert!(
            !result.transformed_source.contains("socket(AF_INET"),
            "stale socket() bare expression still present"
        );
        // Critically: no type was added — the bare expression form is preserved.
        assert!(
            !result.transformed_source.contains("wasi_socket_tcp_t *"),
            "bare expression should not get a type prefix"
        );
    }

    /// `static int fd = socket(...)` (or any other declaration
    /// with a storage-class / type qualifier on the declarator)
    /// must NOT be silently rewritten — dropping the `static`
    /// would change semantics. The MVP fix conservatively routes
    /// these to `manual_review` so the developer sees the call
    /// needs attention rather than getting subtly-wrong code.
    #[test]
    fn test_transform_socket_weird_initializer_marks_manual_review() {
        let source = "int main() { static int fd = socket(AF_INET, SOCK_STREAM, 0); return 0; }\n";
        let mut analyzer = crate::analyzer::CAnalyzer::new();
        let matches = analyzer.analyze(source);
        let result = Transformer::transform(source, matches, None);

        // No WASI emit for the socket call — the bound_var detection
        // skipped this case (saw `storage_class_specifier` in the
        // parent declaration).
        assert!(
            !result.transformed_source.contains("wasi_socket_tcp_create"),
            "weird initializer should not emit a wasi_socket_tcp_create call"
        );
        // The original POSIX call is preserved verbatim.
        assert!(
            result
                .transformed_source
                .contains("static int fd = socket(AF_INET, SOCK_STREAM, 0)"),
            "original static initializer should be preserved verbatim; got:\n{}",
            result.transformed_source
        );
        // And it shows up in manual_review so the developer knows.
        assert_eq!(
            result.manual_review.len(),
            1,
            "expected 1 manual_review entry, got {}",
            result.manual_review.len()
        );
        assert!(matches!(
            result.manual_review[0].pattern,
            PatternKind::Posix(PosixPattern::SocketTcp)
        ));
    }

    /// `socket(...)` as the argument of an outer function call
    /// (e.g. `int fd = wrap(socket(AF_INET, SOCK_STREAM, 0));`)
    /// cannot be safely rewritten — the bare-expression emit form
    /// leaves the surrounding `int fd = ...` with a stale `int`
    /// type. Routes to `manual_review` with the original source
    /// preserved verbatim. This is the follow-up to #129's fd-binding
    /// fix: same class of bug (stale int type), different syntactic
    /// shape.
    #[test]
    fn test_transform_socket_inside_outer_call_preserved_verbatim() {
        let source = "int main() { int fd = wrap(socket(AF_INET, SOCK_STREAM, 0)); return 0; }\n";
        let mut analyzer = crate::analyzer::CAnalyzer::new();
        let matches = analyzer.analyze(source);
        let result = Transformer::transform(source, matches, None);

        // No WASI emit — the classifier detected the parent `arguments`
        // node and flipped the match to NotTransformable.
        assert!(
            !result.transformed_source.contains("wasi_socket_tcp_create"),
            "outer-call socket should not emit a wasi_socket_tcp_create call; got:\n{}",
            result.transformed_source
        );
        // The original POSIX call is preserved verbatim inside its
        // enclosing call.
        assert!(
            result
                .transformed_source
                .contains("wrap(socket(AF_INET, SOCK_STREAM, 0))"),
            "original outer-call socket should be preserved verbatim; got:\n{}",
            result.transformed_source
        );
        // And it shows up in manual_review so the developer knows.
        assert_eq!(
            result.manual_review.len(),
            1,
            "expected 1 manual_review entry, got {}",
            result.manual_review.len()
        );
        assert!(matches!(
            result.manual_review[0].pattern,
            PatternKind::Posix(PosixPattern::SocketTcp)
        ));
    }

    /// Integration test: verify that a full socket sequence transforms to valid C
    /// (at minimum — parses as correct syntax). Runs clang -fsyntax-only if available.
    #[test]
    fn test_transform_socket_sequence_valid_c() {
        let source = r#"
#include <stdio.h>
int main() {
    struct sockaddr_in addr;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    bind(fd, (struct sockaddr*)&addr, sizeof(addr));
    listen(fd, 128);
    int client = accept(fd, NULL, NULL);
    return 0;
}
"#;
        let mut analyzer = crate::analyzer::CAnalyzer::new();
        let matches = analyzer.analyze(source);
        let result = Transformer::transform(source, matches, None);

        // Smoke checks: key WASI markers must be present
        assert!(result.transformed_source.contains("wasi_socket_tcp_create"));
        assert!(result
            .transformed_source
            .contains("wasi_socket_tcp_start_bind"));
        assert!(result
            .transformed_source
            .contains("wasi_socket_tcp_finish_bind"));
        assert!(result
            .transformed_source
            .contains("wasi_socket_tcp_start_listen"));
        // #128: accept is NOT transformed in MVP — no
        // `wasi_socket_tcp_accept` emit. The original accept() call
        // is preserved verbatim (asserted below).

        // Original POSIX calls must NOT be present. The substrings
        // here include the FULL original arg list so they don't
        // accidentally match the new WASI emits (which now contain
        // `wasi_socket_tcp_start_bind(fd, (struct sockaddr*)&addr)` —
        // a superset of the old `bind(fd, ...)` substring).
        assert!(
            !result.transformed_source.contains("socket(AF_INET"),
            "socket(AF_INET still present"
        );
        assert!(
            !result
                .transformed_source
                .contains("bind(fd, (struct sockaddr*)&addr, sizeof(addr))"),
            "bind original still present"
        );
        assert!(
            !result.transformed_source.contains("    listen("),
            "listen original still present (note: 4-space indent \
             distinguishes the original call from the wasi emit, which \
             starts with `    // WASI: two-phase listen`)"
        );
        // #128: accept is NOT transformed in MVP — the original
        // `accept(fd, NULL, NULL)` MUST be present in the output
        // (verbatim, since it's routed to manual_review). Also assert
        // it landed in manual_review so the developer sees it.
        assert!(
            result.transformed_source.contains("accept(fd, NULL, NULL)"),
            "accept original must be preserved verbatim (downgraded to NotTransformable)"
        );
        assert!(
            result
                .manual_review
                .iter()
                .any(|m| matches!(m.pattern, PatternKind::Posix(PosixPattern::Accept))),
            "accept should be in manual_review"
        );

        // If clang is available AND EDGE_TEST_CLANG is set, verify valid C syntax.
        // Gated the same way as the dedicated e2e regression net test
        // (`test_transform_e2e_wasi_stubs_compile` below). Uses
        // `tempfile::NamedTempFile` so the file is auto-cleaned on
        // drop (and on panic in the assert), and `-I` + `-include` so
        // the transformer's emitted `#include <wasi/*.h>` directives
        // and the `wasi_stubs.h` POSIX stubs resolve.
        if clang_available() {
            let mut tmp = tempfile::Builder::new()
                .suffix(".c")
                .tempfile()
                .expect("create temp file");
            std::io::Write::write_all(&mut tmp, result.transformed_source.as_bytes())
                .expect("write transformed source");
            let testdata_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .unwrap()
                .join("testdata");
            let include_flag = format!("-I{}", testdata_dir.display());
            let force_include_flag =
                format!("-include{}", testdata_dir.join("wasi_stubs.h").display());
            let output = std::process::Command::new("clang")
                .args([
                    "-fsyntax-only",
                    "-Werror",
                    "-Wno-unused-variable",
                    "-Wno-unused-but-set-variable",
                    &include_flag,
                    &force_include_flag,
                    tmp.path().to_str().unwrap(),
                ])
                .output()
                .expect("clang runs");
            assert!(
                output.status.success(),
                "clang syntax check failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }

    /// End-to-end regression net: transform `testdata/http_client.c`
    /// (the canonical socket + bind + listen + accept sequence) and
    /// verify the output is syntactically valid C under
    /// `clang -fsyntax-only -Werror`. Gated on `EDGE_TEST_CLANG=1`
    /// AND `clang --version` succeeding, so CI without the wasi-sdk
    /// image still passes.
    ///
    /// This is the test that caught the pollable (#128) and fd (#129)
    /// bugs — the per-pattern unit tests only check marker substrings
    /// and would still pass with those bugs present. The fix path is
    /// to make this test green; both #128 and #129 surface here as
    /// `error: use of undeclared identifier`.
    ///
    /// Fixtures:
    /// - `testdata/http_client.c` — input source
    /// - `testdata/wasi_stubs.h` — declarations for every symbol the
    ///   transformer emits (force-included via `-include`)
    /// - `testdata/wasi/{sockets,io/streams,filesystem,ip-name-lookup}.h` —
    ///   empty `#pragma once` shims so the transformer's emitted
    ///   `#include` directives resolve.
    #[test]
    fn test_transform_e2e_wasi_stubs_compile() {
        if !clang_available() {
            // Silent no-op: keep CI green when clang is unavailable
            // or EDGE_TEST_CLANG is unset. The unit tests above still
            // exercise the marker-substring contract.
            return;
        }
        let testdata_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .join("testdata");
        let fixture_path = testdata_dir.join("http_client.c");
        let source = std::fs::read_to_string(&fixture_path).expect("read http_client.c");

        let mut analyzer = crate::analyzer::CAnalyzer::new();
        let matches = analyzer.analyze(&source);
        let result = Transformer::transform(&source, matches, None);

        // Use `tempfile::NamedTempFile` so the file auto-cleans on
        // drop (and on assert panic). The pre-existing pid-based
        // naming leaked files if `clang` segfaulted mid-invocation
        // and left a stale `.c` in the OS temp dir.
        let mut tmp = tempfile::Builder::new()
            .suffix(".c")
            .tempfile()
            .expect("create temp file");
        std::io::Write::write_all(&mut tmp, result.transformed_source.as_bytes())
            .expect("write transformed source");

        let include_flag = format!("-I{}", testdata_dir.display());
        let force_include_flag = format!("-include{}", testdata_dir.join("wasi_stubs.h").display());
        let output = std::process::Command::new("clang")
            .args([
                "-fsyntax-only",
                "-Werror",
                "-Wno-unused-variable",
                "-Wno-unused-but-set-variable",
                &include_flag,
                &force_include_flag,
                tmp.path().to_str().unwrap(),
            ])
            .output()
            .expect("clang runs");

        assert!(
            output.status.success(),
            "clang syntax check failed (transformer emit no longer compiles).\n\
             stderr:\n{}\n\
             --- transformed source ---\n{}",
            String::from_utf8_lossy(&output.stderr),
            result.transformed_source
        );

        // G3 follow-up: the http_client.c fixture contains a
        // `gethostbyname(...)` call which is NotTransformable in
        // MVP. The transformer must leave the original call
        // verbatim in the source — it must NOT emit the broken
        // `wasi_ip_name_lookup_resolve(...)` form (G3 reason: the
        // runtime's `edge:cloud/networking.resolve(string) ->
        // list<string>` shape doesn't match
        // `wasi:ip-name-lookup.resolve-address`). The call must
        // land in `manual_review` so the developer sees it.
        assert!(
            result.transformed_source.contains("gethostbyname("),
            "G3: gethostbyname must be preserved verbatim; got:\n{}",
            result.transformed_source
        );
        assert!(
            !result.transformed_source.contains("wasi_ip_name_lookup_resolve"),
            "G3: wasi_ip_name_lookup_resolve emit must NOT appear; got:\n{}",
            result.transformed_source
        );
        assert!(
            result
                .manual_review
                .iter()
                .any(|m| matches!(m.pattern, PatternKind::Posix(PosixPattern::GetHostByName))),
            "G3: gethostbyname must be in manual_review"
        );
    }

    /// Returns true iff `EDGE_TEST_CLANG=1` is set in the environment
    /// AND a `clang` binary is reachable on PATH and responds to
    /// `--version`. Mirrors the gate on the marker-substring test
    /// above (`test_transform_socket_sequence_valid_c`).
    fn clang_available() -> bool {
        std::env::var("EDGE_TEST_CLANG").is_ok()
            && std::process::Command::new("clang")
                .arg("--version")
                .output()
                .map(|o| o.status.success())
                .unwrap_or(false)
    }
}
