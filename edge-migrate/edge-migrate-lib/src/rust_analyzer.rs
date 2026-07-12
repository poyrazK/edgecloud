//! Rust source analyzer (M3).
//!
//! Detects `std::net`, `std::fs`, and `std::process` patterns via
//! tree-sitter-rust. Mirrors the `CAnalyzer` shape: a `Parser`-wrapped
//! struct with an `analyze(source) -> Vec<PatternMatch>` entry point.
//!
//! **Scope:** only `std` patterns are detected. `tokio::net`,
//! `async-std`, and `#![no_std]` are out of scope for v1 — see the
//! plan's "Risks and follow-ups" section.
//!
//! **Preprocessor:** none. A future M5 could shell out to
//! `rustc -Zunpretty=expanded` to surface patterns hidden behind
//! `macro_rules!`; for now the analyzer parses the source as-is.
//!
//! **Deny-list (issue #622):** `analyze_with_security` rejects
//! tenant source that uses compile-time host-reach macros
//! (`include_bytes!`, `include_str!`, `include!`, `env!`,
//! `option_env!`, `compile_error!`) or attribute-based module
//! redirection (`#[path = ...]`, `#[include = ...]`). These expand
//! at `rustc` compile time and would let tenant code exfiltrate
//! host files / env vars into the produced wasm. The deny-list
//! emits `ErrorInfo { code: "SECURITY_DENY:RUST_MACRO", ... }`
//! and short-circuits before the regular pattern detector runs,
//! leaving `patterns_detected` empty for the disallowed submission.

use crate::patterns::{PatternKind, PatternMatch, RustPattern};
use crate::report::{ErrorInfo, MigrationReport, MigrationStatus};
use tree_sitter::Parser;

/// Stable code-prefix on `ErrorInfo.code` for Rust-side security
/// denials (issue #622). Mirrored on the Go side as
/// `domain.ErrorInfo.Code` so the SDK can branch on it.
pub const DENY_CODE_RUST_MACRO: &str = "SECURITY_DENY:RUST_MACRO";

/// Rust source code analyzer using tree-sitter-rust.
pub struct RustAnalyzer {
    parser: Parser,
}

impl RustAnalyzer {
    /// Build a new Rust analyzer with a fresh tree-sitter-rust parser.
    pub fn new() -> Self {
        let mut parser = Parser::new();
        parser
            .set_language(&tree_sitter_rust::LANGUAGE.into())
            .expect("Failed to set tree-sitter Rust language");
        Self { parser }
    }

    /// Parse the given Rust source and return all detected `std::*`
    /// patterns. Matches are sorted by `(line, column)` for
    /// deterministic output.
    pub fn analyze(&mut self, source: &str) -> Vec<PatternMatch> {
        let tree = match self.parser.parse(source, None) {
            Some(t) => t,
            None => return Vec::new(),
        };
        let root = tree.root_node();
        let mut matches = Vec::new();
        self.walk_node(source, root, &mut matches);
        matches.sort_by_key(|m| (m.line, m.column.unwrap_or(0)));
        matches
    }

    /// Parse the given Rust source and produce a full `MigrationReport`
    /// with the security deny-list applied (issue #622). On a denied
    /// submission the report's `status` is `MigrationStatus::Failed`,
    /// `errors[]` contains one `ErrorInfo { code: "SECURITY_DENY:RUST_MACRO" }`
    /// entry per denied site, and `patterns_detected` is empty
    /// (the deny-list short-circuits — we do not run the regular
    /// pattern detector against a submission we already know is
    /// hostile, both for efficiency and to avoid giving the caller
    /// any signal beyond the deny-list result).
    pub fn analyze_with_security(&mut self, app_name: &str, source: &str) -> MigrationReport {
        let tree = match self.parser.parse(source, None) {
            Some(t) => t,
            None => {
                // Unparseable source — no deny-list result either.
                // Fall through to a degenerate Success report; the
                // downstream `rustc` invocation will reject it with
                // its own diagnostic.
                return MigrationReport::from_pattern_matches(app_name, Vec::new());
            }
        };
        let root = tree.root_node();
        let mut denied = Vec::new();
        self.collect_deny_violations(source, root, &mut denied);
        if denied.is_empty() {
            let matches = self.collect_pattern_matches(source, root);
            return MigrationReport::from_pattern_matches(app_name, matches);
        }
        MigrationReport {
            status: MigrationStatus::Failed,
            wasm_stored: false,
            deployment_id: None,
            app_name: app_name.to_string(),
            patterns_detected: Vec::new(),
            patterns_transformed: Vec::new(),
            patterns_manual_review: Vec::new(),
            errors: denied,
            preprocessor: None,
        }
    }

    /// Walk the AST and collect denied macro / attribute sites. Each
    /// hit pushes one `ErrorInfo` carrying the stable
    /// `SECURITY_DENY:RUST_MACRO` code. We do NOT abort on the first
    /// hit — every denied site is reported so the tenant sees the
    /// full scope of what they need to fix.
    fn collect_deny_violations(
        &self,
        source: &str,
        node: tree_sitter::Node,
        out: &mut Vec<ErrorInfo>,
    ) {
        if let Some(err) = self.match_deny(source, node) {
            out.push(err);
        }
        for i in 0..node.child_count() {
            if let Some(child) = node.child(i) {
                self.collect_deny_violations(source, child, out);
            }
        }
    }

    /// Walk the AST and collect `std::*` pattern matches.
    fn collect_pattern_matches(&self, source: &str, node: tree_sitter::Node) -> Vec<PatternMatch> {
        let mut matches = Vec::new();
        self.walk_node(source, node, &mut matches);
        matches.sort_by_key(|m| (m.line, m.column.unwrap_or(0)));
        matches
    }

    fn walk_node(&self, source: &str, node: tree_sitter::Node, matches: &mut Vec<PatternMatch>) {
        if let Some(m) = self.match_node(source, node) {
            matches.push(m);
        }
        for i in 0..node.child_count() {
            if let Some(child) = node.child(i) {
                self.walk_node(source, child, matches);
            }
        }
    }

    /// Inspect `node` for a compile-time host-reach macro or
    /// attribute (issue #622 deny-list). Returns the corresponding
    /// `ErrorInfo` if the node is one of the banned sites.
    ///
    /// **Identifier-boundary guard:** we match on the macro identifier
    /// exactly — `include_bytes_helper!()` is NOT a violation, only
    /// `include_bytes!(...)`. The macro identifier is a
    /// `identifier` / `scoped_identifier` child of a `macro_invocation`
    /// node, so this is a structural check rather than a regex.
    fn match_deny(&self, source: &str, node: tree_sitter::Node) -> Option<ErrorInfo> {
        match node.kind() {
            "macro_invocation" => {
                let macro_name = self.macro_invocation_name(source, node)?;
                let reason = match macro_name.as_str() {
                    "include_bytes" => "include_bytes",
                    "include_str" => "include_str",
                    "include" => "include",
                    "env" => "env",
                    "option_env" => "option_env",
                    "compile_error" => "compile_error",
                    _ => return None,
                };
                let line = node.start_position().row + 1;
                let snippet = node.utf8_text(source.as_bytes()).unwrap_or("").to_string();
                Some(ErrorInfo {
                    line,
                    message: format!(
                        "compile-time host-reach macro `{}!` is not permitted in migrated Rust source — `{}` would exfiltrate host files or env vars at rustc compile time (issue #622)",
                        reason, snippet
                    ),
                    code: Some(DENY_CODE_RUST_MACRO.to_string()),
                })
            }
            "attribute_item" | "inner_attribute_item" => {
                let attr_name = self.attribute_name(source, node)?;
                let reason = match attr_name.as_str() {
                    "path" => "path_attr",
                    "include" => "include_attr",
                    _ => return None,
                };
                let line = node.start_position().row + 1;
                let snippet = node.utf8_text(source.as_bytes()).unwrap_or("").to_string();
                Some(ErrorInfo {
                    line,
                    message: format!(
                        "attribute `#[{} = \"...\"]` is not permitted in migrated Rust source — it redirects module resolution to host files at rustc compile time (issue #622)",
                        attr_name
                    ),
                    code: Some(DENY_CODE_RUST_MACRO.to_string()),
                })
                .map(|mut e| {
                    // Encode the reason into the message so callers
                    // that branch on the human-readable string still
                    // see the difference. The structured `code` is
                    // what dashboards grep on.
                    e.message = format!(
                        "compile-time host-reach attribute `#[{} = ...]` is not permitted in migrated Rust source — `{}` would exfiltrate host files at rustc compile time (issue #622, reason={})",
                        attr_name, snippet, reason
                    );
                    e
                })
            }
            _ => None,
        }
    }

    /// Extract the macro identifier from a `macro_invocation` node.
    /// In tree-sitter-rust 0.24, `macro_invocation` has children:
    /// `identifier` | `scoped_identifier` | `token_tree` (the `(...)`).
    /// We only care about the name, not the token tree.
    fn macro_invocation_name(&self, source: &str, node: tree_sitter::Node) -> Option<String> {
        for i in 0..node.child_count() {
            if let Some(child) = node.child(i) {
                match child.kind() {
                    "identifier" | "scoped_identifier" => {
                        return child.utf8_text(source.as_bytes()).ok().map(String::from);
                    }
                    _ => {}
                }
            }
        }
        None
    }

    /// Extract the attribute identifier from an `attribute_item` /
    /// `inner_attribute_item` node. The attribute's `path` is the
    /// `identifier` / `scoped_identifier` inside the `attribute`
    /// wrapper child (verified against tree-sitter-rust 0.24 — the
    /// immediate child is `attribute`, not the identifier directly).
    fn attribute_name(&self, source: &str, node: tree_sitter::Node) -> Option<String> {
        let mut cursor = node.walk();
        for child in node.children(&mut cursor) {
            match child.kind() {
                "identifier" | "scoped_identifier" => {
                    return child.utf8_text(source.as_bytes()).ok().map(String::from);
                }
                "attribute" => {
                    // Descend into the wrapper to find the identifier.
                    let mut inner_cursor = child.walk();
                    for inner in child.children(&mut inner_cursor) {
                        if matches!(inner.kind(), "identifier" | "scoped_identifier") {
                            return inner.utf8_text(source.as_bytes()).ok().map(String::from);
                        }
                    }
                }
                _ => {}
            }
        }
        None
    }

    /// Try to match `node` as a Rust pattern site. Returns `None` for
    /// nodes that don't correspond to a known `std::*` call.
    ///
    /// In tree-sitter-rust 0.24, **both** free functions and method
    /// calls share the `call_expression` node kind. A method call is
    /// distinguished by its `function` child being a
    /// `field_expression` whose `field` is the method name (a
    /// `field_identifier`). A free function's `function` child is a
    /// `scoped_identifier` or `identifier`.
    fn match_node(&self, source: &str, node: tree_sitter::Node) -> Option<PatternMatch> {
        if node.kind() != "call_expression" {
            return None;
        }
        let func_node = node.child_by_field_name("function")?;
        match func_node.kind() {
            // Free function: `std::net::TcpListener::bind(...)`,
            // `std::fs::read(...)`, `std::process::exit(...)`.
            "scoped_identifier" | "identifier" => {
                let func_text = self.scoped_text(source, func_node);
                let pattern = pattern_from_scoped_path(&func_text)?;
                Some(self.build_match(source, node, pattern))
            }
            // Method call: `x.method(...)` — represented as
            // `field_expression { value, field: field_identifier }`.
            "field_expression" => {
                let field_node = func_node.child_by_field_name("field")?;
                if field_node.kind() != "field_identifier" {
                    return None;
                }
                let method_name = field_node.utf8_text(source.as_bytes()).ok()?;
                let pattern = match method_name {
                    "accept" => PatternKind::Rust(RustPattern::TcpAccept),
                    "connect" => PatternKind::Rust(RustPattern::UdpConnect),
                    "close" => PatternKind::Rust(RustPattern::FsClose),
                    _ => return None,
                };
                Some(self.build_match(source, node, pattern))
            }
            _ => None,
        }
    }

    /// Resolve a `scoped_identifier` / `identifier` /
    /// `parenthesized_expression` to its full path text. Returns `""`
    /// if the node can't be read.
    fn scoped_text(&self, source: &str, node: tree_sitter::Node) -> String {
        match node.kind() {
            "scoped_identifier" | "identifier" => {
                node.utf8_text(source.as_bytes()).unwrap_or("").to_string()
            }
            "parenthesized_expression" => {
                // Shouldn't happen for the `function` field, but
                // descend defensively to find the inner identifier.
                let mut out = String::new();
                for i in 0..node.child_count() {
                    if let Some(child) = node.child(i) {
                        let s = self.scoped_text(source, child);
                        if !s.is_empty() {
                            out.push_str(&s);
                            break;
                        }
                    }
                }
                out
            }
            _ => String::new(),
        }
    }

    /// Build a `PatternMatch` from a node and resolved pattern kind.
    fn build_match(
        &self,
        source: &str,
        node: tree_sitter::Node,
        pattern: PatternKind,
    ) -> PatternMatch {
        let line = node.start_position().row + 1;
        let column = node.start_position().column;
        let snippet = node.utf8_text(source.as_bytes()).unwrap_or("").to_string();
        let arg_nodes = extract_call_args(source, node);
        PatternMatch {
            line,
            column: Some(column),
            start_byte: node.start_byte(),
            end_byte: node.end_byte(),
            original_start_byte: node.start_byte(),
            original_end_byte: node.end_byte(),
            pattern,
            snippet,
            arg_nodes,
            transformability: pattern.transformability(),
            bound_var: None,
        }
    }
}

/// Map a `std::path::to::func` text to a `RustPattern`. Returns
/// `None` for paths outside the v1 scope (e.g. `tokio::net::*`,
/// user-defined functions, `std::thread::*`).
fn pattern_from_scoped_path(path: &str) -> Option<PatternKind> {
    let pat = match path {
        "std::net::TcpListener::bind" => RustPattern::TcpBind,
        "std::net::TcpStream::connect" => RustPattern::TcpConnect,
        "std::net::UdpSocket::bind" => RustPattern::UdpBind,
        "std::process::exit" => RustPattern::ProcessExit,
        "std::fs::File::open" => RustPattern::FsOpen,
        "std::fs::read" | "std::fs::read_to_string" => RustPattern::FsRead,
        "std::fs::write" => RustPattern::FsWrite,
        _ => return None,
    };
    Some(PatternKind::Rust(pat))
}

/// Extract the textual representation of each argument to a call or
/// method-call node. Used by the transformer to grab the address /
/// path / data argument for substitution. Returns an empty vec for
/// nodes that don't have a recognized arguments child.
fn extract_call_args(source: &str, node: tree_sitter::Node) -> Vec<String> {
    let args_node = match node.child_by_field_name("arguments") {
        Some(n) => n,
        None => return Vec::new(),
    };
    let mut out = Vec::new();
    for i in 0..args_node.child_count() {
        if let Some(child) = args_node.child(i) {
            // Skip the parens / commas.
            match child.kind() {
                "(" | ")" | "," => continue,
                _ => {
                    let text = child.utf8_text(source.as_bytes()).unwrap_or("").to_string();
                    if !text.is_empty() {
                        out.push(text);
                    }
                }
            }
        }
    }
    out
}

impl Default for RustAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Transformability;

    /// Look up the first match for `pat` in `matches`. Pulled into a
    /// helper to avoid the temporary-lifetime borrow errors that
    /// surface when chaining `.analyze(...)` into `find_one` directly.
    fn first(matches: &[PatternMatch], pat: RustPattern) -> &PatternMatch {
        matches
            .iter()
            .find(|m| m.pattern == PatternKind::Rust(pat))
            .unwrap_or_else(|| panic!("expected match for {:?} in {:?}", pat, matches))
    }

    #[test]
    fn test_detect_tcp_listener_bind() {
        let mut a = RustAnalyzer::new();
        let src = r#"
fn main() {
    let _ = std::net::TcpListener::bind("127.0.0.1:8080");
}
"#;
        let matches = a.analyze(src);
        let m = first(&matches, RustPattern::TcpBind);
        assert_eq!(m.arg_nodes, vec!["\"127.0.0.1:8080\""]);
        assert_eq!(m.transformability, Transformability::AutoTransformable);
    }

    #[test]
    fn test_detect_tcp_stream_connect() {
        let mut a = RustAnalyzer::new();
        let src = r#"
fn main() {
    let _ = std::net::TcpStream::connect("127.0.0.1:9000");
}
"#;
        let matches = a.analyze(src);
        let m = first(&matches, RustPattern::TcpConnect);
        assert_eq!(m.arg_nodes, vec!["\"127.0.0.1:9000\""]);
        assert_eq!(m.transformability, Transformability::AutoTransformable);
    }

    #[test]
    fn test_detect_udp_socket_bind() {
        let mut a = RustAnalyzer::new();
        let src = r#"
fn main() {
    let _ = std::net::UdpSocket::bind("0.0.0.0:5353");
}
"#;
        let matches = a.analyze(src);
        let m = first(&matches, RustPattern::UdpBind);
        assert_eq!(m.arg_nodes, vec!["\"0.0.0.0:5353\""]);
        assert_eq!(m.transformability, Transformability::AutoTransformable);
    }

    #[test]
    fn test_detect_udp_socket_connect_not_transformable() {
        let mut a = RustAnalyzer::new();
        let src = r#"
fn main() {
    let sock = std::net::UdpSocket::bind("0.0.0.0:0").unwrap();
    let _ = sock.connect("127.0.0.1:9000");
}
"#;
        let matches = a.analyze(src);
        let m = first(&matches, RustPattern::UdpConnect);
        assert_eq!(m.transformability, Transformability::NotTransformable);
    }

    #[test]
    fn test_detect_std_process_exit_not_transformable() {
        let mut a = RustAnalyzer::new();
        let src = r#"
fn main() {
    std::process::exit(0);
}
"#;
        let matches = a.analyze(src);
        let m = first(&matches, RustPattern::ProcessExit);
        assert_eq!(m.transformability, Transformability::NotTransformable);
    }

    #[test]
    fn test_detect_std_fs_file_open() {
        let mut a = RustAnalyzer::new();
        let src = r#"
fn main() {
    let _ = std::fs::File::open("data.txt");
}
"#;
        let matches = a.analyze(src);
        let m = first(&matches, RustPattern::FsOpen);
        assert_eq!(m.arg_nodes, vec!["\"data.txt\""]);
        assert_eq!(m.transformability, Transformability::AutoTransformable);
    }

    #[test]
    fn test_detect_std_fs_read_and_write() {
        let mut a = RustAnalyzer::new();
        let src = r#"
fn main() {
    let _ = std::fs::read("a.txt");
    let _ = std::fs::write("b.txt", b"hello");
}
"#;
        let matches = a.analyze(src);
        let _ = first(&matches, RustPattern::FsRead);
        let w = first(&matches, RustPattern::FsWrite);
        assert_eq!(w.arg_nodes, vec!["\"b.txt\"", "b\"hello\""]);
    }

    #[test]
    fn test_detect_method_call_accept() {
        let mut a = RustAnalyzer::new();
        let src = r#"
fn main() {
    let listener = std::net::TcpListener::bind("127.0.0.1:80").unwrap();
    let (stream, _) = listener.accept().unwrap();
}
"#;
        let matches = a.analyze(src);
        let m = first(&matches, RustPattern::TcpAccept);
        assert_eq!(m.transformability, Transformability::NotTransformable);
    }

    #[test]
    fn test_detect_method_call_close() {
        let mut a = RustAnalyzer::new();
        let src = r#"
fn main() {
    let mut f = std::fs::File::open("a.txt").unwrap();
    let _ = f.close();
}
"#;
        let matches = a.analyze(src);
        let m = first(&matches, RustPattern::FsClose);
        assert_eq!(m.transformability, Transformability::AutoTransformable);
    }

    #[test]
    fn test_ignores_user_functions_named_bind() {
        // A user-defined `bind` (no `std::` prefix) must NOT match.
        let mut a = RustAnalyzer::new();
        let src = r#"
fn bind(_x: i32) {}
fn main() { bind(42); }
"#;
        let matches = a.analyze(src);
        assert!(!matches
            .iter()
            .any(|m| m.pattern == PatternKind::Rust(RustPattern::TcpBind)));
    }

    #[test]
    fn test_ignores_tokio_net_out_of_scope() {
        // v1 only matches `std::*`. `tokio::net::TcpListener::bind`
        // is a known gap; the analyzer must not produce a match.
        let mut a = RustAnalyzer::new();
        let src = r#"
async fn main() {
    let _ = tokio::net::TcpListener::bind("127.0.0.1:80").await;
}
"#;
        let matches = a.analyze(src);
        assert!(
            matches.is_empty(),
            "tokio::net is out of v1 scope, got {matches:?}"
        );
    }

    #[test]
    fn test_handles_unparseable_source() {
        // Garbage in, empty result out, no panic.
        let mut a = RustAnalyzer::new();
        let matches = a.analyze("this is not rust {{{}}}");
        assert!(matches.is_empty());
    }

    #[test]
    fn test_column_populated() {
        let mut a = RustAnalyzer::new();
        let src = "fn main() {\n    let _ = std::net::TcpListener::bind(\"127.0.0.1:80\");\n}\n";
        let matches = a.analyze(src);
        let m = first(&matches, RustPattern::TcpBind);
        let col = m.column.expect("column must be populated");
        // 4 spaces + "let _ = " (8 chars) → `std` starts at column 12.
        assert_eq!(col, 12, "expected column 12, got {col}");
    }

    #[test]
    fn test_matches_sorted_by_line() {
        let mut a = RustAnalyzer::new();
        // Out-of-order source: process::exit appears before bind.
        let src = r#"
fn main() {
    std::process::exit(0);
    let _ = std::net::TcpListener::bind("127.0.0.1:80");
}
"#;
        let matches = a.analyze(src);
        let lines: Vec<_> = matches.iter().map(|m| m.line).collect();
        let mut sorted = lines.clone();
        sorted.sort();
        assert_eq!(
            lines, sorted,
            "matches should be sorted by line, got {lines:?}"
        );
    }

    // ─────────────────────────────────────────────────────────────────
    // issue #622 — Rust analyzer deny-list tests
    //
    // These tests pin the contract that `analyze_with_security`
    // rejects compile-time host-reach macros / attributes (issue #622).
    // They MUST NOT regress without an explicit review of the
    // security implications.
    // ─────────────────────────────────────────────────────────────────

    fn assert_deny_failed_with_code(report: &crate::report::MigrationReport, expected_code: &str) {
        use crate::report::MigrationStatus;
        assert!(
            matches!(report.status, MigrationStatus::Failed),
            "expected MigrationStatus::Failed, got {:?}",
            report.status
        );
        assert!(
            report.patterns_detected.is_empty(),
            "denied submission must NOT produce pattern matches, got {:?}",
            report.patterns_detected
        );
        assert!(
            !report.errors.is_empty(),
            "denied submission must populate errors[]"
        );
        for err in &report.errors {
            assert_eq!(
                err.code.as_deref(),
                Some(expected_code),
                "every ErrorInfo must carry code={expected_code}, got {:?}",
                err.code
            );
        }
    }

    #[test]
    fn test_deny_include_bytes_secret_path() {
        let mut a = RustAnalyzer::new();
        let src = r#"
const LEAK: &[u8] = include_bytes!("/etc/edgecloud/signing.key");
fn main() {}
"#;
        let report = a.analyze_with_security("evil", src);
        assert_deny_failed_with_code(&report, DENY_CODE_RUST_MACRO);
        assert_eq!(report.errors.len(), 1);
        assert!(report.errors[0].message.contains("include_bytes"));
    }

    #[test]
    fn test_deny_env_macro() {
        let mut a = RustAnalyzer::new();
        let src = r#"
const SECRET: &str = env!("EDGE_SIGNING_KEY");
fn main() {}
"#;
        let report = a.analyze_with_security("evil", src);
        assert_deny_failed_with_code(&report, DENY_CODE_RUST_MACRO);
        assert!(report.errors[0].message.contains("env"));
    }

    #[test]
    fn test_deny_option_env_macro() {
        let mut a = RustAnalyzer::new();
        let src = r#"
const SECRET: Option<&'static str> = option_env!("EDGE_SIGNING_KEY");
fn main() {}
"#;
        let report = a.analyze_with_security("evil", src);
        assert_deny_failed_with_code(&report, DENY_CODE_RUST_MACRO);
        assert!(report.errors[0].message.contains("option_env"));
    }

    #[test]
    fn test_deny_include_str_macro() {
        let mut a = RustAnalyzer::new();
        let src = r#"
const SHADOW: &str = include_str!("/etc/shadow");
fn main() {}
"#;
        let report = a.analyze_with_security("evil", src);
        assert_deny_failed_with_code(&report, DENY_CODE_RUST_MACRO);
        assert!(report.errors[0].message.contains("include_str"));
    }

    #[test]
    fn test_deny_path_attribute() {
        let mut a = RustAnalyzer::new();
        let src = r#"
#[path = "/etc/passwd"]
mod x;
fn main() {}
"#;
        let report = a.analyze_with_security("evil", src);
        assert_deny_failed_with_code(&report, DENY_CODE_RUST_MACRO);
        assert!(report.errors[0].message.contains("path"));
    }

    #[test]
    fn test_deny_compile_error_macro() {
        // `compile_error!(env!("JWT_SECRET"))` would print the env
        // var's value via cargo's diagnostic, captured into
        // compileErrMsg and forwarded to the tenant. Strip it.
        let mut a = RustAnalyzer::new();
        let src = r#"
fn main() {
    compile_error!("user error");
}
"#;
        let report = a.analyze_with_security("evil", src);
        assert_deny_failed_with_code(&report, DENY_CODE_RUST_MACRO);
        assert!(report.errors[0].message.contains("compile_error"));
    }

    #[test]
    fn test_deny_multiple_sites_all_reported() {
        let mut a = RustAnalyzer::new();
        let src = r#"
const A: &[u8] = include_bytes!("/etc/passwd");
const B: &str = env!("JWT_SECRET");
fn main() {}
"#;
        let report = a.analyze_with_security("evil", src);
        assert_deny_failed_with_code(&report, DENY_CODE_RUST_MACRO);
        // Both sites should be reported — the deny-list does not
        // abort at the first hit.
        assert_eq!(
            report.errors.len(),
            2,
            "expected 2 deny-list errors, got {}",
            report.errors.len()
        );
    }

    // ── Negative tests: identifier-boundary guard ────────────────────

    #[test]
    fn test_deny_negative_user_defined_include_bytes_helper() {
        // A user-defined macro whose NAME contains "include_bytes"
        // must NOT match. The deny-list is structural (matches the
        // exact macro identifier), not a substring search.
        let mut a = RustAnalyzer::new();
        let src = r#"
macro_rules! my_include_bytes_helper { () => { 0 }; }
fn main() { my_include_bytes_helper!(); }
"#;
        let report = a.analyze_with_security("ok", src);
        assert!(
            report.errors.is_empty(),
            "user-defined macro must not match the deny-list, got {:?}",
            report.errors
        );
    }

    #[test]
    fn test_deny_negative_local_variable_named_env() {
        // A Rust variable named `env` must NOT match the deny-list.
        let mut a = RustAnalyzer::new();
        let src = r#"
fn main() {
    let env = "PATH";
    println!("{}", env);
}
"#;
        let report = a.analyze_with_security("ok", src);
        assert!(
            report.errors.is_empty(),
            "variable named `env` must not match, got {:?}",
            report.errors
        );
    }

    #[test]
    fn test_deny_negative_comment_with_macro_name() {
        // A comment mentioning the macro name must NOT match.
        let mut a = RustAnalyzer::new();
        let src = r#"
// include_bytes!("/etc/passwd") is denied.
fn main() {}
"#;
        let report = a.analyze_with_security("ok", src);
        assert!(
            report.errors.is_empty(),
            "comment with macro name must not match, got {:?}",
            report.errors
        );
    }

    #[test]
    fn test_deny_negative_benign_rust_source_still_detects_std_patterns() {
        // When the deny-list passes, the regular pattern detector
        // runs as before.
        let mut a = RustAnalyzer::new();
        let src = r#"
fn main() {
    let _ = std::net::TcpListener::bind("127.0.0.1:8080");
}
"#;
        let report = a.analyze_with_security("hello", src);
        assert!(
            report.errors.is_empty(),
            "benign source must produce no deny-list errors, got {:?}",
            report.errors
        );
        assert_eq!(report.patterns_detected.len(), 1);
    }
}
