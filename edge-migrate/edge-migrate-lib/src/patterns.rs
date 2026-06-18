//! POSIX pattern definitions and WASI equivalents.
//!
//! Defines all POSIX patterns that edge-migrate can detect, their
//! transformability classification, and WASI equivalents.
//!
//! M3 adds a parallel `RustPattern` enum and a `PatternKind` sum type
//! that holds either. The wire format is `serde(untagged)`, so a
//! `PatternKind::Posix(PosixPattern::Listen)` serializes as the bare
//! string `"Listen"` (back-compat with the M1/M2 JSON shape) and
//! `PatternKind::Rust(RustPattern::TcpBind)` as `"TcpBind"`.

use serde::{Deserialize, Serialize};

/// Source language for a pattern match.
///
/// M3 introduces this; M1+M2 only ever produced `Language::C`. The
/// CLI's `--language` flag and the Go control plane's `language` form
/// field both use the lowercase serde representation: `"c"` and `"rust"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Language {
    /// C source (parsed via `tree-sitter-c`, preprocessed via `clang -E`).
    C,
    /// Rust source (parsed via `tree-sitter-rust`, no preprocessor in v1).
    Rust,
}

/// Classification of how transformable a pattern is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Transformability {
    /// Can be automatically transformed to WASI with no manual intervention.
    AutoTransformable,
    /// Transformable but may require developer review (e.g., poll loops).
    BestEffort,
    /// Cannot be auto-transformed — requires manual rewrite.
    NotTransformable,
}

/// A detected pattern in source code.
///
/// M1+M2 had `pattern: PosixPattern` directly. M3 widened this to a
/// `PatternKind` sum type so a single `PatternMatch` can carry either
/// a C/POSIX pattern or a Rust `std::net` / `std::fs` / `std::process`
/// pattern. Serde's `untagged` keeps the wire format a bare variant
/// name (e.g. `"Listen"`, `"TcpBind"`), preserving back-compat for
/// existing M1/M2 reports.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PatternMatch {
    /// 1-based line number where the pattern was detected.
    ///
    /// When the analyzer runs with a preprocessor attached and macro
    /// expansion is successful, this is the **original** source line
    /// (1-based), remapped from the expanded source via the
    /// preprocessor's `line_map`. When the preprocessor is not
    /// attached, fails, or yields no useful mapping, the value is
    /// the 1-based line in the (possibly expanded) source actually
    /// fed to tree-sitter.
    pub line: usize,
    /// 0-based column (byte offset within the line) where the pattern
    /// starts. Populated by both `CAnalyzer` and `RustAnalyzer`.
    #[serde(default)]
    pub column: Option<usize>,
    /// Start byte offset in the source (for replacement).
    pub start_byte: usize,
    /// End byte offset in the source (for replacement).
    pub end_byte: usize,
    /// The kind of pattern detected (POSIX or Rust). See `PatternKind`.
    pub pattern: PatternKind,
    /// The original source code snippet.
    pub snippet: String,
    /// Raw text of each argument node from the AST (for accurate arg extraction).
    pub arg_nodes: Vec<String>,
    /// Whether this pattern can be auto-transformed.
    pub transformability: Transformability,
}

impl Default for PatternMatch {
    /// Used by struct literals that don't yet populate every field
    /// (e.g. `..Default::default()`). `line` defaults to 0 — callers
    /// that go through `analyze()` always get a real value.
    fn default() -> Self {
        Self {
            line: 0,
            column: None,
            start_byte: 0,
            end_byte: 0,
            pattern: PatternKind::Posix(PosixPattern::Unknown),
            snippet: String::new(),
            arg_nodes: Vec::new(),
            transformability: Transformability::NotTransformable,
        }
    }
}

/// All known POSIX patterns that edge-migrate can detect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PosixPattern {
    /// `socket(AF_INET, SOCK_STREAM, 0)` — TCP socket creation.
    SocketTcp,
    /// `socket(AF_INET, SOCK_DGRAM, 0)` — UDP socket creation.
    SocketUdp,
    /// `bind()` — bind socket to address.
    Bind,
    /// `listen()` — mark socket as listening.
    Listen,
    /// `accept()` — accept incoming connection.
    Accept,
    /// `connect()` — connect to remote address.
    Connect,
    /// `recv()` / `read()` on a socket.
    Recv,
    /// `send()` / `write()` on a socket.
    Send,
    /// `gethostbyname()` or `getaddrinfo()` — DNS resolution.
    GetHostByName,
    /// `close()` on a socket file descriptor.
    Close,
    /// `fopen()` — open a file.
    Fopen,
    /// `fread()` — read from file.
    Fread,
    /// `fwrite()` — write to file.
    Fwrite,
    /// `fclose()` — close a file.
    Fclose,
    /// `poll()` — event polling (not transformable).
    Poll,
    /// `select()` — file descriptor set polling (not transformable).
    Select,
    /// `fork()` — process forking (not transformable).
    Fork,
    /// `exec()` / `execve()` — process execution (not transformable).
    Exec,
    /// `socketpair()` — creates connected socket pair (not transformable).
    SocketPair,
    /// `shutdown()` — full-duplex shutdown (not in wasi-sockets).
    Shutdown,
    /// `O_NONBLOCK` flag usage (not applicable in WASI).
    NonBlocking,
    /// `SOCK_RAW` — raw socket (not supported in WASI).
    SockRaw,
    /// Unknown or user-defined pattern (treated as not transformable).
    Unknown,
}

/// All known Rust `std` patterns that edge-migrate can detect.
///
/// M3 only covers the `std` namespace. `tokio::net`, `async-std`, and
/// `#![no_std]` are out of scope for v1; see `rust_analyzer.rs` for
/// the explicit scope statement and the TODO for future expansion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RustPattern {
    /// `std::net::TcpListener::bind(addr)` — bind a TCP listener.
    TcpBind,
    /// `listener.accept()` — accept a connection (transformed with
    /// a poll-loop wrapper, see `BestEffort`).
    TcpAccept,
    /// `std::net::TcpStream::connect(addr)` — open a TCP connection.
    TcpConnect,
    /// `std::net::UdpSocket::bind(addr)` — bind a UDP socket.
    UdpBind,
    /// `udp.connect(addr)` — connect a UDP socket (no direct WASI
    /// equivalent; surfaces as `NotTransformable`).
    UdpConnect,
    /// `std::fs::File::open(path)` — open a file.
    FsOpen,
    /// `std::fs::read(path)` / `read_to_string` — read a file.
    FsRead,
    /// `std::fs::write(path, ...)` — write a file.
    FsWrite,
    /// `file.close()` — close a file (drop shim).
    FsClose,
    /// `std::process::exit(code)` — terminate the process
    /// (WASM has no process model; `NotTransformable`).
    ProcessExit,
}

/// Sum type that holds either a C/POSIX pattern or a Rust `std` pattern.
///
/// Serialized as `serde(untagged)`, so:
///   - `PatternKind::Posix(PosixPattern::Listen)` → `"Listen"`
///   - `PatternKind::Rust(RustPattern::TcpBind)` → `"TcpBind"`
///
/// This keeps the JSON wire format a bare variant name (no wrapper
/// object), which preserves back-compat with the M1/M2 shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PatternKind {
    /// A C/POSIX pattern.
    Posix(PosixPattern),
    /// A Rust `std` pattern.
    Rust(RustPattern),
}

impl PatternKind {
    /// Returns the transformability classification for this pattern.
    pub fn transformability(&self) -> Transformability {
        match self {
            PatternKind::Posix(p) => p.transformability(),
            PatternKind::Rust(p) => p.transformability(),
        }
    }

    /// Returns a short, language-neutral name for the pattern. Used by
    /// `PatternInfo.pattern` (a `String`) in the wire format.
    pub fn name(&self) -> &'static str {
        match self {
            PatternKind::Posix(p) => p.name(),
            PatternKind::Rust(p) => p.name(),
        }
    }

    /// Returns a human-readable description of the WASI equivalent.
    /// For C/POSIX patterns, this is the same string `Transformer`
    /// used pre-M3. For Rust patterns, it points at the corresponding
    /// `wasi::socket::*` / `wasi::filesystem::*` API.
    ///
    /// `report.rs` and `transformer.rs` both call this through
    /// `PatternKind`, so dispatch lives here rather than at every call
    /// site.
    pub fn wasi_equivalent(&self) -> &'static str {
        match self {
            PatternKind::Posix(p) => p.wasi_equivalent(),
            PatternKind::Rust(p) => p.wasi_equivalent(),
        }
    }
}

impl PosixPattern {
    /// Short, language-neutral name. Stable across versions; used in
    /// the JSON wire format (`PatternInfo.pattern`).
    pub fn name(&self) -> &'static str {
        match self {
            PosixPattern::SocketTcp => "SocketTcp",
            PosixPattern::SocketUdp => "SocketUdp",
            PosixPattern::Bind => "Bind",
            PosixPattern::Listen => "Listen",
            PosixPattern::Accept => "Accept",
            PosixPattern::Connect => "Connect",
            PosixPattern::Recv => "Recv",
            PosixPattern::Send => "Send",
            PosixPattern::GetHostByName => "GetHostByName",
            PosixPattern::Close => "Close",
            PosixPattern::Fopen => "Fopen",
            PosixPattern::Fread => "Fread",
            PosixPattern::Fwrite => "Fwrite",
            PosixPattern::Fclose => "Fclose",
            PosixPattern::Poll => "Poll",
            PosixPattern::Select => "Select",
            PosixPattern::Fork => "Fork",
            PosixPattern::Exec => "Exec",
            PosixPattern::SocketPair => "SocketPair",
            PosixPattern::Shutdown => "Shutdown",
            PosixPattern::NonBlocking => "NonBlocking",
            PosixPattern::SockRaw => "SockRaw",
            PosixPattern::Unknown => "Unknown",
        }
    }

    /// Returns the WASI equivalent description for this pattern.
    pub fn wasi_equivalent(&self) -> &'static str {
        match self {
            PosixPattern::SocketTcp => "create-tcp-socket(ipv4)",
            PosixPattern::SocketUdp => "create-udp-socket(ipv4)",
            PosixPattern::Bind => "start-bind() + finish-bind()",
            PosixPattern::Listen => "start-listen() + finish-listen()",
            PosixPattern::Accept => "accept() with poll loop",
            PosixPattern::Connect => "start-connect() + finish-connect()",
            PosixPattern::Recv => "input-stream read via wasi:io/streams",
            PosixPattern::Send => "output-stream write via wasi:io/streams",
            PosixPattern::GetHostByName => "wasi:ip-name-lookup",
            PosixPattern::Close => "drop() on socket resource",
            PosixPattern::Fopen => "wasi:filesystem open",
            PosixPattern::Fread => "wasi:filesystem read",
            PosixPattern::Fwrite => "wasi:filesystem write",
            PosixPattern::Fclose => "wasi:filesystem close",
            PosixPattern::Poll => "no WASI equivalent — restructure event loop",
            PosixPattern::Select => "no WASI equivalent — restructure event loop",
            PosixPattern::Fork => "no WASI equivalent — Wasm has no process model",
            PosixPattern::Exec => "no WASI equivalent — Wasm has no process model",
            PosixPattern::SocketPair => "no WASI equivalent",
            PosixPattern::Shutdown => "not in wasi-sockets",
            PosixPattern::NonBlocking => "WASI sockets are always non-blocking",
            PosixPattern::SockRaw => "raw sockets not supported in WASI",
            PosixPattern::Unknown => "unknown pattern",
        }
    }

    /// Returns the transformability classification for this pattern.
    pub fn transformability(&self) -> Transformability {
        match self {
            PosixPattern::SocketTcp
            | PosixPattern::SocketUdp
            | PosixPattern::Bind
            | PosixPattern::Listen
            | PosixPattern::Connect
            | PosixPattern::Recv
            | PosixPattern::Send
            | PosixPattern::GetHostByName
            | PosixPattern::Close
            | PosixPattern::Fopen
            | PosixPattern::Fread
            | PosixPattern::Fwrite
            | PosixPattern::Fclose => Transformability::AutoTransformable,
            PosixPattern::Accept => Transformability::BestEffort,
            PosixPattern::Poll
            | PosixPattern::Select
            | PosixPattern::Fork
            | PosixPattern::Exec
            | PosixPattern::SocketPair
            | PosixPattern::Shutdown
            | PosixPattern::NonBlocking
            | PosixPattern::SockRaw
            | PosixPattern::Unknown => Transformability::NotTransformable,
        }
    }
}

impl RustPattern {
    /// Short, language-neutral name. Stable across versions; used in
    /// the JSON wire format (`PatternInfo.pattern`).
    pub fn name(&self) -> &'static str {
        match self {
            RustPattern::TcpBind => "TcpBind",
            RustPattern::TcpAccept => "TcpAccept",
            RustPattern::TcpConnect => "TcpConnect",
            RustPattern::UdpBind => "UdpBind",
            RustPattern::UdpConnect => "UdpConnect",
            RustPattern::FsOpen => "FsOpen",
            RustPattern::FsRead => "FsRead",
            RustPattern::FsWrite => "FsWrite",
            RustPattern::FsClose => "FsClose",
            RustPattern::ProcessExit => "ProcessExit",
        }
    }

    /// Returns the transformability classification for this pattern.
    pub fn transformability(&self) -> Transformability {
        match self {
            RustPattern::TcpBind
            | RustPattern::TcpConnect
            | RustPattern::UdpBind
            | RustPattern::FsOpen
            | RustPattern::FsRead
            | RustPattern::FsWrite
            | RustPattern::FsClose => Transformability::AutoTransformable,
            RustPattern::TcpAccept => Transformability::BestEffort,
            RustPattern::UdpConnect | RustPattern::ProcessExit => {
                Transformability::NotTransformable
            }
        }
    }

    /// Returns a human-readable description of the WASI equivalent.
    /// Mirrors `PosixPattern::wasi_equivalent` so `report.rs` and
    /// `transformer.rs` can keep dispatching through `PatternKind`.
    pub fn wasi_equivalent(&self) -> &'static str {
        match self {
            RustPattern::TcpBind => {
                "wasi::socket::tcp::TcpSocket::new + start_bind + finish_bind + \
                 start_listen + finish_listen"
            }
            RustPattern::TcpAccept => {
                "wasi::socket::tcp::TcpSocket::accept wrapped in poll loop"
            }
            RustPattern::TcpConnect => {
                "wasi::socket::tcp::TcpSocket::new + start_connect + finish_connect"
            }
            RustPattern::UdpBind => {
                "wasi::socket::udp::UdpSocket::new + start_bind + finish_bind"
            }
            RustPattern::UdpConnect => {
                "no WASI equivalent — UdpSocket::connect not in wasi-sockets"
            }
            RustPattern::FsOpen => "wasi::filesystem::open",
            RustPattern::FsRead => "wasi::filesystem::read",
            RustPattern::FsWrite => "wasi::filesystem::write",
            RustPattern::FsClose => "drop shim around wasi::filesystem handle",
            RustPattern::ProcessExit => {
                "no WASI equivalent — Wasm has no process model"
            }
        }
    }
}

/// Validate a deployment app name against the public-facing format
/// `^[a-z0-9][a-z0-9-]{0,62}$`.
///
/// Distinct from path-safety checks (no `..`, no `/`). Used by the
/// `edge-migrate --tree` CLI and the Go control plane's
/// `IsValidDeploymentAppName` mirror. Keeping the regex in one place
/// (the shared design doc) — both sides are tested against the same
/// set of valid / invalid examples.
pub fn is_valid_deployment_app_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > 63 {
        return false;
    }
    // First char: lowercase letter or digit.
    let first = bytes[0];
    if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
        return false;
    }
    // Remaining chars: lowercase letter, digit, or '-'.
    for &b in &bytes[1..] {
        if !b.is_ascii_lowercase() && !b.is_ascii_digit() && b != b'-' {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_valid_deployment_app_name_accepts_valid() {
        assert!(is_valid_deployment_app_name("a"));
        assert!(is_valid_deployment_app_name("hello-world"));
        assert!(is_valid_deployment_app_name("my-app-123"));
        assert!(is_valid_deployment_app_name("0"));
        assert!(is_valid_deployment_app_name("a".repeat(63).as_str()));
    }

    #[test]
    fn test_is_valid_deployment_app_name_rejects_invalid() {
        // Empty
        assert!(!is_valid_deployment_app_name(""));
        // Too long (64 chars)
        assert!(!is_valid_deployment_app_name(&"a".repeat(64)));
        // Uppercase
        assert!(!is_valid_deployment_app_name("Hello"));
        assert!(!is_valid_deployment_app_name("HELLO"));
        // Starts with non-alnum
        assert!(!is_valid_deployment_app_name("-hello"));
        assert!(!is_valid_deployment_app_name("_hello"));
        // Contains invalid chars
        assert!(!is_valid_deployment_app_name("hello_world"));
        assert!(!is_valid_deployment_app_name("hello world"));
        assert!(!is_valid_deployment_app_name("hello.world"));
        assert!(!is_valid_deployment_app_name("hello/world"));
        // Path traversal
        assert!(!is_valid_deployment_app_name("../traversal"));
        assert!(!is_valid_deployment_app_name("a/../b"));
    }

    // ─────────────────────────────────────────────────────────────────
    // M3: PatternKind, RustPattern, Language
    // ─────────────────────────────────────────────────────────────────

    #[test]
    fn test_language_serializes_lowercase() {
        let c = serde_json::to_string(&Language::C).unwrap();
        let r = serde_json::to_string(&Language::Rust).unwrap();
        assert_eq!(c, "\"c\"");
        assert_eq!(r, "\"rust\"");
    }

    #[test]
    fn test_language_deserializes_lowercase() {
        let c: Language = serde_json::from_str("\"c\"").unwrap();
        let r: Language = serde_json::from_str("\"rust\"").unwrap();
        assert_eq!(c, Language::C);
        assert_eq!(r, Language::Rust);
    }

    #[test]
    fn test_pattern_kind_posix_legacy_serialization() {
        // Back-compat: a C match serializes as the bare variant name,
        // matching the M1/M2 wire format.
        let kind = PatternKind::Posix(PosixPattern::Listen);
        let s = serde_json::to_string(&kind).unwrap();
        assert_eq!(s, "\"Listen\"");
    }

    #[test]
    fn test_pattern_kind_rust_serialization() {
        let kind = PatternKind::Rust(RustPattern::TcpBind);
        let s = serde_json::to_string(&kind).unwrap();
        assert_eq!(s, "\"TcpBind\"");
    }

    #[test]
    fn test_pattern_match_serializes_with_posix_kind() {
        // End-to-end: PatternMatch.pattern field is a bare string for C.
        let m = PatternMatch {
            line: 5,
            column: Some(0),
            start_byte: 42,
            end_byte: 60,
            pattern: PatternKind::Posix(PosixPattern::Listen),
            snippet: "bind(...)".to_string(),
            arg_nodes: vec![],
            transformability: Transformability::AutoTransformable,
        };
        let j = serde_json::to_string(&m).unwrap();
        let v: serde_json::Value = serde_json::from_str(&j).unwrap();
        assert_eq!(v["pattern"], "Listen");
        assert_eq!(v["line"], 5);
    }

    #[test]
    fn test_pattern_match_serializes_with_rust_kind() {
        let m = PatternMatch {
            line: 7,
            column: Some(0),
            start_byte: 100,
            end_byte: 145,
            pattern: PatternKind::Rust(RustPattern::TcpBind),
            snippet: "TcpListener::bind(\"127.0.0.1:8080\")".to_string(),
            arg_nodes: vec!["\"127.0.0.1:8080\"".to_string()],
            transformability: Transformability::AutoTransformable,
        };
        let j = serde_json::to_string(&m).unwrap();
        let v: serde_json::Value = serde_json::from_str(&j).unwrap();
        assert_eq!(v["pattern"], "TcpBind");
        assert_eq!(v["line"], 7);
    }

    #[test]
    fn test_pattern_match_deserializes_legacy_without_column() {
        // M1 reports had no `column` field; ensure they still parse.
        let j = r#"{
            "line": 3,
            "start_byte": 10,
            "end_byte": 20,
            "pattern": "Accept",
            "snippet": "accept(fd, NULL, NULL)",
            "arg_nodes": [],
            "transformability": "BestEffort"
        }"#;
        let m: PatternMatch = serde_json::from_str(j).unwrap();
        assert_eq!(m.line, 3);
        assert!(m.column.is_none());
        assert_eq!(m.pattern, PatternKind::Posix(PosixPattern::Accept));
        assert_eq!(m.transformability, Transformability::BestEffort);
    }

    #[test]
    fn test_pattern_match_default_is_posix_unknown() {
        // Default::default() should produce a Posix::Unknown match,
        // not panic or pick a Rust variant by accident.
        let m = PatternMatch::default();
        assert_eq!(m.pattern, PatternKind::Posix(PosixPattern::Unknown));
        assert_eq!(m.transformability, Transformability::NotTransformable);
        assert!(m.column.is_none());
    }

    #[test]
    fn test_pattern_kind_transformability_matrix() {
        // C path: AutoTransformable for the bulk, BestEffort for Accept,
        // NotTransformable for the un-fixables.
        assert_eq!(
            PatternKind::Posix(PosixPattern::Listen).transformability(),
            Transformability::AutoTransformable
        );
        assert_eq!(
            PatternKind::Posix(PosixPattern::Accept).transformability(),
            Transformability::BestEffort
        );
        assert_eq!(
            PatternKind::Posix(PosixPattern::Poll).transformability(),
            Transformability::NotTransformable
        );
        // Rust path: TcpAccept is BestEffort (poll loop wrapper),
        // UdpConnect and ProcessExit are NotTransformable, the rest
        // are AutoTransformable.
        assert_eq!(
            PatternKind::Rust(RustPattern::TcpBind).transformability(),
            Transformability::AutoTransformable
        );
        assert_eq!(
            PatternKind::Rust(RustPattern::TcpAccept).transformability(),
            Transformability::BestEffort
        );
        assert_eq!(
            PatternKind::Rust(RustPattern::UdpConnect).transformability(),
            Transformability::NotTransformable
        );
        assert_eq!(
            PatternKind::Rust(RustPattern::ProcessExit).transformability(),
            Transformability::NotTransformable
        );
    }

    #[test]
    fn test_pattern_kind_name_stable() {
        // The wire-format `pattern` field on `PatternInfo` is rendered
        // from `name()`. Lock the strings down so accidental renames
        // surface as test failures.
        assert_eq!(PatternKind::Posix(PosixPattern::Listen).name(), "Listen");
        assert_eq!(PatternKind::Rust(RustPattern::TcpBind).name(), "TcpBind");
        assert_eq!(
            PatternKind::Rust(RustPattern::ProcessExit).name(),
            "ProcessExit"
        );
    }
}
