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
#[serde(rename_all = "kebab-case")]
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
    /// Start byte offset in the **expanded** source (for replacement).
    ///
    /// When the analyzer runs with a preprocessor attached and macro
    /// expansion is successful, this is the byte offset within the
    /// **expanded** C source — i.e. after `clang -E`. This is the
    /// coordinate system that tree-sitter reports matches in.
    ///
    /// For slicing the **original** source (which is what
    /// `Transformer::transform` does), use `original_start_byte`
    /// instead. The two fields differ only when a preprocessor is
    /// attached and expansion succeeded; otherwise they are equal.
    pub start_byte: usize,
    /// End byte offset in the expanded source. See `start_byte` for
    /// the coordinate system distinction.
    pub end_byte: usize,
    /// Start byte offset in the **original** source. This is what
    /// `Transformer::transform` slices against — the bin's
    /// `--transform` path passes the original source to the
    /// transformer, so the byte ranges must be in original
    /// coordinates.
    ///
    /// Populated by `CAnalyzer::analyze_with_preprocessor_info` via
    /// the preprocessor's `byte_map`. When no preprocessor is
    /// attached, expansion failed, or the match falls on a synthetic
    /// line (linemarker / `<built-in>`), this equals `start_byte`.
    /// For Rust analysis (no preprocessor in v1), this always equals
    /// `start_byte`.
    #[serde(default)]
    pub original_start_byte: usize,
    /// End byte offset in the original source. See
    /// `original_start_byte` for semantics.
    #[serde(default)]
    pub original_end_byte: usize,
    /// The kind of pattern detected (POSIX or Rust). See `PatternKind`.
    pub pattern: PatternKind,
    /// The original source code snippet.
    pub snippet: String,
    /// Raw text of each argument node from the AST (for accurate arg extraction).
    pub arg_nodes: Vec<String>,
    /// Whether this pattern can be auto-transformed.
    pub transformability: Transformability,
    /// For `socket()` calls that appear as the initializer of a C
    /// declaration (e.g. `int fd = socket(...)`), the captured
    /// variable binding so the transformer can rewrite the whole
    /// `int fd = socket(...)` line with a correct WASI return type
    /// (`wasi_socket_tcp_t *fd = wasi_socket_tcp_create(...)`)
    /// instead of leaving the stale `int` type in place.
    ///
    /// `None` for bare-expression socket calls (e.g.
    /// `socket(AF_INET, SOCK_STREAM, 0);` as a statement on its
    /// own) and for all non-socket patterns. See
    /// `edge-migrate/docs/design.md` §4.1 (Accepting the fd binding).
    #[serde(default)]
    pub bound_var: Option<BoundVarDecl>,
}

/// Captured variable binding for a `socket()` call that appears as
/// the initializer of a C `declaration`.
///
/// The byte range is the whole declaration INCLUDING the trailing
/// `;` (tree-sitter C grammar's `declaration` node covers the
/// semicolon). The transformer uses this range as the replacement
/// span so it can swap `int fd = socket(...)` for
/// `wasi_socket_tcp_t *fd = wasi_socket_tcp_create(...);`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoundVarDecl {
    /// The declarator's identifier text (e.g. `fd`). Extracted from
    /// the `init_declarator`'s first child, which is expected to be
    /// an `identifier` node. Complex declarators (array, pointer,
    /// function-pointer) are not captured — see `analyzer.rs::extract_socket_bound_var`.
    pub name: String,
    /// Byte offset of the start of the surrounding `declaration` node.
    /// **Expanded-source coordinates** when a preprocessor is attached;
    /// see `original_decl_start_byte` for the original-source pair.
    pub decl_start_byte: usize,
    /// Byte offset of the end of the surrounding `declaration` node
    /// (past the trailing `;`). **Expanded-source coordinates** when a
    /// preprocessor is attached.
    pub decl_end_byte: usize,
    /// Byte offset of the start of the surrounding `declaration` node
    /// in the **original** source. This is what the transformer slices
    /// when rewriting the whole `int fd = socket(...)` declaration.
    /// Equals `decl_start_byte` when no preprocessor is attached or
    /// expansion failed.
    #[serde(default)]
    pub original_decl_start_byte: usize,
    /// Byte offset of the end of the surrounding `declaration` node in
    /// the **original** source. Equals `decl_end_byte` when no
    /// preprocessor is attached.
    #[serde(default)]
    pub original_decl_end_byte: usize,
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
            original_start_byte: 0,
            original_end_byte: 0,
            pattern: PatternKind::Posix(PosixPattern::Unknown),
            snippet: String::new(),
            arg_nodes: Vec::new(),
            transformability: Transformability::NotTransformable,
            bound_var: None,
        }
    }
}

/// All known POSIX patterns that edge-migrate can detect.
///
/// `Copy` is derived because every variant is a unit variant; this
/// keeps analyzer/transformer code and test helpers from cloning the
/// discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
///
/// `Copy` is derived because every variant is a unit variant; this
/// keeps test helpers and analyzer code from having to clone the
/// discriminant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
            PosixPattern::Accept => {
                "accept() — not transformable in MVP (was: poll loop wrapper; #128)"
            }
            PosixPattern::Connect => "start-connect() + finish-connect()",
            PosixPattern::Recv => "input-stream read via wasi:io/streams",
            PosixPattern::Send => "output-stream write via wasi:io/streams",
            PosixPattern::GetHostByName => "wasi:ip-name-lookup — not transformable in MVP (G3; edge:cloud/networking shape mismatch)",
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
            | PosixPattern::Close
            | PosixPattern::Fopen
            | PosixPattern::Fread
            | PosixPattern::Fwrite
            | PosixPattern::Fclose => Transformability::AutoTransformable,
            // G3: gethostbyname / getaddrinfo emit shape
            // (`wasi:ip-name-lookup.resolve-address`) does not match
            // the runtime's `edge:cloud/networking.resolve(string) ->
            // list<string>` shape. Downgrade for MVP — the call
            // stays verbatim in the source and lands in
            // manual_review. Tracked as a #118 follow-up once the
            // runtime lands a wasi:ip-name-lookup host impl.
            PosixPattern::GetHostByName => Transformability::NotTransformable,
            // Resolves #128: the previous BestEffort emit wrapped
            // accept in a `wasi_poll_pollable_block(pollable)` poll
            // loop, but `pollable` was never declared (the
            // subscription API it would come from isn't in WASI
            // Preview 2 yet) and the surrounding `int client = …`
            // initialization shape was syntactically wrong for the
            // brace-wrapped block. Downgrade to NotTransformable for
            // MVP — the call stays in the source verbatim and lands
            // in manual_review.
            PosixPattern::Accept => Transformability::NotTransformable,
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

impl Transformability {
    /// Stable kebab-case form of this classification. Used in
    /// `PatternInfo.transformability` and in any user-facing label.
    /// Keep in sync with `#[serde(rename_all = "kebab-case")]` on the enum.
    pub fn as_str(&self) -> &'static str {
        match self {
            Transformability::AutoTransformable => "auto-transformable",
            Transformability::BestEffort => "best-effort",
            Transformability::NotTransformable => "not-transformable",
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
            RustPattern::TcpAccept => Transformability::NotTransformable,
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
                "create_tcp_socket(IpAddressFamily::Ipv4) + start_bind(&network, \
                 parse_addr_v4(addr)) + finish_bind + start_listen + finish_listen"
            }
            RustPattern::TcpAccept => {
                "TcpListener::accept() — not transformable in MVP (was: poll loop wrapper; #128)"
            }
            RustPattern::TcpConnect => {
                "create_tcp_socket(IpAddressFamily::Ipv4) + start_connect(&network, \
                 parse_addr_v4(addr)) + finish_connect — returns (rx, tx); both bound"
            }
            RustPattern::UdpBind => {
                "create_udp_socket(IpAddressFamily::Ipv4) + start_bind(&network, \
                 parse_addr_v4(addr)) + finish_bind"
            }
            RustPattern::UdpConnect => {
                "no WASI equivalent — UdpSocket::connect not in wasi::sockets::udp"
            }
            RustPattern::FsOpen => {
                "preopens::get_directories()[0] + Descriptor::open_at(PathFlags::empty(), \
                 path, OpenFlags::empty(), DescriptorFlags::READ) — typecheck-only at runtime"
            }
            RustPattern::FsRead => {
                "Descriptor::open_at(...) + Descriptor::read(length, offset) — \
                 length=0 placeholder; typecheck-only at runtime"
            }
            RustPattern::FsWrite => {
                "Descriptor::open_at(..., OpenFlags::CREATE | OpenFlags::TRUNCATE, \
                 DescriptorFlags::WRITE) + Descriptor::write(buffer, 0) — typecheck-only \
                 at runtime"
            }
            RustPattern::FsClose => "drop(var) — bindgen generates a Drop impl for Descriptor",
            RustPattern::ProcessExit => "no WASI equivalent — Wasm has no process model",
        }
    }
}

/// Validate an app name against the edgeCloud public-facing format
/// `^[a-z0-9][a-z0-9.\-_]{0,62}$` — 1–63 chars, lowercase alphanumerics
/// plus dots, underscores, and hyphens. The first character must be a
/// lowercase letter or digit.
///
/// Locks in lockstep with the Go control plane's `IsValidAppName` in
/// `edge-control-plane/internal/service/deployment.go` (issue #438
/// unified the parallel validators and widened the regex to admit
/// semver-ish suffixes like `myapp.v2` and `app_v2`). Defense-in-depth:
/// the CLI's `--tree` flag and `MigrationHandler.MigrateTree` both
/// gate on this; the `..` substring and `/`/`\` characters are
/// additionally rejected by the CLI's path-safety guards before the
/// artifact is built.
pub fn is_valid_app_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > 63 {
        return false;
    }
    // First char: lowercase letter or digit.
    let first = bytes[0];
    if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
        return false;
    }
    // Remaining chars: lowercase letter, digit, '.', '_', or '-'.
    for &b in &bytes[1..] {
        if !b.is_ascii_lowercase() && !b.is_ascii_digit() && b != b'.' && b != b'_' && b != b'-' {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_valid_app_name_accepts_valid() {
        assert!(is_valid_app_name("a"));
        assert!(is_valid_app_name("hello-world"));
        assert!(is_valid_app_name("hello_world"));
        assert!(is_valid_app_name("foo.bar"));
        assert!(is_valid_app_name("myapp.v2"));
        assert!(is_valid_app_name("app_v2"));
        assert!(is_valid_app_name("my-app-123"));
        assert!(is_valid_app_name("0"));
        assert!(is_valid_app_name("a".repeat(63).as_str()));
    }

    #[test]
    fn test_is_valid_app_name_rejects_invalid() {
        // Empty
        assert!(!is_valid_app_name(""));
        // Too long (64 chars)
        assert!(!is_valid_app_name(&"a".repeat(64)));
        // Uppercase
        assert!(!is_valid_app_name("Hello"));
        assert!(!is_valid_app_name("HELLO"));
        // Starts with non-alnum (regex first-char constraint)
        assert!(!is_valid_app_name("-hello"));
        assert!(!is_valid_app_name("_hello"));
        assert!(!is_valid_app_name(".foo"));
        // Whitespace
        assert!(!is_valid_app_name("hello world"));
        // Slashes
        assert!(!is_valid_app_name("hello/world"));
        assert!(!is_valid_app_name(r"hello\world"));
        // Path traversal (rejected by both this regex's first-char
        // rule and the CLI's layered path-safety guards)
        assert!(!is_valid_app_name("../traversal"));
        assert!(!is_valid_app_name("a/../b"));
        // Middle-of-string `..` passes the regex's first-char check;
        // the CLI's path-safety guard is the second defense. Flagged
        // for reviewer visibility.
        assert!(is_valid_app_name("a..b"));
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
            original_start_byte: 42,
            original_end_byte: 60,
            pattern: PatternKind::Posix(PosixPattern::Listen),
            snippet: "bind(...)".to_string(),
            arg_nodes: vec![],
            transformability: Transformability::AutoTransformable,
            bound_var: None,
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
            original_start_byte: 100,
            original_end_byte: 145,
            pattern: PatternKind::Rust(RustPattern::TcpBind),
            snippet: "TcpListener::bind(\"127.0.0.1:8080\")".to_string(),
            arg_nodes: vec!["\"127.0.0.1:8080\"".to_string()],
            transformability: Transformability::AutoTransformable,
            bound_var: None,
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
            "transformability": "best-effort"
        }"#;
        let m: PatternMatch = serde_json::from_str(j).unwrap();
        assert_eq!(m.line, 3);
        assert!(m.column.is_none());
        assert_eq!(m.pattern, PatternKind::Posix(PosixPattern::Accept));
        // #128: legacy M1 reports deserializing as Accept now have
        // whatever transformability the on-wire JSON said (here:
        // best-effort). The wire format is unchanged — the
        // transformability flip lives in `transformability()` for
        // freshly-detected matches only.
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
        // C path: AutoTransformable for the bulk, NotTransformable
        // for the un-fixables. #128: Accept flipped from BestEffort
        // to NotTransformable (poll-loop wrapper was syntactically
        // wrong + referenced an undeclared pollable). G3:
        // GetHostByName flipped from AutoTransformable to
        // NotTransformable (was:ip-name-lookup shape doesn't match
        // edge:cloud/networking).
        assert_eq!(
            PatternKind::Posix(PosixPattern::Listen).transformability(),
            Transformability::AutoTransformable
        );
        assert_eq!(
            PatternKind::Posix(PosixPattern::Accept).transformability(),
            Transformability::NotTransformable
        );
        assert_eq!(
            PatternKind::Posix(PosixPattern::GetHostByName).transformability(),
            Transformability::NotTransformable
        );
        assert_eq!(
            PatternKind::Posix(PosixPattern::Poll).transformability(),
            Transformability::NotTransformable
        );
        // Rust path: TcpAccept was BestEffort (busy-spin poll loop)
        // and is now NotTransformable — same MVP reason as the C
        // Accept flip. UdpConnect and ProcessExit are
        // NotTransformable; the rest are AutoTransformable.
        assert_eq!(
            PatternKind::Rust(RustPattern::TcpBind).transformability(),
            Transformability::AutoTransformable
        );
        assert_eq!(
            PatternKind::Rust(RustPattern::TcpAccept).transformability(),
            Transformability::NotTransformable
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
