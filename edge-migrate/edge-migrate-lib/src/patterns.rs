//! POSIX pattern definitions and WASI equivalents.
//!
//! Defines all POSIX patterns that edge-migrate can detect, their
//! transformability classification, and WASI equivalents.

use serde::{Deserialize, Serialize};

/// Classification of how transformable a POSIX pattern is.
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

/// A detected POSIX pattern in source code.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatternMatch {
    /// 1-based line number where the pattern was detected.
    pub line: usize,
    /// Start byte offset in the source (for replacement).
    pub start_byte: usize,
    /// End byte offset in the source (for replacement).
    pub end_byte: usize,
    /// The kind of POSIX pattern detected.
    pub pattern: PosixPattern,
    /// The original source code snippet.
    pub snippet: String,
    /// Raw text of each argument node from the AST (for accurate arg extraction).
    pub arg_nodes: Vec<String>,
    /// Whether this pattern can be auto-transformed.
    pub transformability: Transformability,
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

impl PosixPattern {
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
