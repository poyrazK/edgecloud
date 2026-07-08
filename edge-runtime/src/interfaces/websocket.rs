//! `edge:cloud/websocket` — WebSocket connection hosting.
//!
//! The full RFC 6455 host implementation (frame encoding, HTTP upgrade,
//! ping/pong, fragmentation) lands in a follow-up PR. This stub reserves
//! the types and structure so the WIT interface, linker registration,
//! and RuntimeState field can ship independently.

/// WebSocket message type as defined by RFC 6455.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MessageType {
    Text,
    Binary,
    Ping,
    Pong,
    Close,
}

/// WebSocket close reason code and message.
#[derive(Debug, Clone)]
pub struct CloseInfo {
    pub code: u16,
    pub reason: String,
}

/// Per-app WebSocket state.
pub struct WebSocket;

impl WebSocket {
    pub fn new() -> Self {
        Self
    }

    pub fn listen(&self, _port: u16) -> Result<u32, String> {
        Err("websocket not yet implemented".into())
    }

    pub fn accept(&self, _listener: u32) -> Result<u32, String> {
        Err("websocket not yet implemented".into())
    }

    pub fn send(&self, _conn: u32, _data: &[u8], _kind: MessageType) {
    }

    pub fn receive(&self, _conn: u32) -> Result<(Vec<u8>, MessageType), CloseInfo> {
        Err(CloseInfo { code: 1011, reason: "not implemented".into() })
    }

    pub fn close(&self, _conn: u32, _info: CloseInfo) {
    }
}

impl Default for WebSocket {
    fn default() -> Self {
        Self::new()
    }
}
