//! `edge:cloud/websocket` — RFC 6455 WebSocket host implementation.
//!
//! Provides the host side of the WebSocket protocol for LongRunning
//! components. The guest acts as a WebSocket **server**:
//!
//! 1. `listen(port)` — binds a TCP listener on the given port
//! 2. `accept(listener)` — accepts a TCP connection and performs the
//!    HTTP Upgrade handshake (101 Switching Protocols)
//! 3. `send(conn, data, kind)` — encodes an RFC 6455 data frame and
//!    writes it to the connection (unmasked — server-to-client)
//! 4. `receive(conn)` — reads and decodes the next complete frame
//!    from the connection (unmasks — client-to-server is masked)
//! 5. `close(conn, info)` — sends a Close frame and tears down the
//!    connection

use sha1::{Digest, Sha1};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
#[cfg(feature = "scheduling")]
use tokio::runtime::Handle;

/// WebSocket message type as defined by RFC 6455.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MessageType {
    Text,
    Binary,
    Ping,
    Pong,
    Close,
}

/// Convert from the WIT `u8` discriminant.
/// 0=text, 1=binary, 2=ping, 3=pong, 4=close.
#[allow(dead_code)]
fn message_type_from_u8(v: u8) -> Result<MessageType, String> {
    match v {
        0 => Ok(MessageType::Text),
        1 => Ok(MessageType::Binary),
        2 => Ok(MessageType::Ping),
        3 => Ok(MessageType::Pong),
        4 => Ok(MessageType::Close),
        _ => Err(format!("unknown message type: {v}")),
    }
}

#[allow(dead_code)]
fn message_type_to_u8(ty: &MessageType) -> u8 {
    match ty {
        MessageType::Text => 0,
        MessageType::Binary => 1,
        MessageType::Ping => 2,
        MessageType::Pong => 3,
        MessageType::Close => 4,
    }
}

fn opcode_for(ty: &MessageType) -> u8 {
    match ty {
        MessageType::Text => 0x1,
        MessageType::Binary => 0x2,
        MessageType::Close => 0x8,
        MessageType::Ping => 0x9,
        MessageType::Pong => 0xA,
    }
}

fn message_type_from_opcode(opcode: u8) -> Result<MessageType, CloseInfo> {
    match opcode {
        0x1 => Ok(MessageType::Text),
        0x2 => Ok(MessageType::Binary),
        0x8 => Ok(MessageType::Close),
        0x9 => Ok(MessageType::Ping),
        0xA => Ok(MessageType::Pong),
        _ => Err(CloseInfo {
            code: 1002,
            reason: format!("unknown opcode: 0x{opcode:x}"),
        }),
    }
}

/// WebSocket close reason code and message.
#[derive(Debug, Clone)]
pub struct CloseInfo {
    pub code: u16,
    pub reason: String,
}

impl CloseInfo {
    /// Create a CloseInfo with the given code and reason.
    /// Using a function instead of struct literal so it can be called
    /// from macro-generated code where type aliases can't use `Type { }`.
    pub fn new(code: u16, reason: impl Into<String>) -> Self {
        Self {
            code,
            reason: reason.into(),
        }
    }

    fn from_frame_payload(payload: &[u8]) -> Self {
        if payload.len() >= 2 {
            let code = u16::from_be_bytes([payload[0], payload[1]]);
            let reason = if payload.len() > 2 {
                String::from_utf8_lossy(&payload[2..]).to_string()
            } else {
                String::new()
            };
            Self { code, reason }
        } else {
            Self {
                code: 1005,
                reason: String::new(),
            }
        }
    }
}

/// A single active WebSocket connection.
struct Connection {
    stream: TcpStream,
    /// Partial frame buffer (incomplete frame data between reads).
    read_buf: Vec<u8>,
    /// Accumulated payload across fragments of a fragmented message.
    fragmented_payload: Vec<u8>,
    /// Opcode of the first frame in a fragmented message (e.g. 0x1 for text).
    /// `None` when not accumulating fragments.
    fragmented_opcode: Option<u8>,
    /// Set to true when a Close frame has been sent.
    close_sent: bool,
    /// Set to true when a Close frame has been received.
    close_received: bool,
}

/// Per-app WebSocket state.
pub struct WebSocket {
    next_handle: AtomicU32,
    listeners: Mutex<HashMap<u32, Arc<TcpListener>>>,
    connections: Mutex<HashMap<u32, Connection>>,
}

impl WebSocket {
    pub fn new() -> Self {
        Self {
            next_handle: AtomicU32::new(1),
            listeners: Mutex::new(HashMap::new()),
            connections: Mutex::new(HashMap::new()),
        }
    }

    fn alloc_handle(&self) -> u32 {
        self.next_handle.fetch_add(1, Ordering::Relaxed)
    }

    /// Listen for WebSocket connections on the given port.
    /// Returns a listener handle on success.
    pub fn listen(&self, port: u16) -> Result<u32, String> {
        let addr = format!("0.0.0.0:{port}");
        let listener =
            TcpListener::bind(&addr).map_err(|e| format!("failed to bind port {port}: {e}"))?;
        // Keep the listener in blocking mode — accept() in the WIT
        // interface is synchronous and blocks until a client connects.
        let handle = self.alloc_handle();
        let mut listeners = self.listeners.lock().unwrap();
        listeners.insert(handle, Arc::new(listener));
        Ok(handle)
    }

    /// Accept an incoming WebSocket connection.
    ///
    /// Blocks until a client connects, performs the HTTP Upgrade
    /// handshake (RFC 6455 Section 4), and returns a connection handle.
    ///
    /// When running under a tokio runtime (production), the blocking
    /// accept + upgrade is offloaded to `spawn_blocking` to avoid
    /// starving the tokio worker thread. Falls back to direct blocking
    /// when no runtime is available (unit tests).
    pub fn accept(&self, listener: u32) -> Result<u32, String> {
        let listener = {
            let listeners = self.listeners.lock().unwrap();
            listeners
                .get(&listener)
                .ok_or_else(|| "invalid listener handle".to_string())?
                .clone()
        };

        #[cfg(feature = "scheduling")]
        let stream = {
            let listener = listener.clone();
            let upgrade = move || -> Result<TcpStream, String> {
                let (mut stream, _peer_addr) = listener
                    .accept()
                    .map_err(|e| format!("accept failed: {e}"))?;
                stream
                    .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                    .map_err(|e| format!("set read timeout: {e}"))?;
                stream
                    .set_write_timeout(Some(std::time::Duration::from_secs(5)))
                    .map_err(|e| format!("set write timeout: {e}"))?;
                Self::perform_upgrade(&mut stream)?;
                Ok(stream)
            };

            if let Ok(rt) = Handle::try_current() {
                rt.block_on(tokio::task::spawn_blocking(upgrade))
                    .map_err(|e| format!("blocking accept panicked: {e}"))?
            } else {
                upgrade()
            }?
        };

        #[cfg(not(feature = "scheduling"))]
        let stream = {
            let (mut stream, _peer_addr) = listener
                .accept()
                .map_err(|e| format!("accept failed: {e}"))?;
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(5)))
                .map_err(|e| format!("set read timeout: {e}"))?;
            stream
                .set_write_timeout(Some(std::time::Duration::from_secs(5)))
                .map_err(|e| format!("set write timeout: {e}"))?;
            Self::perform_upgrade(&mut stream)?;
            stream
        };

        let handle = self.alloc_handle();
        let mut connections = self.connections.lock().unwrap();
        connections.insert(
            handle,
            Connection {
                stream,
                read_buf: Vec::new(),
                fragmented_payload: Vec::new(),
                fragmented_opcode: None,
                close_sent: false,
                close_received: false,
            },
        );
        Ok(handle)
    }

    /// Perform the HTTP Upgrade handshake (RFC 6455 Section 4).
    fn perform_upgrade(stream: &mut TcpStream) -> Result<(), String> {
        // Read the HTTP request. Read up to 4096 bytes.
        let mut buf = [0u8; 4096];
        let mut total_read = 0usize;

        loop {
            match stream.read(&mut buf[total_read..]) {
                Ok(0) => break, // EOF
                Ok(n) => {
                    total_read += n;
                    // Check if we have the full header (ending with \r\n\r\n).
                    if total_read >= 4 && buf[..total_read].windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                    if total_read >= buf.len() {
                        return Err("request header too large".into());
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // No data yet, spin a bit (non-blocking socket).
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    continue;
                }
                Err(e) => return Err(format!("read error: {e}")),
            }
        }

        let request = String::from_utf8_lossy(&buf[..total_read]);

        // Parse the HTTP request line and headers.
        let lines: Vec<&str> = request.lines().collect();
        if lines.is_empty() {
            return Err("empty HTTP request".into());
        }

        // Validate request line: GET / HTTP/1.1
        let request_line = lines[0];
        let parts: Vec<&str> = request_line.split_whitespace().collect();
        if parts.len() < 2 || parts[0] != "GET" {
            return Err(format!("invalid request line: {request_line}"));
        }

        // Parse headers.
        let mut upgrade = false;
        let mut connection_upgrade = false;
        let mut websocket_key = None;

        let header_lines: Vec<&str> = lines
            .iter()
            .skip(1)
            .take_while(|l| !l.is_empty())
            .copied()
            .collect();
        for hl in &header_lines {
            if let Some((key, value)) = hl.split_once(':') {
                let k = key.trim().to_lowercase();
                let v = value.trim();
                match k.as_str() {
                    "upgrade" if v.eq_ignore_ascii_case("websocket") => upgrade = true,
                    "connection" if v.to_lowercase().contains("upgrade") => {
                        connection_upgrade = true
                    }
                    "sec-websocket-key" => websocket_key = Some(v.to_string()),
                    _ => {}
                }
            }
        }

        if !upgrade {
            return Err("missing Upgrade: websocket header".into());
        }
        if !connection_upgrade {
            return Err("missing Connection: Upgrade header".into());
        }
        let key = websocket_key.ok_or_else(|| "missing Sec-WebSocket-Key header".to_string())?;

        // Compute Sec-WebSocket-Accept.
        let accept = compute_accept_key(&key);

        // Send 101 Switching Protocols response.
        let response = format!(
            "HTTP/1.1 101 Switching Protocols\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Accept: {accept}\r\n\
             \r\n"
        );
        stream
            .write_all(response.as_bytes())
            .map_err(|e| format!("write upgrade response: {e}"))?;

        Ok(())
    }

    /// Send a WebSocket data frame (unmasked — server-to-client).
    pub fn send(&self, conn: u32, data: &[u8], kind: MessageType) -> Result<(), String> {
        let mut connections = self.connections.lock().unwrap();
        let conn = connections
            .get_mut(&conn)
            .ok_or_else(|| "invalid connection handle".to_string())?;

        if conn.close_sent {
            return Err("connection already closed".into());
        }

        if kind == MessageType::Close {
            conn.close_sent = true;
        }

        // Encode and send the frame.
        let frame = encode_frame(data, &kind, false); // unmasked
        conn.stream
            .write_all(&frame)
            .map_err(|e| format!("send failed: {e}"))?;

        Ok(())
    }

    /// Receive the next complete WebSocket message.
    ///
    /// Handles fragmentation (FIN + continuation frames) transparently
    /// per RFC 6455 §5.4 — accumulates fragments until FIN=1 is received.
    /// Client-to-server frames are masked; we unmask them here.
    /// Control frames (Ping/Pong/Close) interleaved between fragments are
    /// handled immediately (Ping → Pong response, Pong → ignored).
    pub fn receive(&self, conn: u32) -> Result<(Vec<u8>, MessageType), CloseInfo> {
        let mut connections = self.connections.lock().unwrap();
        let conn = connections.get_mut(&conn).ok_or_else(|| CloseInfo {
            code: 1005,
            reason: "invalid connection handle".into(),
        })?;

        if conn.close_received {
            return Err(CloseInfo {
                code: 1005,
                reason: "connection already closed".into(),
            });
        }

        loop {
            // Get data: prefer buffered data, otherwise read from stream.
            let buf = if !conn.read_buf.is_empty() {
                std::mem::take(&mut conn.read_buf)
            } else {
                let mut read_buf = [0u8; 65536];
                match conn.stream.read(&mut read_buf) {
                    Ok(0) => {
                        return Err(CloseInfo {
                            code: 1006,
                            reason: "connection closed".into(),
                        })
                    }
                    Ok(n) => read_buf[..n].to_vec(),
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        if conn.fragmented_opcode.is_some() {
                            return Err(CloseInfo {
                                code: 1006,
                                reason: "fragmentation timed out".into(),
                            });
                        }
                        return Err(CloseInfo {
                            code: 1006,
                            reason: "no data available".into(),
                        });
                    }
                    Err(e) => {
                        return Err(CloseInfo {
                            code: 1006,
                            reason: format!("read error: {e}"),
                        })
                    }
                }
            };

            match Self::parse_frame(&buf, conn) {
                FrameParseOutcome::Message(data, ty) => return Ok((data, ty)),
                FrameParseOutcome::MoreFragments => continue,
                FrameParseOutcome::Pong => continue,
                FrameParseOutcome::CloseReceived(ci) => return Err(ci),
                FrameParseOutcome::ProtocolError(ci) => return Err(ci),
            }
        }
    }

    /// Parse a single WebSocket frame from `buf`.
    ///
    /// **RFC 6455 §5.4 fragmentation handling:**
    /// - FIN=0 + opcode ≠ 0x0 → first fragment: stores opcode + payload in
    ///   `conn.fragmented_opcode` / `conn.fragmented_payload`, returns `MoreFragments`.
    /// - FIN=0/1 + opcode = 0x0 → continuation: appends payload to
    ///   `conn.fragmented_payload`. Returns `MoreFragments` unless FIN=1 (final),
    ///   in which case returns the reassembled `Message`.
    /// - FIN=1 + opcode ≠ 0x0 → non-fragmented: returns `Message` directly.
    ///
    /// **Control frames** (opcode 0x8-0xF) are handled immediately per
    /// RFC 6455 §5.4: Ping → Pong response written to stream, returns `Pong`.
    /// Pong → returns `Pong`. Close → returns `CloseReceived`.
    fn parse_frame(buf: &[u8], conn: &mut Connection) -> FrameParseOutcome {
        if buf.len() < 2 {
            conn.read_buf = buf.to_vec();
            return FrameParseOutcome::MoreFragments;
        }

        let b0 = buf[0];
        let b1 = buf[1];
        let fin = (b0 & 0x80) != 0;
        let opcode = b0 & 0x0F;
        let masked = (b1 & 0x80) != 0;
        let mut payload_len = (b1 & 0x7F) as u64;
        let mut offset = 2usize;

        // Extended payload length.
        if payload_len == 126 {
            if buf.len() < 4 {
                conn.read_buf = buf.to_vec();
                return FrameParseOutcome::MoreFragments;
            }
            payload_len = u16::from_be_bytes([buf[2], buf[3]]) as u64;
            offset = 4;
        } else if payload_len == 127 {
            if buf.len() < 10 {
                conn.read_buf = buf.to_vec();
                return FrameParseOutcome::MoreFragments;
            }
            payload_len = u64::from_be_bytes([
                buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9],
            ]);
            offset = 10;
        }

        // Masking key.
        let mask_key = if masked {
            if buf.len() < offset + 4 {
                conn.read_buf = buf.to_vec();
                return FrameParseOutcome::MoreFragments;
            }
            let key = [
                buf[offset],
                buf[offset + 1],
                buf[offset + 2],
                buf[offset + 3],
            ];
            offset += 4;
            Some(key)
        } else {
            None
        };

        // Check we have the full payload.
        let end = offset + payload_len as usize;
        if buf.len() < end {
            conn.read_buf = buf.to_vec();
            return FrameParseOutcome::MoreFragments;
        }

        let mut payload = buf[offset..end].to_vec();

        // Unmask if needed (client-to-server frames are masked).
        if let Some(key) = mask_key {
            for (i, byte) in payload.iter_mut().enumerate() {
                *byte ^= key[i % 4];
            }
        }

        // Store any extra data after this frame for the next read.
        if end < buf.len() {
            conn.read_buf = buf[end..].to_vec();
        } else {
            conn.read_buf.clear();
        }

        // Control frames (opcode 0x8-0xF) — handle immediately per RFC 6455 §5.4.
        // Control frames MAY be interleaved between fragments of a data message.
        if opcode >= 0x8 {
            return match opcode {
                0x8 => {
                    conn.close_received = true;
                    FrameParseOutcome::CloseReceived(CloseInfo::from_frame_payload(&payload))
                }
                0x9 => {
                    // Respond with Pong frame immediately (RFC 6455 §5.5.3).
                    // We have `&mut Connection` so we can write to the stream.
                    let pong_frame = encode_frame(&payload, &MessageType::Pong, false);
                    let _ = (&conn.stream).write_all(&pong_frame);
                    FrameParseOutcome::Pong
                }
                0xA => FrameParseOutcome::Pong,
                _ => FrameParseOutcome::ProtocolError(CloseInfo {
                    code: 1002,
                    reason: format!("unknown control opcode: 0x{opcode:x}"),
                }),
            };
        }

        // Data frames (opcode 0x0-0x7)

        // Continuation frame (opcode = 0x0) — must be mid-fragmentation.
        if opcode == 0x0 {
            let Some(_) = conn.fragmented_opcode else {
                return FrameParseOutcome::ProtocolError(CloseInfo {
                    code: 1002,
                    reason: "unexpected continuation frame".into(),
                });
            };
            conn.fragmented_payload.extend_from_slice(&payload);
            if fin {
                // Final continuation frame — complete message ready.
                let full_payload = std::mem::take(&mut conn.fragmented_payload);
                let original_opcode = conn.fragmented_opcode.take().unwrap();
                let msg_type = match message_type_from_opcode(original_opcode) {
                    Ok(ty) => ty,
                    Err(ci) => return FrameParseOutcome::ProtocolError(ci),
                };
                return FrameParseOutcome::Message(full_payload, msg_type);
            }
            // More continuation frames coming.
            return FrameParseOutcome::MoreFragments;
        }

        // Non-continuation data frame
        if conn.fragmented_opcode.is_some() {
            // Already accumulating fragments — expected a continuation frame.
            return FrameParseOutcome::ProtocolError(CloseInfo {
                code: 1002,
                reason: "expected continuation frame".into(),
            });
        }

        if !fin {
            // First fragment of a fragmented message (FIN=0, opcode ≠ 0x0).
            conn.fragmented_opcode = Some(opcode);
            conn.fragmented_payload = payload;
            return FrameParseOutcome::MoreFragments;
        }

        // Non-fragmented message (FIN=1, opcode ≠ 0x0).
        let msg_type = match message_type_from_opcode(opcode) {
            Ok(ty) => ty,
            Err(ci) => return FrameParseOutcome::ProtocolError(ci),
        };
        FrameParseOutcome::Message(payload, msg_type)
    }

    /// Close the WebSocket connection.
    ///
    /// Sends a Close frame if one hasn't been sent yet, then closes the
    /// TCP stream.
    pub fn close(&self, conn_handle: u32, info: CloseInfo) -> Result<(), String> {
        let mut connections = self.connections.lock().unwrap();
        // Scope the mutable borrow so we can call remove after.
        {
            let entry = connections
                .get_mut(&conn_handle)
                .ok_or_else(|| "invalid connection handle".to_string())?;

            if !entry.close_sent {
                // Encode and send close frame.
                let mut payload = Vec::with_capacity(2 + info.reason.len());
                payload.extend_from_slice(&info.code.to_be_bytes());
                payload.extend_from_slice(info.reason.as_bytes());
                let frame = encode_frame(&payload, &MessageType::Close, false);
                let _ = entry.stream.write_all(&frame);
                entry.close_sent = true;
            }
        } // entry dropped here, borrow released

        // Remove from connections map (drops the TcpStream).
        let _ = connections.remove(&conn_handle);
        Ok(())
    }
}

impl Default for WebSocket {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of parsing a single WebSocket frame.
enum FrameParseOutcome {
    /// A complete message (non-fragmented or final-continuation).
    Message(Vec<u8>, MessageType),
    /// Start or continuation of a fragmented message — need more data.
    MoreFragments,
    /// A Pong frame received (or a Ping that we responded to) — continue.
    Pong,
    /// A Close frame received.
    CloseReceived(CloseInfo),
    /// A protocol error occurred.
    ProtocolError(CloseInfo),
}

// ── RFC 6455 helpers ─────────────────────────────────────────────────────

/// The magic GUID used in the WebSocket upgrade handshake (RFC 6455 §4.2.2).
const WS_GUID: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/// Compute `Sec-WebSocket-Accept` from the client's `Sec-WebSocket-Key`.
fn compute_accept_key(key: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(key.as_bytes());
    hasher.update(WS_GUID);
    let result = hasher.finalize();
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(result)
}

/// Encode an RFC 6455 data frame.
///
/// `masked` should be `false` for server-to-client frames (per RFC 6455
/// §5.1: "A server MUST NOT mask any frames that it sends to the client").
fn encode_frame(data: &[u8], kind: &MessageType, masked: bool) -> Vec<u8> {
    let opcode = opcode_for(kind);
    let mut frame = Vec::new();

    // Byte 0: FIN=1, opcode
    frame.push(0x80 | opcode);

    // Byte 1+: payload length
    let len = data.len();
    if len < 126 {
        let mut b1 = len as u8;
        if masked {
            b1 |= 0x80;
        }
        frame.push(b1);
    } else if len <= 0xFFFF {
        let mut b1 = 126u8;
        if masked {
            b1 |= 0x80;
        }
        frame.push(b1);
        frame.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        let mut b1 = 127u8;
        if masked {
            b1 |= 0x80;
        }
        frame.push(b1);
        frame.extend_from_slice(&(len as u64).to_be_bytes());
    }

    // Masking key (only for client-to-server frames).
    if masked {
        // Use a zero mask key for simplicity (acceptable per RFC 6455).
        frame.extend_from_slice(&[0u8; 4]);
    }

    // Payload.
    frame.extend_from_slice(data);

    frame
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpStream;
    use std::thread;
    use std::time::Duration;

    // ── Unit tests (existing) ─────────────────────────────────────────

    #[test]
    fn compute_accept_key_known_value() {
        // From RFC 6455 Section 4.2.2 example.
        let key = "dGhlIHNhbXBsZSBub25jZQ==";
        let accept = compute_accept_key(key);
        assert_eq!(accept, "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
    }

    #[test]
    fn encode_decode_text_frame() {
        let data = b"Hello, WebSocket!";
        let frame = encode_frame(data, &MessageType::Text, false);
        assert_eq!(frame[0], 0x81, "FIN=1, opcode=text");
        assert_eq!(frame[1] & 0x80, 0, "unmasked");
        assert_eq!((frame[1] & 0x7F) as usize, data.len(), "payload length");
        assert_eq!(&frame[2..], data, "payload");
    }

    #[test]
    fn encode_decode_binary_frame() {
        let data = vec![0x00, 0x01, 0x02, 0xFF, 0xFE];
        let frame = encode_frame(&data, &MessageType::Binary, false);
        assert_eq!(frame[0], 0x82, "FIN=1, opcode=binary");
        assert_eq!(&frame[2..], &data);
    }

    #[test]
    fn encode_close_frame() {
        let payload = vec![0x03, 0xE8]; // code 1000
        let frame = encode_frame(&payload, &MessageType::Close, false);
        assert_eq!(frame[0], 0x88, "FIN=1, opcode=close");
        assert_eq!(&frame[2..], &payload);
    }

    #[test]
    fn encode_large_frame() {
        let data = vec![0x42u8; 200];
        let frame = encode_frame(&data, &MessageType::Text, false);
        assert_eq!(frame[0], 0x81);
        assert_eq!(frame[1], 126u8, "126 => extended 16-bit length");
        assert_eq!(
            u16::from_be_bytes([frame[2], frame[3]]) as usize,
            data.len()
        );
        assert_eq!(&frame[4..], &data);
    }

    #[test]
    fn encode_very_large_frame() {
        let data = vec![0x42u8; 70000];
        let frame = encode_frame(&data, &MessageType::Text, false);
        assert_eq!(frame[0], 0x81);
        assert_eq!(frame[1], 127u8, "127 => extended 64-bit length");
        assert_eq!(
            u64::from_be_bytes([
                frame[2], frame[3], frame[4], frame[5], frame[6], frame[7], frame[8], frame[9]
            ]) as usize,
            data.len()
        );
    }

    #[test]
    fn masked_frame_encode_decode() {
        let data = b"masked data";
        let frame = encode_frame(data, &MessageType::Text, true);
        assert_eq!(frame[0], 0x81);
        assert!(frame[1] & 0x80 != 0, "masked bit must be set");
        let masked_len = (frame[1] & 0x7F) as usize;
        assert_eq!(masked_len, data.len());
        let payload = &frame[6..];
        let mask_key = &frame[2..6];
        let mut unmasked = payload.to_vec();
        for (i, byte) in unmasked.iter_mut().enumerate() {
            *byte ^= mask_key[i % 4];
        }
        assert_eq!(&unmasked, data);
    }

    #[test]
    fn message_type_conversion_roundtrip() {
        let cases = [
            (0, MessageType::Text),
            (1, MessageType::Binary),
            (2, MessageType::Ping),
            (3, MessageType::Pong),
            (4, MessageType::Close),
        ];
        for (disc, expected) in &cases {
            let ty = message_type_from_u8(*disc).unwrap();
            assert_eq!(ty, *expected);
        }
    }

    #[test]
    fn message_type_from_u8_unknown_is_error() {
        assert!(message_type_from_u8(5).is_err());
        assert!(message_type_from_u8(255).is_err());
    }

    #[test]
    fn opcode_to_message_type_roundtrip() {
        let cases = [
            (0x1, MessageType::Text),
            (0x2, MessageType::Binary),
            (0x8, MessageType::Close),
            (0x9, MessageType::Ping),
            (0xA, MessageType::Pong),
        ];
        for (op, expected) in &cases {
            let ty = message_type_from_opcode(*op).unwrap();
            assert_eq!(ty, *expected);
        }
    }

    #[test]
    fn close_info_from_frame_payload() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&1001u16.to_be_bytes());
        payload.extend_from_slice(b"going away");
        let ci = CloseInfo::from_frame_payload(&payload);
        assert_eq!(ci.code, 1001);
        assert_eq!(ci.reason, "going away");
    }

    #[test]
    fn close_info_from_empty_frame_payload() {
        let ci = CloseInfo::from_frame_payload(&[]);
        assert_eq!(ci.code, 1005);
    }

    // ── Integration tests: listen → accept → send/receive → close ─────

    /// Full WebSocket round-trip: listen on a port, connect a client,
    /// perform HTTP Upgrade, exchange a text frame, exchange a binary
    /// frame, then close.
    #[test]
    fn ws_full_round_trip() {
        let ws = Arc::new(WebSocket::new());
        let port = get_free_port();

        // Server: listen
        let listener = ws.listen(port).expect("listen");

        // Server thread: accept and echo. The ws instance is shared
        // via Arc so listener state is visible from both threads.
        let ws_server = ws.clone();
        let server = thread::spawn(move || {
            let conn = ws_server.accept(listener).expect("accept");
            // Receive text frame
            let (data, msg_type) = ws_server.receive(conn).expect("receive text");
            assert_eq!(msg_type, MessageType::Text);
            assert_eq!(data, b"Hello, server!");

            // Echo back
            ws_server
                .send(conn, &data, MessageType::Text)
                .expect("send text");

            // Receive binary frame
            let (data, msg_type) = ws_server.receive(conn).expect("receive binary");
            assert_eq!(msg_type, MessageType::Binary);
            assert_eq!(data, b"\x00\x01\x02\xFF");

            // Echo back
            ws_server
                .send(conn, &data, MessageType::Binary)
                .expect("send binary");

            // Receive close
            let result = ws_server.receive(conn);
            assert!(result.is_err(), "close frame should return Err");
            let close_err = result.unwrap_err();
            assert_eq!(close_err.code, 1000);

            // Send close response
            ws_server
                .close(conn, CloseInfo::new(1000, "bye"))
                .expect("close");
        });

        // Give server a moment to bind
        thread::sleep(Duration::from_millis(100));

        // Client: connect and perform HTTP Upgrade
        let key = "dGhlIHNhbXBsZSBub25jZQ==";
        let upgrade_req = format!(
            "GET /chat HTTP/1.1\r\n\
             Host: server.example.com\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Key: {key}\r\n\
             Sec-WebSocket-Version: 13\r\n\
             \r\n"
        );

        let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("client connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();

        use std::io::{Read, Write};
        stream
            .write_all(upgrade_req.as_bytes())
            .expect("write upgrade");

        // Read the 101 response
        let mut resp_buf = [0u8; 4096];
        let n = stream.read(&mut resp_buf).expect("read response");
        let resp = String::from_utf8_lossy(&resp_buf[..n]);
        assert!(resp.contains("101"), "expected 101, got: {resp}");
        assert!(
            resp.contains("Sec-WebSocket-Accept"),
            "missing accept: {resp}"
        );
        assert_eq!(
            compute_accept_key(key),
            parse_header(&resp, "sec-websocket-accept"),
            "accept key mismatch"
        );

        // Client: send a masked text frame
        let text_frame = encode_frame(b"Hello, server!", &MessageType::Text, true);
        stream.write_all(&text_frame).expect("write text frame");

        // Read the unmasked echo
        let mut buf = [0u8; 4096];
        let n = stream.read(&mut buf).expect("read echo");
        let (payload, msg_type) = parse_client_frame(&buf[..n]);
        assert_eq!(msg_type, MessageType::Text);
        assert_eq!(payload, b"Hello, server!");

        // Client: send a masked binary frame
        let bin_frame = encode_frame(b"\x00\x01\x02\xFF", &MessageType::Binary, true);
        stream.write_all(&bin_frame).expect("write bin frame");

        let n = stream.read(&mut buf).expect("read bin echo");
        let (payload, msg_type) = parse_client_frame(&buf[..n]);
        assert_eq!(msg_type, MessageType::Binary);
        assert_eq!(payload, b"\x00\x01\x02\xFF");

        // Client: send close
        let close_payload = vec![0x03, 0xE8]; // code 1000
        let close_frame = encode_frame(&close_payload, &MessageType::Close, true);
        stream.write_all(&close_frame).expect("write close");

        // Read close response
        let n = stream.read(&mut buf).expect("read close resp");
        let (_payload, msg_type) = parse_client_frame(&buf[..n]);
        assert_eq!(msg_type, MessageType::Close);

        server.join().expect("server thread");
    }

    // ── Fragmentation tests ──────────────────────────────────────────

    /// Helper: encode a WebSocket frame with FIN=0 (fragment).
    /// `fin` controls the FIN bit; pass `false` for a fragment frame.
    fn encode_fragment(data: &[u8], kind: &MessageType, fin: bool, masked: bool) -> Vec<u8> {
        let opcode = opcode_for(kind);
        let mut frame = Vec::new();
        let b0 = if fin { 0x80 | opcode } else { opcode };
        frame.push(b0);
        let len = data.len();
        if len < 126 {
            let mut b1 = len as u8;
            if masked {
                b1 |= 0x80;
            }
            frame.push(b1);
        } else if len <= 0xFFFF {
            let mut b1 = 126u8;
            if masked {
                b1 |= 0x80;
            }
            frame.push(b1);
            frame.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            let mut b1 = 127u8;
            if masked {
                b1 |= 0x80;
            }
            frame.push(b1);
            frame.extend_from_slice(&(len as u64).to_be_bytes());
        }
        if masked {
            frame.extend_from_slice(&[0u8; 4]);
        }
        frame.extend_from_slice(data);
        frame
    }

    /// Helper: encode a continuation frame (opcode=0x0).
    fn encode_continuation(data: &[u8], fin: bool, masked: bool) -> Vec<u8> {
        let mut frame = Vec::new();
        let b0 = if fin { 0x80 } else { 0x00 };
        frame.push(b0);
        let len = data.len();
        if len < 126 {
            let mut b1 = len as u8;
            if masked {
                b1 |= 0x80;
            }
            frame.push(b1);
        } else if len <= 0xFFFF {
            let mut b1 = 126u8;
            if masked {
                b1 |= 0x80;
            }
            frame.push(b1);
            frame.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            let mut b1 = 127u8;
            if masked {
                b1 |= 0x80;
            }
            frame.push(b1);
            frame.extend_from_slice(&(len as u64).to_be_bytes());
        }
        if masked {
            frame.extend_from_slice(&[0u8; 4]);
        }
        frame.extend_from_slice(data);
        frame
    }

    #[test]
    fn fragmented_text_message_2_frames() {
        let ws = WebSocket::new();
        let port = get_free_port();
        let listener = ws.listen(port).expect("listen");

        let ws_server = Arc::new(ws);
        let ws_clone = ws_server.clone();
        let server = thread::spawn(move || {
            let conn = ws_clone.accept(listener).expect("accept");
            let (data, msg_type) = ws_clone.receive(conn).expect("receive fragmented");
            assert_eq!(msg_type, MessageType::Text);
            assert_eq!(String::from_utf8_lossy(&data), "Hello, fragmented!");
            // Echo back
            ws_clone
                .send(conn, &data, MessageType::Text)
                .expect("send echo");
            // Close
            let result = ws_clone.receive(conn);
            assert!(result.is_err());
            ws_clone
                .close(conn, CloseInfo::new(1000, "ok"))
                .expect("close");
        });

        thread::sleep(Duration::from_millis(100));

        // Client: connect + upgrade
        let mut stream = do_upgrade_client(port, "dGhlIHNhbXBsZSBub25jZQ==");
        // Send first fragment: "Hello, "
        let frag1 = encode_fragment(b"Hello, ", &MessageType::Text, false, true);
        stream.write_all(&frag1).expect("write frag1");
        // Send continuation (final): "fragmented!"
        let frag2 = encode_continuation(b"fragmented!", true, true);
        stream.write_all(&frag2).expect("write frag2");
        // Read echo
        let mut buf = [0u8; 4096];
        let n = stream.read(&mut buf).expect("read echo");
        let (payload, msg_type) = parse_client_frame(&buf[..n]);
        assert_eq!(msg_type, MessageType::Text);
        assert_eq!(String::from_utf8_lossy(&payload), "Hello, fragmented!");
        // Close
        let close = encode_frame(&1000u16.to_be_bytes(), &MessageType::Close, true);
        stream.write_all(&close).expect("write close");
        server.join().expect("server thread");
    }

    #[test]
    fn fragmented_text_message_3_frames() {
        let ws = Arc::new(WebSocket::new());
        let port = get_free_port();
        let listener = ws.listen(port).expect("listen");

        let ws_server = ws.clone();
        let server = thread::spawn(move || {
            let conn = ws_server.accept(listener).expect("accept");
            let (data, msg_type) = ws_server.receive(conn).expect("receive 3-fragment");
            assert_eq!(msg_type, MessageType::Text);
            assert_eq!(String::from_utf8_lossy(&data), "Three fragments!");
            ws_server
                .send(conn, &data, MessageType::Text)
                .expect("send echo");
            let _ = ws_server.receive(conn);
            ws_server
                .close(conn, CloseInfo::new(1000, "ok"))
                .expect("close");
        });

        thread::sleep(Duration::from_millis(100));

        let mut stream = do_upgrade_client(port, "dGhlIHNhbXBsZSBub25jZQ==");
        stream
            .write_all(&encode_fragment(b"Three ", &MessageType::Text, false, true))
            .expect("frag1");
        stream
            .write_all(&encode_continuation(b"fragm", false, true))
            .expect("frag2");
        stream
            .write_all(&encode_continuation(b"ents!", true, true))
            .expect("frag3");

        let mut buf = [0u8; 4096];
        let n = stream.read(&mut buf).expect("read echo");
        let (payload, msg_type) = parse_client_frame(&buf[..n]);
        assert_eq!(msg_type, MessageType::Text);
        assert_eq!(String::from_utf8_lossy(&payload), "Three fragments!");

        let close = encode_frame(&1000u16.to_be_bytes(), &MessageType::Close, true);
        stream.write_all(&close).expect("write close");
        server.join().expect("server thread");
    }

    #[test]
    fn fragmented_binary_3_frames() {
        let ws = Arc::new(WebSocket::new());
        let port = get_free_port();
        let listener = ws.listen(port).expect("listen");

        let ws_server = ws.clone();
        let server = thread::spawn(move || {
            let conn = ws_server.accept(listener).expect("accept");
            let (data, msg_type) = ws_server.receive(conn).expect("receive fragmented binary");
            assert_eq!(msg_type, MessageType::Binary);
            assert_eq!(data, vec![0x01, 0x02, 0x03, 0x04, 0x05]);
            ws_server
                .send(conn, &data, MessageType::Binary)
                .expect("send echo");
            let _ = ws_server.receive(conn);
            ws_server
                .close(conn, CloseInfo::new(1000, "ok"))
                .expect("close");
        });

        thread::sleep(Duration::from_millis(100));

        let mut stream = do_upgrade_client(port, "dGhlIHNhbXBsZSBub25jZQ==");
        stream
            .write_all(&encode_fragment(
                &[0x01, 0x02],
                &MessageType::Binary,
                false,
                true,
            ))
            .expect("frag1");
        stream
            .write_all(&encode_continuation(&[0x03], false, true))
            .expect("frag2");
        stream
            .write_all(&encode_continuation(&[0x04, 0x05], true, true))
            .expect("frag3");

        let mut buf = [0u8; 4096];
        let n = stream.read(&mut buf).expect("read echo");
        let (payload, msg_type) = parse_client_frame(&buf[..n]);
        assert_eq!(msg_type, MessageType::Binary);
        assert_eq!(payload, vec![0x01, 0x02, 0x03, 0x04, 0x05]);

        let close = encode_frame(&1000u16.to_be_bytes(), &MessageType::Close, true);
        stream.write_all(&close).expect("write close");
        server.join().expect("server thread");
    }

    #[test]
    fn interleaved_ping_during_fragmentation_does_not_disrupt() {
        let ws = Arc::new(WebSocket::new());
        let port = get_free_port();
        let listener = ws.listen(port).expect("listen");

        let ws_server = ws.clone();
        let server = thread::spawn(move || {
            let conn = ws_server.accept(listener).expect("accept");
            // Receive fragmented message with interleaved ping
            let (data, msg_type) = ws_server.receive(conn).expect("receive after ping");
            assert_eq!(msg_type, MessageType::Text);
            assert_eq!(String::from_utf8_lossy(&data), "Hello!");
            ws_server
                .send(conn, &data, MessageType::Text)
                .expect("send echo");
            let _ = ws_server.receive(conn);
            ws_server
                .close(conn, CloseInfo::new(1000, "ok"))
                .expect("close");
        });

        thread::sleep(Duration::from_secs(1));

        let mut stream = do_upgrade_client(port, "dGhlIHNhbXBsZSBub25jZQ==");
        // Send first fragment
        stream
            .write_all(&encode_fragment(b"He", &MessageType::Text, false, true))
            .expect("frag1");
        // Send a ping mid-fragmentation
        let ping = encode_fragment(b"ping", &MessageType::Ping, true, true);
        stream.write_all(&ping).expect("ping");
        // Wait for server's pong response
        let mut buf = [0u8; 4096];
        let n = stream.read(&mut buf).expect("read pong");
        let (_payload, msg_type) = parse_client_frame(&buf[..n]);
        assert_eq!(msg_type, MessageType::Pong, "should get Pong for our Ping");
        // Send final continuation
        stream
            .write_all(&encode_continuation(b"llo!", true, true))
            .expect("frag3");

        let n = stream.read(&mut buf).expect("read echo");
        let (payload, msg_type) = parse_client_frame(&buf[..n]);
        assert_eq!(msg_type, MessageType::Text);
        assert_eq!(String::from_utf8_lossy(&payload), "Hello!");

        let close = encode_frame(&1000u16.to_be_bytes(), &MessageType::Close, true);
        stream.write_all(&close).expect("write close");
        server.join().expect("server thread");
    }

    #[test]
    fn unexpected_continuation_frame_returns_protocol_error() {
        let ws = Arc::new(WebSocket::new());
        let port = get_free_port();
        let listener = ws.listen(port).expect("listen");

        let ws_server = ws.clone();
        let server = thread::spawn(move || {
            let conn = ws_server.accept(listener).expect("accept");
            let result = ws_server.receive(conn);
            assert!(
                result.is_err(),
                "continuation without fragment should error"
            );
            let err = result.unwrap_err();
            assert_eq!(err.code, 1002, "should get protocol error");
        });

        thread::sleep(Duration::from_millis(100));

        let mut stream = do_upgrade_client(port, "dGhlIHNhbXBsZSBub25jZQ==");
        // Send a continuation frame without a preceding fragment
        stream
            .write_all(&encode_continuation(b"no fragment!", true, true))
            .expect("write continuation");
        server.join().expect("server thread");
    }

    // ── Helper for client WebSocket upgrade ──────────────────────────

    /// Connect to `port` and perform the HTTP Upgrade handshake.
    /// Returns the upgraded `TcpStream`.
    fn do_upgrade_client(port: u16, key: &str) -> TcpStream {
        let upgrade_req = format!(
            "GET /chat HTTP/1.1\r\n\
             Host: server.example.com\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Key: {key}\r\n\
             Sec-WebSocket-Version: 13\r\n\
             \r\n"
        );
        let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("client connect");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        use std::io::Write;
        stream
            .write_all(upgrade_req.as_bytes())
            .expect("write upgrade");
        // Read and validate the 101 response
        let mut resp_buf = [0u8; 4096];
        let n = stream.read(&mut resp_buf).expect("read response");
        let resp = String::from_utf8_lossy(&resp_buf[..n]);
        assert!(resp.contains("101"), "expected 101, got: {resp}");
        stream
    }

    // ── Helpers ───────────────────────────────────────────────────────

    /// Parse a server-to-client (unmasked) frame.
    fn parse_client_frame(buf: &[u8]) -> (Vec<u8>, MessageType) {
        assert!(buf.len() >= 2, "frame too short");
        let opcode = buf[0] & 0x0F;
        let payload_len = (buf[1] & 0x7F) as usize;
        let msg_type = message_type_from_opcode(opcode).expect("valid opcode");
        let payload = if payload_len < 126 {
            buf[2..2 + payload_len].to_vec()
        } else if payload_len == 126 {
            let len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
            buf[4..4 + len].to_vec()
        } else {
            let len = u64::from_be_bytes([
                buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9],
            ]) as usize;
            buf[10..10 + len].to_vec()
        };
        (payload, msg_type)
    }

    fn parse_header(resp: &str, name: &str) -> String {
        for line in resp.lines() {
            if let Some((k, v)) = line.split_once(':') {
                if k.trim().eq_ignore_ascii_case(name) {
                    return v.trim().to_string();
                }
            }
        }
        String::new()
    }

    fn get_free_port() -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        port
    }
}
