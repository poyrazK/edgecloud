//! `edge:http-server` — inbound HTTP serving.

use crate::metering::RequestMeter;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, Mutex as TokioMutex, RwLock};
use tokio::sync::{oneshot, Semaphore};
use tokio::time::{timeout_at, Instant};

/// Enum to hold either a plain TCP stream or a TLS stream.
/// Allows a single handle_connection to work with both without dyn Trait.
enum StreamKind {
    Plain(tokio::net::TcpStream),
    Tls(Box<tokio_rustls::server::TlsStream<tokio::net::TcpStream>>),
}

impl AsyncRead for StreamKind {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            StreamKind::Plain(s) => std::pin::Pin::new(s).poll_read(cx, buf),
            StreamKind::Tls(s) => std::pin::Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for StreamKind {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::result::Result<usize, std::io::Error>> {
        match self.get_mut() {
            StreamKind::Plain(s) => std::pin::Pin::new(s).poll_write(cx, buf),
            StreamKind::Tls(s) => std::pin::Pin::new(s).poll_write(cx, buf),
        }
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        match self.get_mut() {
            StreamKind::Plain(s) => std::pin::Pin::new(s).poll_flush(cx),
            StreamKind::Tls(s) => std::pin::Pin::new(s).poll_flush(cx),
        }
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), std::io::Error>> {
        match self.get_mut() {
            StreamKind::Plain(s) => std::pin::Pin::new(s).poll_shutdown(cx),
            StreamKind::Tls(s) => std::pin::Pin::new(s).poll_shutdown(cx),
        }
    }
}

impl Unpin for StreamKind {}

/// Default maximum concurrent connections.
const DEFAULT_MAX_CONNECTIONS: usize = 100;
/// Default per-connection read/write timeout in seconds.
const DEFAULT_CONN_TIMEOUT_SECS: u64 = 30;
/// Maximum header buffer size (16KB).
const MAX_HEADER_SIZE: usize = 16384;
/// Default maximum request body size (10MB) — prevents memory exhaustion.
const DEFAULT_MAX_BODY_SIZE: u64 = 10 * 1024 * 1024;
/// Minimum allowed max body size (1KB).
const MIN_MAX_BODY_SIZE: u64 = 1024;
/// Threshold in bytes above which gzip compression kicks in.
const GZIP_COMPRESSION_THRESHOLD: usize = 512;

/// Environment variable names for TLS configuration.
const ENV_TLS_CERT_PATH: &str = "EDGE_TLS_CERT_PATH";
const ENV_TLS_KEY_PATH: &str = "EDGE_TLS_KEY_PATH";
/// Environment variable name for max body size limit.
const ENV_MAX_BODY_SIZE: &str = "EDGE_MAX_BODY_SIZE";

/// Parts of an HTTP response sent back to the connection handler.
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

/// A received HTTP request delivered to the guest.
#[derive(Debug, Clone)]
pub struct IncomingRequest {
    pub id: u64,
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub trace: Option<TraceContext>,
}

/// W3C Trace Context parsed from inbound request headers.
#[derive(Debug, Clone, Default)]
pub struct TraceContext {
    pub traceparent: String,
    pub tracestate: Option<String>,
}

pub struct HttpServer {
    port: Option<u16>,
    /// Sends incoming parsed requests toward `poll`.
    tx: Arc<RwLock<Option<mpsc::Sender<IncomingRequest>>>>,
    /// Receives incoming parsed requests in `poll`.
    rx: Arc<TokioMutex<Option<mpsc::Receiver<IncomingRequest>>>>,
    /// Maps request-id -> response sender, so `respond` can route data
    /// back to the correct connection handler.
    responses:
        Arc<StdMutex<std::collections::HashMap<u64, tokio::sync::oneshot::Sender<HttpResponse>>>>,
    /// Request counter — must be shared with the accept loop.
    next_id: Arc<AtomicU64>,
    pub meter: Option<Arc<RequestMeter>>,
    /// Keep accept loop task alive so it doesn't get dropped.
    accept_task: Option<tokio::task::JoinHandle<()>>,

    // --- New fields ---
    /// Shutdown signal sender. When dropped or explicitly triggered, the accept
    /// loop exits cleanly.
    shutdown_tx: Arc<StdMutex<Option<oneshot::Sender<()>>>>,
    /// Limits concurrent connections.
    conn_limit: Arc<Semaphore>,
    max_connections: usize,
    /// Per-connection read/write deadline.
    conn_timeout: Duration,
    /// TLS server configuration, if TLS is enabled via env vars.
    tls_config: Option<Arc<rustls::ServerConfig>>,
    /// Maximum request body size in bytes.
    max_body_size: u64,
}

impl HttpServer {
    pub fn new() -> Self {
        let max_body_size = std::env::var(ENV_MAX_BODY_SIZE)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_MAX_BODY_SIZE);

        Self {
            port: None,
            tx: Arc::new(RwLock::new(None)),
            rx: Arc::new(TokioMutex::new(None)),
            responses: Arc::new(StdMutex::new(std::collections::HashMap::new())),
            next_id: Arc::new(AtomicU64::new(1)),
            meter: None,
            accept_task: None,
            shutdown_tx: Arc::new(StdMutex::new(None)),
            conn_limit: Arc::new(Semaphore::new(DEFAULT_MAX_CONNECTIONS)),
            max_connections: DEFAULT_MAX_CONNECTIONS,
            conn_timeout: Duration::from_secs(DEFAULT_CONN_TIMEOUT_SECS),
            tls_config: try_load_tls_config(),
            max_body_size,
        }
    }

    /// Construct with custom connection limit and timeout.
    pub fn with_limits(max_connections: usize, conn_timeout_secs: u64) -> Self {
        let max_body_size = std::env::var(ENV_MAX_BODY_SIZE)
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_MAX_BODY_SIZE);

        Self {
            port: None,
            tx: Arc::new(RwLock::new(None)),
            rx: Arc::new(TokioMutex::new(None)),
            responses: Arc::new(StdMutex::new(std::collections::HashMap::new())),
            next_id: Arc::new(AtomicU64::new(1)),
            meter: None,
            accept_task: None,
            shutdown_tx: Arc::new(StdMutex::new(None)),
            conn_limit: Arc::new(Semaphore::new(max_connections)),
            max_connections,
            conn_timeout: Duration::from_secs(conn_timeout_secs),
            tls_config: try_load_tls_config(),
            max_body_size,
        }
    }

    /// Set the connection read/write timeout in seconds.
    pub fn with_connection_timeout(mut self, secs: u64) -> Self {
        self.conn_timeout = Duration::from_secs(secs);
        self
    }

    /// Start the HTTP server on the given port, spawning the TCP accept loop.
    pub async fn start(&mut self, port: u16, host: Option<String>) -> Result<(), String> {
        let addr = format!("{}:{}", host.as_deref().unwrap_or("0.0.0.0"), port);
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .map_err(|e| format!("failed to bind {}: {}", addr, e))?;
        self.port = Some(port);

        let (tx, rx) = mpsc::channel::<IncomingRequest>(100);
        *self.tx.write().await = Some(tx.clone());
        *self.rx.lock().await = Some(rx);

        // Create a fresh shutdown channel for this accept loop.
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        *self.shutdown_tx.lock().unwrap() = Some(shutdown_tx);

        let next_id = self.next_id.clone();
        let responses = self.responses.clone();
        let meter = self.meter.clone();
        let conn_limit = self.conn_limit.clone();
        let conn_timeout = self.conn_timeout;
        let max_connections = self.max_connections;
        let tls_config = self.tls_config.clone();
        let max_body_size = self.max_body_size;

        let handle = tokio::spawn(async move {
            tokio::pin!(shutdown_rx);

            loop {
                tokio::select! {
                    // Graceful shutdown: exit when shutdown_tx is triggered.
                    _ = &mut shutdown_rx => {
                        tracing::info!("http-server accept loop shutting down");
                        break;
                    }
                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((stream, peer_addr)) => {
                                let id = next_id.fetch_add(1, Ordering::Relaxed);
                                let (ch_tx, ch_rx) = tokio::sync::oneshot::channel();
                                responses.lock().unwrap().insert(id, ch_tx);

                                let tx = tx.clone();
                                let meter = meter.clone();
                                let conn_timeout = conn_timeout;
                                let conn_limit = conn_limit.clone();
                                let tls_config = tls_config.clone();
                                let max_body_size = max_body_size;

                                // Spawn a task that handles the connection and
                                // acquires/releases the connection permit.
                                tokio::spawn(async move {
                                    let permit = match conn_limit.acquire().await {
                                        Ok(p) => p,
                                        Err(_) => return, // Semaphore closed — shouldn't happen.
                                    };

                                    // Perform TLS handshake if configured.
                                    let stream =
                                        match tls_config.as_ref() {
                                            Some(tls_cfg) => {
                                                let acceptor =
                                                    tokio_rustls::TlsAcceptor::from(tls_cfg.clone());
                                                match acceptor.accept(stream).await {
                                                    Ok(tls_stream) => {
                                                        tracing::debug!(
                                                            peer = %peer_addr,
                                                            "TLS handshake complete",
                                                        );
                                                        StreamKind::Tls(Box::new(tls_stream))
                                                    }
                                                    Err(e) => {
                                                        tracing::warn!(
                                                            peer = %peer_addr,
                                                            err = %e,
                                                            "TLS handshake failed",
                                                        );
                                                        return;
                                                    }
                                                }
                                            }
                                            None => StreamKind::Plain(stream),
                                        };

                                    Self::handle_connection(
                                        id, stream, tx, ch_rx, meter, conn_timeout, max_body_size,
                                    )
                                    .await;
                                    drop(permit); // release connection slot
                                });
                            }
                            Err(e) => {
                                tracing::warn!(err = %e, "accept error");
                            }
                        }
                    }
                }
            }
        });

        self.accept_task = Some(handle);
        tracing::info!(
            addr = %addr,
            max_connections,
            "http-server listening"
        );
        Ok(())
    }

    /// Initiate graceful shutdown of the accept loop.
    /// Idempotent — subsequent calls after the first are no-ops.
    pub async fn shutdown(&self) {
        if let Some(tx) = self.shutdown_tx.lock().unwrap().take() {
            let _ = tx.send(());
        }
    }

    /// Handle one TCP connection: read and parse HTTP, send request to guest,
    /// then wait for the guest's response and write it to the socket.
    async fn handle_connection(
        id: u64,
        mut stream: StreamKind,
        tx: mpsc::Sender<IncomingRequest>,
        ch_rx: tokio::sync::oneshot::Receiver<HttpResponse>,
        meter: Option<Arc<RequestMeter>>,
        conn_timeout: Duration,
        max_body_size: u64,
    ) {
        // Per-connection deadline. Each read/write operation must complete within
        // this window. If exceeded, the connection is aborted.
        let deadline = Instant::now() + conn_timeout;

        let request = match Self::read_request(&mut stream, deadline, id, max_body_size).await {
            Ok(Some(req)) => req,
            Ok(None) => {
                tracing::debug!(req_id = %id, "connection closed or parse error");
                return;
            }
            Err(e) => {
                tracing::warn!(req_id = %id, err = %e, "connection timeout/error");
                return;
            }
        };

        if let Some(ref m) = meter {
            m.record_request();
        }

        // Send request to the guest via poll().
        if tx.send(request.clone()).await.is_err() {
            tracing::debug!(req_id = %id, "poll channel closed, closing connection");
            return;
        }

        // Wait for the guest's response with the connection deadline.
        let HttpResponse {
            status,
            headers,
            body,
        } = match timeout_at(deadline, ch_rx).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => {
                tracing::debug!(req_id = %id, "response channel closed");
                return;
            }
            Err(_) => {
                tracing::warn!(req_id = %id, "guest respond timeout");
                return;
            }
        };

        // Use the same deadline for write (does not refresh).
        if let Err(e) = Self::write_response(
            &mut stream,
            status,
            &headers,
            &body,
            deadline,
            &request.headers,
        )
        .await
        {
            tracing::warn!(req_id = %id, err = %e, "response write error");
        }
    }

    /// Read and parse one HTTP request from the stream. Returns None on EOF or
    /// parse failure. Returns the parsed IncomingRequest on success.
    async fn read_request(
        stream: &mut StreamKind,
        deadline: Instant,
        id: u64,
        max_body_size: u64,
    ) -> Result<Option<IncomingRequest>, std::io::Error> {
        let mut header_buf = vec![0u8; MAX_HEADER_SIZE];
        let mut total_read = 0usize;

        // Read headers (up to double CRLF) with deadline.
        loop {
            let read_fut = stream.read(&mut header_buf[total_read..]);
            match timeout_at(deadline, read_fut).await {
                Ok(Ok(0)) => {
                    if total_read == 0 {
                        return Ok(None); // Clean EOF.
                    }
                    return Ok(None);
                }
                Ok(Ok(n)) => {
                    total_read += n;
                    // Check for double CRLF.
                    if header_buf[..total_read]
                        .windows(4)
                        .any(|w| w == b"\r\n\r\n")
                    {
                        break;
                    }
                    if total_read >= header_buf.len() {
                        tracing::warn!(req_id = %id, "header exceeds max size");
                        return Ok(None);
                    }
                }
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "read deadline",
                    ));
                }
            }
        }

        // Parse headers with httparse.
        let mut req = httparse::Request::new(&mut []);
        match req.parse(&header_buf[..total_read]) {
            Ok(httparse::Status::Complete(_)) => {}
            Ok(httparse::Status::Partial) => {
                tracing::debug!(req_id = %id, "incomplete request");
                return Ok(None);
            }
            Err(e) => {
                tracing::warn!(req_id = %id, err = %e, "malformed request");
                return Ok(None);
            }
        }

        // Extract method, path.
        let method = req.method.unwrap_or("").to_string();
        let path = req.path.unwrap_or("/").to_string();

        // Parse query string.
        let (path, query) = if let Some(idx) = path.find('?') {
            (path[..idx].to_string(), Some(path[idx + 1..].to_string()))
        } else {
            (path.clone(), None)
        };

        // Parse headers — convert only values to String (header names are ASCII).
        let headers: Vec<(String, String)> = req
            .headers
            .iter()
            .map(|h| {
                (
                    h.name.to_string(),
                    String::from_utf8_lossy(h.value).trim().to_string(),
                )
            })
            .collect();

        // Parse W3C Trace Context headers.
        let traceparent = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("traceparent"))
            .map(|(_, v)| v.clone());
        let tracestate = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("tracestate"))
            .map(|(_, v)| v.clone());
        let trace = traceparent.map(|traceparent| TraceContext {
            traceparent,
            tracestate,
        });

        // Determine body length from Content-Length.
        let body_len = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("Content-Length"))
            .and_then(|(_, v)| v.parse::<usize>().ok())
            .unwrap_or(0);

        // Reject oversized bodies to prevent memory exhaustion.
        if body_len > max_body_size as usize {
            tracing::warn!(
                req_id = %id,
                body_len,
                max = %max_body_size,
                "request body exceeds max size",
            );
            return Ok(None);
        }

        // Read body.
        let mut body = Vec::new();
        if body_len > 0 {
            let mut remaining = body_len;
            let mut read_buf = vec![0u8; body_len.min(65536)]; // 64KB temp buffer

            while remaining > 0 {
                let chunk_size = remaining.min(read_buf.len());
                let buf = &mut read_buf[..chunk_size];
                let read_fut = stream.read(buf);
                match timeout_at(deadline, read_fut).await {
                    Ok(Ok(0)) => break, // EOF.
                    Ok(Ok(n)) => {
                        body.extend_from_slice(&buf[..n]);
                        remaining -= n;
                    }
                    Ok(Err(e)) => return Err(e),
                    Err(_) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "body read deadline",
                        ));
                    }
                }
            }
        }

        Ok(Some(IncomingRequest {
            id,
            method,
            path,
            query,
            headers,
            body,
            trace,
        }))
    }

    /// Write an HTTP/1.1 response back to the socket, with optional gzip compression.
    async fn write_response(
        stream: &mut StreamKind,
        status: u16,
        headers: &[(String, String)],
        body: &[u8],
        deadline: Instant,
        request_headers: &[(String, String)],
    ) -> Result<(), std::io::Error> {
        let accept_gzip = request_headers
            .iter()
            .any(|(k, v)| k.eq_ignore_ascii_case("Accept-Encoding") && v.contains("gzip"));

        let (body_to_send, is_compressed) = try_compress(body, accept_gzip);

        let status_line = format!("HTTP/1.1 {} {}\r\n", status, Self::status_text(status));
        let mut response = status_line.into_bytes();
        for (k, v) in headers {
            response.extend(format!("{}: {}\r\n", k, v).bytes());
        }
        if is_compressed {
            response.extend(b"Content-Encoding: gzip\r\n");
            response.extend(b"Vary: Accept-Encoding\r\n");
        }
        response.extend(format!("Content-Length: {}\r\n\r\n", body_to_send.len()).bytes());
        response.extend(&body_to_send);

        timeout_at(deadline, stream.write_all(&response)).await??;
        timeout_at(deadline, stream.flush()).await??;
        Ok(())
    }

    fn status_text(status: u16) -> &'static str {
        match status {
            200 => "OK",
            201 => "Created",
            204 => "No Content",
            301 => "Moved Permanently",
            304 => "Not Modified",
            400 => "Bad Request",
            401 => "Unauthorized",
            403 => "Forbidden",
            404 => "Not Found",
            405 => "Method Not Allowed",
            500 => "Internal Server Error",
            502 => "Bad Gateway",
            503 => "Service Unavailable",
            _ => "Unknown",
        }
    }

    /// Poll for an incoming request (delivered by the accept loop).
    #[allow(clippy::await_holding_lock)]
    pub async fn poll(&mut self) -> Result<Option<IncomingRequest>, String> {
        let mut rx = self.rx.lock().await;
        if let Some(rx) = rx.as_mut() {
            match rx.recv().await {
                Some(req) => Ok(Some(req)),
                None => Err("http-server channel closed".to_string()),
            }
        } else {
            Err("http-server not started".to_string())
        }
    }

    /// Deliver a response for the given request-id back to its connection handler.
    pub async fn respond(
        &self,
        req_id: u64,
        status: u16,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Result<(), String> {
        let ch = self
            .responses
            .lock()
            .unwrap()
            .remove(&req_id)
            .ok_or("unknown request ID")?;
        ch.send(HttpResponse {
            status,
            headers,
            body,
        })
        .map_err(|_| "response channel closed".to_string())?;
        Ok(())
    }

    #[cfg(test)]
    pub fn inject_request(&self, request: IncomingRequest) {
        // No-op helper for testing — the server doesn't support direct injection.
        let _ = request;
    }
}

impl Drop for HttpServer {
    fn drop(&mut self) {
        // Take and drop the shutdown sender so the accept loop exits.
        if let Some(tx) = self.shutdown_tx.lock().unwrap().take() {
            drop(tx);
        }
        // Abort the accept loop if still running.
        if let Some(handle) = self.accept_task.take() {
            handle.abort();
        }
    }
}

impl Default for HttpServer {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpServer {
    /// Create an HttpServer with a pre-set request meter.
    pub fn with_meter(meter: Option<Arc<RequestMeter>>) -> Self {
        let mut server = Self::new();
        server.meter = meter;
        server
    }

    /// Set the maximum request body size in bytes.
    pub fn with_max_body_size(mut self, bytes: u64) -> Self {
        self.max_body_size = bytes.max(MIN_MAX_BODY_SIZE);
        self
    }
}

/// Try to load TLS configuration from EDGE_TLS_CERT_PATH and EDGE_TLS_KEY_PATH.
/// Returns None if the env vars are absent or the cert/key files are invalid.
/// When None, the server falls back to plain HTTP.
fn try_load_tls_config() -> Option<Arc<rustls::ServerConfig>> {
    let cert_path = std::env::var(ENV_TLS_CERT_PATH).ok()?;
    let key_path = std::env::var(ENV_TLS_KEY_PATH).ok()?;

    let cert = std::fs::read(&cert_path)
        .map_err(|e| tracing::warn!(path = %cert_path, err = %e, "failed to read TLS certificate"))
        .ok()?;
    let key = std::fs::read(&key_path)
        .map_err(|e| tracing::warn!(path = %key_path, err = %e, "failed to read TLS private key"))
        .ok()?;

    if cert.is_empty() || key.is_empty() {
        tracing::warn!("TLS certificate or key file is empty");
        return None;
    }

    let certs: Vec<_> = rustls_pemfile::certs(&mut std::io::Cursor::new(&cert))
        .filter_map(Result::ok)
        .collect();
    let key = rustls_pemfile::private_key(&mut std::io::Cursor::new(&key))
        .ok()
        .flatten()?;

    let cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .ok()?;

    tracing::info!(cert_path = %cert_path, "TLS configured");
    Some(Arc::new(cfg))
}

/// Attempt to gzip-compress `body` if it exceeds the compression threshold and
/// the client signaled it accepts gzip via the Accept-Encoding request header.
fn try_compress(body: &[u8], accept_gzip: bool) -> (Vec<u8>, bool) {
    if !accept_gzip || body.len() < GZIP_COMPRESSION_THRESHOLD {
        return (body.to_vec(), false);
    }
    let mut compressed = Vec::new();
    let mut encoder =
        flate2::GzBuilder::new().write(&mut compressed, flate2::Compression::default());
    if std::io::Write::write_all(&mut encoder, body).is_ok()
        && std::io::Write::flush(&mut encoder).is_ok()
        && encoder.try_finish().is_ok()
    {
        drop(encoder); // release borrow on compressed
        return (compressed, true);
    }
    (body.to_vec(), false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_status_text_known_codes() {
        for (code, expected) in &[
            (200, "OK"),
            (201, "Created"),
            (204, "No Content"),
            (301, "Moved Permanently"),
            (400, "Bad Request"),
            (404, "Not Found"),
            (500, "Internal Server Error"),
            (503, "Service Unavailable"),
        ] {
            assert_eq!(HttpServer::status_text(*code), *expected);
        }
    }

    #[test]
    fn test_status_text_unknown_code() {
        assert_eq!(HttpServer::status_text(999), "Unknown");
    }

    #[test]
    fn test_server_new_has_defaults() {
        let server = HttpServer::new();
        assert!(server.port.is_none());
        assert_eq!(server.max_connections, DEFAULT_MAX_CONNECTIONS);
        assert_eq!(
            server.conn_timeout,
            Duration::from_secs(DEFAULT_CONN_TIMEOUT_SECS)
        );
    }

    #[test]
    fn test_server_with_limits() {
        let server = HttpServer::with_limits(50, 10);
        assert_eq!(server.max_connections, 50);
        assert_eq!(server.conn_timeout, Duration::from_secs(10));
    }

    #[test]
    fn test_server_with_connection_timeout() {
        let server = HttpServer::new().with_connection_timeout(60);
        assert_eq!(server.conn_timeout, Duration::from_secs(60));
    }

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_MAX_CONNECTIONS, 100);
        assert_eq!(DEFAULT_CONN_TIMEOUT_SECS, 30);
        assert_eq!(MAX_HEADER_SIZE, 16384);
        assert_eq!(DEFAULT_MAX_BODY_SIZE, 10 * 1024 * 1024);
    }

    #[test]
    fn test_try_compress_small_body_below_threshold() {
        // Body below GZIP_COMPRESSION_THRESHOLD should not be compressed
        let small = b"hello".to_vec();
        let (result, compressed) = try_compress(&small, true);
        assert!(!compressed);
        assert_eq!(result, small);
    }

    #[test]
    fn test_try_compress_no_accept_gzip() {
        // Without Accept-Encoding gzip, compression should not happen
        let large: Vec<u8> = (0..255u8).collect();
        let (result, compressed) = try_compress(&large, false);
        assert!(!compressed);
        assert_eq!(result, large);
    }

    #[test]
    fn test_try_compress_with_gzip_above_threshold() {
        // Large body with Accept-Encoding gzip should compress
        let large: Vec<u8> = vec![0u8; 1024];
        let (result, compressed) = try_compress(&large, true);
        assert!(compressed);
        assert!(result.len() < large.len());
    }

    #[test]
    fn test_with_max_body_size() {
        let server = HttpServer::new().with_max_body_size(5_000_000);
        assert_eq!(server.max_body_size, 5_000_000);
    }

    #[test]
    fn test_with_max_body_size_clamped_to_minimum() {
        let server = HttpServer::new().with_max_body_size(100);
        assert_eq!(server.max_body_size, MIN_MAX_BODY_SIZE);
    }

    #[test]
    fn test_try_load_tls_config_returns_none_when_no_env() {
        // When neither EDGE_TLS_CERT_PATH nor EDGE_TLS_KEY_PATH is set,
        // try_load_tls_config should return None (not panic).
        let result = try_load_tls_config();
        assert!(result.is_none());
    }
}
