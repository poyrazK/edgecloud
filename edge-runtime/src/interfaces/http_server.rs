//! `edge:http-server` — inbound HTTP serving.

use crate::metering::RequestMeter;
use crate::streams::{
    IncomingProducer, IncomingStream, OutgoingStreamAdapter, DEFAULT_STREAM_CAPACITY,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{mpsc, Mutex as TokioMutex, RwLock};
use tokio::sync::{oneshot, Semaphore};
use tokio::time::{timeout, timeout_at, Instant};

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

/// Shared inner handle for split read/write halves. Both halves lock the
/// `StdMutex` synchronously during their `poll_*` calls — the lock is dropped
/// before returning (no awaiting while holding), so a slow task on one half
/// does not block the executor thread indefinitely. Used to split a
/// `StreamKind` into independent read and write halves that can be moved
/// across tasks (e.g. body-pipeline task reads, response writer writes).
type SharedInner = Arc<StdMutex<StreamKind>>;

/// Read half of a split `StreamKind`. Locks the inner mutex during `poll_read`.
pub struct SharedReadHalf(SharedInner);

/// Write half of a split `StreamKind`. Locks the inner mutex during `poll_write`.
pub struct SharedWriteHalf(SharedInner);

impl AsyncRead for SharedReadHalf {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let mut guard = self.0.lock().unwrap_or_else(|e| e.into_inner());
        std::pin::Pin::new(&mut *guard).poll_read(cx, buf)
    }
}

impl AsyncWrite for SharedWriteHalf {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let mut guard = self.0.lock().unwrap_or_else(|e| e.into_inner());
        std::pin::Pin::new(&mut *guard).poll_write(cx, buf)
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let mut guard = self.0.lock().unwrap_or_else(|e| e.into_inner());
        std::pin::Pin::new(&mut *guard).poll_flush(cx)
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let mut guard = self.0.lock().unwrap_or_else(|e| e.into_inner());
        std::pin::Pin::new(&mut *guard).poll_shutdown(cx)
    }
}

impl Unpin for SharedReadHalf {}
impl Unpin for SharedWriteHalf {}

/// Split a `StreamKind` into independent read and write halves. The two halves
/// share an `Arc<StdMutex<StreamKind>>` and serialize I/O — they must not be
/// used simultaneously to read and write the same socket from the same task.
/// In `handle_connection`, the body-pipeline task owns the read half (active
/// only until the body is fully consumed) and the response writer owns the
/// write half (active only after the body-pipeline task is done).
fn shared_split(stream: StreamKind) -> (SharedReadHalf, SharedWriteHalf) {
    let inner = Arc::new(StdMutex::new(stream));
    (SharedReadHalf(inner.clone()), SharedWriteHalf(inner))
}

/// Background task that reads the request body from the split read half and
/// pushes chunks into the `IncomingProducer` so the guest can poll them via
/// the `IncomingStream`. Stops early on EOF, deadline, or consumer drop.
///
/// `body_prefix` is bytes already read past the `\r\n\r\n` header delimiter
/// (preserved from `read_headers`); they are pushed as the first chunk
/// before reading more from the stream.
async fn body_pipeline(
    body_prefix: Vec<u8>,
    mut rh: SharedReadHalf,
    body_len: usize,
    deadline: Instant,
    producer: IncomingProducer,
) {
    let mut remaining = body_len;
    // First, push the bytes already consumed past the header delimiter.
    if !body_prefix.is_empty() {
        let take = body_prefix.len().min(remaining);
        let chunk = body_prefix[..take].to_vec();
        if producer.push(Ok(chunk)).await.is_err() {
            return; // Consumer dropped.
        }
        remaining -= take;
    }
    let mut buf = vec![0u8; 65536];
    while remaining > 0 {
        let want = remaining.min(buf.len());
        let n = match timeout_at(deadline, rh.read(&mut buf[..want])).await {
            Ok(Ok(0)) => break, // EOF.
            Ok(Ok(n)) => n,
            Ok(Err(e)) => {
                let _ = producer.push(Err(e.to_string())).await;
                break;
            }
            Err(_) => {
                let _ = producer.push(Err("body read deadline".to_string())).await;
                break;
            }
        };
        let chunk = buf[..n].to_vec();
        if producer.push(Ok(chunk)).await.is_err() {
            break; // Consumer dropped — guest cancelled.
        }
        remaining -= n;
    }
    // If the client closed the connection before delivering all promised
    // bytes, surface the truncation so the guest does not treat a partial
    // body as complete. Dropping `producer` afterwards closes the channel;
    // the guest's next `read_chunk` observes `StreamError::Closed`.
    if remaining > 0 {
        let _ = producer
            .push(Err(format!("truncated body: {} bytes short", remaining)))
            .await;
    }
}

/// Inline body read for small requests: reads `body_len` bytes from the split
/// read half into a `Vec<u8>` and returns them. Bounded by `body_len` and the
/// per-connection deadline. `body_prefix` is bytes already consumed past the
/// header delimiter — they seed the returned body.
async fn read_body_inline(
    body_prefix: Vec<u8>,
    mut rh: SharedReadHalf,
    body_len: usize,
    deadline: Instant,
) -> Result<Vec<u8>, std::io::Error> {
    if body_len == 0 {
        return Ok(Vec::new());
    }
    let mut body = Vec::with_capacity(body_len);
    let mut remaining = body_len;
    // Seed with the bytes already past the header delimiter.
    let take = body_prefix.len().min(remaining);
    body.extend_from_slice(&body_prefix[..take]);
    remaining -= take;
    let mut buf = vec![0u8; 65536];
    while remaining > 0 {
        let want = remaining.min(buf.len());
        let n = match timeout_at(deadline, rh.read(&mut buf[..want])).await {
            Ok(Ok(0)) => break, // EOF.
            Ok(Ok(n)) => n,
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "body read deadline",
                ));
            }
        };
        body.extend_from_slice(&buf[..n]);
        remaining -= n;
    }
    if body.len() < body_len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            format!(
                "short body: got {} bytes, expected {}",
                body.len(),
                body_len
            ),
        ));
    }
    Ok(body)
}

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
/// Threshold in bytes above which an inbound request body is exposed as a
/// streaming IncomingStream instead of a fully-buffered Vec<u8>. Below this,
/// the host buffers the entire body before delivering to the guest.
pub const DEFAULT_STREAM_THRESHOLD: u64 = 1024 * 1024;

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

/// Parsed result of `read_headers` — the request line + headers + Content-Length,
/// but not the body. The body is consumed separately by `read_body_inline`
/// (small bodies, inline) or `body_pipeline` (large bodies, streamed).
struct ParsedHeaders {
    method: String,
    path: String,
    query: Option<String>,
    headers: Vec<(String, String)>,
    body_len: usize,
    /// Bytes already read past the `\r\n\r\n` delimiter — TCP can deliver
    /// the body in the same read as the headers, so we have to preserve these
    /// bytes and pass them to the body reader (otherwise the body reader would
    /// block on `read` while the bytes are sitting in our header buffer).
    body_prefix: Vec<u8>,
    trace: Option<TraceContext>,
}

/// Map of req-id -> streaming-response-parts sender. Aliased so the
/// `HttpServer` struct doesn't trip clippy::type_complexity.
type StreamingResponseMap =
    std::collections::HashMap<u64, tokio::sync::oneshot::Sender<StreamingResponseParts>>;

/// Parts delivered through `respond_stream`: the head + the adapter the
/// per-connection task drains to write chunks to the socket.
pub struct StreamingResponseParts {
    pub head: StreamingResponseHead,
    pub adapter: OutgoingStreamAdapter,
}

/// Head of a streaming response: status + headers, then chunks stream in via
/// the OutgoingStreamAdapter. Content-Length MUST be present in headers.
pub struct StreamingResponseHead {
    pub status: u16,
    pub headers: Vec<(String, String)>,
}

/// Body shape delivered to the guest via `IncomingRequest.body`.
pub enum BodySource {
    /// No body (Content-Length: 0 or absent on a body-less request).
    None,
    /// Fully-buffered body bytes (Content-Length <= STREAM_THRESHOLD).
    Buffered(Vec<u8>),
    /// Streaming body (Content-Length > STREAM_THRESHOLD). The host's
    /// body-pipeline task pushes chunks into this stream's producer.
    Streamed(IncomingStream),
}

impl std::fmt::Debug for BodySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BodySource::None => f.write_str("BodySource::None"),
            BodySource::Buffered(b) => f
                .debug_tuple("BodySource::Buffered")
                .field(&b.len())
                .finish(),
            BodySource::Streamed(_) => f.write_str("BodySource::Streamed(<stream>)"),
        }
    }
}

/// A received HTTP request delivered to the guest.
#[derive(Debug)]
pub struct IncomingRequest {
    pub id: u64,
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    pub headers: Vec<(String, String)>,
    pub body: BodySource,
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
    /// Maps request-id -> streaming-response parts sender. `respond_stream`
    /// sends the (head, adapter) via this oneshot; the per-connection task
    /// writes the head, then drains the OutgoingStreamAdapter to write
    /// chunks with timeout_at(deadline, write_all).
    streaming_responses: Arc<StdMutex<StreamingResponseMap>>,
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
    /// Threshold above which inbound request bodies are streamed instead of
    /// buffered. Configurable via `with_stream_threshold`.
    stream_threshold: u64,
    /// Active connection handler task handles — drained on shutdown.
    conn_handles: Arc<StdMutex<Vec<tokio::task::JoinHandle<()>>>>,
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
            streaming_responses: Arc::new(StdMutex::new(std::collections::HashMap::new())),
            next_id: Arc::new(AtomicU64::new(1)),
            meter: None,
            accept_task: None,
            shutdown_tx: Arc::new(StdMutex::new(None)),
            conn_limit: Arc::new(Semaphore::new(DEFAULT_MAX_CONNECTIONS)),
            max_connections: DEFAULT_MAX_CONNECTIONS,
            conn_timeout: Duration::from_secs(DEFAULT_CONN_TIMEOUT_SECS),
            tls_config: try_load_tls_config(),
            max_body_size,
            stream_threshold: DEFAULT_STREAM_THRESHOLD,
            conn_handles: Arc::new(StdMutex::new(Vec::new())),
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
            streaming_responses: Arc::new(StdMutex::new(std::collections::HashMap::new())),
            next_id: Arc::new(AtomicU64::new(1)),
            meter: None,
            accept_task: None,
            shutdown_tx: Arc::new(StdMutex::new(None)),
            conn_limit: Arc::new(Semaphore::new(max_connections)),
            max_connections,
            conn_timeout: Duration::from_secs(conn_timeout_secs),
            tls_config: try_load_tls_config(),
            max_body_size,
            stream_threshold: DEFAULT_STREAM_THRESHOLD,
            conn_handles: Arc::new(StdMutex::new(Vec::new())),
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
        // Capture the actual bound port — callers that pass `port = 0` for an
        // OS-assigned ephemeral port need `get_assigned_port()` to return the
        // real port, not `0`.
        self.port = Some(
            listener
                .local_addr()
                .map_err(|e| format!("local_addr: {e}"))?
                .port(),
        );

        let (tx, rx) = mpsc::channel::<IncomingRequest>(100);
        *self.tx.write().await = Some(tx.clone());
        *self.rx.lock().await = Some(rx);

        // Create a fresh shutdown channel for this accept loop.
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        *self.shutdown_tx.lock().unwrap() = Some(shutdown_tx);

        let next_id = self.next_id.clone();
        let responses = self.responses.clone();
        let streaming_responses = self.streaming_responses.clone();
        let meter = self.meter.clone();
        let conn_limit = self.conn_limit.clone();
        let conn_handles = self.conn_handles.clone();
        let conn_timeout = self.conn_timeout;
        let max_connections = self.max_connections;
        let tls_config = self.tls_config.clone();
        let max_body_size = self.max_body_size;
        let stream_threshold = self.stream_threshold;

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
                                let (stream_tx, stream_rx) =
                                    tokio::sync::oneshot::channel::<StreamingResponseParts>();
                                responses.lock().unwrap().insert(id, ch_tx);
                                streaming_responses.lock().unwrap().insert(id, stream_tx);

                                let tx = tx.clone();
                                let meter = meter.clone();
                                let conn_timeout = conn_timeout;
                                let conn_limit = conn_limit.clone();
                                let tls_config = tls_config.clone();
                                let max_body_size = max_body_size;
                                let stream_threshold = stream_threshold;

                                // Spawn a task that handles the connection and
                                // acquires/releases the connection permit.
                                let handle = tokio::spawn(async move {
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
                                                        drop(permit); // release connection slot before returning
                                                        return;
                                                    }
                                                }
                                            }
                                            None => StreamKind::Plain(stream),
                                        };

                                    Self::handle_connection(
                                        id, stream, tx, ch_rx, stream_rx, meter, conn_timeout, max_body_size, stream_threshold,
                                    )
                                    .await;
                                    drop(permit); // release connection slot
                                });
                                conn_handles.lock().unwrap().push(handle);
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

    /// Initiate graceful shutdown of the accept loop and drain all in-flight
    /// connection handler tasks. Idempotent — subsequent calls after the first
    /// are no-ops.
    ///
    /// Each in-flight connection is given up to `drain_timeout` seconds to finish.
    /// Connections that exceed the timeout are dropped (their tasks continue running
    /// independently — we simply stop waiting).
    pub async fn shutdown(&self) {
        // Signal the accept loop to stop accepting new connections.
        if let Some(tx) = self.shutdown_tx.lock().unwrap().take() {
            let _ = tx.send(());
        }
        // Drain all in-flight connection handler tasks with a per-connection timeout.
        // Each handle is awaited with its own timeout so a single slow connection
        // cannot block the drain of other connections.
        let handles: Vec<_> = self.conn_handles.lock().unwrap().drain(..).collect();
        let drain_timeout = Duration::from_secs(5);
        for handle in handles {
            let _ = timeout(drain_timeout, handle).await;
        }
    }

    /// Alias for shutdown — used by the WIT stop() call.
    /// Spawns shutdown as a background task since WIT cannot call async functions.
    pub fn stop(&self) {
        let shutdown_tx = self.shutdown_tx.clone();
        let conn_handles = self.conn_handles.clone();
        // We intentionally do NOT await the JoinHandle — fire-and-forget graceful shutdown.
        std::mem::drop(tokio::spawn(async move {
            if let Some(tx) = shutdown_tx.lock().unwrap().take() {
                let _ = tx.send(());
            }
            let handles: Vec<_> = conn_handles.lock().unwrap().drain(..).collect();
            let drain_timeout = Duration::from_secs(5);
            for handle in handles {
                let _ = tokio::time::timeout(drain_timeout, handle).await;
            }
        }));
    }

    /// Returns the port the server is bound to, if it has been started.
    pub fn get_assigned_port(&self) -> Option<u16> {
        self.port
    }

    /// Handle one TCP connection: read and parse HTTP, send request to guest,
    /// then wait for the guest's response and write it to the socket.
    ///
    /// For requests whose Content-Length exceeds `stream_threshold`, the
    /// stream is split via `shared_split`: a spawned body-pipeline task reads
    /// remaining body bytes and pushes them into an `IncomingStream` (delivered
    /// to the guest as `BodySource::Streamed`), while this function keeps the
    /// write half for the response. For smaller requests, the body is read
    /// fully inline and returned as `BodySource::Buffered`.
    #[allow(clippy::too_many_arguments)]
    async fn handle_connection(
        id: u64,
        mut stream: StreamKind,
        tx: mpsc::Sender<IncomingRequest>,
        ch_rx: tokio::sync::oneshot::Receiver<HttpResponse>,
        stream_ch_rx: tokio::sync::oneshot::Receiver<StreamingResponseParts>,
        meter: Option<Arc<RequestMeter>>,
        conn_timeout: Duration,
        max_body_size: u64,
        stream_threshold: u64,
    ) {
        // Per-connection deadline. Each read/write operation must complete within
        // this window. If exceeded, the connection is aborted.
        let deadline = Instant::now() + conn_timeout;

        // 1. Read headers only (the body is consumed separately below).
        let parsed = match Self::read_headers(&mut stream, deadline, id, max_body_size).await {
            Ok(Some(p)) => p,
            Ok(None) => {
                tracing::debug!(req_id = %id, "connection closed or parse error");
                return;
            }
            Err(e) => {
                tracing::warn!(req_id = %id, err = %e, "connection timeout/error");
                return;
            }
        };

        // 2. Split for concurrent body read / response write. The body-pipeline
        //    task owns the read half (active until body is fully consumed); the
        //    response writer owns the write half (active after the body-pipeline
        //    task is done). They serialize I/O via the inner Mutex — fine for
        //    HTTP/1.1, which is request/response-sequential on one connection.
        let (read_half, mut write_half) = shared_split(stream);

        // 3. Read the body: streamed if it exceeds the threshold, buffered
        //    otherwise. `body_len == 0` always takes the buffered (empty) path.
        //    `body_prefix` is the bytes already consumed past the \r\n\r\n —
        //    TCP can deliver the body in the same packet as the headers, so we
        //    have to preserve those bytes rather than losing them.
        let body = if parsed.body_len > stream_threshold as usize && parsed.body_len > 0 {
            let (producer, incoming_stream) =
                crate::streams::incoming_pair(DEFAULT_STREAM_CAPACITY);
            tokio::spawn(body_pipeline(
                parsed.body_prefix,
                read_half,
                parsed.body_len,
                deadline,
                producer,
            ));
            BodySource::Streamed(incoming_stream)
        } else {
            match read_body_inline(parsed.body_prefix, read_half, parsed.body_len, deadline).await {
                Ok(bytes) => BodySource::Buffered(bytes),
                Err(e) => {
                    tracing::warn!(req_id = %id, err = %e, "body read error");
                    return;
                }
            }
        };

        let request = IncomingRequest {
            id,
            method: parsed.method,
            path: parsed.path,
            query: parsed.query,
            headers: parsed.headers,
            body,
            trace: parsed.trace,
        };

        if let Some(ref m) = meter {
            m.record_request();
        }

        // Save the request headers for response-side processing (gzip, etc.)
        // before moving `request` to the guest. Streamed bodies are moved
        // into the request — they cannot be cloned.
        let request_headers = request.headers.clone();

        // Send request to the guest via poll().
        if tx.send(request).await.is_err() {
            tracing::debug!(req_id = %id, "poll channel closed, closing connection");
            return;
        }

        // Wait for either a buffered or a streaming response. The guest picks
        // one path per request.
        tokio::select! {
            biased;
            res = timeout_at(deadline, ch_rx) => {
                match res {
                    Ok(Ok(HttpResponse { status, headers, body })) => {
                        if let Err(e) = Self::write_response(
                            &mut write_half,
                            status,
                            &headers,
                            &body,
                            deadline,
                            &request_headers,
                        )
                        .await
                        {
                            tracing::warn!(req_id = %id, err = %e, "response write error");
                        }
                    }
                    Ok(Err(_)) => {
                        tracing::debug!(req_id = %id, "buffered response channel closed");
                    }
                    Err(_) => {
                        tracing::warn!(req_id = %id, "guest respond timeout (buffered)");
                    }
                }
            }
            res = timeout_at(deadline, stream_ch_rx) => {
                match res {
                    Ok(Ok(StreamingResponseParts { head, mut adapter })) => {
                        if let Err(e) =
                            Self::write_streaming_response(&mut write_half, &head, &mut adapter, deadline)
                                .await
                        {
                            tracing::warn!(req_id = %id, err = %e, "streaming response write error");
                        }
                    }
                    Ok(Err(_)) => {
                        tracing::debug!(req_id = %id, "streaming response channel closed");
                    }
                    Err(_) => {
                        tracing::warn!(req_id = %id, "guest respond timeout (streaming)");
                    }
                }
            }
        }
    }

    /// Write a streaming response: status line, headers, then drain the
    /// OutgoingStreamAdapter chunks with `timeout_at(deadline, write_all)`
    /// per chunk. Requires Content-Length in headers (v1 — no chunked TE).
    async fn write_streaming_response(
        stream: &mut SharedWriteHalf,
        head: &StreamingResponseHead,
        adapter: &mut OutgoingStreamAdapter,
        deadline: Instant,
    ) -> Result<(), std::io::Error> {
        use futures::StreamExt;
        let status_line = format!(
            "HTTP/1.1 {} {}\r\n",
            head.status,
            Self::status_text(head.status)
        );
        let mut response = status_line.into_bytes();
        for (k, v) in &head.headers {
            response.extend(format!("{}: {}\r\n", k, v).bytes());
        }
        response.extend(b"\r\n");
        timeout_at(deadline, stream.write_all(&response)).await??;
        timeout_at(deadline, stream.flush()).await??;

        // Drain the adapter to EOF. EOF arrives either from the guest calling
        // `finish()` (sender dropped) or from the bindgen releasing the
        // Outgoing resource (sender dropped on drop). See streams::OutgoingStream.
        //
        // Each iteration is bounded by `deadline` via tokio::select!: a
        // stalled guest (writes one chunk, never calls finish, never drops
        // the Outgoing resource) is torn down at the connection deadline
        // instead of pinning a connection task indefinitely. The
        // `biased;` modifier matches the rest of this file and prefers
        // making progress on chunks before checking the timer.
        loop {
            let chunk_fut = adapter.next();
            tokio::select! {
                biased;
                item = chunk_fut => {
                    let Some(item) = item else { break; };
                    let chunk = item.map_err(|e| std::io::Error::other(e.to_string()))?;
                    timeout_at(deadline, stream.write_all(&chunk)).await??;
                }
                _ = tokio::time::sleep_until(deadline) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "streaming response deadline",
                    ));
                }
            }
        }
        timeout_at(deadline, stream.flush()).await??;
        Ok(())
    }

    /// Read and parse the HTTP headers only — the body is consumed by
    /// `read_body_inline` (small bodies) or `body_pipeline` (large bodies).
    /// Returns `None` on EOF or parse failure.
    #[allow(unused_assignments)] // `header_end` is only assigned inside the loop before any read.
    async fn read_headers(
        stream: &mut StreamKind,
        deadline: Instant,
        id: u64,
        max_body_size: u64,
    ) -> Result<Option<ParsedHeaders>, std::io::Error> {
        let mut header_buf = vec![0u8; MAX_HEADER_SIZE];
        let mut total_read = 0usize;
        // Offset right after the `\r\n\r\n` header delimiter, populated when
        // we find the delimiter during the read loop.
        let mut header_end = 0usize;

        // Read headers (up to double CRLF) with deadline. TCP can deliver the
        // request body in the same read as the headers, so once we find the
        // header delimiter we capture any post-delimiter bytes as `body_prefix`
        // and return them with the parsed headers — the body reader uses them
        // before reading more from the stream.
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
                    // Find the double CRLF.
                    if let Some(pos) = header_buf[..total_read]
                        .windows(4)
                        .position(|w| w == b"\r\n\r\n")
                    {
                        header_end = pos + 4;
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

        // Parse headers with httparse. Allocate a header slot array — an empty
        // slice (`&mut []`) makes `parse` fail with `TooManyHeaders` for any
        // non-empty header set. 32 slots is generous for the typical request.
        // We only parse up to `header_end` so the body bytes that share the
        // buffer do not get misinterpreted as additional header lines.
        let mut headers = [httparse::EMPTY_HEADER; 32];
        let mut req = httparse::Request::new(&mut headers);
        match req.parse(&header_buf[..header_end]) {
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

        // Body bytes that arrived in the same read as the headers — preserve
        // them so the body reader does not stall.
        let body_prefix = if total_read > header_end {
            header_buf[header_end..total_read].to_vec()
        } else {
            Vec::new()
        };

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

        Ok(Some(ParsedHeaders {
            method,
            path,
            query,
            headers,
            body_len,
            body_prefix,
            trace,
        }))
    }

    /// Write an HTTP/1.1 response back to the socket, with optional gzip compression.
    async fn write_response(
        stream: &mut SharedWriteHalf,
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

    /// Deliver a streaming response: send (head, adapter) to the per-connection
    /// task, which writes the head then drains the adapter chunks to the
    /// socket. Content-Length MUST be present in `headers` — v1 does not
    /// implement chunked transfer encoding.
    pub async fn respond_stream(
        &self,
        req_id: u64,
        status: u16,
        headers: Vec<(String, String)>,
        adapter: OutgoingStreamAdapter,
    ) -> Result<(), String> {
        let ch = self
            .streaming_responses
            .lock()
            .unwrap()
            .remove(&req_id)
            .ok_or("unknown request ID")?;
        ch.send(StreamingResponseParts {
            head: StreamingResponseHead { status, headers },
            adapter,
        })
        .map_err(|_| "streaming response channel closed".to_string())?;
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

    /// Set the threshold (in bytes) above which inbound request bodies are
    /// exposed to the guest as a streaming `Incoming` resource. Bodies at or
    /// below this threshold are fully buffered as `BodySource::Buffered`.
    pub fn with_stream_threshold(mut self, bytes: u64) -> Self {
        self.stream_threshold = bytes;
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

    let mut cfg = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .ok()?;

    // Advertise HTTP/2 via ALPN so clients can negotiate it over TLS.
    cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

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

    /// F1 regression: `read_body_inline` must return `Err(UnexpectedEof)`
    /// when the source stream is closed before the promised `body_len`
    /// bytes have been read — not a silently truncated `Vec<u8>`.
    #[tokio::test]
    async fn test_read_body_inline_truncated_body_returns_unexpected_eof() {
        use std::net::SocketAddr;
        use tokio::io::AsyncWriteExt;
        use tokio::net::TcpListener;
        use tokio::time::Duration;

        // Listener that writes 5 bytes then closes the connection. The
        // client side (read half) gets an EOF after 5 bytes.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        let writer = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            sock.write_all(b"hello").await.unwrap();
            sock.shutdown().await.unwrap();
            // Hold the socket open until the client finishes reading EOF.
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (read_half, _write_half) = shared_split(StreamKind::Plain(client));
        let deadline = Instant::now() + Duration::from_secs(2);

        let err = read_body_inline(Vec::new(), read_half, 100, deadline)
            .await
            .expect_err("expected truncation error");
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        assert!(
            err.to_string().contains("short body"),
            "unexpected error message: {err}"
        );
        writer.await.unwrap();
    }

    /// F1 regression: `read_body_inline` returns the full body when the
    /// source delivers exactly the promised `body_len` bytes (no spurious
    /// truncation error).
    #[tokio::test]
    async fn test_read_body_inline_full_body_succeeds() {
        use std::net::SocketAddr;
        use tokio::io::AsyncWriteExt;
        use tokio::net::TcpListener;
        use tokio::time::Duration;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        let payload = b"hello, world!".to_vec();
        let expected = payload.clone();
        let writer = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            sock.write_all(&payload).await.unwrap();
            sock.shutdown().await.unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (read_half, _write_half) = shared_split(StreamKind::Plain(client));
        let deadline = Instant::now() + Duration::from_secs(2);

        let body = read_body_inline(Vec::new(), read_half, expected.len(), deadline)
            .await
            .expect("full body read");
        assert_eq!(body, expected);
        writer.await.unwrap();
    }

    /// F1 regression: `body_pipeline` must push an `Err("truncated body: ...")`
    /// chunk into the producer when the source stream is closed before the
    /// promised `body_len` bytes are read — the guest's `read_chunk` will
    /// see the error before EOF.
    #[tokio::test]
    async fn test_body_pipeline_truncated_body_pushes_error() {
        use std::net::SocketAddr;
        use tokio::io::AsyncWriteExt;
        use tokio::net::TcpListener;
        use tokio::time::Duration;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        let writer = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            sock.write_all(b"partial").await.unwrap();
            sock.shutdown().await.unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (read_half, _write_half) = shared_split(StreamKind::Plain(client));
        let (producer, mut stream) = crate::streams::incoming_pair(8);
        let deadline = Instant::now() + Duration::from_secs(2);

        tokio::spawn(body_pipeline(
            Vec::new(),
            read_half,
            100,
            deadline,
            producer,
        ));

        // First read might be the partial data (Ok), or the truncation error.
        // The error MUST be surfaced before the stream closes.
        let mut saw_error = false;
        loop {
            match stream.read_chunk().await {
                Ok(_chunk) => continue,
                Err(crate::streams::StreamError::Io(msg)) => {
                    assert!(msg.contains("truncated body"), "unexpected error: {msg}");
                    saw_error = true;
                    break;
                }
                Err(crate::streams::StreamError::Closed) => break,
                Err(crate::streams::StreamError::Cancelled) => break,
            }
        }
        assert!(saw_error, "expected a truncation error chunk");
        writer.await.unwrap();
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

    #[test]
    fn test_get_assigned_port_before_start() {
        let server = HttpServer::new();
        assert!(server.get_assigned_port().is_none());
    }

    #[tokio::test]
    async fn test_shutdown_is_idempotent() {
        let server = HttpServer::new();
        server.shutdown().await;
        server.shutdown().await;
    }

    #[test]
    fn test_with_stream_threshold() {
        let server = HttpServer::new().with_stream_threshold(2_000_000);
        assert_eq!(server.stream_threshold, 2_000_000);
    }

    #[test]
    fn test_default_stream_threshold_is_1mb() {
        let server = HttpServer::new();
        assert_eq!(server.stream_threshold, 1024 * 1024);
    }

    #[test]
    fn test_body_source_debug_redacts_streamed_payload() {
        // Debug impl must not print stream internals — they don't impl Debug.
        let s = BodySource::None;
        assert!(format!("{:?}", s).contains("None"));
        let b = BodySource::Buffered(vec![0xde, 0xad, 0xbe, 0xef]);
        let dbg = format!("{:?}", b);
        assert!(dbg.contains("Buffered"));
        assert!(dbg.contains("4"));
    }
}
