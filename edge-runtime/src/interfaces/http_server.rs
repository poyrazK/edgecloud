//! `edge:http-server` — inbound HTTP serving.

use crate::metering::RequestMeter;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, RwLock};
use tokio::time::{timeout, Duration};

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
}

pub struct HttpServer {
    port: Option<u16>,
    /// Sends incoming parsed requests toward `poll`.
    tx: Arc<RwLock<Option<mpsc::Sender<IncomingRequest>>>>,
    /// Receives incoming parsed requests in `poll`.
    rx: Arc<std::sync::Mutex<Option<mpsc::Receiver<IncomingRequest>>>>,
    /// Maps request-id -> response sender, so `respond` can route data
    /// back to the correct connection handler.
    responses: Arc<
        std::sync::Mutex<
            std::collections::HashMap<u64, tokio::sync::oneshot::Sender<HttpResponse>>,
        >,
    >,
    /// Request counter — must be shared with the accept loop.
    next_id: Arc<AtomicU64>,
    pub meter: Option<Arc<RequestMeter>>,
    /// Keep accept loop task alive so it doesn't get dropped.
    accept_task: Option<tokio::task::JoinHandle<()>>,
    /// Buffer size for reading HTTP requests (default 8KB).
    buffer_size: usize,
    /// Connection read/write timeout in seconds (default 30s).
    connection_timeout_secs: u64,
}

impl HttpServer {
    pub fn new() -> Self {
        Self {
            port: None,
            tx: Arc::new(RwLock::new(None)),
            rx: Arc::new(Mutex::new(None)),
            responses: Arc::new(Mutex::new(std::collections::HashMap::new())),
            next_id: Arc::new(AtomicU64::new(1)),
            meter: None,
            accept_task: None,
            buffer_size: 8192,
            connection_timeout_secs: 30,
        }
    }

    /// Set the read buffer size for incoming HTTP requests.
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    /// Set the connection read/write timeout in seconds.
    pub fn with_connection_timeout(mut self, secs: u64) -> Self {
        self.connection_timeout_secs = secs;
        self
    }

    /// Start the HTTP server on the given port, spawning the TCP accept loop.
    /// This is synchronous — it spawns the async accept loop onto the current runtime
    /// and returns immediately without blocking.
    pub fn start(&mut self, port: u16, host: Option<String>) -> Result<(), String> {
        // Abort any previous accept loop before spawning a new one.
        if let Some(prev) = self.accept_task.take() {
            prev.abort();
        }

        let rt = tokio::runtime::Handle::current();
        let addr = format!("{}:{}", host.as_deref().unwrap_or("0.0.0.0"), port);
        let addr_for_error = addr.clone();

        // Clone shared state for the async accept loop.
        let next_id = self.next_id.clone();
        let responses = self.responses.clone();
        let meter = self.meter.clone();
        let buffer_size = self.buffer_size;
        let connection_timeout_secs = self.connection_timeout_secs;

        // Flag set by the spawned task if bind fails — allows us to return an error.
        let bind_failed = Arc::new(AtomicBool::new(false));
        let bind_failed_clone = bind_failed.clone();

        // Set up channel for delivering incoming requests.
        let (tx, rx) = mpsc::channel::<IncomingRequest>(100);
        let tx_clone = tx.clone();

        // Spawn the async bind + accept loop onto the runtime.
        let accept_task = rt.spawn(async move {
            let listener = match tokio::net::TcpListener::bind(&addr).await {
                Ok(l) => l,
                Err(e) => {
                    tracing::error!(err = %e, "failed to bind {}", addr);
                    bind_failed_clone.store(true, Ordering::Relaxed);
                    return;
                }
            };
            tracing::info!(addr = %addr, "http-server listening");

            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let id = next_id.fetch_add(1, Ordering::Relaxed);
                        let (ch_tx, ch_rx) = tokio::sync::oneshot::channel();
                        responses.lock().unwrap().insert(id, ch_tx);

                        let tx = tx_clone.clone();
                        let meter = meter.clone();
                        tokio::spawn(Self::handle_connection(
                            id,
                            stream,
                            tx,
                            ch_rx,
                            meter,
                            buffer_size,
                            connection_timeout_secs,
                        ));
                    }
                    Err(e) => {
                        tracing::warn!(err = %e, "accept error");
                    }
                }
            }
        });

        // Check if bind failed before returning success.
        if bind_failed.load(Ordering::Relaxed) {
            self.accept_task = Some(accept_task);
            return Err(format!("failed to bind {}", addr_for_error));
        }

        // Store channel endpoints and task handle.
        self.port = Some(port);
        self.tx = Arc::new(RwLock::new(Some(tx)));
        self.rx = Arc::new(Mutex::new(Some(rx)));
        self.accept_task = Some(accept_task);
        Ok(())
    }

    /// Handle one TCP connection: read and parse HTTP, send request to guest,
    /// then wait for the guest's response and write it to the socket.
    async fn handle_connection(
        id: u64,
        mut stream: tokio::net::TcpStream,
        tx: mpsc::Sender<IncomingRequest>,
        ch_rx: tokio::sync::oneshot::Receiver<HttpResponse>,
        meter: Option<Arc<RequestMeter>>,
        buffer_size: usize,
        connection_timeout_secs: u64,
    ) {
        let timeout_duration = Duration::from_secs(connection_timeout_secs);

        // Read the HTTP request from the socket with a timeout.
        let mut buf = vec![0u8; buffer_size];
        let n = match timeout(timeout_duration, stream.read(&mut buf)).await {
            Ok(Ok(0)) => return,
            Ok(Ok(n)) => n,
            Ok(Err(e)) => {
                tracing::warn!(err = %e, "read error");
                return;
            }
            Err(_) => {
                tracing::warn!(req_id = %id, "connection read timeout");
                return;
            }
        };

        let request = match Self::parse_request(id, &buf[..n]) {
            Some(req) => req,
            None => return,
        };

        if let Some(ref m) = meter {
            m.record_request();
        }

        // Send request to the guest via poll().
        if tx.send(request).await.is_err() {
            return;
        }

        // Wait for the guest's response via respond() with a timeout.
        let HttpResponse {
            status,
            headers,
            body,
        } = match timeout(timeout_duration, ch_rx).await {
            Ok(Ok(r)) => r,
            Ok(Err(_)) => {
                tracing::warn!(req_id = %id, "response channel closed");
                return;
            }
            Err(_) => {
                tracing::warn!(req_id = %id, "guest respond timeout");
                return;
            }
        };

        // Write the HTTP/1.1 response back to the socket with a timeout.
        let status_line = format!("HTTP/1.1 {} {}\r\n", status, Self::status_text(status));
        let mut response = status_line.into_bytes();
        for (k, v) in headers {
            response.extend(format!("{}: {}\r\n", k, v).bytes());
        }
        response.extend(b"Content-Length: ");
        response.extend(body.len().to_string().bytes());
        response.extend(b"\r\n\r\n");
        response.extend(&body);

        if let Err(e) = timeout(timeout_duration, stream.write_all(&response)).await {
            tracing::warn!(err = %e, req_id = %id, "response write timeout or error");
        }
    }

    /// Parse a raw HTTP/1.1 request bytes into an `IncomingRequest`.
    fn parse_request(id: u64, buf: &[u8]) -> Option<IncomingRequest> {
        let s = String::from_utf8_lossy(buf);
        let mut lines = s.lines();

        let request_line = lines.next()?;
        let parts: Vec<_> = request_line.splitn(3, ' ').collect();
        if parts.len() != 3 {
            return None;
        }
        let method = parts[0].to_string();
        let path = parts[1].to_string();
        let (path, query) = if let Some(idx) = path.find('?') {
            (path[..idx].to_string(), Some(path[idx + 1..].to_string()))
        } else {
            (path.clone(), None)
        };

        let mut headers = Vec::new();
        for (i, line) in lines.enumerate() {
            if line.is_empty() {
                break;
            }
            if i == 0 {
                continue;
            }
            if line.find(':').is_some() {
                let (k, v) = line.split_once(':').unwrap();
                headers.push((k.trim().to_string(), v.trim().to_string()));
            }
        }

        // Find body: after double CRLF
        let body = if let Some(crlf_pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let start = crlf_pos + 4;
            buf[start..].to_vec()
        } else {
            Vec::new()
        };

        Some(IncomingRequest {
            id,
            method,
            path,
            query,
            headers,
            body,
        })
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
    pub async fn poll(&mut self) -> Result<Option<IncomingRequest>, String> {
        let mut rx = self.rx.lock().unwrap();
        if let Some(rx) = rx.as_mut() {
            Ok(rx.blocking_recv())
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
}

impl Default for HttpServer {
    fn default() -> Self {
        Self::new()
    }
}