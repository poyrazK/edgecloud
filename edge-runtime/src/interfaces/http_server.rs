//! `edge:http-server` — inbound HTTP serving (stub).

use std::sync::Arc;

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
    next_id: Arc<std::sync::atomic::AtomicU64>,
}

impl HttpServer {
    pub fn new() -> Self {
        Self {
            port: None,
            next_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        }
    }

    pub fn start(&mut self, port: u16, host: Option<String>) -> Result<(), String> {
        let addr = format!("{}:{}", host.as_deref().unwrap_or("0.0.0.0"), port);
        self.port = Some(port);
        tracing::info!(addr = %addr, "http-server start (stub)");
        Ok(())
    }

    pub fn poll(&mut self) -> Option<IncomingRequest> {
        None
    }

    pub fn respond(
        &self,
        _req_id: u64,
        _status: u16,
        _headers: Vec<(String, String)>,
        _body: Vec<u8>,
    ) -> Result<(), String> {
        Ok(())
    }
}
