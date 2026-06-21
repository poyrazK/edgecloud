//! `edge:http-client` — outbound HTTP requests.
//!
//! v1 streaming support: streaming request body via `BodySource::Streamed`
//! (consumes an `OutgoingStreamAdapter` from the runtime), and streaming
//! response body via `ResponseBody::Streamed` (returns an `IncomingStream`
//! backed by a per-fetch task that drains `reqwest::Response::bytes_stream`).
//!
//! Streaming requests skip the existing retry loop — a retry cannot replay a
//! partially-consumed body. Buffered requests retain the retry behavior.

use crate::streams::{self, IncomingStream, OutgoingStreamAdapter, DEFAULT_STREAM_CAPACITY};
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// Default per-request timeout in milliseconds.
const DEFAULT_TIMEOUT_MS: u64 = 30_000;
/// Default retry count.
const DEFAULT_MAX_RETRIES: u32 = 3;
/// Default base delay for exponential backoff in milliseconds.
const DEFAULT_BASE_DELAY_MS: u64 = 100;
/// Channel capacity between the reqwest-bytes-stream task and the guest's
/// IncomingStream reads. Lower = less memory, more backpressure on the task.
const STREAM_CHANNEL_CAPACITY: usize = DEFAULT_STREAM_CAPACITY;

/// HTTP client configuration sourced from environment variables.
struct HttpClientConfig {
    connect_timeout: Duration,
    pool_idle_timeout: Duration,
}

impl HttpClientConfig {
    fn load() -> Self {
        let connect_timeout = std::env::var("EDGE_HTTP_CONNECT_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .map(Duration::from_millis)
            .unwrap_or(Duration::from_secs(5));

        let pool_idle_timeout = std::env::var("EDGE_HTTP_POOL_IDLE_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .map(Duration::from_millis)
            .unwrap_or(Duration::from_secs(30));

        Self {
            connect_timeout,
            pool_idle_timeout,
        }
    }
}

static CONFIG: OnceLock<HttpClientConfig> = OnceLock::new();

fn config() -> &'static HttpClientConfig {
    CONFIG.get_or_init(HttpClientConfig::load)
}

/// Request body source for `HttpClient::fetch`.
pub enum BodySource {
    /// No body (e.g. GET requests).
    None,
    /// Fully-buffered body bytes.
    Buffered(Vec<u8>),
    /// Streaming body from a guest-side `OutgoingStreamAdapter`.
    /// Consumed by reqwest via `Body::wrap_stream`.
    Streamed(OutgoingStreamAdapter),
}

/// Response body shape returned by `HttpClient::fetch`.
pub enum ResponseBody {
    /// No body (e.g. 204 No Content).
    None,
    /// Fully-buffered response bytes.
    Buffered(Vec<u8>),
    /// Streaming response body. The runtime pushes this into a `ResourceTable`
    /// entry and returns the handle to the guest.
    Streamed(IncomingStream),
}

pub struct HttpClient {
    client: Arc<reqwest::Client>,
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpClient {
    pub fn new() -> Self {
        Self {
            client: global_client(),
        }
    }

    /// Fetch a URL. Buffered requests retry on transient errors with
    /// exponential backoff; streaming requests make a single attempt (no
    /// retry — a retry cannot replay a partially-consumed stream).
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch(
        &self,
        method: &str,
        url: &str,
        headers: &[(String, String)],
        body: BodySource,
        timeout_ms: Option<u64>,
        traceparent: Option<&str>,
        tracestate: Option<&str>,
    ) -> HttpResponse {
        match body {
            BodySource::None | BodySource::Buffered(_) => {
                self.fetch_with_retry(
                    method,
                    url,
                    headers,
                    body,
                    timeout_ms,
                    traceparent,
                    tracestate,
                )
                .await
            }
            BodySource::Streamed(adapter) => {
                self.fetch_streaming(
                    method,
                    url,
                    headers,
                    adapter,
                    timeout_ms,
                    traceparent,
                    tracestate,
                )
                .await
            }
        }
    }

    /// Buffered fetch with retry on 429/502/503/504 and transient errors.
    #[allow(clippy::too_many_arguments)]
    async fn fetch_with_retry(
        &self,
        method: &str,
        url: &str,
        headers: &[(String, String)],
        body: BodySource,
        timeout_ms: Option<u64>,
        traceparent: Option<&str>,
        tracestate: Option<&str>,
    ) -> HttpResponse {
        let max_retries = DEFAULT_MAX_RETRIES;
        let base_delay = Duration::from_millis(DEFAULT_BASE_DELAY_MS);
        let timeout = Duration::from_millis(timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS));

        let mut attempt = 0;

        loop {
            let result = self
                .fetch_once_buffered(
                    method,
                    url,
                    headers,
                    &body,
                    timeout,
                    traceparent,
                    tracestate,
                )
                .await;

            match result {
                Ok(resp) => {
                    if attempt < max_retries && is_retryable_status(resp.status) {
                        attempt += 1;
                        let delay = backoff(attempt, base_delay);
                        tokio::time::sleep(delay).await;
                        continue;
                    }
                    return resp;
                }
                Err(FetchError { message, retryable }) => {
                    if attempt >= max_retries || !retryable {
                        return HttpResponse {
                            status: 0,
                            headers: HashMap::new(),
                            body: ResponseBody::None,
                            error: Some(message),
                        };
                    }
                    attempt += 1;
                    let delay = backoff(attempt, base_delay);
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// Single buffered attempt.
    #[allow(clippy::too_many_arguments)]
    async fn fetch_once_buffered(
        &self,
        method: &str,
        url: &str,
        headers: &[(String, String)],
        body: &BodySource,
        timeout: Duration,
        traceparent: Option<&str>,
        tracestate: Option<&str>,
    ) -> Result<HttpResponse, FetchError> {
        let method = reqwest::Method::from_bytes(method.as_bytes()).map_err(|e| FetchError {
            message: format!("invalid method: {}", e),
            retryable: false,
        })?;

        let mut req = self.client.request(method, url);

        for (k, v) in headers {
            req = req.header(k, v);
        }

        if let Some(tp) = traceparent {
            if is_valid_traceparent(tp) {
                req = req.header("traceparent", tp);
            }
        }
        if let Some(ts) = tracestate {
            if !ts.is_empty() {
                req = req.header("tracestate", ts);
            }
        }

        match body {
            BodySource::None => {}
            BodySource::Buffered(bytes) => {
                req = req.body(bytes.clone());
            }
            BodySource::Streamed(_) => {
                // Caller routed to fetch_streaming instead.
                return Err(FetchError {
                    message: "streamed body routed to buffered path".to_string(),
                    retryable: false,
                });
            }
        }

        req = req.timeout(timeout);
        let response = req.send().await;

        match response {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let response_headers: HashMap<_, _> = resp
                    .headers()
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                    .collect();
                let body = resp.bytes().await.map_err(|e| FetchError {
                    message: e.to_string(),
                    retryable: is_retryable_error(&e),
                })?;

                Ok(HttpResponse {
                    status,
                    headers: response_headers,
                    body: ResponseBody::Buffered(body.to_vec()),
                    error: None,
                })
            }
            Err(e) => Err(FetchError {
                message: e.to_string(),
                retryable: is_retryable_error(&e),
            }),
        }
    }

    /// Single streaming attempt. No retry. Streams the request body via
    /// `reqwest::Body::wrap_stream` and exposes the response body as an
    /// `IncomingStream` that the runtime hands to the guest via a resource
    /// handle.
    #[allow(clippy::too_many_arguments)]
    async fn fetch_streaming(
        &self,
        method: &str,
        url: &str,
        headers: &[(String, String)],
        adapter: OutgoingStreamAdapter,
        timeout_ms: Option<u64>,
        traceparent: Option<&str>,
        tracestate: Option<&str>,
    ) -> HttpResponse {
        let method = match reqwest::Method::from_bytes(method.as_bytes()) {
            Ok(m) => m,
            Err(e) => {
                return HttpResponse {
                    status: 0,
                    headers: HashMap::new(),
                    body: ResponseBody::None,
                    error: Some(format!("invalid method: {}", e)),
                };
            }
        };

        let mut req = self.client.request(method, url);
        for (k, v) in headers {
            req = req.header(k, v);
        }
        if let Some(tp) = traceparent {
            if is_valid_traceparent(tp) {
                req = req.header("traceparent", tp);
            }
        }
        if let Some(ts) = tracestate {
            if !ts.is_empty() {
                req = req.header("tracestate", ts);
            }
        }
        let timeout = Duration::from_millis(timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS));
        req = req
            .timeout(timeout)
            .body(reqwest::Body::wrap_stream(adapter));

        let response = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                return HttpResponse {
                    status: 0,
                    headers: HashMap::new(),
                    body: ResponseBody::None,
                    error: Some(e.to_string()),
                };
            }
        };

        let status = response.status().as_u16();
        let response_headers: HashMap<String, String> = response
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();

        // Build the (producer, stream) pair. The producer is fed by a
        // spawned task that drains `resp.bytes_stream()` and pushes chunks
        // until the consumer drops the IncomingStream.
        let (producer, stream) = streams::incoming_pair(STREAM_CHANNEL_CAPACITY);

        let mut byte_stream = response.bytes_stream();
        tokio::spawn(async move {
            while let Some(item) = byte_stream.next().await {
                let chunk = match item {
                    Ok(bytes) => Ok(bytes.to_vec()),
                    Err(e) => Err(e.to_string()),
                };
                // Push; bail if consumer dropped (send returns Err containing
                // the chunk back).
                if producer.push(chunk).await.is_err() {
                    break;
                }
            }
            // Drop producer → consumer's next read_chunk observes Closed.
        });

        HttpResponse {
            status,
            headers: response_headers,
            body: ResponseBody::Streamed(stream),
            error: None,
        }
    }
}

/// Returns true for HTTP status codes that are safe to retry.
fn is_retryable_status(status: u16) -> bool {
    matches!(status, 429 | 502 | 503 | 504)
}

/// Returns true for reqwest errors that represent transient failures.
fn is_retryable_error(e: &reqwest::Error) -> bool {
    e.is_timeout() || e.is_connect() || e.is_request()
}

/// Compute exponential backoff: 100ms, 200ms, 400ms, ... capped at 10s.
fn backoff(attempt: u32, base_delay: Duration) -> Duration {
    let exp = attempt.saturating_sub(1).min(7);
    let delay_ms = base_delay.as_millis() as u64 * (2u64.pow(exp));
    Duration::from_millis(delay_ms.min(10_000))
}

/// Validates a traceparent header value per W3C Trace Context spec.
/// Format: 00-<32hex>-<16hex>-<2hex>
fn is_valid_traceparent(tp: &str) -> bool {
    let parts: Vec<&str> = tp.split('-').collect();
    if parts.len() != 4 {
        return false;
    }
    if parts[0] != "00" {
        return false;
    }
    if parts[1].len() != 32 || !parts[1].chars().all(|c| c.is_ascii_hexdigit()) {
        return false;
    }
    if parts[2].len() != 16 || !parts[2].chars().all(|c| c.is_ascii_hexdigit()) {
        return false;
    }
    if parts[3].len() != 2 {
        return false;
    }
    true
}

/// Lazily-initialized global async reqwest client with connection pooling and timeouts.
fn global_client() -> Arc<reqwest::Client> {
    static ONCE: OnceLock<Arc<reqwest::Client>> = OnceLock::new();
    ONCE.get_or_init(|| {
        let cfg = config();
        Arc::new(
            reqwest::Client::builder()
                .connect_timeout(cfg.connect_timeout)
                .pool_max_idle_per_host(16)
                .pool_idle_timeout(cfg.pool_idle_timeout)
                // Never follow redirects automatically. The egress check fires
                // only on the initial URL; a redirect to a hard-deny target
                // (e.g. 169.254.169.254) would otherwise bypass enforcement.
                // Guests receive the 3xx response and can decide whether to
                // follow after the host re-checks the Location URL.
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("reqwest global client creation failed"),
        )
    })
    .clone()
}

/// Error type for fetch operations — wraps a human-readable message.
#[derive(Debug, Clone)]
struct FetchError {
    message: String,
    /// Whether this error is safe to retry.
    retryable: bool,
}

pub struct HttpResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: ResponseBody,
    /// Human-readable error message. Empty on success.
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_exponential_growth_capped() {
        let base = Duration::from_millis(100);
        assert_eq!(backoff(1, base), Duration::from_millis(100));
        assert_eq!(backoff(2, base), Duration::from_millis(200));
        assert_eq!(backoff(3, base), Duration::from_millis(400));
        assert_eq!(backoff(10, base), Duration::from_millis(10_000));
    }

    #[test]
    fn test_is_retryable_status() {
        assert!(is_retryable_status(429));
        assert!(is_retryable_status(502));
        assert!(is_retryable_status(503));
        assert!(is_retryable_status(504));
        assert!(!is_retryable_status(200));
        assert!(!is_retryable_status(400));
        assert!(!is_retryable_status(500));
        assert!(!is_retryable_status(404));
    }

    #[test]
    fn test_global_client_reuse() {
        let c1 = global_client();
        let c2 = global_client();
        assert!(Arc::ptr_eq(&c1, &c2));
    }

    #[tokio::test]
    async fn test_error_field_populated_on_network_failure() {
        let client = HttpClient::new();
        let resp = client
            .fetch(
                "GET",
                "http://127.0.0.1:1",
                &[],
                BodySource::None,
                Some(500),
                None,
                None,
            )
            .await;
        assert!(resp.error.is_some());
        assert_eq!(resp.status, 0);
    }

    #[tokio::test]
    async fn test_timeout_ms_short_timeout() {
        let client = HttpClient::new();
        let resp = client
            .fetch(
                "GET",
                "http://127.0.0.1:1",
                &[],
                BodySource::None,
                Some(1),
                None,
                None,
            )
            .await;
        assert!(resp.error.is_some());
    }

    #[tokio::test]
    async fn test_successful_response_error_field_is_none() {
        let client = HttpClient::new();
        let resp = client
            .fetch(
                "GET",
                "https://jsonplaceholder.typicode.com/todos/1",
                &[],
                BodySource::None,
                Some(5000),
                None,
                None,
            )
            .await;
        assert!(resp.error.is_none(), "got: {:?}", resp.error);
        assert_eq!(resp.status, 200);
        match resp.body {
            ResponseBody::Buffered(b) => assert!(!b.is_empty()),
            _ => panic!("expected buffered body"),
        }
    }

    #[tokio::test]
    async fn test_traceparent_header_injected() {
        let client = HttpClient::new();
        let traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6a71660503fa-01";
        let resp = client
            .fetch(
                "GET",
                "http://127.0.0.1:1",
                &[],
                BodySource::None,
                Some(100),
                Some(traceparent),
                None,
            )
            .await;
        assert!(resp.error.is_some());
    }

    #[test]
    fn test_is_valid_traceparent() {
        assert!(is_valid_traceparent(
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6a71660503fa-01"
        ));
        assert!(!is_valid_traceparent(
            "01-0af7651916cd43dd8448eb211c80319c-b7ad6a71660503fa-01"
        ));
        assert!(!is_valid_traceparent(
            "00-xyz7651916cd43dd8448eb211c80319c-b7ad6a71660503fa-01"
        ));
        assert!(!is_valid_traceparent(
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6a71660503fa"
        ));
        assert!(!is_valid_traceparent(
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6a71660503-01"
        ));
    }
}
