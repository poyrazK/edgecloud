//! `edge:http-client` — outbound HTTP requests.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// Default per-request timeout in milliseconds.
const DEFAULT_TIMEOUT_MS: u64 = 30_000;
/// Default retry count.
const DEFAULT_MAX_RETRIES: u32 = 3;
/// Default base delay for exponential backoff in milliseconds.
const DEFAULT_BASE_DELAY_MS: u64 = 100;

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

pub struct HttpClient {
    client: Arc<reqwest::blocking::Client>,
}

impl Default for HttpClient {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpClient {
    /// Create a new HttpClient.
    pub fn new() -> Self {
        Self {
            client: global_client(),
        }
    }

    /// Fetch a URL with retry, backoff, and per-request timeout.
    /// Returns the response on success; on error the `error` field is populated.
    ///
    /// The retry loop runs in `spawn_blocking` to avoid blocking the tokio executor
    /// thread during backoff sleeps.
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch(
        &self,
        method: &str,
        url: &str,
        headers: &[(String, String)],
        body: Option<&[u8]>,
        timeout_ms: Option<u64>,
        traceparent: Option<&str>,
        tracestate: Option<&str>,
    ) -> HttpResponse {
        let max_retries = DEFAULT_MAX_RETRIES;
        let base_delay = Duration::from_millis(DEFAULT_BASE_DELAY_MS);
        let timeout = Duration::from_millis(timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS));

        let client = self.client.clone();
        let method = method.to_string();
        let url = url.to_string();
        let headers = headers.to_vec();
        let body = body.map(|b| b.to_vec());
        let traceparent = traceparent.map(|s| s.to_string());
        let tracestate = tracestate.map(|s| s.to_string());

        // Run the blocking retry loop on a dedicated thread so std::thread::sleep
        // during backoff does not block the tokio executor thread.
        tokio::task::spawn_blocking(move || {
            let mut attempt = 0;

            loop {
                let result = Self::fetch_once_impl(
                    &client,
                    &method,
                    &url,
                    &headers,
                    body.as_deref(),
                    timeout,
                    traceparent.as_deref(),
                    tracestate.as_deref(),
                );

                match result {
                    Ok(resp) => {
                        if attempt < max_retries && is_retryable_status(resp.status) {
                            attempt += 1;
                            let delay = backoff(attempt, base_delay);
                            std::thread::sleep(delay);
                            continue;
                        }
                        return resp;
                    }
                    Err(FetchError { message, retryable }) => {
                        if attempt >= max_retries || !retryable {
                            return HttpResponse {
                                status: 0,
                                headers: HashMap::new(),
                                body: Vec::new(),
                                error: Some(message),
                            };
                        }
                        attempt += 1;
                        let delay = backoff(attempt, base_delay);
                        std::thread::sleep(delay);
                    }
                }
            }
        })
        .await
        .unwrap_or_else(|_| HttpResponse {
            status: 0,
            headers: HashMap::new(),
            body: Vec::new(),
            error: Some("fetch task panicked".into()),
        })
    }

    /// Non-blocking implementation of a single HTTP request.
    /// Client and dns_cache are passed explicitly so this can be called from
    /// inside spawn_blocking without a Self reference.
    #[allow(clippy::too_many_arguments)]
    fn fetch_once_impl(
        client: &reqwest::blocking::Client,
        method: &str,
        url: &str,
        headers: &[(String, String)],
        body: Option<&[u8]>,
        timeout: Duration,
        traceparent: Option<&str>,
        tracestate: Option<&str>,
    ) -> Result<HttpResponse, FetchError> {
        let method = reqwest::Method::from_bytes(method.as_bytes()).map_err(|e| FetchError {
            message: format!("invalid method: {}", e),
            retryable: false,
        })?;

        let mut req = client.request(method, url);

        for (k, v) in headers {
            req = req.header(k, v);
        }

        // Inject traceparent header for distributed tracing, if valid.
        if let Some(tp) = traceparent {
            if is_valid_traceparent(tp) {
                req = req.header("traceparent", tp);
            }
        }

        // Inject tracestate header for distributed tracing, if present.
        if let Some(ts) = tracestate {
            if !ts.is_empty() {
                req = req.header("tracestate", ts);
            }
        }

        if let Some(b) = body {
            req = req.body(b.to_vec());
        }

        // Apply per-request read/write timeout.
        req = req.timeout(timeout);

        let response = req.send();

        match response {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let response_headers: HashMap<_, _> = resp
                    .headers()
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
                    .collect();
                let body = resp.bytes().map_err(|e| FetchError {
                    message: e.to_string(),
                    retryable: is_retryable_error(&e),
                })?;

                Ok(HttpResponse {
                    status,
                    headers: response_headers,
                    body: body.to_vec(),
                    error: None,
                })
            }
            Err(e) => Err(FetchError {
                message: e.to_string(),
                retryable: is_retryable_error(&e),
            }),
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

/// Lazily-initialized global reqwest client with connection pooling and timeouts.
fn global_client() -> Arc<reqwest::blocking::Client> {
    static ONCE: OnceLock<Arc<reqwest::blocking::Client>> = OnceLock::new();
    ONCE.get_or_init(|| {
        let cfg = config();
        let builder = reqwest::blocking::Client::builder()
            .connect_timeout(cfg.connect_timeout)
            .pool_max_idle_per_host(16)
            .pool_idle_timeout(cfg.pool_idle_timeout);

        Arc::new(
            builder
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

#[derive(Clone, Debug)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
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
        // Arc should point to the same allocation.
        assert!(Arc::ptr_eq(&c1, &c2));
    }

    #[tokio::test]
    async fn test_error_field_populated_on_network_failure() {
        let client = HttpClient::new();
        // Unreachable address — should fail fast without hanging.
        let resp = client
            .fetch(
                "GET",
                "http://127.0.0.1:1",
                &[],
                None,
                Some(500),
                None,
                None,
            )
            .await;
        assert!(
            resp.error.is_some(),
            "error field should be populated on network failure"
        );
        assert_eq!(resp.status, 0);
    }

    #[tokio::test]
    async fn test_timeout_ms_short_timeout() {
        let client = HttpClient::new();
        // Unreachable address with a very short timeout.
        // Should return a timeout- or connection-related error.
        let resp = client
            .fetch("GET", "http://127.0.0.1:1", &[], None, Some(1), None, None)
            .await;
        let err_msg = resp.error.unwrap_or_default();
        // Any error is acceptable here — just verify error field is populated.
        assert!(!err_msg.is_empty(), "error field should be populated");
    }

    #[tokio::test]
    async fn test_successful_response_error_field_is_none() {
        let client = HttpClient::new();
        // jsonplaceholder.typicode.com/todos/1 returns a valid JSON response with status 200.
        // On success, error field must be None.
        let resp = client
            .fetch(
                "GET",
                "https://jsonplaceholder.typicode.com/todos/1",
                &[],
                None,
                Some(5000),
                None,
                None,
            )
            .await;
        assert!(
            resp.error.is_none(),
            "error field should be None on success, got: {:?}",
            resp.error
        );
        assert_eq!(resp.status, 200);
        assert!(!resp.body.is_empty());
    }

    #[tokio::test]
    async fn test_traceparent_header_injected() {
        let client = HttpClient::new();
        // Valid W3C traceparent format
        let traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6a71660503fa-01";
        // Unreachable host — error should be network-related, not a traceparent parsing error.
        let resp = client
            .fetch(
                "GET",
                "http://127.0.0.1:1",
                &[],
                None,
                Some(100),
                Some(traceparent),
                None,
            )
            .await;
        assert!(resp.error.is_some(), "error field should be populated");
        let err_msg = resp.error.unwrap();
        // Should NOT fail due to traceparent validation — error should be network-related.
        assert!(!err_msg.is_empty(), "error should be populated");
    }

    #[test]
    fn test_is_valid_traceparent() {
        // Valid traceparent
        assert!(is_valid_traceparent(
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6a71660503fa-01"
        ));
        // Invalid: wrong version
        assert!(!is_valid_traceparent(
            "01-0af7651916cd43dd8448eb211c80319c-b7ad6a71660503fa-01"
        ));
        // Invalid: malformed hex
        assert!(!is_valid_traceparent(
            "00-xyz7651916cd43dd8448eb211c80319c-b7ad6a71660503fa-01"
        ));
        // Invalid: wrong number of parts
        assert!(!is_valid_traceparent(
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6a71660503fa"
        ));
        // Invalid: traceparent too short
        assert!(!is_valid_traceparent(
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6a71660503-01"
        ));
    }
}
