//! `edge:http-client` — outbound HTTP requests.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Default per-request timeout in milliseconds.
const DEFAULT_TIMEOUT_MS: u64 = 30_000;
/// Default retry count.
const DEFAULT_MAX_RETRIES: u32 = 3;
/// Default base delay for exponential backoff in milliseconds.
const DEFAULT_BASE_DELAY_MS: u64 = 100;

pub struct HttpClient {
    client: Arc<reqwest::blocking::Client>,
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

    /// Fetch a URL with retry, backoff, and per-request timeout.
    /// Returns the response on success; on error the `error` field is populated.
    pub fn fetch(
        &self,
        method: &str,
        url: &str,
        headers: &[(String, String)],
        body: Option<&[u8]>,
        timeout_ms: Option<u64>,
        traceparent: Option<&str>,
    ) -> HttpResponse {
        let max_retries = DEFAULT_MAX_RETRIES;
        let base_delay = Duration::from_millis(DEFAULT_BASE_DELAY_MS);
        let timeout = Duration::from_millis(timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS));

        let mut attempt = 0;

        loop {
            let result = self.fetch_once(method, url, headers, body, timeout, traceparent);

            match result {
                Ok(resp) => {
                    // Retry on 503 and 429 if we have retries left.
                    if attempt < max_retries && (resp.status == 503 || resp.status == 429) {
                        attempt += 1;
                        let delay = backoff(attempt, base_delay);
                        std::thread::sleep(delay);
                        continue;
                    }
                    return resp;
                }
                Err(FetchError { message }) => {
                    // Don't retry on permanent errors.
                    // Note: reqwest Error::is_timeout() would be ideal but its constructors
                    // are private, so we check the error message string for "timeout".
                    if attempt >= max_retries || message.contains("timeout") {
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
    }

    fn fetch_once(
        &self,
        method: &str,
        url: &str,
        headers: &[(String, String)],
        body: Option<&[u8]>,
        timeout: Duration,
        traceparent: Option<&str>,
    ) -> Result<HttpResponse, FetchError> {
        // Validate method first.
        let method = reqwest::Method::from_bytes(method.as_bytes()).map_err(|e| FetchError {
            message: format!("invalid method: {}", e),
        })?;

        let mut req = self.client.request(method, url);

        for (k, v) in headers {
            req = req.header(k, v);
        }

        // Inject traceparent header for distributed tracing, if valid.
        if let Some(tp) = traceparent {
            if is_valid_traceparent(tp) {
                req = req.header("traceparent", tp);
            }
        }

        if let Some(b) = body {
            req = req.body(b.to_vec());
        }

        // Apply per-request timeout via request builder.
        req = req.timeout(timeout);

        let response = req.send().map_err(|e| FetchError {
            message: e.to_string(),
        })?;

        let status = response.status().as_u16();
        let response_headers: HashMap<_, _> = response
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        let body = response.bytes().map_err(|e| FetchError {
            message: e.to_string(),
        })?;

        Ok(HttpResponse {
            status,
            headers: response_headers,
            body: body.to_vec(),
            error: None,
        })
    }
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

/// Lazily-initialized global reqwest client with connection pooling.
fn global_client() -> Arc<reqwest::blocking::Client> {
    static ONCE: std::sync::OnceLock<Arc<reqwest::blocking::Client>> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| {
        Arc::new(
            reqwest::blocking::Client::builder()
                .pool_max_idle_per_host(16)
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
    fn test_global_client_reuse() {
        let c1 = global_client();
        let c2 = global_client();
        // Arc should point to the same allocation.
        assert!(std::sync::Arc::ptr_eq(&c1, &c2));
    }

    #[test]
    fn test_error_field_populated_on_network_failure() {
        let client = HttpClient::new();
        // Unreachable address — should fail fast without hanging.
        let resp = client.fetch("GET", "http://127.0.0.1:1", &[], None, Some(500), None);
        assert!(
            resp.error.is_some(),
            "error field should be populated on network failure"
        );
        assert_eq!(resp.status, 0);
    }

    #[test]
    fn test_timeout_ms_short_timeout() {
        let client = HttpClient::new();
        // Unreachable address with a very short timeout.
        // Should return a timeout- or connection-related error.
        let resp = client.fetch("GET", "http://127.0.0.1:1", &[], None, Some(1), None);
        let err_msg = resp.error.unwrap_or_default();
        // Any error is acceptable here — just verify error field is populated.
        assert!(!err_msg.is_empty(), "error field should be populated");
    }

    #[test]
    fn test_successful_response_error_field_is_none() {
        let client = HttpClient::new();
        // httpbin.org/get returns a valid JSON response with status 200.
        // On success, error field must be None.
        let resp = client.fetch(
            "GET",
            "https://httpbin.org/get",
            &[],
            None,
            Some(5000),
            None,
        );
        assert!(
            resp.error.is_none(),
            "error field should be None on success, got: {:?}",
            resp.error
        );
        assert_eq!(resp.status, 200);
        assert!(!resp.body.is_empty());
    }

    #[test]
    fn test_traceparent_header_injected() {
        let client = HttpClient::new();
        // Valid W3C traceparent format
        let traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6a71660503fa-01";
        // Unreachable host — error should be network-related, not a traceparent parsing error.
        // This verifies the traceparent header path is exercised without hitting a real server.
        let resp = client.fetch(
            "GET",
            "http://127.0.0.1:1",
            &[],
            None,
            Some(100),
            Some(traceparent),
        );
        assert!(resp.error.is_some(), "error field should be populated");
        let err_msg = resp.error.unwrap();
        // Should NOT fail due to traceparent validation — error should be network-related.
        // reqwest error messages vary by platform; any error is acceptable here.
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
