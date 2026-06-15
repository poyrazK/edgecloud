//! `edge:http-client` — outbound HTTP requests.

use std::sync::Arc;
use std::time::Duration;
use std::collections::HashMap;

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
    ) -> HttpResponse {
        let max_retries = DEFAULT_MAX_RETRIES;
        let base_delay = Duration::from_millis(DEFAULT_BASE_DELAY_MS);
        let timeout = Duration::from_millis(timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS));

        let mut attempt = 0;

        loop {
            let result = self.fetch_once(method, url, headers, body, timeout);

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
                Err(e) => {
                    // Don't retry on permanent errors.
                    if attempt >= max_retries || e.contains("request timeout") {
                        return HttpResponse {
                            status: 0,
                            headers: HashMap::new(),
                            body: Vec::new(),
                            error: Some(e),
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
    ) -> Result<HttpResponse, String> {
        let method = reqwest::Method::from_bytes(method.as_bytes())
            .map_err(|e| format!("invalid method: {}", e))?;

        let mut req = self.client.request(method, url);

        for (k, v) in headers {
            req = req.header(k, v);
        }

        if let Some(b) = body {
            req = req.body(b.to_vec());
        }

        // Apply per-request timeout via request builder.
        req = req.timeout(timeout);

        let response = req.send().map_err(|e| format!("request failed: {}", e))?;

        let status = response.status().as_u16();
        let response_headers: HashMap<_, _> = response
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        let body = response
            .bytes()
            .map_err(|e| format!("failed to read body: {}", e))?;

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
}
