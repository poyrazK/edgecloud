//! Worker `LogForwarder` — receives tenant `emit_log` records from the
//! runtime and ships them to the control plane's `POST /api/internal/logs`.
//!
//! Architecture
//! ============
//!
//! `LogForwarder` implements `edge_runtime::LogSink`. The runtime calls
//! `push()` synchronously from inside the guest's WIT call, so `push()`
//! must be cheap: it appends to an in-memory buffer and (if the buffer is
//! full enough to warrant an early flush) signals a background task.
//!
//! A separate task (`flush_loop`) periodically flushes the buffer over HTTP:
//! it reads `flush_interval` (default 1s), or reacts to the early-flush
//! notification, or drains the final batch on shutdown.
//!
//! Failure handling
//! ================
//!
//! - 2xx: batch dropped (no ack).
//! - 4xx (except 429): log error and drop. Bad request won't get better.
//! - 5xx / network: log error and drop. No retry queue in MVP.
//! - overflow: drop new entries past `max_buffer_len * HARD_CAP_MULT`.
//!   This is the only backpressure in MVP — a flood of logs loses recent
//!   entries rather than OOMing the worker. Per-tenant quota and disk
//!   spool are follow-ups.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use edge_runtime::interfaces::observe::{AppLogContext, LogLevel, LogRecord, LogSink};
use serde::Serialize;
use tokio::sync::broadcast;
use tokio::sync::Notify;

use crate::auth::WorkerJwtSigner;

// ---------------------------------------------------------------------------
// Tunables
// ---------------------------------------------------------------------------

/// Default cap on buffered entries before an early flush is triggered.
const DEFAULT_MAX_BUFFER_LEN: usize = 100;
/// Hard cap on the buffer when under flood: `max_buffer_len * HARD_CAP_MULT`.
/// New `push()` calls beyond this drop the entry and log a warning.
const HARD_CAP_MULT: usize = 10;
/// Default flush interval — drives the periodic flush in `flush_loop`.
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(1);
/// Per-request timeout for the HTTP POST.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Wire format — matches `domain.LogEntry` (Go control plane)
// ---------------------------------------------------------------------------

/// JSON shape posted to `/api/internal/logs`. The Go control plane accepts
/// `labels` as JSON RawMessage; we send a JSON object (object form is the
/// canonical input for the handler).
///
/// `ts` is intentionally absent — the DB DEFAULT NOW() stamps the row at
/// insert time, avoiding per-worker clock skew from contaminating the
/// ordering of logs in the same batch.
#[derive(Debug, Serialize)]
struct WireEntry {
    tenant_id: String,
    deployment_id: String,
    app_name: String,
    worker_id: String,
    region: String,
    level: &'static str,
    message: String,
    labels: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct IngestLogsRequest {
    entries: Vec<WireEntry>,
}

// ---------------------------------------------------------------------------
// ForwarderState — shared between push() and flush_loop()
// ---------------------------------------------------------------------------

struct ForwarderState {
    /// Buffer of pending entries. Drained (swapped with empty Vec) on each
    /// flush. Mutex is fine — contention is negligible compared to HTTP RTT.
    buffer: Vec<WireEntry>,
    /// Soft cap: when `buffer.len() >= max_buffer_len`, signal an early flush.
    max_buffer_len: usize,
    /// Hard cap: when `buffer.len() > hard_cap`, drop incoming pushes.
    hard_cap: usize,
}

// ---------------------------------------------------------------------------
// LogForwarder
// ---------------------------------------------------------------------------

/// Per-worker log shipper. One instance is shared across all apps the
/// worker hosts — the per-app `AppLogContext` travels with each record.
pub struct LogForwarder {
    state: Mutex<ForwarderState>,
    /// Signals `flush_loop` when an early flush should be considered.
    notify: Notify,
    client: reqwest::Client,
    control_plane_url: String,
    worker_id: String,
    region: String,
    jwt_signer: Arc<WorkerJwtSigner>,
    flush_interval: Duration,
}

impl LogForwarder {
    pub fn new(
        control_plane_url: impl Into<String>,
        worker_id: impl Into<String>,
        region: impl Into<String>,
        jwt_signer: Arc<WorkerJwtSigner>,
    ) -> Arc<Self> {
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .expect("reqwest client builder should not fail");

        let max_buffer_len = DEFAULT_MAX_BUFFER_LEN;
        Arc::new(Self {
            state: Mutex::new(ForwarderState {
                buffer: Vec::with_capacity(max_buffer_len),
                max_buffer_len,
                hard_cap: max_buffer_len * HARD_CAP_MULT,
            }),
            notify: Notify::new(),
            client,
            control_plane_url: control_plane_url.into(),
            worker_id: worker_id.into(),
            region: region.into(),
            jwt_signer,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
        })
    }

    /// Run the flush loop until the shutdown signal fires. Performs one
    /// final flush before returning so in-flight logs survive a clean
    /// worker shutdown.
    pub async fn flush_loop(self: Arc<Self>, mut shutdown: broadcast::Receiver<()>) {
        let mut ticker = tokio::time::interval(self.flush_interval);
        // Skip the immediate-tick that `interval()` fires on creation; we
        // want a steady cadence starting at `flush_interval` from now.
        ticker.tick().await;

        loop {
            tokio::select! {
                // biased: shutdown always wins when both are ready.
                biased;
                _ = shutdown.recv() => {
                    tracing::info!("log_forwarder: shutdown signal received; final flush");
                    self.flush_now().await;
                    break;
                }
                _ = ticker.tick() => {
                    self.flush_now().await;
                }
                _ = self.notify.notified() => {
                    self.flush_now().await;
                }
            }
        }
    }

    /// Drain the buffer and POST it. Public-via-`pub(super)` for tests that
    /// want to drive a flush without a ticker / notify.
    pub async fn flush_now(&self) {
        let entries = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.buffer.is_empty() {
                return;
            }
            std::mem::take(&mut state.buffer)
        };

        let count = entries.len();
        let body = IngestLogsRequest { entries };
        let url = format!("{}/api/internal/logs", self.control_plane_url);
        let token = self.jwt_signer.sign();

        let result = self
            .client
            .post(&url)
            .bearer_auth(token)
            .json(&body)
            .send()
            .await;

        match result {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    tracing::debug!(count, status = status.as_u16(), "logs flushed");
                } else if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                    tracing::warn!(count, "logs dropped: 429 (control plane backpressure)");
                } else if status.is_client_error() {
                    tracing::error!(
                        count,
                        status = status.as_u16(),
                        "logs dropped: 4xx (bad batch — won't retry)"
                    );
                } else {
                    tracing::error!(
                        count,
                        status = status.as_u16(),
                        "logs dropped: 5xx (no retry in MVP)"
                    );
                }
            }
            Err(e) => {
                tracing::error!(count, err = %e, "logs dropped: HTTP error");
            }
        }
    }
}

impl LogSink for LogForwarder {
    fn push(&self, record: LogRecord, ctx: AppLogContext) {
        let entry = WireEntry {
            tenant_id: ctx.tenant_id,
            deployment_id: ctx.deployment_id,
            app_name: ctx.app_name,
            worker_id: self.worker_id.clone(),
            region: self.region.clone(),
            level: log_level_to_string(record.level),
            message: record.message,
            labels: labels_to_json(record.labels),
        };

        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.buffer.len() >= state.hard_cap {
            tracing::warn!(
                buffer_len = state.buffer.len(),
                "log_forwarder: dropping emit_log past hard cap"
            );
            return;
        }

        let should_notify = {
            state.buffer.push(entry);
            state.buffer.len() >= state.max_buffer_len
        };
        if should_notify {
            self.notify.notify_one();
        }
    }
}

fn log_level_to_string(level: LogLevel) -> &'static str {
    match level {
        LogLevel::Error => "error",
        LogLevel::Warn => "warn",
        LogLevel::Info => "info",
        LogLevel::Debug => "debug",
        LogLevel::Trace => "trace",
    }
}

fn labels_to_json(labels: Vec<(String, String)>) -> serde_json::Value {
    let mut obj = serde_json::Map::with_capacity(labels.len());
    for (k, v) in labels {
        obj.insert(k, serde_json::Value::String(v));
    }
    serde_json::Value::Object(obj)
}

#[cfg(test)]
mod tests {
    use super::*;
    use edge_runtime::interfaces::observe::LogRecord;
    use serde_json::json;

    fn forwarder() -> Arc<LogForwarder> {
        let signer = crate::auth::WorkerJwtSigner::new(
            b"test-secret".to_vec(),
            "edgecloud",
            "w_test",
            "test-region",
            "t_test",
        );
        LogForwarder::new("http://127.0.0.1:0", "w_test", "test-region", signer)
    }

    fn ctx() -> AppLogContext {
        AppLogContext {
            app_name: "my-app".into(),
            tenant_id: "t_tenant1".into(),
            deployment_id: "d_xyz".into(),
        }
    }

    fn record(level: LogLevel, msg: &str) -> LogRecord {
        LogRecord {
            timestamp_ms: 0,
            level,
            message: msg.into(),
            labels: vec![("k".into(), "v".into())],
        }
    }

    #[test]
    fn push_appends_to_buffer() {
        let f = forwarder();
        f.push(record(LogLevel::Info, "hello"), ctx());
        let state = f.state.lock().unwrap();
        assert_eq!(state.buffer.len(), 1);
        assert_eq!(state.buffer[0].message, "hello");
        assert_eq!(state.buffer[0].level, "info");
        assert_eq!(state.buffer[0].app_name, "my-app");
        assert_eq!(state.buffer[0].worker_id, "w_test");
        assert_eq!(state.buffer[0].region, "test-region");
    }

    #[test]
    fn push_past_hard_cap_drops() {
        let f = forwarder();
        let hard_cap = DEFAULT_MAX_BUFFER_LEN * HARD_CAP_MULT;
        for i in 0..hard_cap + 5 {
            f.push(record(LogLevel::Info, &format!("m{i}")), ctx());
        }
        let state = f.state.lock().unwrap();
        assert!(
            state.buffer.len() <= hard_cap,
            "buffer must not exceed hard cap ({}), got {}",
            hard_cap,
            state.buffer.len()
        );
    }

    #[test]
    fn level_serializes_to_canonical_strings() {
        let f = forwarder();
        f.push(record(LogLevel::Error, "e"), ctx());
        f.push(record(LogLevel::Warn, "w"), ctx());
        f.push(record(LogLevel::Info, "i"), ctx());
        f.push(record(LogLevel::Debug, "d"), ctx());
        f.push(record(LogLevel::Trace, "t"), ctx());
        let state = f.state.lock().unwrap();
        let levels: Vec<&str> = state.buffer.iter().map(|e| e.level).collect();
        assert_eq!(levels, vec!["error", "warn", "info", "debug", "trace"]);
    }

    #[test]
    fn labels_serialize_as_json_object() {
        let f = forwarder();
        f.push(
            LogRecord {
                timestamp_ms: 0,
                level: LogLevel::Info,
                message: "with labels".into(),
                labels: vec![("a".into(), "1".into()), ("b".into(), "2".into())],
            },
            ctx(),
        );
        let state = f.state.lock().unwrap();
        let expected = json!({"a": "1", "b": "2"});
        assert_eq!(state.buffer[0].labels, expected);
    }

    #[tokio::test]
    async fn flush_drains_buffer() {
        // Mount a wiremock that returns 204; flush_now() should POST and
        // drain the buffer.
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/internal/logs"))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&server)
            .await;

        let signer = crate::auth::WorkerJwtSigner::new(
            b"test-secret".to_vec(),
            "edgecloud",
            "w_test",
            "test-region",
            "t_test",
        );
        let f = LogForwarder::new(server.uri(), "w_test", "test-region", signer);

        f.push(record(LogLevel::Info, "1"), ctx());
        f.push(record(LogLevel::Info, "2"), ctx());
        assert_eq!(f.state.lock().unwrap().buffer.len(), 2);

        f.flush_now().await;

        // After flush, the buffer must be empty and the mock must have
        // received exactly one request.
        assert_eq!(f.state.lock().unwrap().buffer.len(), 0);
        let received = server.received_requests().await.expect("received");
        assert_eq!(received.len(), 1);
    }

    #[tokio::test]
    async fn flush_on_empty_buffer_is_noop() {
        use wiremock::MockServer;
        let server = MockServer::start().await;

        let signer = crate::auth::WorkerJwtSigner::new(
            b"test-secret".to_vec(),
            "edgecloud",
            "w_test",
            "test-region",
            "t_test",
        );
        let f = LogForwarder::new(server.uri(), "w_test", "test-region", signer);

        f.flush_now().await;

        // No request should have been sent.
        let received = server.received_requests().await.expect("received");
        assert!(received.is_empty());
    }

    #[tokio::test]
    async fn flush_on_5xx_drops_batch_does_not_panic() {
        // A 5xx response means the batch is dropped — no retry in MVP.
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/internal/logs"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let signer = crate::auth::WorkerJwtSigner::new(
            b"test-secret".to_vec(),
            "edgecloud",
            "w_test",
            "test-region",
            "t_test",
        );
        let f = LogForwarder::new(server.uri(), "w_test", "test-region", signer);

        f.push(record(LogLevel::Info, "drop me"), ctx());
        f.flush_now().await;

        // Buffer is drained regardless — drops happen post-drain.
        assert_eq!(f.state.lock().unwrap().buffer.len(), 0);
    }

    #[tokio::test]
    async fn flush_loop_drains_remaining_on_shutdown() {
        // Push 3 entries; signal shutdown; loop must do a final flush.
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/internal/logs"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;

        let signer = crate::auth::WorkerJwtSigner::new(
            b"test-secret".to_vec(),
            "edgecloud",
            "w_test",
            "test-region",
            "t_test",
        );
        let f = LogForwarder::new(server.uri(), "w_test", "test-region", signer);

        f.push(record(LogLevel::Info, "shutdown-1"), ctx());
        f.push(record(LogLevel::Info, "shutdown-2"), ctx());
        f.push(record(LogLevel::Info, "shutdown-3"), ctx());

        let (tx, rx) = broadcast::channel::<()>(1);
        let f_clone = f.clone();
        let handle = tokio::spawn(async move {
            f_clone.flush_loop(rx).await;
        });

        // Give the loop a moment to start (so it doesn't tick and flush early).
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = tx.send(());

        // Wait for the loop to exit (final flush must complete).
        tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("flush_loop did not exit")
            .expect("flush_loop task panicked");

        assert_eq!(f.state.lock().unwrap().buffer.len(), 0);
        let received = server.received_requests().await.expect("received");
        assert_eq!(received.len(), 1, "expected exactly one final flush");
    }
}
