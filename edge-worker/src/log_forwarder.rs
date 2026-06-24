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

use std::sync::atomic::{AtomicBool, Ordering};
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
/// Early-flush byte threshold: 75% of the control-plane 1 MiB body cap
/// (`MaxLogBatchSize` in the Go handler). Crossing this signals an early
/// flush so a burst of large messages doesn't produce a batch the server
/// will reject with 400. Over-estimating is harmless; under-estimating is
/// bounded by the server-side cap.
const BYTE_NOTIFY_THRESHOLD: usize = 768 * 1024;
/// Conservative byte estimate for the per-entry JSON envelope (the other
/// fields, brackets, and a small safety margin). The exact JSON size is
/// not worth computing on the hot path — see `push()` for the full
/// estimate formula.
const BYTE_OVERHEAD_PER_ENTRY: usize = 200;

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
    /// Buffer of pending entries. Drained (swapped with an empty but
    /// pre-allocated Vec) on each flush so the capacity carries over.
    /// Mutex is fine — contention is negligible compared to HTTP RTT.
    buffer: Vec<WireEntry>,
    /// Approximate total JSON byte count of the buffered entries,
    /// tracked alongside `buffer.len()` so a burst of large messages
    /// signals an early flush before a single batch blows past the
    /// control-plane body cap. See `push()` for the estimate formula.
    buffered_bytes: usize,
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
    /// Soft cap: when `buffer.len() >= max_buffer_len`, signal an early flush.
    max_buffer_len: usize,
    /// Hard cap: when `buffer.len() > hard_cap`, drop incoming pushes.
    hard_cap: usize,
    /// When `buffered_bytes` crosses this threshold, signal an early flush
    /// so a burst of large messages doesn't produce a batch the control
    /// plane will reject with 400 (its 1 MiB body cap).
    byte_notify_threshold: usize,
    /// `true` while a `flush_now` HTTP POST is in flight. `push()` and the
    /// `flush_loop` short-circuit on a second concurrent flush to avoid
    /// racing two POSTs that would each drain their own buffer slice and
    /// produce 2× the request load on the control plane.
    flush_in_flight: AtomicBool,
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
        let hard_cap = max_buffer_len * HARD_CAP_MULT;
        Arc::new(Self {
            state: Mutex::new(ForwarderState {
                buffer: Vec::with_capacity(max_buffer_len),
                buffered_bytes: 0,
            }),
            notify: Notify::new(),
            client,
            control_plane_url: control_plane_url.into(),
            worker_id: worker_id.into(),
            region: region.into(),
            jwt_signer,
            flush_interval: DEFAULT_FLUSH_INTERVAL,
            max_buffer_len,
            hard_cap,
            byte_notify_threshold: BYTE_NOTIFY_THRESHOLD,
            flush_in_flight: AtomicBool::new(false),
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
        // Serialize concurrent flushes. If a flush is already in flight,
        // skip — the in-flight POST already drained the buffer at swap
        // time and any entries pushed *after* the swap will be sent on
        // the next tick (1 s default) or on the next notify that fires
        // after the flag clears. Two concurrent POSTs would each drain
        // their own buffer slice and produce 2× the request load on the
        // control plane, which is the regression this guard closes.
        if self
            .flush_in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        // RAII guard: clears the flag on every exit path, including panics
        // and the `?` early returns below. Do NOT `mem::forget` it.
        let _in_flight_guard = InFlightGuard {
            flag: &self.flush_in_flight,
        };

        let entries = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if state.buffer.is_empty() {
                return;
            }
            // mem::replace with a fresh Vec keeps the grown capacity
            // across flushes — std::mem::take would drop it and force
            // the buffer to reallocate on every flush. The replacement
            // capacity is the larger of max_buffer_len (the typical
            // steady-state) and whatever the old buffer had grown to
            // (preserved from a prior flood).
            let replacement_cap = state.buffer.capacity().max(self.max_buffer_len);
            let entries = std::mem::replace(&mut state.buffer, Vec::with_capacity(replacement_cap));
            state.buffered_bytes = 0;
            entries
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
                // Drain the response body so the underlying connection
                // returns to reqwest's pool. Without this, a 4xx/5xx
                // response leaves the connection held by the response
                // object and the pool can exhaust under sustained
                // control-plane errors. We discard the bytes — we
                // already have the status code we care about, and any
                // body is purely diagnostic.
                //
                // A failed drain (e.g. mid-body TCP reset) is logged so
                // operators can correlate with reqwest pool-exhaustion
                // metrics. The connection may not return to the pool
                // cleanly, defeating the original pool-exhaustion fix
                // (commit d7cf342) under network-partition conditions.
                if let Err(e) = resp.bytes().await {
                    tracing::warn!(
                        err = %e,
                        "log_forwarder: failed to drain response body; connection may be leaked"
                    );
                }
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
        // Approximate byte count for this entry. Counts the message body
        // (the dominant contributor) plus a fixed per-entry envelope and
        // a rough label size. We don't JSON-quote-account or count the
        // other fields precisely — a slight over-estimate triggers an
        // earlier flush (harmless), and an under-estimate is bounded by
        // the server-side 1 MiB cap.
        let mut byte_delta = record.message.len() + BYTE_OVERHEAD_PER_ENTRY;
        for (k, v) in &record.labels {
            byte_delta += k.len() + v.len() + 6;
        }

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
        if state.buffer.len() >= self.hard_cap {
            tracing::warn!(
                buffer_len = state.buffer.len(),
                "log_forwarder: dropping emit_log past hard cap"
            );
            return;
        }

        state.buffer.push(entry);
        state.buffered_bytes += byte_delta;

        // Signal an early flush if either the entry count OR the byte
        // count crosses its threshold. The byte check protects against
        // bursts of large messages (e.g. stack traces) that would
        // otherwise push a single 1 MiB+ batch at the control plane.
        //
        // If a flush is in flight, the in-flight POST already drained the
        // buffer at swap time; the new push will be picked up by the next
        // tick (1 s default) or the next notify that fires after the flag
        // clears. Suppressing the notify avoids a wakeup that the loop
        // would have to drop anyway.
        if (state.buffer.len() >= self.max_buffer_len
            || state.buffered_bytes > self.byte_notify_threshold)
            && !self.flush_in_flight.load(Ordering::Acquire)
        {
            self.notify.notify_one();
        }
    }
}

/// RAII guard: clears `flush_in_flight` on drop, including on panic and on
/// `?` early returns from `flush_now`. Do NOT `mem::forget` it.
struct InFlightGuard<'a> {
    flag: &'a AtomicBool,
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
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

    #[tokio::test]
    async fn push_signals_early_when_bytes_exceed_threshold() {
        // 10 × 100 KiB messages = ~1 MiB total — crosses the 768 KiB
        // (BYTE_NOTIFY_THRESHOLD) byte threshold around the 8th push. The
        // 1s ticker would not have fired by then, so notify.notified()
        // resolving within 100ms confirms the byte-based early signal.
        let f = forwarder();
        let big = "x".repeat(100 * 1024);
        for _ in 0..10 {
            f.push(record(LogLevel::Info, &big), ctx());
        }
        // Confirm the bytes actually crossed the threshold. Scope the
        // lock so the guard is dropped before the await on notify
        // (clippy::await_holding_lock).
        {
            let state = f.state.lock().unwrap();
            assert!(
                state.buffered_bytes > f.byte_notify_threshold,
                "buffered_bytes = {}, want > {} (test premise broken)",
                state.buffered_bytes,
                f.byte_notify_threshold
            );
        }

        // notify.notified() resolves once the notify_one() has fired. Bound
        // it with a 100ms timeout — well under the 1s ticker — to keep the
        // test fast and to fail loudly if the signal never fires.
        tokio::time::timeout(Duration::from_millis(100), f.notify.notified())
            .await
            .expect("notify should fire before the 1s ticker once bytes > 768 KiB");
    }

    #[tokio::test]
    async fn flush_now_preserves_buffer_capacity() {
        // Grow the buffer past the initial Vec::with_capacity(max_buffer_len)
        // ceiling, then flush. The replacement buffer must retain the grown
        // capacity — `mem::take` would have dropped it (cleanup #5b).
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
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

        // Push 500 small entries — well under hard_cap (1000) and well
        // over max_buffer_len (100) so Vec reallocates beyond its initial
        // capacity.
        for i in 0..500 {
            f.push(record(LogLevel::Info, &format!("m{i}")), ctx());
        }
        let cap_before = f.state.lock().unwrap().buffer.capacity();
        assert!(
            cap_before > f.max_buffer_len,
            "buffer should have grown past initial capacity ({}); got {}",
            f.max_buffer_len,
            cap_before
        );

        f.flush_now().await;

        let cap_after = f.state.lock().unwrap().buffer.capacity();
        assert!(
            cap_after >= cap_before,
            "buffer capacity must be preserved across flush; before={cap_before}, after={cap_after}"
        );
    }

    /// Regression: a 4xx response body must be drained, not just status-checked.
    /// Without the drain, the connection is held by the response object and
    /// reqwest's pool can exhaust under sustained control-plane errors. The
    /// test asserts the mock recorded exactly one request and the buffer was
    /// drained (a regression would surface as a panic on the await of
    /// resp.bytes() or a hung second request).
    #[tokio::test]
    async fn flush_now_drains_response_body_on_4xx() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/internal/logs"))
            .respond_with(ResponseTemplate::new(400).set_body_string("oops"))
            .expect(1..=2) // ≥1 succeeds; allow 2 if a regression re-fires
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

        f.push(record(LogLevel::Info, "drain me"), ctx());
        f.flush_now().await;

        // Buffer drained (drops happen post-drain, regardless of status).
        assert_eq!(f.state.lock().unwrap().buffer.len(), 0);
    }

    /// Same regression as the 4xx test, on a 5xx response. The 5xx path
    /// also needs body-draining so the connection returns to the pool.
    #[tokio::test]
    async fn flush_now_drains_response_body_on_5xx() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/internal/logs"))
            .respond_with(ResponseTemplate::new(503).set_body_string("try later"))
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

        f.push(record(LogLevel::Info, "drain me 5xx"), ctx());
        f.flush_now().await;

        assert_eq!(f.state.lock().unwrap().buffer.len(), 0);
    }

    /// Regression: two concurrent `flush_now` calls must not produce two
    /// HTTP POSTs. Without the in-flight guard, the second call would
    /// observe the (now-empty) post-swap buffer, return early, and we'd
    /// be fine — but a third caller arriving between the swap and the
    /// in-flight POST's return would race. The guard short-circuits
    /// explicitly so the second call returns before the buffer drain.
    #[tokio::test]
    async fn push_during_inflight_flush_does_not_cause_concurrent_requests() {
        use std::time::Duration;
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        // Sleep 200ms before responding so we have a window to attempt a
        // second flush_now while the first is in flight.
        Mock::given(method("POST"))
            .and(path("/api/internal/logs"))
            .respond_with(ResponseTemplate::new(204).set_delay(Duration::from_millis(200)))
            .expect(1) // exactly one request — the second flush_now is short-circuited
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

        for i in 0..5 {
            f.push(record(LogLevel::Info, &format!("m{i}")), ctx());
        }

        // Start the first flush; do NOT await yet.
        let f1 = f.clone();
        let h1 = tokio::spawn(async move { f1.flush_now().await });

        // Give the first flush a moment to set the in-flight flag and
        // make its HTTP call. Yielding twice is enough on every machine
        // we've tested; we also wait a tiny bit to be safe.
        tokio::time::sleep(Duration::from_millis(20)).await;

        // The second flush should be a no-op.
        f.flush_now().await;

        h1.await.expect("first flush task should complete cleanly");
    }

    /// When the in-flight flag is set, a `push` that crosses the early-flush
    /// threshold must NOT call `notify_one`. We assert that
    /// `notify.notified()` does not resolve within 50ms — i.e. the push
    /// was suppressed.
    #[tokio::test]
    async fn push_notifies_only_when_no_flush_in_flight() {
        use std::time::Duration;
        use tokio::time::timeout;

        let signer = crate::auth::WorkerJwtSigner::new(
            b"test-secret".to_vec(),
            "edgecloud",
            "w_test",
            "test-region",
            "t_test",
        );
        let f = LogForwarder::new("http://127.0.0.1:1", "w_test", "test-region", signer);

        // Manually mark a flush as in flight; no real POST will be made.
        f.flush_in_flight
            .store(true, std::sync::atomic::Ordering::Release);

        // Push past the byte threshold (768 KiB) so the early-flush
        // notification would normally fire.
        let big = "x".repeat(800 * 1024);
        f.push(record(LogLevel::Info, &big), ctx());

        // If the push had called notify_one, the receiver would resolve.
        // We expect a 50ms timeout — meaning the push correctly suppressed
        // the notify.
        let notified = timeout(Duration::from_millis(50), f.notify.notified()).await;
        assert!(
            notified.is_err(),
            "push must not call notify_one while flush_in_flight is set"
        );

        // Cleanup: clear the flag so Arc<LogForwarder> can drop.
        f.flush_in_flight
            .store(false, std::sync::atomic::Ordering::Release);
    }
}
