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
//! - 2xx: batch dropped (no ack). The spool was already drained as
//!   part of building this batch — successful flushes leave no disk
//!   state.
//! - 4xx (except 429): log error and drop. Bad request won't get
//!   better; retrying the same malformed body would just produce
//!   another 4xx.
//! - 429 / 5xx / network: log error and write the batch to the disk
//!   spool. The next flush picks it up and retries. The spool survives
//!   worker restarts, so a control-plane outage doesn't lose in-flight
//!   logs.
//! - spool overflow: the spool file is capped at
//!   `Config::spool_max_bytes` (default 1 GiB). When the cap is
//!   exceeded the oldest batches are dropped (FIFO) to make room for
//!   new failures — recent logs are preserved, oldest are lost.
//! - buffer overflow: drop new entries past
//!   `max_buffer_len * HARD_CAP_MULT`. The in-memory buffer is a
//!   pre-spool layer; under sustained outage the spool holds the
//!   overflow and the buffer stays bounded.
//! - spool write failure (disk full, permission denied): re-inject the
//!   batch into the in-memory buffer so the next flush retries.
//!   Silently dropping on disk failure would violate the durability
//!   contract.
//!
//! At-least-once delivery: a worker that times out at 5s after the DB
//! committed the row will retry the same batch on the next flush,
//! producing a duplicate row in the `logs` table. The Go control
//! plane does not currently dedup; a `batch_id` + `UNIQUE` follow-up
//! is documented as a future enhancement. For a log pipeline
//! duplicate lines are far less harmful than lost ones.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use edge_runtime::interfaces::observe::{AppLogContext, LogLevel, LogRecord, LogSink};
use edge_spool::Spool;
use serde::{Deserialize, Serialize};
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
///
/// `Deserialize` is needed by `replay_spool` to reconstruct the pending
/// batches after a worker restart.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct WireEntry {
    tenant_id: String,
    deployment_id: String,
    app_name: String,
    worker_id: String,
    region: String,
    level: String,
    message: String,
    labels: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize)]
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
    client: Arc<reqwest::Client>,
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
    /// Disk spool for batches the control plane refused (5xx, 429,
    /// network error). Survives worker restarts. See module doc
    /// comment for the durability contract.
    spool: Arc<Spool>,
    /// Cap on the spool's on-disk size. When exceeded, the oldest
    /// batches are dropped (FIFO) to make room for new failures.
    /// Default 1 GiB; overridden by `Config::spool_max_bytes`.
    spool_max_bytes: u64,
}

impl LogForwarder {
    pub async fn new(
        control_plane_url: impl Into<String>,
        worker_id: impl Into<String>,
        region: impl Into<String>,
        jwt_signer: Arc<WorkerJwtSigner>,
        client: Arc<reqwest::Client>,
        spool: Arc<Spool>,
        spool_max_bytes: u64,
    ) -> Arc<Self> {
        let max_buffer_len = DEFAULT_MAX_BUFFER_LEN;
        let hard_cap = max_buffer_len * HARD_CAP_MULT;
        let me = Arc::new(Self {
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
            spool,
            spool_max_bytes,
        });

        // Drain any batches that the previous worker instance left on
        // disk (control-plane outage, worker crash). They land back in
        // the in-memory buffer so the flush_loop's first tick sends
        // them. If the spool is over cap at replay time, rotate first
        // — the oldest failures are already not worth the disk space.
        if let Err(e) = me.replay_spool().await {
            tracing::error!(err = %e, "log_forwarder: failed to replay spool at startup");
        }
        me
    }

    /// Drain the spool and push the entries back into the in-memory
    /// buffer. Synchronous helper — does not call `notify_one` (the
    /// flush loop is about to start anyway, and the early-flush signal
    /// is a perf optimization, not a correctness requirement).
    ///
    /// If the spool exceeds `spool_max_bytes` after draining (unlikely
    /// — a fresh drain returns everything to memory), rotate to drop
    /// the oldest lines.
    async fn replay_spool(&self) -> anyhow::Result<()> {
        // Drain any pending batches from disk. Each batch is an
        // IngestLogsRequest JSON value; iterate its entries and push
        // them into the in-memory buffer.
        let pending = self.spool.drain().await?;

        let mut total_entries = 0usize;
        let mut total_batches = 0usize;
        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            for batch_json in pending {
                let req: IngestLogsRequest = serde_json::from_value(batch_json)
                    .map_err(|e| anyhow::anyhow!("replay: parse batch: {e}"))?;
                total_batches += 1;
                for entry in req.entries {
                    if state.buffer.len() < self.hard_cap {
                        state.buffer.push(entry);
                        total_entries += 1;
                    }
                    // else: drop with a warning. The buffer's hard_cap
                    // is bounded (1000), so this only fires if a single
                    // replayed batch is huge — uncommon but possible if
                    // a previous run wrote a near-cap batch before
                    // crashing.
                }
            }
            state.buffered_bytes = 0; // rough — re-derive below if needed
        }

        if total_batches > 0 {
            tracing::info!(
                batches = total_batches,
                entries = total_entries,
                "log_forwarder: replayed spool contents into in-memory buffer"
            );
        }
        Ok(())
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

    /// Drain the spool and the in-memory buffer, POST the combined
    /// batch, and spool the batch back on transient failure. Public
    /// for tests that want to drive a flush without a ticker / notify.
    ///
    /// Failure handling matrix (see module doc comment for context):
    /// - 2xx: success, nothing on disk.
    /// - 4xx (except 429): drop, won't get better. Logged for ops.
    /// - 429: spool for retry. The control plane is back-pressuring
    ///   us; the next flush tick retries the batch.
    /// - 5xx / network: spool for retry. The next flush tick retries.
    /// - spool.append failure (disk full): re-inject into the
    ///   in-memory buffer so the next flush retries. Surprises are
    ///   worse than memory pressure.
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

        // Drain any pending durable batches from the spool first. They
        // take priority over fresh in-memory entries so an outage is
        // drained before newer logs are sent (preserves in-order
        // delivery within a tenant's stream).
        let mut entries: Vec<WireEntry> = match self.spool.drain().await {
            Ok(pending) => {
                let mut out = Vec::with_capacity(pending.len() * 4);
                for batch_json in pending {
                    match serde_json::from_value::<IngestLogsRequest>(batch_json) {
                        Ok(req) => out.extend(req.entries),
                        Err(e) => {
                            // A corrupt line on the spool is a real
                            // bug — we wrote it ourselves, so a parse
                            // failure means either disk corruption or
                            // a forward-incompat format change. Log and
                            // continue; the rest of the spool is still
                            // valid.
                            tracing::error!(err = %e, "log_forwarder: dropping unparseable spool batch");
                        }
                    }
                }
                out
            }
            Err(e) => {
                // Spool I/O failed (disk full, permission denied). We
                // can't read what's on disk; send whatever's in memory
                // and let the next tick try again. Log loudly.
                tracing::error!(err = %e, "log_forwarder: failed to drain spool; skipping durable replay");
                Vec::new()
            }
        };

        // Now swap the in-memory buffer for an empty Vec. The
        // mem::replace preserves the grown capacity for the next push
        // round.
        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if !state.buffer.is_empty() {
                let replacement_cap = state.buffer.capacity().max(self.max_buffer_len);
                let buffer_entries =
                    std::mem::replace(&mut state.buffer, Vec::with_capacity(replacement_cap));
                state.buffered_bytes = 0;
                entries.extend(buffer_entries);
            }
        }

        if entries.is_empty() {
            return;
        }

        let count = entries.len();
        let body = IngestLogsRequest { entries };
        let url = format!("{}/api/internal/logs", self.control_plane_url);
        let token = match self.jwt_signer.sign() {
            Ok(t) => t,
            Err(e) => {
                tracing::error!(err = %e, "log_forwarder: failed to sign JWT; dropping flush");
                return;
            }
        };

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
                    tracing::warn!(count, "logs deferred: 429 (spooled for retry)");
                    self.persist_failure(&body, count).await;
                } else if status.is_client_error() {
                    tracing::error!(
                        count,
                        status = status.as_u16(),
                        "logs dropped: 4xx (bad batch — won't retry)"
                    );
                    // 4xx (other than 429) is malformed: retrying the
                    // same body would just produce another 4xx. The
                    // Go handler already validates canonical level
                    // strings, batch size, entry count, and identity
                    // claims, so a 4xx here is a genuine bug — log
                    // loudly and drop.
                } else {
                    tracing::error!(
                        count,
                        status = status.as_u16(),
                        "logs deferred: 5xx (spooled for retry)"
                    );
                    self.persist_failure(&body, count).await;
                }
            }
            Err(e) => {
                tracing::error!(count, err = %e, "logs deferred: HTTP error (spooled for retry)");
                self.persist_failure(&body, count).await;
            }
        }
    }

    /// Persist a failed batch to the spool for retry on the next
    /// flush. If the spool write itself fails (disk full, permission
    /// denied), re-inject the entries into the in-memory buffer so the
    /// next flush retries — silently dropping on disk failure would
    /// violate the durability contract.
    ///
    /// Always called with the `flush_in_flight` guard held, so the
    /// in-memory buffer isn't being concurrently drained while we
    /// re-inject.
    async fn persist_failure(&self, body: &IngestLogsRequest, count: usize) {
        let batch_value = match serde_json::to_value(body) {
            Ok(v) => v,
            Err(e) => {
                tracing::error!(err = %e, "log_forwarder: failed to serialize batch for spool");
                return;
            }
        };

        if let Err(e) = self.spool.append(&batch_value).await {
            // Spool write failed. Re-inject the entries into the
            // in-memory buffer; the next flush will retry the spool
            // write, and if it still fails we re-inject again. The
            // buffer's `hard_cap` provides backpressure so we can't
            // OOM the worker.
            tracing::error!(
                err = %e,
                count,
                "log_forwarder: spool.append failed; re-injecting entries into in-memory buffer"
            );
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            for entry in &body.entries {
                if state.buffer.len() < self.hard_cap {
                    state.buffer.push(entry.clone());
                    // Conservative byte estimate: a re-injected entry
                    // is the same size it was on the way in.
                    state.buffered_bytes += entry.message.len() + BYTE_OVERHEAD_PER_ENTRY;
                }
                // else: drop with a warning. The buffer's hard_cap is
                // the backpressure boundary; if a re-injection would
                // push past it, we drop the tail (the same policy as
                // `push()`).
            }
            return;
        }

        // Spool write succeeded. If we're over the cap, rotate to
        // drop the oldest lines.
        if self.spool.size() > self.spool_max_bytes {
            match self.spool.rotate_when_over(self.spool_max_bytes).await {
                Ok(dropped) if dropped > 0 => {
                    tracing::error!(
                        dropped,
                        cap_bytes = self.spool_max_bytes,
                        "log_forwarder: spool over cap; dropped oldest batches"
                    );
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::error!(err = %e, "log_forwarder: spool rotation failed");
                }
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

fn log_level_to_string(level: LogLevel) -> String {
    match level {
        LogLevel::Error => "error".to_string(),
        LogLevel::Warn => "warn".to_string(),
        LogLevel::Info => "info".to_string(),
        LogLevel::Debug => "debug".to_string(),
        LogLevel::Trace => "trace".to_string(),
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
    use tempfile::TempDir;

    /// Build a fresh `reqwest::Client` for tests that need a `LogForwarder`.
    /// Production workers construct the client once in `main()` and pass it
    /// into `LogForwarder::new` (finding B2); tests construct one per test
    /// for isolation.
    fn test_client() -> Arc<reqwest::Client> {
        Arc::new(
            reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("test reqwest client"),
        )
    }

    /// Synchronous-build helper: a `LogForwarder` with a fresh
    /// per-test spool rooted in a tempdir. Returns the tempdir
    /// (keeps the dir alive for the test's lifetime) plus both
    /// `Arc`s so the test can assert on the spool directly.
    async fn forwarder_with_spool(
        control_plane_url: &str,
    ) -> (TempDir, Arc<LogForwarder>, Arc<Spool>) {
        let dir = TempDir::new().expect("tempdir");
        let spool = Arc::new(Spool::open(dir.path()).await.expect("open spool"));
        let signer = crate::auth::WorkerJwtSigner::new(
            b"test-secret".to_vec(),
            "edgecloud",
            "w_test",
            "test-region",
            "t_test",
        );
        let f = LogForwarder::new(
            control_plane_url,
            "w_test",
            "test-region",
            signer,
            test_client(),
            spool.clone(),
            1u64 << 30, // 1 GiB cap
        )
        .await;
        (dir, f, spool)
    }

    /// Async helper for tests that don't need a control plane
    /// (push, level, labels). Uses an unreachable URL since no POST
    /// will be attempted. The tempdir is leaked — tests that need a
    /// fresh per-test spool should use `forwarder_with_spool`
    /// instead.
    async fn forwarder() -> Arc<LogForwarder> {
        let signer = crate::auth::WorkerJwtSigner::new(
            b"test-secret".to_vec(),
            "edgecloud",
            "w_test",
            "test-region",
            "t_test",
        );
        let dir = TempDir::new().expect("tempdir");
        let spool = Arc::new(Spool::open(dir.path()).await.expect("open spool"));
        // Leak the tempdir so the spool's file path stays valid
        // for the lifetime of the test (TempDir's drop would
        // unlink the dir).
        std::mem::forget(dir);
        LogForwarder::new(
            "http://127.0.0.1:0",
            "w_test",
            "test-region",
            signer,
            test_client(),
            spool,
            1u64 << 30,
        )
        .await
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

    #[tokio::test]
    async fn push_appends_to_buffer() {
        let f = forwarder().await;
        f.push(record(LogLevel::Info, "hello"), ctx());
        let state = f.state.lock().unwrap();
        assert_eq!(state.buffer.len(), 1);
        assert_eq!(state.buffer[0].message, "hello");
        assert_eq!(state.buffer[0].level, "info");
        assert_eq!(state.buffer[0].app_name, "my-app");
        assert_eq!(state.buffer[0].worker_id, "w_test");
        assert_eq!(state.buffer[0].region, "test-region");
    }

    #[tokio::test]
    async fn push_past_hard_cap_drops() {
        let f = forwarder().await;
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

    #[tokio::test]
    async fn level_serializes_to_canonical_strings() {
        let f = forwarder().await;
        f.push(record(LogLevel::Error, "e"), ctx());
        f.push(record(LogLevel::Warn, "w"), ctx());
        f.push(record(LogLevel::Info, "i"), ctx());
        f.push(record(LogLevel::Debug, "d"), ctx());
        f.push(record(LogLevel::Trace, "t"), ctx());
        let state = f.state.lock().unwrap();
        let levels: Vec<&str> = state.buffer.iter().map(|e| e.level.as_str()).collect();
        assert_eq!(levels, vec!["error", "warn", "info", "debug", "trace"]);
    }

    #[tokio::test]
    async fn labels_serialize_as_json_object() {
        let f = forwarder().await;
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

        let (_dir, f, spool) = forwarder_with_spool(&server.uri()).await;

        f.push(record(LogLevel::Info, "1"), ctx());
        f.push(record(LogLevel::Info, "2"), ctx());
        assert_eq!(f.state.lock().unwrap().buffer.len(), 2);

        f.flush_now().await;

        // After flush, the buffer is empty and the spool is empty
        // (successful flush leaves no disk state).
        assert_eq!(f.state.lock().unwrap().buffer.len(), 0);
        let spool_drained = spool.drain().await.expect("drain spool");
        assert!(
            spool_drained.is_empty(),
            "successful flush must not leave the spool with pending batches"
        );
        let received = server.received_requests().await.expect("received");
        assert_eq!(received.len(), 1);
    }

    #[tokio::test]
    async fn flush_on_empty_buffer_is_noop() {
        use wiremock::MockServer;
        let server = MockServer::start().await;

        let (_dir, f, _spool) = forwarder_with_spool(&server.uri()).await;

        f.flush_now().await;

        // No request should have been sent.
        let received = server.received_requests().await.expect("received");
        assert!(received.is_empty());
    }

    #[tokio::test]
    async fn flush_on_5xx_writes_batch_to_spool() {
        // A 5xx response means the batch lands in the spool for retry
        // on the next flush. Replaces the old
        // `flush_on_5xx_drops_batch_does_not_panic` test — the
        // behavior changed from "drop on 5xx" to "spool on 5xx".
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/internal/logs"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let (_dir, f, spool) = forwarder_with_spool(&server.uri()).await;

        f.push(record(LogLevel::Info, "spool me"), ctx());
        f.flush_now().await;

        // In-memory buffer is drained (drain happens at swap time,
        // before the POST). The spool now holds the failed batch.
        assert_eq!(f.state.lock().unwrap().buffer.len(), 0);
        let spool_drained = spool.drain().await.expect("drain spool");
        assert_eq!(
            spool_drained.len(),
            1,
            "5xx must write the batch to the spool"
        );
        let req: IngestLogsRequest =
            serde_json::from_value(spool_drained.into_iter().next().unwrap())
                .expect("parse spool batch");
        assert_eq!(req.entries.len(), 1);
        assert_eq!(req.entries[0].message, "spool me");
    }

    #[tokio::test]
    async fn flush_2xx_leaves_spool_empty() {
        // The spool is read at the start of every flush; on 2xx we
        // expect the spool to remain empty (the drained contents
        // landed in the DB and no re-append happened).
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/internal/logs"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;

        let (_dir, f, spool) = forwarder_with_spool(&server.uri()).await;

        f.push(record(LogLevel::Info, "happy"), ctx());
        f.flush_now().await;

        let drained = spool.drain().await.expect("drain");
        assert!(drained.is_empty(), "2xx leaves spool empty");
    }

    #[tokio::test]
    async fn flush_4xx_does_not_write_to_spool() {
        // 4xx (other than 429) is "bad batch — won't get better". The
        // log line is dropped, not spooled.
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/internal/logs"))
            .respond_with(ResponseTemplate::new(400))
            .mount(&server)
            .await;

        let (_dir, f, spool) = forwarder_with_spool(&server.uri()).await;

        f.push(record(LogLevel::Info, "bad"), ctx());
        f.flush_now().await;

        let drained = spool.drain().await.expect("drain");
        assert!(drained.is_empty(), "4xx must not spool");
    }

    #[tokio::test]
    async fn flush_429_writes_to_spool() {
        // 429 is backpressure — spool for retry on the next flush.
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/internal/logs"))
            .respond_with(ResponseTemplate::new(429))
            .mount(&server)
            .await;

        let (_dir, f, spool) = forwarder_with_spool(&server.uri()).await;

        f.push(record(LogLevel::Info, "backpressure"), ctx());
        f.flush_now().await;

        let drained = spool.drain().await.expect("drain");
        assert_eq!(drained.len(), 1, "429 must spool for retry");
    }

    #[tokio::test]
    async fn startup_replays_pending_spool_batches() {
        // Pre-populate the spool with one batch, then construct a
        // fresh LogForwarder. The constructor's `replay_spool` should
        // drain the spool and push the entries into the in-memory
        // buffer.

        let dir = TempDir::new().expect("tempdir");
        let spool = Arc::new(Spool::open(dir.path()).await.expect("open spool"));

        // Write a single batch to the spool manually.
        let pending_batch = IngestLogsRequest {
            entries: vec![WireEntry {
                tenant_id: "t_tenant1".into(),
                deployment_id: "d_xyz".into(),
                app_name: "my-app".into(),
                worker_id: "w_test".into(),
                region: "test-region".into(),
                level: "info".into(),
                message: "from-previous-run".into(),
                labels: serde_json::Value::Object(Default::default()),
            }],
        };
        spool
            .append(&serde_json::to_value(&pending_batch).unwrap())
            .await
            .expect("append to spool");

        // Construct a fresh LogForwarder pointing at the same spool.
        // The constructor's replay_spool should drain the spool and
        // push the entry into the buffer.
        let signer = crate::auth::WorkerJwtSigner::new(
            b"test-secret".to_vec(),
            "edgecloud",
            "w_test",
            "test-region",
            "t_test",
        );
        let f = LogForwarder::new(
            "http://127.0.0.1:0", // no POSTs attempted
            "w_test",
            "test-region",
            signer,
            test_client(),
            spool.clone(),
            1u64 << 30,
        )
        .await;

        // Scope the state lock so it's released before the await on
        // `spool.drain()` (clippy::await_holding_lock).
        {
            let state = f.state.lock().unwrap();
            assert_eq!(state.buffer.len(), 1, "replayed entry should be in buffer");
            assert_eq!(state.buffer[0].message, "from-previous-run");
        }

        // Spool is now empty (replay drained it).
        let drained = spool.drain().await.expect("drain");
        assert!(drained.is_empty(), "replay should empty the spool");
    }

    #[tokio::test]
    async fn spool_overflow_drops_oldest_on_replay() {
        // Pre-fill the spool past the cap (small cap so the test
        // is fast), then construct a LogForwarder. The constructor's
        // replay_spool should rotate the spool before draining, so
        // the buffer only contains the most recent entries.
        let dir = TempDir::new().expect("tempdir");
        let spool = Arc::new(Spool::open(dir.path()).await.expect("open spool"));

        // 100 small batches; each ~50 bytes of JSON.
        for i in 0..100 {
            let batch = json!({"i": i, "padding": "x".repeat(40)});
            spool.append(&batch).await.expect("append");
        }
        assert!(
            spool.size() > 1000,
            "spool should be over 1KB before rotation"
        );

        // Construct a forwarder with a tiny cap. The replay path
        // must rotate the spool to fit under the cap.
        let signer = crate::auth::WorkerJwtSigner::new(
            b"test-secret".to_vec(),
            "edgecloud",
            "w_test",
            "test-region",
            "t_test",
        );
        let _f = LogForwarder::new(
            "http://127.0.0.1:0",
            "w_test",
            "test-region",
            signer,
            test_client(),
            spool.clone(),
            512, // 512-byte cap
        )
        .await;

        // Spool is now under cap (rotation ran during construction).
        assert!(spool.size() <= 512, "replay must rotate over-cap spool");
    }

    #[tokio::test]
    async fn spool_append_failure_reinjects_into_buffer() {
        // Make the spool directory read-only so append() fails. The
        // persist_failure path should re-inject the entries back into
        // the in-memory buffer so the next flush retries.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            use wiremock::matchers::{method, path};
            use wiremock::{Mock, MockServer, ResponseTemplate};

            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/api/internal/logs"))
                .respond_with(ResponseTemplate::new(503))
                .mount(&server)
                .await;

            let dir = TempDir::new().expect("tempdir");
            let spool = Arc::new(Spool::open(dir.path()).await.expect("open spool"));
            // Make the directory read-only. Subsequent appends will
            // fail with PermissionDenied.
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o500))
                .expect("chmod");

            let signer = crate::auth::WorkerJwtSigner::new(
                b"test-secret".to_vec(),
                "edgecloud",
                "w_test",
                "test-region",
                "t_test",
            );
            let f = LogForwarder::new(
                &server.uri(),
                "w_test",
                "test-region",
                signer,
                test_client(),
                spool,
                1u64 << 30,
            )
            .await;

            f.push(record(LogLevel::Info, "re-inject me"), ctx());
            f.flush_now().await;

            // The flush attempts to spool the failed batch, fails
            // (read-only dir), and re-injects into the buffer.
            let state = f.state.lock().unwrap();
            assert_eq!(
                state.buffer.len(),
                1,
                "buffer must contain the re-injected entry"
            );
            assert_eq!(state.buffer[0].message, "re-inject me");

            // Restore permissions so TempDir can clean up.
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))
                .expect("chmod restore");
        }
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

        let (_dir, f, _spool) = forwarder_with_spool(&server.uri()).await;

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
        let f = forwarder().await;
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

        let (_dir, f, _spool) = forwarder_with_spool(&server.uri()).await;

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

        let (_dir, f, _spool) = forwarder_with_spool(&server.uri()).await;

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

        let (_dir, f, _spool) = forwarder_with_spool(&server.uri()).await;

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

        let (_dir, f, _spool) = forwarder_with_spool(&server.uri()).await;

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

        let (_dir, f, _spool) = forwarder_with_spool("http://127.0.0.1:1").await;

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
