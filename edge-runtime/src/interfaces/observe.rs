//! `edge:observe` — metrics and structured logging.

use metrics::NoopRecorder;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// Default labels used when none are provided.
const DEFAULT_LABELS: &[(String, String)] = &[];

/// Maximum size of a single log message after which the record is dropped.
/// 16 KiB is well above any reasonable log line and small enough that a
/// pathological guest can't OOM the forwarder, blow past the per-batch
/// 1 MiB cap with a single message, or wedge the in-memory buffer with
/// a 1 MB+ entry. The check lives in `emit_log_record_inner` (the
/// chokepoint for both `emit_log` and `emit_log_record`) so both entry
/// points are covered.
pub const MAX_LOG_MESSAGE_BYTES: usize = 16 * 1024;

/// Label pairs for metric metadata.
type MetricLabels = Vec<(String, String)>;

/// Guard that ensures the global metrics recorder is set exactly once.
static RECORDER_GUARD: std::sync::OnceLock<()> = std::sync::OnceLock::new();

// ---------------------------------------------------------------------------
// Log level — mirrors the WIT `edge:observe::log-level` enum
// ---------------------------------------------------------------------------

/// Typed log level, matching the WIT `edge:observe/log-level` enum.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum LogLevel {
    #[default]
    Info,
    Error,
    Warn,
    Debug,
    Trace,
}

impl LogLevel {
    /// Parse from a WIT string level (backward compat with string-based emit-log).
    ///
    /// Unknown levels map to `Trace` — the lowest floor — so that future WIT enum
    /// variants are silently clamped rather than silently dropped.
    pub fn from_level_str(s: &str) -> Self {
        match s {
            "error" => LogLevel::Error,
            "warn" => LogLevel::Warn,
            "info" => LogLevel::Info,
            "debug" => LogLevel::Debug,
            _ => LogLevel::Trace,
        }
    }

    /// Convert to tracing::Level.
    pub fn to_tracing_level(self) -> tracing::Level {
        match self {
            LogLevel::Error => tracing::Level::ERROR,
            LogLevel::Warn => tracing::Level::WARN,
            LogLevel::Info => tracing::Level::INFO,
            LogLevel::Debug => tracing::Level::DEBUG,
            LogLevel::Trace => tracing::Level::TRACE,
        }
    }
}

// ---------------------------------------------------------------------------
// Log record — mirrors the WIT `edge:observe::log-record`
// ---------------------------------------------------------------------------

/// Structured log record, matching the WIT `edge:observe/log-record` type.
#[derive(Clone, Debug)]
pub struct LogRecord {
    pub timestamp_ms: u64,
    pub level: LogLevel,
    pub message: String,
    pub labels: Vec<(String, String)>,
}

// ---------------------------------------------------------------------------
// AppLogContext — per-app identity stamped on every emitted log
// ---------------------------------------------------------------------------

/// Per-app identity that the runtime attaches to every emitted log record.
///
/// The runtime doesn't know which app it's hosting on its own — the
/// supervisor passes this in at construction time. The LogSink downstream
/// (worker → HTTP → control plane) stamps it onto the record so the
/// control plane can route logs back to the right tenant/app/deployment.
#[derive(Clone, Debug, Default)]
pub struct AppLogContext {
    pub app_name: String,
    pub tenant_id: String,
    pub deployment_id: String,
}

impl AppLogContext {
    /// Empty context for tests / callers that don't have app identity yet.
    pub fn empty() -> Self {
        Self::default()
    }
}

// ---------------------------------------------------------------------------
// LogSink — destination for tenant-emitted log records
// ---------------------------------------------------------------------------

/// Sink that receives structured log records from the guest (via
/// `edge:observe.emit_log`).
///
/// Implementations decide where the record goes: a worker ships it to
/// the control plane over HTTP; a unit test captures it in a `Vec`;
/// `NoopLogSink` discards it.
///
/// `push` is called synchronously from the guest's WIT call. Implementations
/// must be cheap (no I/O on the hot path) — buffer and forward async. The
/// worker's `LogForwarder` is the canonical example: it appends to an
/// in-memory buffer and a background task flushes batches over HTTP.
pub trait LogSink: Send + Sync {
    fn push(&self, record: LogRecord, ctx: AppLogContext);
}

/// Drop-on-the-floor sink. Used by `Observer::new()` and tests that don't
/// care about log emission. Cheap to construct (no allocation).
pub struct NoopLogSink;
impl LogSink for NoopLogSink {
    fn push(&self, _record: LogRecord, _ctx: AppLogContext) {}
}

// ---------------------------------------------------------------------------
// NATS publisher trait (legacy)
// ---------------------------------------------------------------------------

/// Legacy interface for forwarding structured log records to a message bus.
///
/// Retained for backward compatibility with any caller that wired
/// `ObserveConfig::with_nats_publisher`. The runtime no longer uses this
/// path on the worker side; new code should implement `LogSink` instead.
#[allow(dead_code)]
pub trait NatsPublisher: Send + Sync {
    /// Publish a log record to the given region.
    fn publish_log(&self, region: &str, record: &LogRecord);
}

// ---------------------------------------------------------------------------
// Observer config
// ---------------------------------------------------------------------------

/// Configuration for the Observer.
#[derive(Clone)]
pub struct ObserveConfig {
    /// Minimum log level to emit and forward.
    pub min_log_level: LogLevel,
    /// Destination for emitted log records. Defaults to `NoopLogSink`.
    pub log_sink: Arc<dyn LogSink>,
    /// Per-app identity stamped on every emitted record.
    pub app_ctx: AppLogContext,
}

impl Default for ObserveConfig {
    fn default() -> Self {
        Self {
            min_log_level: LogLevel::Info,
            log_sink: Arc::new(NoopLogSink),
            app_ctx: AppLogContext::default(),
        }
    }
}

impl ObserveConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_log_sink(mut self, sink: Arc<dyn LogSink>) -> Self {
        self.log_sink = sink;
        self
    }

    pub fn with_app_ctx(mut self, ctx: AppLogContext) -> Self {
        self.app_ctx = ctx;
        self
    }

    pub fn with_min_log_level(mut self, level: LogLevel) -> Self {
        self.min_log_level = level;
        self
    }
}

// ---------------------------------------------------------------------------
// Observer
// ---------------------------------------------------------------------------

/// Metrics exporter backed by a no-op recorder.
///
/// This is **intentional for now**: metrics are accumulated in local storage
/// (visible to tests and logging) but not exported to a real backend (Prometheus,
/// DataDog, etc.). Production deployments must replace the global recorder by
/// calling `metrics::set_global_recorder` with a real exporter before instantiating
/// this type.
pub struct Observer {
    /// Local counters for observability.
    counters: RwLock<HashMap<String, (u64, MetricLabels)>>,
    gauges: RwLock<HashMap<String, (f64, MetricLabels)>>,
    histograms: RwLock<HashMap<String, Vec<(f64, MetricLabels)>>>,
    /// Destination for emitted log records.
    log_sink: Arc<dyn LogSink>,
    /// Per-app identity stamped on every emitted record.
    app_ctx: AppLogContext,
    /// Minimum log level to emit.
    min_log_level: LogLevel,
    /// Counter for records dropped at the size cap (see
    /// `MAX_LOG_MESSAGE_BYTES`). Surfaced via `dropped_record_count()`
    /// so the runtime's metrics interface can expose it without
    /// changing the wire format. The guest still sees a silent no-op
    /// on the drop — only operators see the count.
    dropped_size_cap_count: AtomicU64,
}

impl Default for Observer {
    fn default() -> Self {
        Self::new()
    }
}

impl Observer {
    /// Creates a new `Observer` and (once per process) installs a no-op global
    /// metrics recorder.
    ///
    /// Uses `NoopLogSink` so emitted log records are dropped. Production code
    /// (the worker) constructs an `Observer` via `ObserveConfig::with_log_sink`
    /// so logs reach the control plane.
    pub fn new() -> Self {
        Self::from_config(ObserveConfig::new())
    }

    /// Create a new Observer from an ObserveConfig.
    pub fn from_config(config: ObserveConfig) -> Self {
        // Set a no-op global recorder on first construction.
        let _ = RECORDER_GUARD.get_or_init(|| {
            metrics::set_global_recorder(&NoopRecorder)
                .expect("failed to set global metrics recorder");
        });
        Self {
            counters: RwLock::new(HashMap::new()),
            gauges: RwLock::new(HashMap::new()),
            histograms: RwLock::new(HashMap::new()),
            log_sink: config.log_sink,
            app_ctx: config.app_ctx,
            min_log_level: config.min_log_level,
            dropped_size_cap_count: AtomicU64::new(0),
        }
    }

    /// Increment a counter by 1.
    pub fn increment_counter(&self, name: &str, labels: &[(String, String)]) {
        let effective_labels = if labels.is_empty() {
            DEFAULT_LABELS
        } else {
            labels
        };
        if let Ok(mut counters) = self.counters.write() {
            let entry = counters
                .entry(name.to_string())
                .or_insert_with(|| (0, Vec::new()));
            entry.0 += 1;
            entry.1 = effective_labels.to_vec();
        }
        tracing::debug!(counter = name, labels = ?effective_labels, "counter incremented");
    }

    /// Set a gauge to a specific value.
    pub fn record_gauge(&self, name: &str, value: f64, labels: &[(String, String)]) {
        let effective_labels = if labels.is_empty() {
            DEFAULT_LABELS
        } else {
            labels
        };
        if let Ok(mut gauges) = self.gauges.write() {
            gauges.insert(name.to_string(), (value, effective_labels.to_vec()));
        }
        tracing::debug!(gauge = name, value = value, labels = ?effective_labels, "gauge recorded");
    }

    /// Record a histogram sample.
    pub fn record_histogram(&self, name: &str, value: f64, labels: &[(String, String)]) {
        let effective_labels = if labels.is_empty() {
            DEFAULT_LABELS
        } else {
            labels
        };
        if let Ok(mut histograms) = self.histograms.write() {
            histograms
                .entry(name.to_string())
                .or_insert_with(Vec::new)
                .push((value, effective_labels.to_vec()));
        }
        tracing::debug!(histogram = name, value = value, labels = ?effective_labels, "histogram recorded");
    }

    /// Emit a structured log message with optional label key-value pairs.
    pub fn emit_log(&self, level: &str, message: &str, labels: &[(String, String)]) {
        let parsed_level = LogLevel::from_level_str(level);
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let record = LogRecord {
            timestamp_ms,
            level: parsed_level,
            message: message.to_string(),
            labels: labels.to_vec(),
        };
        self.emit_log_record_inner(&record);
    }

    /// Emit a typed structured log record.
    pub fn emit_log_record(&self, record: &LogRecord) {
        self.emit_log_record_inner(record);
    }

    /// Internal emit path — handles level filtering, tracing, and sink forwarding.
    fn emit_log_record_inner(&self, record: &LogRecord) {
        // Filter by minimum log level. tracing::Level ordering treats lower
        // ordinal as MORE severe: ERROR(1) < WARN(2) < INFO(3) < DEBUG(4) < TRACE(5).
        // Standard log-level semantics: min=Info means "show Info and anything
        // MORE severe" — i.e. ERROR/WARN/INFO pass, DEBUG/TRACE are dropped.
        // In tracing's ordinal scheme that's "level <= min", so drop when
        // `record.level > min_log_level`.
        if record.level.to_tracing_level() > self.min_log_level.to_tracing_level() {
            return;
        }

        // Drop oversized messages. Without this cap, a guest could call
        // emit_log with a multi-MB string (e.g. an unintentional stack
        // dump) and the worker would buffer it, the forwarder would ship
        // it, and the control plane would 400 on the body. We log a
        // warning and return — the guest sees a no-op, not a partial
        // record. The constant is documented at the top of the file.
        //
        // We also bump `dropped_size_cap_count` so operators can spot
        // a guest that is silently losing records (e.g. one that
        // accumulates stack-trace messages). Surfaced via
        // `dropped_record_count()`.
        if record.message.len() > MAX_LOG_MESSAGE_BYTES {
            let dropped = self.dropped_size_cap_count.fetch_add(1, Ordering::Relaxed) + 1;
            tracing::warn!(
                size = record.message.len(),
                max = MAX_LOG_MESSAGE_BYTES,
                total_dropped = dropped,
                "emit_log: dropping oversized message"
            );
            return;
        }

        let label_strs: Vec<_> = record
            .labels
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect();

        // Emit to tracing (local stdout).
        match record.level {
            LogLevel::Error => tracing::error!(labels = ?label_strs, "{}", record.message),
            LogLevel::Warn => tracing::warn!(labels = ?label_strs, "{}", record.message),
            LogLevel::Info => tracing::info!(labels = ?label_strs, "{}", record.message),
            LogLevel::Debug => tracing::debug!(labels = ?label_strs, "{}", record.message),
            LogLevel::Trace => tracing::trace!(labels = ?label_strs, "{}", record.message),
        }

        // Forward to the configured LogSink. The sink handles stamping
        // tenant/worker identity, batching, and transport. Per-app
        // AppLogContext travels with the record so downstream sinks don't
        // need a separate lookup.
        self.log_sink.push(record.clone(), self.app_ctx.clone());
    }

    /// Returns the current value of a counter for testing.
    #[cfg(test)]
    pub fn get_counter(&self, name: &str) -> Option<u64> {
        self.counters
            .read()
            .ok()
            .and_then(|c| c.get(name).map(|(v, _)| *v))
    }

    /// Returns the total number of records dropped at the
    /// `MAX_LOG_MESSAGE_BYTES` size cap since this `Observer` was
    /// constructed. Exposed for tests and for the runtime's metrics
    /// interface; the wire format (`LogRecord`/`LogSink`) is unchanged —
    /// the guest still sees a silent no-op on the drop.
    ///
    /// `Ordering::Relaxed` is sufficient: the counter is purely a
    /// metric (no happens-before relationship with other state), and
    /// its only consumer is `metrics::counter!`-style aggregation.
    pub fn dropped_record_count(&self) -> u64 {
        self.dropped_size_cap_count.load(Ordering::Relaxed)
    }

    /// Returns the current value of a gauge for testing.
    #[cfg(test)]
    pub fn get_gauge(&self, name: &str) -> Option<f64> {
        self.gauges
            .read()
            .ok()
            .and_then(|g| g.get(name).map(|(v, _)| *v))
    }

    /// Returns all recorded values for a histogram for testing.
    #[cfg(test)]
    pub fn get_histogram(&self, name: &str) -> Option<Vec<f64>> {
        self.histograms
            .read()
            .ok()
            .and_then(|h| h.get(name).map(|v| v.iter().map(|(val, _)| *val).collect()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test sink that records every record it sees along with the
    /// `AppLogContext` it was called with.
    pub struct RecordingLogSink {
        pub pushed: RwLock<Vec<(LogRecord, AppLogContext)>>,
    }
    impl RecordingLogSink {
        pub fn new() -> Self {
            Self {
                pushed: RwLock::new(Vec::new()),
            }
        }
        pub fn records(&self) -> Vec<(LogRecord, AppLogContext)> {
            self.pushed.read().unwrap().clone()
        }
    }
    impl LogSink for RecordingLogSink {
        fn push(&self, record: LogRecord, ctx: AppLogContext) {
            self.pushed.write().unwrap().push((record, ctx));
        }
    }

    #[test]
    fn test_log_level_from_level_str() {
        assert_eq!(LogLevel::from_level_str("error"), LogLevel::Error);
        assert_eq!(LogLevel::from_level_str("warn"), LogLevel::Warn);
        assert_eq!(LogLevel::from_level_str("info"), LogLevel::Info);
        assert_eq!(LogLevel::from_level_str("debug"), LogLevel::Debug);
        assert_eq!(LogLevel::from_level_str("unknown"), LogLevel::Trace);
    }

    #[test]
    fn test_increment_counter() {
        let observer = Observer::new();
        observer.increment_counter("requests_total", &[]);
        observer.increment_counter("requests_total", &[("method".into(), "GET".into())]);
        assert_eq!(observer.get_counter("requests_total"), Some(2));
    }

    #[test]
    fn test_record_gauge() {
        let observer = Observer::new();
        observer.record_gauge("memory_usage_bytes", 1024.0, &[]);
        assert_eq!(observer.get_gauge("memory_usage_bytes"), Some(1024.0));
    }

    #[test]
    fn test_record_histogram() {
        let observer = Observer::new();
        observer.record_histogram("request_duration_ms", 50.0, &[]);
        observer.record_histogram("request_duration_ms", 100.0, &[]);
        let values = observer.get_histogram("request_duration_ms");
        assert_eq!(values, Some(vec![50.0, 100.0]));
    }

    #[test]
    fn test_emit_log_does_not_panic() {
        let observer = Observer::new();
        observer.emit_log("info", "test message", &[("key".into(), "value".into())]);
        observer.emit_log("error", "error message", &[]);
    }

    #[test]
    fn test_emit_log_record_does_not_panic() {
        let observer = Observer::new();
        observer.emit_log_record(&LogRecord {
            timestamp_ms: 0,
            level: LogLevel::Info,
            message: "structured message".into(),
            labels: vec![("k".into(), "v".into())],
        });
    }

    /// Emit forwards the record to the configured LogSink alongside the
    /// app context. This is the core contract for #76's worker→CP pipeline.
    #[test]
    fn test_emit_log_forwards_to_sink() {
        let sink = Arc::new(RecordingLogSink::new());
        let observer = Observer::from_config(
            ObserveConfig::new()
                .with_log_sink(sink.clone())
                .with_app_ctx(AppLogContext {
                    app_name: "my-app".into(),
                    tenant_id: "t_tenant1".into(),
                    deployment_id: "d_xyz".into(),
                }),
        );

        observer.emit_log("info", "hello world", &[("k".into(), "v".into())]);
        observer.emit_log("error", "boom", &[]);

        let pushed = sink.records();
        assert_eq!(pushed.len(), 2);

        let (rec1, ctx1) = &pushed[0];
        assert_eq!(rec1.message, "hello world");
        assert_eq!(rec1.level, LogLevel::Info);
        assert_eq!(rec1.labels, vec![("k".into(), "v".into())]);
        assert_eq!(ctx1.app_name, "my-app");
        assert_eq!(ctx1.tenant_id, "t_tenant1");
        assert_eq!(ctx1.deployment_id, "d_xyz");

        let (rec2, _) = &pushed[1];
        assert_eq!(rec2.level, LogLevel::Error);
        assert_eq!(rec2.message, "boom");
    }

    /// min_log_level filters records BEFORE forwarding to the sink, so the
    /// sink only sees records that pass the threshold. Standard semantics:
    /// min=Info passes ERROR/WARN/INFO and drops DEBUG/TRACE.
    #[test]
    fn test_min_log_level_filters_correctly() {
        let sink = Arc::new(RecordingLogSink::new());
        let observer = Observer::from_config(
            ObserveConfig::new()
                .with_log_sink(sink.clone())
                .with_min_log_level(LogLevel::Info),
        );

        // min=Info: ERROR, WARN, INFO pass; DEBUG, TRACE are blocked
        // (DEBUG/TRACE are less severe than INFO).
        for (msg, lvl) in [
            ("error", LogLevel::Error),
            ("warn", LogLevel::Warn),
            ("info", LogLevel::Info),
            ("debug", LogLevel::Debug),
            ("trace", LogLevel::Trace),
        ] {
            observer.emit_log_record(&LogRecord {
                timestamp_ms: 0,
                level: lvl,
                message: msg.into(),
                labels: vec![],
            });
        }

        let pushed = sink.records();
        let levels: Vec<LogLevel> = pushed.iter().map(|(r, _)| r.level).collect();
        assert!(
            levels.contains(&LogLevel::Error),
            "Error must pass (more severe than Info)"
        );
        assert!(
            levels.contains(&LogLevel::Warn),
            "Warn must pass (more severe than Info)"
        );
        assert!(levels.contains(&LogLevel::Info), "Info must pass");
        assert!(
            !levels.contains(&LogLevel::Debug),
            "Debug should be filtered (less severe than Info)"
        );
        assert!(
            !levels.contains(&LogLevel::Trace),
            "Trace should be filtered (less severe than Info)"
        );
    }

    /// Default Observer (no sink configured) uses NoopLogSink; emit must
    /// not panic even though no consumer exists.
    #[test]
    fn test_default_observer_does_not_panic() {
        let observer = Observer::new();
        observer.emit_log("debug", "below default min", &[]);
        observer.emit_log("info", "above default min", &[]);
    }

    /// Oversized messages are dropped at the chokepoint. The sink must
    /// see zero records and the call must not panic — the guest gets a
    /// silent no-op, not a partial record. The dropped-record counter
    /// must increment so operators can spot a guest that's silently
    /// losing records.
    #[test]
    fn test_emit_log_drops_oversized_message() {
        let sink = Arc::new(RecordingLogSink::new());
        let observer = Observer::from_config(
            ObserveConfig::new()
                .with_log_sink(sink.clone())
                .with_min_log_level(LogLevel::Info),
        );

        let huge = "x".repeat(MAX_LOG_MESSAGE_BYTES + 1);
        observer.emit_log("info", &huge, &[]);

        assert!(
            sink.records().is_empty(),
            "oversized message must be dropped before reaching the sink"
        );
        assert_eq!(
            observer.dropped_record_count(),
            1,
            "drop counter must increment by 1"
        );
    }

    /// Boundary case: a message of exactly `MAX_LOG_MESSAGE_BYTES` bytes
    /// must pass. The cap is inclusive (`>` not `>=`).
    #[test]
    fn test_emit_log_accepts_message_at_cap() {
        let sink = Arc::new(RecordingLogSink::new());
        let observer = Observer::from_config(
            ObserveConfig::new()
                .with_log_sink(sink.clone())
                .with_min_log_level(LogLevel::Info),
        );

        let at_cap = "x".repeat(MAX_LOG_MESSAGE_BYTES);
        observer.emit_log("info", &at_cap, &[]);

        let pushed = sink.records();
        assert_eq!(
            pushed.len(),
            1,
            "message at exactly MAX_LOG_MESSAGE_BYTES must pass"
        );
        assert_eq!(pushed[0].0.message.len(), MAX_LOG_MESSAGE_BYTES);
    }

    /// The cap is enforced at the chokepoint (`emit_log_record_inner`),
    /// so the typed `emit_log_record` entry point is also covered. This
    /// test pins that contract — if someone moves the check to `emit_log`
    /// only, this test fails.
    #[test]
    fn test_emit_log_record_drops_oversized() {
        let sink = Arc::new(RecordingLogSink::new());
        let observer = Observer::from_config(
            ObserveConfig::new()
                .with_log_sink(sink.clone())
                .with_min_log_level(LogLevel::Info),
        );

        let huge = "y".repeat(MAX_LOG_MESSAGE_BYTES + 1);
        observer.emit_log_record(&LogRecord {
            timestamp_ms: 0,
            level: LogLevel::Info,
            message: huge,
            labels: vec![],
        });

        assert!(
            sink.records().is_empty(),
            "emit_log_record with oversized message must also be dropped"
        );
        assert_eq!(
            observer.dropped_record_count(),
            1,
            "drop counter must increment by 1"
        );
    }

    /// The drop counter only counts oversized-message drops — successful
    /// records must not bump it. Mix 5 valid + 1 oversized and assert
    /// the counter reads 1 (not 6).
    #[test]
    fn test_emit_log_drops_count_independent_of_successful_records() {
        let sink = Arc::new(RecordingLogSink::new());
        let observer = Observer::from_config(
            ObserveConfig::new()
                .with_log_sink(sink.clone())
                .with_min_log_level(LogLevel::Info),
        );

        for i in 0..5 {
            observer.emit_log("info", &format!("valid {i}"), &[]);
        }
        let huge = "z".repeat(MAX_LOG_MESSAGE_BYTES + 1);
        observer.emit_log("info", &huge, &[]);

        assert_eq!(sink.records().len(), 5, "sink should have 5 valid records");
        assert_eq!(
            observer.dropped_record_count(),
            1,
            "drop counter should reflect only the 1 oversized record"
        );
    }
}
