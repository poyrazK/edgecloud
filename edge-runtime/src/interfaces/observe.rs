//! `edge:observe` — metrics and structured logging.

use metrics::NoopRecorder;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Default labels used when none are provided.
const DEFAULT_LABELS: &[(String, String)] = &[];

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
// NATS publisher trait
// ---------------------------------------------------------------------------

/// Interface for forwarding structured log records to a message bus.
/// Implement this trait to enable log forwarding to the control plane.
pub trait NatsPublisher: Send + Sync {
    /// Publish a log record to the given region.
    fn publish_log(&self, region: &str, record: &LogRecord);
}

// ---------------------------------------------------------------------------
// Observer config
// ---------------------------------------------------------------------------

/// Configuration for the Observer.
#[derive(Clone, Default)]
pub struct ObserveConfig {
    /// Optional NATS publisher for forwarding logs to the control plane.
    pub nats_publisher: Option<Arc<dyn NatsPublisher>>,
    /// Minimum log level to emit and forward.
    pub min_log_level: LogLevel,
}

impl ObserveConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_nats_publisher(mut self, publisher: Arc<dyn NatsPublisher>) -> Self {
        self.nats_publisher = Some(publisher);
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
#[derive(Default)]
pub struct Observer {
    /// Local counters for observability.
    counters: RwLock<HashMap<String, (u64, MetricLabels)>>,
    gauges: RwLock<HashMap<String, (f64, MetricLabels)>>,
    histograms: RwLock<HashMap<String, Vec<(f64, MetricLabels)>>>,
    /// NATS publisher for log forwarding.
    nats_publisher: Option<Arc<dyn NatsPublisher>>,
    /// Minimum log level to emit.
    min_log_level: LogLevel,
}

impl Observer {
    /// Creates a new `Observer` and (once per process) installs a no-op global
    /// metrics recorder.
    ///
    /// # Noop by Design
    ///
    /// `metrics::counter!`, `metrics::gauge!`, and `metrics::histogram!` macros
    /// are no-ops until a real exporter is installed via
    /// `metrics::set_global_recorder`. This struct also stores all increments /
    /// recordings locally so unit tests can inspect them without a real backend.
    ///
    /// To enable production metrics: call `metrics::set_global_recorder` with a
    /// suitable exporter (e.g. `metrics_exporter::Prometheus`) **before** constructing
    /// the first `Observer`.
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
            nats_publisher: config.nats_publisher,
            min_log_level: config.min_log_level,
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

    /// Internal emit path — handles level filtering, tracing, and NATS forwarding.
    fn emit_log_record_inner(&self, record: &LogRecord) {
        // Filter by minimum log level — drop records less severe than min_log_level.
        // tracing::Level ordering: ERROR(1) < WARN(2) < INFO(3) < DEBUG(4) < TRACE(5).
        // So a record is below minimum when its numeric level is less than the configured minimum.
        if record.level.to_tracing_level() < self.min_log_level.to_tracing_level() {
            return;
        }

        let label_strs: Vec<_> = record
            .labels
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect();

        // Emit to tracing.
        match record.level {
            LogLevel::Error => tracing::error!(labels = ?label_strs, "{}", record.message),
            LogLevel::Warn => tracing::warn!(labels = ?label_strs, "{}", record.message),
            LogLevel::Info => tracing::info!(labels = ?label_strs, "{}", record.message),
            LogLevel::Debug => tracing::debug!(labels = ?label_strs, "{}", record.message),
            LogLevel::Trace => tracing::trace!(labels = ?label_strs, "{}", record.message),
        }

        // Forward via NATS if configured.
        // TODO: region routing — pull region from runtime context / tenant metadata
        // instead of hardcoding "global".
        if let Some(ref publisher) = self.nats_publisher {
            publisher.publish_log("global", record);
        }
    }

    /// Returns the current value of a counter for testing.
    #[cfg(test)]
    pub fn get_counter(&self, name: &str) -> Option<u64> {
        self.counters
            .read()
            .ok()
            .and_then(|c| c.get(name).map(|(v, _)| *v))
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

    #[test]
    fn test_observer_with_nats_publisher_does_not_panic() {
        struct MockPublisher;
        impl NatsPublisher for MockPublisher {
            fn publish_log(&self, _region: &str, _record: &LogRecord) {}
        }
        let observer = Observer::from_config(
            ObserveConfig::new()
                .with_nats_publisher(Arc::new(MockPublisher))
                .with_min_log_level(LogLevel::Debug),
        );
        observer.emit_log("debug", "test", &[]);
    }

    /// Mock publisher that records which log levels were published.
    struct RecordingPublisher {
        published: RwLock<Vec<LogLevel>>,
    }
    impl RecordingPublisher {
        fn new() -> Self {
            Self {
                published: RwLock::new(Vec::new()),
            }
        }
        fn published_levels(&self) -> Vec<LogLevel> {
            self.published.read().unwrap().clone()
        }
    }
    impl NatsPublisher for RecordingPublisher {
        fn publish_log(&self, _region: &str, record: &LogRecord) {
            self.published.write().unwrap().push(record.level);
        }
    }

    #[test]
    fn test_min_log_level_filters_correctly() {
        // min=Info: Error and Warn should be blocked; Info/Debug/Trace should pass.
        let publisher = Arc::new(RecordingPublisher::new());
        let observer = Observer::from_config(
            ObserveConfig::new()
                .with_nats_publisher(publisher.clone())
                .with_min_log_level(LogLevel::Info),
        );

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

        let published = publisher.published_levels();
        assert!(
            !published.contains(&LogLevel::Error),
            "Error should be filtered"
        );
        assert!(
            !published.contains(&LogLevel::Warn),
            "Warn should be filtered"
        );
        assert!(published.contains(&LogLevel::Info), "Info should pass");
        assert!(published.contains(&LogLevel::Debug), "Debug should pass");
        assert!(published.contains(&LogLevel::Trace), "Trace should pass");
    }
}
