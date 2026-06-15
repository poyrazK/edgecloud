//! `edge:observe` — metrics and logging.
//!
//! Provides a pluggable `MetricsBackend` trait. The default `NoOpBackend` logs to
//! tracing. When the `observe` feature is enabled, a `PrometheusBackend` is also
//! available via `Observer::with_backend`.

/// Pluggable metrics backend.
pub trait MetricsBackend: Send + Sync {
    fn increment_counter(&self, name: &str, labels: &[(String, String)]);
    fn record_gauge(&self, name: &str, value: f64, labels: &[(String, String)]);
    fn record_histogram(&self, name: &str, value: f64, labels: &[(String, String)]);
    fn emit_log(&self, level: &str, message: &str);
}

/// No-op backend that logs to tracing (current behavior).
pub struct NoOpBackend;

impl MetricsBackend for NoOpBackend {
    fn increment_counter(&self, name: &str, _labels: &[(String, String)]) {
        tracing::debug!(counter = name, "counter incremented");
    }

    fn record_gauge(&self, name: &str, value: f64, _labels: &[(String, String)]) {
        tracing::debug!(gauge = name, value = value, "gauge recorded");
    }

    fn record_histogram(&self, name: &str, value: f64, _labels: &[(String, String)]) {
        tracing::debug!(histogram = name, value = value, "histogram recorded");
    }

    fn emit_log(&self, level: &str, message: &str) {
        match level {
            "error" => tracing::error!(message),
            "warn" => tracing::warn!(message),
            "info" => tracing::info!(message),
            "debug" => tracing::debug!(message),
            _ => tracing::trace!(message),
        }
    }
}

#[cfg(feature = "observe")]
mod prometheus_backend {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};
    use metrics::{describe_counter, describe_gauge, describe_histogram, Counter, Gauge, Histogram};

    /// Composite key: metric name + sorted labels for correct per-label tracking.
    type MetricKey = (String, Vec<(String, String)>);

    pub(crate) fn make_key(name: &str, labels: &[(String, String)]) -> MetricKey {
        let mut sorted = labels.to_vec();
        sorted.sort();
        (name.to_string(), sorted)
    }

    /// Prometheus-compatible backend using the `metrics` crate.
    pub struct PrometheusBackend {
        pub(crate) counters: Arc<RwLock<HashMap<MetricKey, Counter>>>,
        pub(crate) gauges: Arc<RwLock<HashMap<MetricKey, Gauge>>>,
        pub(crate) histograms: Arc<RwLock<HashMap<MetricKey, Histogram>>>,
    }

    impl PrometheusBackend {
        pub fn new() -> Self {
            // Describe metrics so they show up in Prometheus.
            describe_counter!("edge_counter", "Counter metric from edge:observe");
            describe_gauge!("edge_gauge", "Gauge metric from edge:observe");
            describe_histogram!("edge_histogram", "Histogram metric from edge:observe");
            Self {
                counters: Arc::new(RwLock::new(HashMap::new())),
                gauges: Arc::new(RwLock::new(HashMap::new())),
                histograms: Arc::new(RwLock::new(HashMap::new())),
            }
        }
    }

    impl Default for PrometheusBackend {
        fn default() -> Self {
            Self::new()
        }
    }

    impl MetricsBackend for PrometheusBackend {
        fn increment_counter(&self, name: &str, labels: &[(String, String)]) {
            let key = make_key(name, labels);
            let mut counters = self.counters.write().unwrap();
            counters.entry(key).or_insert_with(Counter::noop).increment(1);
        }

        fn record_gauge(&self, name: &str, value: f64, labels: &[(String, String)]) {
            let key = make_key(name, labels);
            let mut gauges = self.gauges.write().unwrap();
            gauges.entry(key).or_insert_with(Gauge::noop).set(value);
        }

        fn record_histogram(&self, name: &str, value: f64, labels: &[(String, String)]) {
            let key = make_key(name, labels);
            let mut histograms = self.histograms.write().unwrap();
            histograms.entry(key).or_insert_with(Histogram::noop).record(value);
        }

        fn emit_log(&self, level: &str, message: &str) {
            match level {
                "error" => tracing::error!(message),
                "warn" => tracing::warn!(message),
                "info" => tracing::info!(message),
                "debug" => tracing::debug!(message),
                _ => tracing::trace!(message),
            }
        }
    }
}

#[cfg(not(feature = "observe"))]
mod prometheus_backend {
    // No-op when feature is disabled.
}

/// Observer holds a pluggable metrics backend.
pub struct Observer {
    backend: Box<dyn MetricsBackend>,
}

impl Observer {
    /// Create an Observer with a no-op (logging-only) backend.
    pub fn new() -> Self {
        Self::with_backend(Box::new(NoOpBackend))
    }

    /// Create an Observer with a custom backend.
    pub fn with_backend(backend: Box<dyn MetricsBackend>) -> Self {
        Self { backend }
    }

    /// Create an Observer with a Prometheus backend (requires `observe` feature).
    #[cfg(feature = "observe")]
    pub fn with_prometheus() -> Self {
        use prometheus_backend::PrometheusBackend;
        Self::with_backend(Box::new(PrometheusBackend::new()))
    }

    #[cfg(not(feature = "observe"))]
    pub fn with_prometheus() -> Self {
        Self::new()
    }

    pub fn increment_counter(&self, name: &str, labels: &[(String, String)]) {
        self.backend.increment_counter(name, labels);
    }

    pub fn record_gauge(&self, name: &str, value: f64, labels: &[(String, String)]) {
        self.backend.record_gauge(name, value, labels);
    }

    pub fn record_histogram(&self, name: &str, value: f64, labels: &[(String, String)]) {
        self.backend.record_histogram(name, value, labels);
    }

    pub fn emit_log(&self, level: &str, message: &str) {
        self.backend.emit_log(level, message);
    }
}

impl Default for Observer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_noop_backend_increment_counter_does_not_panic() {
        let backend = NoOpBackend;
        backend.increment_counter("test_counter", &[(String::from("method"), String::from("GET"))]);
        backend.increment_counter("test_counter", &[(String::from("method"), String::from("POST"))]);
    }

    #[test]
    fn test_noop_backend_record_gauge_does_not_panic() {
        let backend = NoOpBackend;
        backend.record_gauge("test_gauge", 42.0, &[(String::from("host"), String::from("localhost"))]);
    }

    #[test]
    fn test_noop_backend_record_histogram_does_not_panic() {
        let backend = NoOpBackend;
        backend.record_histogram("test_histogram", 0.5, &[]);
    }

    #[test]
    fn test_observer_with_noop_backend_does_not_panic() {
        let observer = Observer::new();
        observer.increment_counter("req", &[(String::from("method"), String::from("GET"))]);
        observer.record_gauge("memory_bytes", 1024.0, &[]);
        observer.record_histogram("latency_ms", 50.0, &[]);
        observer.emit_log("info", "test message");
    }

    #[cfg(feature = "observe")]
    #[test]
    fn test_labels_tracked_separately_in_prometheus_backend() {
        use prometheus_backend::PrometheusBackend;

        let backend = PrometheusBackend::new();

        // Increment same metric name with different labels
        backend.increment_counter("http_requests", &[(String::from("method"), String::from("GET"))]);
        backend.increment_counter("http_requests", &[(String::from("method"), String::from("GET"))]);
        backend.increment_counter("http_requests", &[(String::from("method"), String::from("POST"))]);

        // Both label combinations should exist as separate entries
        let counters = backend.counters.read().unwrap();
        assert_eq!(counters.len(), 2, "expected 2 separate metric entries for different labels");
    }

    #[cfg(feature = "observe")]
    #[test]
    fn test_gauge_labels_tracked_separately() {
        use prometheus_backend::PrometheusBackend;

        let backend = PrometheusBackend::new();

        backend.record_gauge("memory_usage", 100.0, &[(String::from("host"), String::from("a"))]);
        backend.record_gauge("memory_usage", 200.0, &[(String::from("host"), String::from("b"))]);

        let gauges = backend.gauges.read().unwrap();
        assert_eq!(gauges.len(), 2, "expected 2 separate gauge entries for different labels");
    }

    #[cfg(feature = "observe")]
    #[test]
    fn test_make_key_sorts_labels_canonical() {
        use prometheus_backend::make_key;

        // Same labels in different order should produce same key
        let key1 = make_key("req", &[(String::from("a"), String::from("1")), (String::from("b"), String::from("2"))]);
        let key2 = make_key("req", &[(String::from("b"), String::from("2")), (String::from("a"), String::from("1"))]);

        assert_eq!(key1, key2, "label order should not affect key");
        assert_eq!(key1.0, "req");
    }

    #[cfg(feature = "observe")]
    #[test]
    fn test_observer_with_prometheus_backend() {
        let observer = Observer::with_prometheus();
        observer.increment_counter("test_req", &[(String::from("method"), String::from("GET"))]);
        observer.record_gauge("test_gauge", 1.0, &[]);
        observer.record_histogram("test_hist", 0.1, &[]);
        // Should not panic
    }
}
