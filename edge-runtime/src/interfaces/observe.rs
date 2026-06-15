//! `edge:observe` — metrics and logging.

use std::collections::HashMap;
use std::sync::RwLock;

/// Default labels used when none are provided.
const DEFAULT_LABELS: &[(String, String)] = &[];

/// Label pairs for metric metadata.
type MetricLabels = Vec<(String, String)>;

#[derive(Default)]
pub struct Observer {
    /// Local counters for observability — mirrors what the global registry records.
    /// Used for unit testing and direct access without Prometheus scraping.
    counters: RwLock<HashMap<String, (u64, MetricLabels)>>,
    gauges: RwLock<HashMap<String, (f64, MetricLabels)>>,
    histograms: RwLock<HashMap<String, Vec<(f64, MetricLabels)>>>,
}

impl Observer {
    pub fn new() -> Self {
        Self {
            counters: RwLock::new(HashMap::new()),
            gauges: RwLock::new(HashMap::new()),
            histograms: RwLock::new(HashMap::new()),
        }
    }

    /// Increment a counter by 1. Records to local storage for test access
    /// and emits a structured log.
    pub fn increment_counter(&self, name: &str, labels: &[(String, String)]) {
        let effective_labels = if labels.is_empty() {
            DEFAULT_LABELS
        } else {
            labels
        };
        if let Ok(mut counters) = self.counters.write() {
            let entry = counters.entry(name.to_string()).or_insert_with(|| (0, Vec::new()));
            entry.0 += 1;
            entry.1 = effective_labels.to_vec();
        }
        tracing::debug!(counter = name, labels = ?effective_labels, "counter incremented");
    }

    /// Set a gauge to a specific value. Records to local storage for test access
    /// and emits a structured log.
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

    /// Record a histogram sample. Records to local storage for test access
    /// and emits a structured log.
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
        let label_strs: Vec<_> = labels
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect();
        match level {
            "error" => tracing::error!(labels = ?label_strs, "{}", message),
            "warn" => tracing::warn!(labels = ?label_strs, "{}", message),
            "info" => tracing::info!(labels = ?label_strs, "{}", message),
            "debug" => tracing::debug!(labels = ?label_strs, "{}", message),
            _ => tracing::trace!(labels = ?label_strs, "{}", message),
        }
    }

    /// Returns the current value of a counter for testing.
    #[cfg(test)]
    pub fn get_counter(&self, name: &str) -> Option<u64> {
        self.counters.read().ok().and_then(|c| c.get(name).map(|(v, _)| *v))
    }

    /// Returns the current value of a gauge for testing.
    #[cfg(test)]
    pub fn get_gauge(&self, name: &str) -> Option<f64> {
        self.gauges.read().ok().and_then(|g| g.get(name).map(|(v, _)| *v))
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
}
