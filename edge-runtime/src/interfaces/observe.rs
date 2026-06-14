//! `edge:observe` — metrics and logging.

pub struct Observer {}

impl Observer {
    pub fn new() -> Self {
        Self {}
    }

    pub fn increment_counter(&self, name: &str, _labels: &[(String, String)]) {
        tracing::debug!(counter = name, "counter incremented");
    }

    pub fn record_gauge(&self, name: &str, value: f64, _labels: &[(String, String)]) {
        tracing::debug!(gauge = name, value = value, "gauge recorded");
    }

    pub fn record_histogram(&self, name: &str, value: f64, _labels: &[(String, String)]) {
        tracing::debug!(histogram = name, value = value, "histogram recorded");
    }

    pub fn emit_log(&self, level: &str, message: &str) {
        match level {
            "error" => tracing::error!(message),
            "warn" => tracing::warn!(message),
            "info" => tracing::info!(message),
            "debug" => tracing::debug!(message),
            _ => tracing::trace!(message),
        }
    }
}
