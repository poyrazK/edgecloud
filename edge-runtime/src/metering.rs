//! Request metering for per-request billing.
//!
//! Tracks the number of HTTP requests handled by an app instance.
//! The count is read by the Worker Supervisor during heartbeat reporting
//! and sent to the control plane for billing aggregation.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Request meter for tracking billable requests per deployment.
#[derive(Debug, Clone)]
pub struct RequestMeter {
    /// Atomic request counter.
    count: Arc<AtomicU64>,
    /// Tenant ID for reporting.
    pub tenant_id: String,
    /// Deployment ID for reporting.
    pub deployment_id: String,
}

impl RequestMeter {
    /// Create a new meter for a deployment.
    pub fn new(tenant_id: String, deployment_id: String) -> Self {
        Self {
            count: Arc::new(AtomicU64::new(0)),
            tenant_id,
            deployment_id,
        }
    }

    /// Record a single request. Called by http-server on each incoming request.
    pub fn record_request(&self) {
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the current request count.
    pub fn get_count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Reset the counter (called after reporting to control plane).
    pub fn reset(&self) {
        self.count.store(0, Ordering::Relaxed);
    }

    /// Get a snapshot of the meter state for reporting.
    pub fn snapshot(&self) -> MeterSnapshot {
        MeterSnapshot {
            tenant_id: self.tenant_id.clone(),
            deployment_id: self.deployment_id.clone(),
            request_count: self.get_count(),
        }
    }
}

/// A snapshot of metering state for a reporting interval.
#[derive(Debug, Clone)]
pub struct MeterSnapshot {
    pub tenant_id: String,
    pub deployment_id: String,
    pub request_count: u64,
}
