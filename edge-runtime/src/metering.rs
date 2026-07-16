//! Request metering for per-request and per-byte billing.
//!
//! Tracks HTTP request counts, outbound byte totals, resident-seconds
//! (issue #484), Handler (FaaS) per-request duration in milliseconds
//! (issue #555), and per-request 5xx error counts (issue #84 ask 6/7)
//! per app instance. All five counters are read by the Worker
//! Supervisor during heartbeat reporting and sent to the control plane
//! for billing aggregation, quota enforcement, and canary auto-ramp.
//!
//! The fourth dimension (`duration_ms`) follows the same shape as
//! `resident_seconds` (issue #484): a sibling field on `RequestMeter`
//! rather than a separate struct, so `MeterSnapshot` carries all
//! deltas atomically and the heartbeat wire format stays a single
//! struct. Handler (FaaS) apps stamp from the dispatch path;
//! LongRunning apps leave the duration field at 0 (the dispatch path
//! never fires for LR). The fifth dimension (`errors`) is stamped
//! from the three error terminal arms of the FaaS dispatch path;
//! LongRunning apps stamp it from the per-connection reject paths
//! (currently zero — LongRunning per-connection errors aren't
//! surfaced at the request level today).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Request meter for tracking billable requests, outbound bytes, resident
/// seconds, and Handler (FaaS) request duration per deployment.
#[derive(Debug, Clone)]
pub struct RequestMeter {
    /// Atomic request counter.
    count: Arc<AtomicU64>,
    /// Atomic outbound byte counter. Accumulates response bytes from
    /// http-client fetches and response bodies written by http-server.
    outbound_bytes: Arc<AtomicU64>,
    /// Atomic resident-seconds counter (issue #484). LongRunning apps
    /// bump this from a per-app ticker that fires every
    /// `RESIDENT_TICK_SECS` (default 30). Handler (FaaS) apps leave it
    /// at 0 — the worker stamps `resident_seconds = None` on the
    /// heartbeat when this counter is 0 AND the app is Handler, so the
    /// control plane's `applyTenantDelta` treats FaaS as a zero
    /// contribution and never calls `AddResidentSeconds`.
    resident_seconds: Arc<AtomicU64>,
    /// Atomic FaaS duration counter (issue #555). Sum of elapsed
    /// wall-clock milliseconds across all Handler requests since the
    /// last reset (heartbeat interval delta). Captured at dispatch
    /// accept (after the body-cap 413 early return), stamped at the
    /// terminal response in each of the four `handle_request` arms
    /// (`Ok(Ok)`, `Ok(Err)`, `Err(_dropped)` with `exit_code != 0`,
    /// `Err(_dropped)` with `exit_code == 0`). LongRunning apps
    /// never touch this — the field is constructed at `start_app`
    /// but no stamp site fires for the LR path. Wire field
    /// `duration_ms_total` defaults to 0 on legacy workers and is
    /// ignored by legacy control planes.
    duration_ms: Arc<AtomicU64>,
    /// Atomic 5xx error counter (issue #84 asks 6/7). Stamped at
    /// the three FaaS dispatch terminal arms that surface a 500 to
    /// the caller: `Ok(Err(error_code))` (guest returned an HTTP
    /// error response), `Err(_dropped)` with `exit_code != 0`
    /// (guest cleanly called `process.exit`), `Err(_dropped)` with
    /// `exit_code == 0` (real wasm trap or hang). NOT stamped on
    /// the body-cap 413 early return — that is a tenant-config
    /// violation, not a guest error, and the existing
    /// `record_request` comment already excludes it from billing.
    /// Reset on each heartbeat via `subtract_error_delta` (mirror
    /// of `subtract_duration_ms`). Wire field `status_5xx_count`
    /// defaults to 0 on legacy workers; the canary auto-ramp
    /// service (issue #84 ask 6, CP-side `CanaryRampService`)
    /// reads this off heartbeats to compute rolling-window error
    /// rates.
    errors: Arc<AtomicU64>,
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
            outbound_bytes: Arc::new(AtomicU64::new(0)),
            resident_seconds: Arc::new(AtomicU64::new(0)),
            duration_ms: Arc::new(AtomicU64::new(0)),
            errors: Arc::new(AtomicU64::new(0)),
            tenant_id,
            deployment_id,
        }
    }

    /// Record a single request. Called by http-server on each incoming request.
    pub fn record_request(&self) {
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record outbound bytes. Called after each http-client response is received
    /// and after each http-server response body is written to the caller.
    pub fn record_outbound_bytes(&self, n: u64) {
        self.outbound_bytes.fetch_add(n, Ordering::Relaxed);
    }

    /// Record resident seconds. Called by the per-app resident ticker
    /// (LongRunning apps only, issue #484) every `RESIDENT_TICK_SECS`.
    /// Atomically consistent with the request-count and outbound-byte
    /// counters so the same `MeterSnapshot` includes all three deltas
    /// (no TOCTOU at heartbeat time).
    pub fn record_resident_seconds(&self, n: u64) {
        self.resident_seconds.fetch_add(n, Ordering::Relaxed);
    }

    /// Record FaaS request duration in milliseconds (issue #555).
    /// Called by `HandlerDispatch::handle_request` at the terminal
    /// response arm. The Duration is taken from `Instant::now()`
    /// captured before the guest `tokio::spawn` at the start of
    /// `handle_request` so the stamp reflects user-visible latency
    /// from dispatch accept to response complete. Sub-millisecond
    /// values are truncated (not rounded up) — verified by the
    /// `record_duration_truncates_submillisecond` test below.
    ///
    /// Billability (issue #555 acceptance criterion): no FaaS grace
    /// period exists today. Hung handlers are caught by the wasmtime
    /// epoch deadline (`store.set_epoch_deadline` at dispatch.rs:820),
    /// not a separate `tokio::time::timeout` + grace window — there is
    /// no grace period to bill. The `Err(_dropped)` arm's two
    /// sub-cases — `exit_code != 0` (clean `process.exit`) and
    /// `exit_code == 0` (real trap) — are both billed here, consistent
    /// with `record_request()` being called for both. If a future
    /// issue introduces a grace period, the billability question is
    /// decided there.
    pub fn record_duration(&self, d: Duration) {
        self.duration_ms
            .fetch_add(d.as_millis() as u64, Ordering::Relaxed);
    }

    /// Record a 5xx-class request outcome (issue #84 asks 6/7).
    /// Called by `HandlerDispatch::handle_request` at the three
    /// terminal arms that surface a 500 to the caller
    /// (`synthetic_500` is invoked from each). NOT called on the
    /// body-cap 413 early return — that is a tenant-config
    /// violation that never reached the guest, so it isn't a guest
    /// error. Mirrors `record_request`'s counter discipline:
    /// monotonic, atomic, no fence. The heartbeat reads
    /// `get_errors()` and subtracts the delta via
    /// `subtract_error_delta`, the same snapshot-and-subtract
    /// pattern as the other four dimensions.
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the current request count.
    pub fn get_count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Get the current outbound byte total.
    pub fn get_outbound_bytes(&self) -> u64 {
        self.outbound_bytes.load(Ordering::Relaxed)
    }

    /// Get the current resident-seconds total. Returns 0 for Handler
    /// (FaaS) apps because their ticker is never spawned.
    pub fn get_resident_seconds(&self) -> u64 {
        self.resident_seconds.load(Ordering::Relaxed)
    }

    /// Get the current FaaS duration total in milliseconds
    /// (issue #555). Returns 0 for LongRunning apps because the
    /// dispatch path never stamps.
    pub fn get_duration_ms(&self) -> u64 {
        self.duration_ms.load(Ordering::Relaxed)
    }

    /// Get the current 5xx error count (issue #84 asks 6/7).
    /// Returns 0 for LongRunning apps because the dispatch path
    /// never stamps (LR per-connection errors aren't surfaced at
    /// the request level today).
    pub fn get_errors(&self) -> u64 {
        self.errors.load(Ordering::Relaxed)
    }

    /// Subtract previously-snapshotted values from the counters. Called after a
    /// successful heartbeat publish so only the delta not yet reported remains.
    /// Using fetch_sub rather than store(0) preserves any bytes recorded after
    /// the snapshot was taken — those will appear in the next heartbeat interval.
    pub fn subtract_delta(&self, count_delta: u64, bytes_delta: u64) {
        self.count.fetch_sub(count_delta, Ordering::Relaxed);
        self.outbound_bytes
            .fetch_sub(bytes_delta, Ordering::Relaxed);
    }

    /// Subtract previously-snapshotted resident-seconds. Mirrors
    /// `subtract_delta` but for the third metered dimension (issue
    /// #484). Kept as a separate method (not folded into
    /// `subtract_delta`) so the existing paired-axis test contract
    /// doesn't churn — the resident-seconds ticker is LR-only and the
    /// reset happens alongside deployment-state reset_meters_after.
    pub fn subtract_resident_seconds(&self, n: u64) {
        self.resident_seconds.fetch_sub(n, Ordering::Relaxed);
    }

    /// Subtract previously-snapshotted FaaS duration milliseconds
    /// (issue #555). Mirrors `subtract_resident_seconds` for the
    /// fourth metered dimension. Kept as a separate method (not
    /// folded into `subtract_delta`) so the existing two-axis
    /// contract test doesn't churn. The deployment-mismatch guard
    /// in `reset_meters_after` short-circuits all four subtractions
    /// together when a heartbeat arrives for a stale deployment.
    pub fn subtract_duration_ms(&self, ms_delta: u64) {
        self.duration_ms.fetch_sub(ms_delta, Ordering::Relaxed);
    }

    /// Subtract previously-snapshotted 5xx error count
    /// (issue #84 asks 6/7). Mirrors `subtract_duration_ms` for
    /// the fifth metered dimension. Kept as a separate method (not
    /// folded into `subtract_delta`) so the existing paired-axis
    /// contract test doesn't churn. The deployment-mismatch guard
    /// in `reset_meters_after` short-circuits all subtractions
    /// together when a heartbeat arrives for a stale deployment.
    pub fn subtract_error_delta(&self, errors_delta: u64) {
        self.errors.fetch_sub(errors_delta, Ordering::Relaxed);
    }

    /// Get a snapshot of the meter state for reporting.
    pub fn snapshot(&self) -> MeterSnapshot {
        MeterSnapshot {
            tenant_id: self.tenant_id.clone(),
            deployment_id: self.deployment_id.clone(),
            request_count: self.get_count(),
            outbound_bytes: self.get_outbound_bytes(),
            resident_seconds: self.get_resident_seconds(),
            duration_ms: self.get_duration_ms(),
            errors: self.get_errors(),
        }
    }
}

/// A snapshot of metering state for a reporting interval.
#[derive(Debug, Clone)]
pub struct MeterSnapshot {
    pub tenant_id: String,
    pub deployment_id: String,
    pub request_count: u64,
    /// Total outbound bytes since the last reset (heartbeat interval delta).
    pub outbound_bytes: u64,
    /// Total resident seconds since the last reset (heartbeat interval
    /// delta, issue #484). Always 0 for Handler (FaaS) apps because the
    /// per-app resident ticker is only spawned for LongRunning apps.
    /// The worker stamps `resident_seconds = Some(0)` (not None) when
    /// this is 0 for a LongRunning app so the control plane can
    /// distinguish "just-started LR with 0s uptime" from "FaaS that
    /// doesn't contribute" — applyTenantDelta folds both to delta=0
    /// but the wire shape preserves the distinction for future
    /// debugging.
    pub resident_seconds: u64,
    /// Total FaaS request duration in milliseconds since the last
    /// reset (heartbeat interval delta, issue #555). Always 0 for
    /// LongRunning apps because the dispatch path never stamps;
    /// the wire field `duration_ms_total` defaults to 0 on legacy
    /// workers (LongRunning in particular) so the control plane
    /// applies zero contribution via `checkComputeMs`.
    pub duration_ms: u64,
    /// Total 5xx error count since the last reset (heartbeat
    /// interval delta, issue #84 asks 6/7). Always 0 for
    /// LongRunning apps because the dispatch path never stamps.
    /// The wire field `status_5xx_count` defaults to 0 on legacy
    /// workers so the canary auto-ramp service
    /// (`CanaryRampService`) treats them as "no errors this
    /// interval" — backward-compatible with pre-#84 deployments.
    pub errors: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_request_increments_count() {
        let m = RequestMeter::new("t_test".into(), "d_test".into());
        m.record_request();
        m.record_request();
        assert_eq!(m.snapshot().request_count, 2);
    }

    #[test]
    fn record_outbound_bytes_accumulates() {
        let m = RequestMeter::new("t_test".into(), "d_test".into());
        m.record_outbound_bytes(1024);
        m.record_outbound_bytes(512);
        assert_eq!(m.snapshot().outbound_bytes, 1536);
    }

    #[test]
    fn subtract_delta_removes_only_snapshotted_values() {
        let m = RequestMeter::new("t_test".into(), "d_test".into());
        m.record_request();
        m.record_request();
        m.record_outbound_bytes(4096);
        let snap = m.snapshot();
        // Simulate a new request arriving after the snapshot but before reset.
        m.record_request();
        m.record_outbound_bytes(100);
        m.subtract_delta(snap.request_count, snap.outbound_bytes);
        // Only the post-snapshot delta should remain.
        let after = m.snapshot();
        assert_eq!(after.request_count, 1);
        assert_eq!(after.outbound_bytes, 100);
    }

    #[test]
    fn clone_shares_counters() {
        let m = RequestMeter::new("t_test".into(), "d_test".into());
        let m2 = m.clone();
        m.record_outbound_bytes(100);
        m2.record_outbound_bytes(50);
        // Both clones share the same Arc, so the total is 150.
        assert_eq!(m.snapshot().outbound_bytes, 150);
    }

    // -- issue #484 resident-seconds tests ---------------------------------

    #[test]
    fn record_resident_seconds_accumulates() {
        let m = RequestMeter::new("t_test".into(), "d_test".into());
        m.record_resident_seconds(30);
        m.record_resident_seconds(30);
        assert_eq!(m.snapshot().resident_seconds, 60);
        assert_eq!(m.get_resident_seconds(), 60);
    }

    #[test]
    fn subtract_resident_seconds_removes_only_snapshotted_values() {
        let m = RequestMeter::new("t_test".into(), "d_test".into());
        m.record_resident_seconds(60);
        let snap = m.snapshot();
        // Simulate a tick landing after the snapshot but before reset.
        m.record_resident_seconds(30);
        m.subtract_resident_seconds(snap.resident_seconds);
        // Only the post-snapshot tick should remain.
        assert_eq!(m.snapshot().resident_seconds, 30);
    }

    #[test]
    fn clone_shares_resident_seconds() {
        let m = RequestMeter::new("t_test".into(), "d_test".into());
        let m2 = m.clone();
        m.record_resident_seconds(100);
        m2.record_resident_seconds(50);
        // Both clones share the same Arc, so the total is 150.
        assert_eq!(m.snapshot().resident_seconds, 150);
    }

    // -- issue #555 FaaS duration tests -------------------------------------

    #[test]
    fn record_duration_accumulates_millis() {
        let m = RequestMeter::new("t_test".into(), "d_test".into());
        m.record_duration(Duration::from_millis(120));
        m.record_duration(Duration::from_millis(80));
        assert_eq!(m.snapshot().duration_ms, 200);
        assert_eq!(m.get_duration_ms(), 200);
    }

    #[test]
    fn subtract_duration_ms_removes_only_snapshotted_values() {
        let m = RequestMeter::new("t_test".into(), "d_test".into());
        m.record_duration(Duration::from_millis(60));
        let snap = m.snapshot();
        // Simulate a stamp landing after the snapshot but before reset.
        m.record_duration(Duration::from_millis(40));
        m.subtract_duration_ms(snap.duration_ms);
        // Only the post-snapshot stamp should remain.
        assert_eq!(m.snapshot().duration_ms, 40);
    }

    #[test]
    fn clone_shares_duration_ms() {
        let m = RequestMeter::new("t_test".into(), "d_test".into());
        let m2 = m.clone();
        m.record_duration(Duration::from_millis(100));
        m2.record_duration(Duration::from_millis(50));
        // Both clones share the same Arc, so the total is 150.
        assert_eq!(m.snapshot().duration_ms, 150);
    }

    #[test]
    fn record_duration_truncates_submillisecond() {
        let m = RequestMeter::new("t_test".into(), "d_test".into());
        // 500 µs — well below the millisecond resolution we bill on.
        m.record_duration(Duration::from_micros(500));
        assert_eq!(m.snapshot().duration_ms, 0);
        // 999 µs still rounds down to 0.
        m.record_duration(Duration::from_micros(999));
        assert_eq!(m.snapshot().duration_ms, 0);
        // 1 ms exactly bills as 1.
        m.record_duration(Duration::from_millis(1));
        assert_eq!(m.snapshot().duration_ms, 1);
        // 1.999 ms truncates to 1 (no rounding up). `Duration::as_millis`
        // does integer division (1_999_000 ns / 1_000_000 = 1), so the
        // cast to u64 yields 1 — verified against the Rust stdlib.
        // Cumulative snapshot: prior 1ms + 1ms (truncated from 1.999ms) = 2.
        m.record_duration(Duration::from_nanos(1_999_000));
        assert_eq!(m.snapshot().duration_ms, 2);
    }

    // -- issue #84 asks 6/7 — 5xx error counter tests --------------------

    #[test]
    fn record_error_increments_count() {
        let m = RequestMeter::new("t_test".into(), "d_test".into());
        assert_eq!(m.snapshot().errors, 0, "fresh meter errors = 0");
        m.record_error();
        m.record_error();
        m.record_error();
        assert_eq!(m.snapshot().errors, 3);
        assert_eq!(m.get_errors(), 3);
    }

    #[test]
    fn subtract_error_delta_removes_only_snapshotted_values() {
        let m = RequestMeter::new("t_test".into(), "d_test".into());
        m.record_error();
        m.record_error();
        let snap = m.snapshot();
        // Simulate an error stamp landing after the snapshot but
        // before reset — the subtract must preserve the
        // post-snapshot delta.
        m.record_error();
        m.subtract_error_delta(snap.errors);
        let after = m.snapshot();
        assert_eq!(after.errors, 1, "only post-snapshot delta remains");
    }

    #[test]
    fn clone_shares_errors_counter() {
        let m = RequestMeter::new("t_test".into(), "d_test".into());
        let m2 = m.clone();
        m.record_error();
        m2.record_error();
        m2.record_error();
        // Both clones share the same Arc, so the total is 3.
        assert_eq!(m.snapshot().errors, 3);
        assert_eq!(m2.get_errors(), 3);
    }

    #[test]
    fn errors_independent_from_request_count() {
        // Mirror: `record_request` bumps count only; `record_error`
        // bumps errors only. The two counters are independent so
        // `request_count - errors` is the number of 2xx/3xx/4xx
        // responses.
        let m = RequestMeter::new("t_test".into(), "d_test".into());
        m.record_request();
        m.record_request();
        m.record_error();
        let snap = m.snapshot();
        assert_eq!(snap.request_count, 2);
        assert_eq!(snap.errors, 1);
    }

    #[test]
    fn snapshot_includes_all_five_dimensions() {
        // Snapshot regression guard: the heartbeat wire carries all
        // five dimensions from a single atomic snapshot, so a
        // future field addition that forgets to plumb the new
        // counter into `snapshot()` would silently drop data on
        // the wire. Assert every field is populated.
        let m = RequestMeter::new("t_test".into(), "d_test".into());
        m.record_request();
        m.record_outbound_bytes(100);
        m.record_resident_seconds(30);
        m.record_duration(Duration::from_millis(40));
        m.record_error();
        let snap = m.snapshot();
        assert_eq!(snap.tenant_id, "t_test");
        assert_eq!(snap.deployment_id, "d_test");
        assert_eq!(snap.request_count, 1);
        assert_eq!(snap.outbound_bytes, 100);
        assert_eq!(snap.resident_seconds, 30);
        assert_eq!(snap.duration_ms, 40);
        assert_eq!(snap.errors, 1);
    }
}
