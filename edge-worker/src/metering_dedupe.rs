//! Metering deduplication helpers (issue #418).
//!
//! Workers publish `request_count` and `outbound_bytes` deltas in each
//! `HeartbeatMessage`. The control plane's metering pipeline (see
//! `edge-control-plane/internal/service/worker.go::applyTenantDelta`)
//! atomically adds those deltas to `quotas.used_*`. JetStream redelivery
//! or `reconcile.Service` replay can cause the same delta to be applied
//! twice on a single CP instance, inflating the cumulative counter.
//!
//! This module provides a stable, content-derived identifier that the
//! worker stamps on each `AppStatus` in the heartbeat. The CP keeps an
//! in-memory cache of recently-seen IDs and skips re-applying the delta
//! when it sees a duplicate. The bucket math ensures the ID is stable
//! across multiple deliveries of the *same* heartbeat (so JetStream
//! redelivery hits the cache) and rotates across heartbeat intervals
//! (so the next legitimate heartbeat doesn't collide with the cache).
//!
//! Format: `{worker_id}:{deployment_id}:{bucket}` where `bucket` is
//! `unix_seconds / HEARTBEAT_BUCKET_SECS`. The 30-second bucket matches
//! the default `EDGE_HEARTBEAT_INTERVAL_SECS`, so two heartbeats within
//! the same interval share the same ID, but heartbeats in different
//! intervals produce different IDs.

/// Width of the dedupe bucket in seconds. Chosen to match the default
/// heartbeat interval (30s, see `EDGE_HEARTBEAT_INTERVAL_SECS` in
/// `edge-worker/src/config.rs`). If the operator tunes the heartbeat
/// interval down below 30s, buckets may overlap — but the only failure
/// mode is "two distinct heartbeats share a dedupe ID", which causes
/// the second to be skipped. That's a *conservative* error (under-count
/// by one interval), which is acceptable for billing: the worker just
/// re-publishes on the next tick.
pub const HEARTBEAT_BUCKET_SECS: i64 = 30;

/// Compute the dedupe ID for one `(worker, deployment)` pair at the
/// given wall-clock instant. Pure function — no I/O, no clock reads —
/// so the same `(worker_id, deployment_id, now)` always produces the
/// same ID, and tests can pin the clock deterministically.
///
/// The bucket is `floor(now_unix / HEARTBEAT_BUCKET_SECS)`. Two calls
/// within the same bucket return the same ID; calls in adjacent
/// buckets return different IDs. The ID is safe to embed in NATS
/// subjects, JSON fields, and HTTP headers — only contains
/// alphanumerics, `:`, and `_`.
pub fn dedupe_id(worker_id: &str, deployment_id: &str, now_unix_secs: i64) -> String {
    let bucket = now_unix_secs / HEARTBEAT_BUCKET_SECS;
    format!("{}:{}:{}", worker_id, deployment_id, bucket)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedupe_id_is_stable_within_bucket() {
        // Two heartbeats 10s apart, same bucket (bucket = 100 / 30 = 3)
        let a = dedupe_id("w_fra_a", "d_abc", 100);
        let b = dedupe_id("w_fra_a", "d_abc", 110);
        assert_eq!(a, b, "10s apart, same bucket — IDs must match");
    }

    #[test]
    fn dedupe_id_rotates_across_buckets() {
        // bucket 3 (seconds 90-119) vs bucket 4 (seconds 120-149)
        let a = dedupe_id("w_fra_a", "d_abc", 119);
        let b = dedupe_id("w_fra_a", "d_abc", 120);
        assert_ne!(a, b, "adjacent buckets must produce different IDs");
    }

    #[test]
    fn dedupe_id_distinct_workers() {
        // Two workers hosting the same deployment must NOT share a dedupe
        // ID — otherwise one worker's redelivery would skip the other's
        // legitimate heartbeat.
        let a = dedupe_id("w_fra_a", "d_abc", 100);
        let b = dedupe_id("w_fra_sfo", "d_abc", 100);
        assert_ne!(a, b, "different workers must produce different IDs");
    }

    #[test]
    fn dedupe_id_distinct_deployments() {
        // Same worker, two different deployments of the same app name —
        // each gets its own dedupe slot.
        let a = dedupe_id("w_fra_a", "d_abc", 100);
        let b = dedupe_id("w_fra_a", "d_def", 100);
        assert_ne!(a, b, "different deployments must produce different IDs");
    }

    #[test]
    fn dedupe_id_handles_negative_clock() {
        // Pre-1970 timestamps should still produce a stable string —
        // bucket arithmetic floors toward negative infinity for negative
        // inputs in Rust, which is the same as Python's // operator.
        let id = dedupe_id("w", "d", -30);
        assert!(id.starts_with("w:d:"));
    }

    #[test]
    fn dedupe_id_handles_zero_clock() {
        let id = dedupe_id("w_fra_a", "d_abc", 0);
        assert_eq!(id, "w_fra_a:d_abc:0");
    }
}
