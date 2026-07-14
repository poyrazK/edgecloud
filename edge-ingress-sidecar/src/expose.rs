//! UDS datagram writer (issue #665, PR B).
//!
//! Sends one SOCK_DGRAM datagram per aggregator tick carrying
//! ```json
//! {"ts":..., "configured":M, "platform_total":N,
//!  "this_replica":K, "replicas_seen":L, "local_cap":R}
//! ```
//! to a well-known socket path (default
//! `/var/run/edge-ingress/global-rps.sock`). The ingress binary
//! (PR D) is the receiver; it `bind()`s the socket, `chmod()`s it
//! 0660, and `recv()`s the datagrams.
//!
//! ## UDS SOCK_DGRAM ownership (PR B review fix)
//!
//! In the original PR B (commit `7384be63`) the sidecar owned the
//! socket via `UnixDatagram::bind(path)`, and `write()` called
//! `socket.send(bytes)` — an addressless send. That works in tests
//! where a paired peer manually `connect()`s the sidecar socket,
//! but in production a bound Unix datagram socket is NOT auto-
//! connected, so `send()` returns `ENOTCONN`/`EDESTADDRREQ`.
//!
//! The correct architecture is **receiver owns the bind**:
//!
//! - Receiver (ingress process, PR D) calls `bind(path)`,
//!   `chmod(path, 0o660)`, and `unlink(stale_path)` on startup.
//! - Sender (this sidecar) uses `UnixDatagram::unbound()` +
//!   `send_to(bytes, path)` and treats `ENOENT` as "receiver not
//!   ready; drop and retry next tick."
//!
//! `send_to(path)` works across receiver restarts because the path
//! string is stable even though the inode is fresh; `connect()`
//! would follow the inode and break on restart.
//!
//! ## Why UDS datagram (not stream, not TCP, not a file)
//!
//! Alternatives considered in the issue #665 plan:
//!   - **Shared file + polling**: filesystem polling is slow and torn
//!     reads across the sidecar restart would briefly serve a
//!     half-written JSON line.
//!   - **Local HTTP admin endpoint on Caddy**: would require a new
//!     Caddy route and break the "stock Caddy" constraint (issue #665
//!     plan explicitly forbids touching Caddy config for cross-
//!     replica aggregation).
//!   - **UDS SOCK_STREAM**: heavier than we need (the wire payload
//!     is a single datagram per tick) and would force a length-
//!     framed protocol on the ingress side.
//!
//! UDS SOCK_DGRAM is atomic per packet, no torn reads, no Caddy
//! config touch.
//!
//! ## Failure modes
//!
//!   - Socket missing (receiver hasn't started or just restarted) ⇒
//!     `ENOENT` on `send_to` ⇒ log + drop; next tick republishes.
//!   - Write fails (EAGAIN, EPIPE) ⇒ log + drop; next tick
//!     republishes.
//!   - Receiver restarts mid-publish ⇒ next `send_to` finds the
//!     fresh inode at the same path (no client-side reconnect
//!     needed).

use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use anyhow::Context;
use serde::Serialize;
use tokio::net::UnixDatagram;
use tracing::{debug, warn};

use crate::aggregate::Snapshot;

/// Wire shape published to the UDS datagram socket per tick.
///
/// Mirrors the issue #665 plan field list. `local_cap` is the
/// precomputed per-replica cap (`Snapshot::per_replica_cap()`);
/// carrying it on the wire lets the ingress binary render with no
/// arithmetic — the sidecar owns the "what does the platform cap
/// translate to for this replica" calculation, and the ingress
/// just reads.
#[derive(Debug, Clone, Serialize)]
pub struct DatagramPayload {
    /// Unix milliseconds since epoch at write time.
    pub ts: u64,
    /// Operator's configured platform cap.
    pub configured: u32,
    /// Sum of every replica's latest delta inside the window.
    /// `u64` mirrors `Snapshot::platform_total` so the wire can
    /// carry 64-replica aggregates without truncation.
    pub platform_total: u64,
    /// This replica's latest delta inside the window.
    pub this_replica: u32,
    /// Number of distinct replicas that published in the window.
    pub replicas_seen: u32,
    /// Precomputed per-replica cap. `None` ⇒ ingress should emit
    /// NO global route (fail-closed; either operator disabled
    /// enforcement via `configured_cap == 0`, or no traffic has
    /// been seen in the window).
    pub local_cap: Option<u32>,
}

impl From<&Snapshot> for DatagramPayload {
    fn from(snap: &Snapshot) -> Self {
        Self {
            ts: unix_ms_now(),
            configured: snap.configured_cap,
            platform_total: snap.platform_total,
            this_replica: snap.this_replica_rps,
            replicas_seen: snap.replicas_seen,
            local_cap: snap.per_replica_cap(),
        }
    }
}

fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Write one datagram to `path`. Best-effort: on `ENOENT` (receiver
/// not bound yet, or just restarted) and on `EAGAIN`/`EPIPE` we
/// log and return `Ok(())` — the next tick republishes. Only
/// surface hard errors (serialization failure, etc.) as `Err`.
///
/// Uses an **unbound** `UnixDatagram` per call. This is cheap
/// (one `socket(AF_UNIX, SOCK_DGRAM, 0)` + close per call) and
/// keeps the function stateless so the caller doesn't need to
/// hold a socket across ticks. Tokio's runtime reuses the file
/// descriptor cache for short-lived sockets.
///
/// Returns `Ok(())` even when the receiver is not bound — the
/// production semantic is "fire and forget; the next tick
/// republishes." A persistent `ENOENT` shows up in the operator's
/// logs as repeated `warn!`s and in the sidecar's missing-
/// receiver metric (future work).
pub async fn write_to_path(payload: &DatagramPayload, path: &Path) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec(payload).context("serialize datagram")?;
    let sock = UnixDatagram::unbound().context("create unbound UDS datagram socket")?;
    match sock.send_to(&bytes, path).await {
        Ok(n) => {
            debug!(
                bytes = n,
                configured = payload.configured,
                platform_total = payload.platform_total,
                local_cap = ?payload.local_cap,
                "exposer: wrote datagram"
            );
            Ok(())
        }
        Err(e) => {
            // ENOENT (receiver not bound), ENOTDIR (parent missing),
            // ECONNREFUSED (linux returns this on a path with no
            // listener) all mean "receiver not ready." Log + drop
            // rather than fail the tick — the ingress keeps serving
            // the last value it had.
            warn!(
                err = %e,
                path = %path.display(),
                "exposer: send_to failed; receiver probably not bound (drop)"
            );
            Ok(())
        }
    }
}

/// Bridge from the aggregator's tick callback to the UDS writer.
/// Returns an `Arc<dyn Fn(Snapshot) + Send + Sync>` so the
/// aggregator's `spawn_aggregator` signature stays unchanged.
pub fn snapshot_to_writer(path: Arc<Path>) -> impl Fn(crate::aggregate::Snapshot) + Send + Sync {
    move |snap| {
        let payload = DatagramPayload::from(&snap);
        let path = Arc::clone(&path);
        tokio::spawn(async move {
            if let Err(e) = write_to_path(&payload, &path).await {
                // write_to_path already swallows transient errors;
                // only a serialization failure reaches here. Log
                // at error level because the wire shape will be
                // stuck until the next code deploy.
                tracing::error!(err = %e, "snapshot_to_writer: write_to_path returned Err");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate::Snapshot;

    fn snap(configured: u32, total: u64, this_replica: u32, replicas: u32) -> Snapshot {
        Snapshot {
            configured_cap: configured,
            platform_total: total,
            this_replica_rps: this_replica,
            replicas_seen: replicas,
        }
    }

    // ── DatagramPayload::from(&Snapshot) tests ─────────────────────

    #[test]
    fn payload_carries_precomputed_local_cap() {
        let p = DatagramPayload::from(&snap(10_000, 30_000, 10_000, 3));
        assert_eq!(p.configured, 10_000);
        assert_eq!(p.platform_total, 30_000);
        assert_eq!(p.this_replica, 10_000);
        assert_eq!(p.replicas_seen, 3);
        assert_eq!(p.local_cap, Some(1), "verification target");
        assert!(p.ts > 0);
    }

    #[test]
    fn payload_local_cap_none_when_zero_replicas() {
        // Fail-closed: ingress emits NO global route.
        let p = DatagramPayload::from(&snap(10_000, 0, 0, 0));
        assert_eq!(p.local_cap, None);
    }

    #[test]
    fn payload_local_cap_none_when_zero_configured() {
        // NEW (PR B review): configured=0 must also be fail-closed.
        // The .max(1, ...) floor in Snapshot::per_replica_cap is
        // suppressed by an earlier guard; pin the wire shape so
        // the ingress sees `local_cap: null` rather than `1`.
        let p = DatagramPayload::from(&snap(0, 30_000, 10_000, 3));
        assert_eq!(p.local_cap, None, "configured=0 must be fail-closed");
    }

    #[test]
    fn payload_serializes_to_json_with_expected_fields() {
        // Pin the wire shape — the ingress binary (PR D) reads
        // exactly these field names.
        let p = DatagramPayload::from(&snap(1000, 500, 250, 2));
        let v: serde_json::Value = serde_json::to_value(&p).unwrap();
        assert_eq!(v["configured"], 1000);
        assert_eq!(v["platform_total"], 500);
        assert_eq!(v["this_replica"], 250);
        assert_eq!(v["replicas_seen"], 2);
        assert!(v["local_cap"].is_u64());
        assert!(v["ts"].is_u64());
    }

    // ── write_to_path tests (UDS ownership flip) ────────────────────

    #[tokio::test]
    async fn write_to_path_delivers_datagram_to_bound_receiver() {
        // Real-receiver pattern: a second `UnixDatagram::bind(path)`
        // is the receiver. The sidecar's `write_to_path` uses an
        // UNBOUND socket + `send_to(path)` — no `connect` on the
        // sender side, no `connect` of paired peers. This mirrors
        // production: ingress process binds, sidecar sends.
        let dir = tempfile::TempDir::new().expect("tempdir");
        let path = dir.path().join("global-rps.sock");
        let receiver = tokio::net::UnixDatagram::bind(&path).expect("bind receiver");

        let payload = DatagramPayload::from(&snap(10_000, 5_000, 5_000, 1));
        write_to_path(&payload, &path).await.expect("write_to_path");

        let mut buf = vec![0u8; 1024];
        let n = receiver.recv(&mut buf).await.expect("recv");
        let json: serde_json::Value = serde_json::from_slice(&buf[..n]).expect("parse json");
        assert_eq!(json["configured"], 10_000);
        assert_eq!(json["platform_total"], 5_000);
        assert_eq!(json["this_replica"], 5_000);
        assert_eq!(json["replicas_seen"], 1);
        // N=1 ⇒ others_share=0 ⇒ per_replica_cap=configured=10000.
        assert_eq!(json["local_cap"], 10_000);
        assert!(json["ts"].as_u64().unwrap() > 0);
    }

    #[tokio::test]
    async fn write_to_path_treats_enoent_as_drop() {
        // Receiver has never bound the socket. Production semantic
        // is "fire and forget; the next tick republishes" — must
        // return Ok(()), not Err, so a missing receiver doesn't
        // fail the sidecar's tick loop.
        let dir = tempfile::TempDir::new().expect("tempdir");
        let path = dir.path().join("never-bound.sock");
        let payload = DatagramPayload::from(&snap(10_000, 0, 0, 0));
        write_to_path(&payload, &path)
            .await
            .expect("ENOENT is dropped, not surfaced");
    }

    #[tokio::test]
    async fn write_to_path_delivers_after_receiver_rebind() {
        // The load-bearing production scenario: the ingress
        // process restarts. The path string is stable; a new
        // inode is created at the same path. The sidecar's next
        // `send_to(path)` finds the new inode without any client-
        // side reconnect — unlike `connect()`, which follows the
        // inode and would break.
        //
        // The receiver must `unlink(stale)` before rebinding
        // (production receiver behavior; see expose.rs module
        // docs). Without unlink, EADDRINUSE fires immediately on
        // rebind because the kernel hasn't garbage-collected the
        // old inode yet. We mirror that here so the test catches
        // the unlink-required regression.
        let dir = tempfile::TempDir::new().expect("tempdir");
        let path = dir.path().join("global-rps.sock");

        // First receiver.
        let receiver1 = tokio::net::UnixDatagram::bind(&path).expect("bind receiver1");
        let payload1 = DatagramPayload::from(&snap(10_000, 1_000, 1_000, 1));
        write_to_path(&payload1, &path).await.expect("write 1");
        let mut buf = vec![0u8; 1024];
        let n = receiver1.recv(&mut buf).await.expect("recv 1");
        let json1: serde_json::Value = serde_json::from_slice(&buf[..n]).expect("parse 1");
        assert_eq!(json1["this_replica"], 1_000);

        // Receiver restarts: drop the old socket (releases the
        // handle), unlink the stale path (mirrors the production
        // receiver's `remove_file` before `bind`), bind a new one
        // at the same path.
        drop(receiver1);
        // Best-effort unlink — if it raced the kernel cleanup
        // that's still fine (NotFound is harmless).
        let _ = std::fs::remove_file(&path);
        let receiver2 = tokio::net::UnixDatagram::bind(&path).expect("bind receiver2");
        let payload2 = DatagramPayload::from(&snap(10_000, 2_000, 2_000, 1));
        write_to_path(&payload2, &path).await.expect("write 2");
        let n = receiver2.recv(&mut buf).await.expect("recv 2");
        let json2: serde_json::Value = serde_json::from_slice(&buf[..n]).expect("parse 2");
        assert_eq!(json2["this_replica"], 2_000);
    }
}
