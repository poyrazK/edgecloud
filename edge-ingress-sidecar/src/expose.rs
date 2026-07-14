//! UDS datagram writer (issue #665, PR B).
//!
//! Owns a single SOCK_DGRAM socket at
//! `/var/run/edge-ingress/global-rps.sock` (mode 0660, group
//! `edge-ingress` shared between Caddy + the sidecar) and writes a
//! single datagram per aggregator tick carrying
//! ```json
//! {"ts":..., "configured":M, "platform_total":N,
//!  "this_replica":K, "replicas_seen":L, "local_cap":R}
//! ```
//!
//! The ingress binary (PR D) reads this socket at 1 Hz and consults
//! it in `caddy.rs:719-746` to render the per-replica `rates.rps`.
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
//! ## Mode 0660 / group ownership
//!
//! The socket directory is created with mode 0750 and the socket
//! file with mode 0660. Both Caddy and the sidecar run with primary
//! group `edge-ingress`, so the OS-enforced permission gates
//! cross-process access. Operators chown the socket directory
//! out-of-band (the sidecar does NOT change group ownership at
//! startup — touching ownership from a non-root process would
//! silently fail, and pulling `libc` into the dep tree just for a
//! chown() we don't strictly need would balloon the binary). On a
//! well-configured deployment the runtime directory already has the
//! right owner (see `scripts/dev-install.sh`).
//!
//! ## Failure modes
//!
//!   - Socket directory missing → `bind()` fails → the sidecar logs a
//!     WARN and retries on the next tick. The ingress keeps
//!     rendering the previous cache value (fail-closed).
//!   - Stale socket file from a prior process → `unlink()` before
//!     `bind()`.
//!   - Write fails (EAGAIN, EPIPE) → log + drop. The next tick
//!     republishes.

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
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
    pub platform_total: u32,
    /// This replica's latest delta inside the window.
    pub this_replica: u32,
    /// Number of distinct replicas that published in the window.
    pub replicas_seen: u32,
    /// Precomputed per-replica cap. `None` ⇒ ingress should emit
    /// NO global route (fail-closed; zero replicas in window).
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

/// Writer state. Owns the bound socket; `write` is called once per
/// aggregator tick.
pub struct Exposer {
    /// `pub(crate)` so the test module can call `connect()` to set a
    /// peer for the end-to-end roundtrip test. Production callers go
    /// through `Exposer::write`, which doesn't need a connected peer
    /// (it sends one datagram per tick into the kernel buffer; the
    /// ingress binary opens its own `recv` socket and reads).
    pub(crate) socket: UnixDatagram,
    path: PathBuf,
}

impl Exposer {
    /// Bind the UDS datagram socket. The parent directory must
    /// exist with mode 0750 and group ownership matching the
    /// ingress process; the sidecar creates the directory if
    /// missing and chmod's it 0750. Group ownership is left to
    /// the operator (see module-level docs).
    ///
    /// `path` is the full socket path (default
    /// `/var/run/edge-ingress/global-rps.sock`).
    pub fn bind(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create socket dir {}", parent.display()))?;
            // Mode 0750 on the runtime directory. The sidecar does
            // NOT chown — operators own the directory lifecycle
            // (see module docs).
            let perms = std::fs::Permissions::from_mode(0o750);
            std::fs::set_permissions(parent, perms)
                .with_context(|| format!("chmod 0750 on {}", parent.display()))?;
        }
        // Stale socket from a previous process — unlink before bind.
        let _ = std::fs::remove_file(path);
        let socket = UnixDatagram::bind(path)
            .with_context(|| format!("bind UDS datagram at {}", path.display()))?;
        // Mode 0660 on the socket itself.
        let perms = std::fs::Permissions::from_mode(0o660);
        std::fs::set_permissions(path, perms)
            .with_context(|| format!("chmod 0660 on {}", path.display()))?;
        Ok(Self {
            socket,
            path: path.to_path_buf(),
        })
    }

    /// Write one datagram. Best-effort: on EAGAIN / EPIPE we log
    /// and drop; the next tick republishes. The send is async —
    /// tokio's `UnixDatagram::send` returns a future that completes
    /// when the kernel accepts the datagram into the socket buffer.
    pub async fn write(&self, payload: &DatagramPayload) -> anyhow::Result<()> {
        let bytes = serde_json::to_vec(payload).context("serialize datagram")?;
        match self.socket.send(&bytes).await {
            Ok(_) => {
                debug!(
                    configured = payload.configured,
                    platform_total = payload.platform_total,
                    local_cap = ?payload.local_cap,
                    "exposer: wrote datagram"
                );
                Ok(())
            }
            Err(e) => {
                warn!(err = %e, path = %self.path.display(), "exposer: send failed");
                Err(e.into())
            }
        }
    }
}

/// Bridge from the aggregator's tick callback to the exposer's write
/// method. Builds a [`DatagramPayload`] from the [`Snapshot`] and
/// forwards it. The write is async, so the synchronous callback
/// spawns a fire-and-forget tokio task — the aggregator's tick
/// cadence is 1 Hz and the kernel socket buffer absorbs any back-
/// pressure; a write failure is logged and dropped (the next tick
/// republishes a fresh snapshot).
#[allow(dead_code)] // reserved for the integration test that wires aggregator → exposer directly
pub fn snapshot_to_writer(
    exposer: Arc<Exposer>,
) -> impl Fn(crate::aggregate::Snapshot) + Send + Sync {
    move |snap| {
        let payload = DatagramPayload::from(&snap);
        let exposer = Arc::clone(&exposer);
        tokio::spawn(async move {
            if let Err(e) = exposer.write(&payload).await {
                warn!(err = %e, "snapshot_to_writer: write failed");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregate::Snapshot;

    fn snap(configured: u32, total: u32, this_replica: u32, replicas: u32) -> Snapshot {
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

    // ── Exposer::bind + write roundtrip test ────────────────────────

    #[tokio::test]
    async fn exposer_bind_sets_mode_0660_and_writes_datagram() {
        // Bind the exposer in a per-test tempdir so we don't
        // pollute /var/run/edge-ingress during `cargo test`. Set
        // up a paired SOCK_DGRAM peer to receive the datagram
        // and assert the JSON wire shape end-to-end.
        let dir = tempfile::TempDir::new().expect("tempdir");
        let path = dir.path().join("global-rps.sock");
        let peer_path = dir.path().join("peer.sock");
        let exposer = Exposer::bind(&path).expect("bind");
        assert!(path.exists(), "socket file should exist after bind");
        let meta = std::fs::metadata(&path).expect("stat");
        let mode = meta.permissions().mode() & 0o777;
        assert_eq!(mode, 0o660, "socket must be mode 0660, got {mode:o}");

        // Bind the peer socket and `connect()` the exposer to it
        // so a `send()` (no address) routes to the peer.
        let peer = tokio::net::UnixDatagram::bind(&peer_path).expect("bind peer");
        exposer
            .socket
            .connect(&peer_path)
            .expect("connect exposer to peer");

        let payload = DatagramPayload::from(&snap(10_000, 5_000, 5_000, 1));
        exposer.write(&payload).await.expect("write");
        let mut buf = vec![0u8; 1024];
        let n = peer.recv(&mut buf).await.expect("recv");
        let json: serde_json::Value = serde_json::from_slice(&buf[..n]).expect("parse json");
        assert_eq!(json["configured"], 10_000);
        assert_eq!(json["platform_total"], 5_000);
        assert_eq!(json["this_replica"], 5_000);
        assert_eq!(json["replicas_seen"], 1);
        // N=1 replica ⇒ others_share=0 ⇒ per_replica_cap=10000.
        assert_eq!(json["local_cap"], 10_000);
        assert!(json["ts"].as_u64().unwrap() > 0);
    }

    #[tokio::test]
    async fn exposer_bind_unlinks_stale_socket() {
        // A prior process left a stale socket file — bind() must
        // succeed anyway. (Without the unlink, the bind would
        // error with EADDRINUSE.)
        let dir = tempfile::TempDir::new().expect("tempdir");
        let path = dir.path().join("global-rps.sock");
        std::fs::write(&path, b"stale").expect("create stale file");
        let exposer = Exposer::bind(&path).expect("bind despite stale");
        assert!(path.exists());
        drop(exposer);
    }
}
