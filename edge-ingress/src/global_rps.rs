//! Cross-replica global RPS cache + UDS datagram reader (issue #665 PR D).
//!
//! The sidecar (`edge-ingress-sidecar`) scrapes Caddy's `/metrics`, publishes
//! per-replica RPS deltas to a JetStream stream, consumes the platform-wide
//! delta stream, computes a per-replica cap, and writes a `DatagramPayload`
//! to a UDS SOCK_DGRAM socket once per tick (default 1 Hz). This module is
//! the **receiver** side: it binds the well-known socket path, parses each
//! datagram, and exposes the latest `local_cap` to the Caddy renderer so
//! the renderer can emit a `global_route` between the tenant-rl splice and
//! per-app routes.
//!
//! ## Fail-closed contract
//!
//! `GlobalRpsCache::current_local_cap(stale_after)` returns `None` when:
//!   - (a) cache is empty (cold start — no sidecar datagram yet),
//!   - (b) `local_cap` is `None` on the wire (operator disabled
//!     enforcement via `configured_cap == 0`, or no traffic has been
//!     seen in the aggregator's window),
//!   - (c) `received_at.elapsed() > 2 × tick_interval` (sidecar stale —
//!     crashed, network partition, or simply not running).
//!
//! The renderer reads this and emits **no global route** on `None`. This
//! matches the broader ingress contract — caches that haven't seen their
//! backing signal yet behave as if the cross-replica feature is off, not
//! as if traffic is unlimited.
//!
//! ## UDS SOCK_DGRAM ownership (PR B review fix)
//!
//! The ingress process is the **receiver** of the UDS datagrams. Per the
//! ownership flip (see `edge-ingress-sidecar/src/expose.rs` module docs
//! and [[issue-665-uds-ownership-flip]]): the sidecar uses an unbound
//! `UnixDatagram` + `send_to(path)` per tick; the ingress owns `bind()`,
//! `chmod()`, and `unlink(stale)` on startup. `send_to` works across
//! receiver restarts because the path string is stable even though the
//! inode is fresh; `connect()` would follow the inode and break on restart.
//!
//! ## Why a separate cache (not `quota.rs`)
//!
//! `quota.rs` is per-tenant (`HashMap<tenant_id, QuotaState>`) and
//! fetched from a CP HTTP endpoint. The cross-replica cap is platform-
//! wide (a single `u32` per ingress process) and arrives via UDS, not
//! HTTP. Splitting them keeps each module focused on a single backing
//! signal and avoids an "is this quota or global_rps?" branch in the
//! renderer.

use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde::Deserialize;
use tokio::net::UnixDatagram;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

/// Latest datagram received from the sidecar.
///
/// `local_cap: None` is the **fail-closed signal** — see module docs.
/// The renderer reads this and emits no global route when it's `None`.
#[derive(Debug, Clone)]
pub struct GlobalRpsEntry {
    /// Precomputed per-replica cap from the sidecar's aggregator.
    /// `None` ⇒ ingress should emit NO global route (fail-closed; either
    /// operator disabled enforcement via `configured_cap == 0`, or no
    /// traffic has been seen in the window).
    pub local_cap: Option<u32>,
    /// Operator's configured platform cap (echo of `GLOBAL_RATE_LIMIT_RPS`).
    pub configured: u32,
    /// Sum of every replica's latest delta inside the window.
    pub platform_total: u64,
    /// Number of distinct replicas that published in the window.
    pub replicas_seen: u32,
    /// Wall-clock instant this entry was received (NOT the wire `ts` —
    /// `ts` is the sidecar's clock, which is what `received_at` lets us
    /// distinguish from local skew).
    pub received_at: Instant,
}

/// Shared cache type for the renderer + the UDS reader task. Single
/// global entry, not per-tenant — the cap is platform-wide.
pub type SharedGlobalRpsCache = Arc<RwLock<GlobalRpsCache>>;

/// The cache itself. Holds at most one entry (the latest datagram).
/// `None` represents "no sidecar contact yet" — the renderer treats
/// this as fail-closed.
#[derive(Default)]
pub struct GlobalRpsCache {
    inner: Option<GlobalRpsEntry>,
}

impl GlobalRpsCache {
    /// Replace the cached entry. Called by the UDS reader task on every
    /// received datagram.
    pub fn update(&mut self, entry: GlobalRpsEntry) {
        self.inner = Some(entry);
    }

    /// The renderer's hot path: a single `Option` lookup with the
    /// fail-closed contract applied.
    ///
    /// Returns `None` when:
    ///   - cache is empty (no sidecar contact yet),
    ///   - `local_cap` is `None` on the wire (operator disabled),
    ///   - `received_at.elapsed() > stale_after` (sidecar stale — the
    ///     default threshold is `2 × global_rps_tick_interval`, which
    ///     gives the sidecar one full tick of grace before we declare
    ///     it dead).
    pub fn current_local_cap(&self, stale_after: Duration) -> Option<u32> {
        let entry = self.inner.as_ref()?;
        let cap = entry.local_cap?;
        if entry.received_at.elapsed() > stale_after {
            return None;
        }
        Some(cap)
    }

    /// Read-only accessor for the full cached entry (used by tests +
    /// future introspection / metrics surface).
    #[allow(dead_code)]
    pub fn latest(&self) -> Option<&GlobalRpsEntry> {
        self.inner.as_ref()
    }
}

/// Wire shape for the UDS datagram. Mirrors
/// `edge_ingress_sidecar::expose::DatagramPayload`. Defined here so
/// `serde` can deserialize without a roundtrip through the sidecar
/// crate; the sidecar and ingress are independently compiled, so
/// drift would surface as a runtime `Err`, not a compile error —
/// pinning the field names + types here keeps the wire contract
/// traceable in code review.
///
/// Kept private because only this module consumes it.
#[derive(Debug, Deserialize)]
struct DatagramPayload {
    #[serde(default)]
    configured: u32,
    #[serde(default)]
    platform_total: u64,
    #[serde(default)]
    replicas_seen: u32,
    /// `None` is the **fail-closed** signal — see module docs.
    /// `#[serde(default)]` so a missing key (future wire version)
    /// degrades to `None` rather than panicking on parse.
    #[serde(default)]
    local_cap: Option<u32>,
}

/// Bind the UDS datagram socket, chmod it 0660 so the sidecar (same
/// uid group, typically the pod's runAsGroup) can `send_to(path)`, and
/// spawn the recv loop. Returns the spawned task's `JoinHandle` (so
/// callers can `await` it during graceful shutdown).
///
/// Best-effort `remove_file` before `bind` — the kernel doesn't GC a
/// stale datagram socket inode until the last reference closes, so a
/// crash+restart loop on the ingress side would otherwise hit
/// `EADDRINUSE` until manual cleanup. `NotFound` is ignored (first
/// boot).
pub fn spawn_global_rps_reader(
    socket_path: PathBuf,
    cache: SharedGlobalRpsCache,
    shutdown: CancellationToken,
) -> Result<tokio::task::JoinHandle<()>> {
    // Best-effort unlink of stale inode. Matches the comment on
    // EADDRINUSE: the kernel holds the old inode until the last fd
    // closes, so a crash+restart can land the new bind on EADDRINUSE
    // without this. NotFound is the common case on first boot.
    match std::fs::remove_file(&socket_path) {
        Ok(()) => debug!(
            path = %socket_path.display(),
            "global_rps: removed stale socket from previous boot"
        ),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => {
            // Don't bail — the bind below will surface a clearer
            // error if the path is genuinely unusable.
            warn!(
                err = %e,
                path = %socket_path.display(),
                "global_rps: remove_file on stale socket returned non-fatal error"
            );
        }
    }

    let sock = UnixDatagram::bind(&socket_path)
        .with_context(|| format!("bind global-rps UDS datagram socket at {:?}", socket_path))?;

    // chmod 0660 — the sidecar runs in the same pod (same uid, same
    // supplementary group) and writes via `send_to(path)`. Without
    // group-write, the sidecar's `send_to` returns EACCES and the
    // ingress sees nothing. 0660 (not 0666) so an unrelated process
    // on the host can't flood the socket.
    let perms = std::fs::Permissions::from_mode(0o660);
    if let Err(e) = std::fs::set_permissions(&socket_path, perms) {
        warn!(
            err = %e,
            path = %socket_path.display(),
            "global_rps: chmod 0660 on UDS socket failed; sidecar may not be able to send_to"
        );
    }

    let handle = tokio::spawn(async move {
        run_recv_loop(sock, cache, shutdown).await;
    });
    Ok(handle)
}

/// The recv loop. Single `recv` at a time (the cache is the latest
/// datagram — out-of-order is fine because each datagram is fully
/// self-describing). Shutdown cancels the loop immediately.
///
/// Parse errors are logged and dropped — never panic or tear down
/// the reader. A single malformed datagram from the sidecar (schema
/// drift, partial write on a crashed sidecar) must not poison the
/// whole cross-replica feature.
async fn run_recv_loop(
    sock: UnixDatagram,
    cache: SharedGlobalRpsCache,
    shutdown: CancellationToken,
) {
    // 8 KiB is plenty — DatagramPayload serializes to ~150 bytes.
    // Linux's `SO_SNDBUF` on Unix datagrams defaults to ~200 KiB so
    // a single recv per datagram is fine; no risk of truncation as
    // long as we keep the buffer > max packet size.
    let mut buf = vec![0u8; 8192];

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => {
                debug!("global_rps: shutdown received, dropping recv loop");
                return;
            }
            res = sock.recv(&mut buf) => {
                let n = match res {
                    Ok(n) => n,
                    Err(e) => {
                        warn!(err = %e, "global_rps: recv error; continuing");
                        continue;
                    }
                };
                match serde_json::from_slice::<DatagramPayload>(&buf[..n]) {
                    Ok(payload) => {
                        let entry = GlobalRpsEntry {
                            local_cap: payload.local_cap,
                            configured: payload.configured,
                            platform_total: payload.platform_total,
                            replicas_seen: payload.replicas_seen,
                            received_at: Instant::now(),
                        };
                        cache.write().await.update(entry);
                    }
                    Err(e) => {
                        warn!(
                            err = %e,
                            bytes = n,
                            "global_rps: datagram parse failed; dropping"
                        );
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(
        cap: Option<u32>,
        configured: u32,
        total: u64,
        replicas: u32,
        age: Duration,
    ) -> GlobalRpsEntry {
        GlobalRpsEntry {
            local_cap: cap,
            configured,
            platform_total: total,
            replicas_seen: replicas,
            received_at: Instant::now() - age,
        }
    }

    #[test]
    fn empty_cache_returns_none() {
        let cache = GlobalRpsCache::default();
        assert_eq!(cache.current_local_cap(Duration::from_secs(1)), None);
    }

    #[test]
    fn fresh_cap_returns_value() {
        let mut cache = GlobalRpsCache::default();
        cache.update(entry(Some(7_500), 10_000, 30_000, 3, Duration::ZERO));
        assert_eq!(cache.current_local_cap(Duration::from_secs(2)), Some(7_500));
    }

    #[test]
    fn none_cap_is_fail_closed() {
        // Sidecar says "no traffic seen in window" → local_cap=None →
        // ingress must NOT emit the global route. Pin so a future
        // refactor that "unwraps" the Option doesn't accidentally
        // turn fail-closed into "render at 0 rps" or panic.
        let mut cache = GlobalRpsCache::default();
        cache.update(entry(None, 10_000, 0, 0, Duration::ZERO));
        assert_eq!(cache.current_local_cap(Duration::from_secs(2)), None);
    }

    #[test]
    fn stale_entry_returns_none() {
        // received_at older than `stale_after` → fail-closed. This is
        // the recovery path: sidecar crashes → datagrams stop
        // arriving → after 2 × tick_interval the ingress drops the
        // route, freeing the platform from the last-known cap.
        let mut cache = GlobalRpsCache::default();
        cache.update(entry(
            Some(7_500),
            10_000,
            30_000,
            3,
            Duration::from_secs(10),
        ));
        assert_eq!(cache.current_local_cap(Duration::from_secs(2)), None);
    }

    #[test]
    fn within_stale_window_returns_value() {
        // Slightly aged entry that's still inside the window — must
        // NOT trip the staleness guard.
        let mut cache = GlobalRpsCache::default();
        cache.update(entry(
            Some(7_500),
            10_000,
            30_000,
            3,
            Duration::from_millis(500),
        ));
        assert_eq!(cache.current_local_cap(Duration::from_secs(2)), Some(7_500));
    }

    #[tokio::test]
    async fn deserializes_datagram_payload_shape() {
        // Pin the wire shape — the sidecar's `DatagramPayload::from(&Snapshot)`
        // writes these field names; ingress must read them exactly. A
        // rename on either side silently breaks the cross-replica route
        // (cache stays empty; renderer emits no route).
        let json = r#"{"ts":12345,"configured":10000,"platform_total":30000,"this_replica":10000,"replicas_seen":3,"local_cap":1}"#;
        let p: DatagramPayload = serde_json::from_str(json).expect("parse");
        assert_eq!(p.configured, 10_000);
        assert_eq!(p.platform_total, 30_000);
        assert_eq!(p.replicas_seen, 3);
        assert_eq!(p.local_cap, Some(1));
    }

    #[tokio::test]
    async fn deserializes_datagram_with_null_local_cap() {
        // Fail-closed wire shape: local_cap = null (operator disabled).
        let json = r#"{"configured":0,"platform_total":30000,"replicas_seen":3,"local_cap":null}"#;
        let p: DatagramPayload = serde_json::from_str(json).expect("parse");
        assert_eq!(p.local_cap, None);
    }

    #[tokio::test]
    async fn deserializes_datagram_with_missing_local_cap() {
        // Forward-compat: a missing `local_cap` key (e.g. sidecar code
        // regresses to not writing the field) must default to None,
        // not panic on parse. This is the same fail-closed semantic as
        // explicit null.
        let json = r#"{"configured":10000,"platform_total":500,"replicas_seen":1}"#;
        let p: DatagramPayload = serde_json::from_str(json).expect("parse");
        assert_eq!(p.local_cap, None);
    }

    #[tokio::test]
    async fn reader_binds_socket_and_chmods() {
        // End-to-end UDS roundtrip (the production semantic): the
        // reader binds, an unbound sender `send_to`s, the cache
        // receives the parsed entry. Mirrors the sidecar's
        // `write_to_path_delivers_datagram_to_bound_receiver` test
        // (expose.rs:247) but from the RECEIVER side.
        let dir = tempfile::TempDir::new().expect("tempdir");
        let path = dir.path().join("global-rps.sock");
        let cache: SharedGlobalRpsCache = Default::default();
        let shutdown = CancellationToken::new();

        let handle = spawn_global_rps_reader(path.clone(), cache.clone(), shutdown.clone())
            .expect("spawn reader");

        // Give the recv loop a beat to register on the socket.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Sender: unbound + send_to (no connect — matches sidecar).
        let sender = UnixDatagram::unbound().expect("unbound sender");
        let json =
            r#"{"configured":10000,"platform_total":25000,"replicas_seen":2,"local_cap":5000}"#;
        sender
            .send_to(json.as_bytes(), &path)
            .await
            .expect("send_to");

        // Poll up to 2s for the cache to reflect the datagram.
        let mut found = false;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let r = cache.read().await;
            if let Some(e) = r.latest() {
                if e.local_cap == Some(5000) {
                    assert_eq!(e.configured, 10_000);
                    assert_eq!(e.platform_total, 25_000);
                    assert_eq!(e.replicas_seen, 2);
                    found = true;
                    break;
                }
            }
        }
        assert!(found, "reader did not pick up the datagram within 2s");

        // Verify chmod 0660 was applied (best-effort; we logged +
        // continued on error, but the test fixture is a fresh dir so
        // it must have succeeded).
        let meta = std::fs::metadata(&path).expect("stat");
        let mode = meta.permissions().mode() & 0o777;
        assert_eq!(mode, 0o660, "UDS socket mode was {:o}", mode);

        shutdown.cancel();
        let _ = handle.await;
    }

    #[tokio::test]
    async fn reader_tolerates_malformed_datagram() {
        // A sidecar crash that writes a partial / non-JSON datagram
        // must NOT poison the recv loop. Subsequent well-formed
        // datagrams must still land in the cache.
        let dir = tempfile::TempDir::new().expect("tempdir");
        let path = dir.path().join("global-rps.sock");
        let cache: SharedGlobalRpsCache = Default::default();
        let shutdown = CancellationToken::new();
        let handle = spawn_global_rps_reader(path.clone(), cache.clone(), shutdown.clone())
            .expect("spawn reader");

        tokio::time::sleep(Duration::from_millis(50)).await;

        let sender = UnixDatagram::unbound().expect("unbound sender");
        sender
            .send_to(b"not json {", &path)
            .await
            .expect("send garbage");
        sender
            .send_to(
                br#"{"configured":10000,"platform_total":5000,"replicas_seen":1,"local_cap":5000}"#,
                &path,
            )
            .await
            .expect("send good");

        let mut found = false;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let r = cache.read().await;
            if r.latest().and_then(|e| e.local_cap) == Some(5000) {
                found = true;
                break;
            }
        }
        assert!(
            found,
            "reader should ignore malformed datagrams and still land well-formed ones"
        );

        shutdown.cancel();
        let _ = handle.await;
    }
}
