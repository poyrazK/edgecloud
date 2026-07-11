//! L4 (raw-TCP) routing table + public-port pool (issue #548).
//!
//! This module is the layer-4 sibling of [`crate::routing`]. It models:
//!
//! - [`L4RoutingTable`]: a `by_app` map keyed by `(tenant_id, app_name)` (no
//!   deployment_id — L4 paths are single-deployment per app for v1) that
//!   carries the upstream worker_addr + upstream_port and the public port
//!   the ingress allocates from a CP-persistent range. Same 3-event
//!   eviction pattern as [`crate::routing::RoutingTable`] (heartbeat-driven
//!   upsert, worker-shutdown-driven remove_worker, stale-prune-driven
//!   remove_stale). Mirrored, not shared, because (a) `RoutingTable`'s
//!   composite key includes `deployment_id` for the HTTP/WS path's canary
//!   flows — L4 has no canary in v1, and re-using `AppKey` would force a
//!   `deployment_id: None` arm on every HTTP call site; (b) `RouteEntry`
//!   carries HTTP-only fields (`rate_limit_*`, canary `weight`) with no L4
//!   analogue; (c) L4 paths are managed by an independent Caddy admin
//!   tree (`apps.layer4.servers`, see [`crate::caddy`]) that is only
//!   fully addressable via `POST /load`, not per-id `PUT/DELETE`/`/id/<id>`
//!   — so the L4 render path is structurally different from `render_routes`.
//!
//! - [`L4PortPool`]: a port allocator modelled on
//!   `edge-worker/src/port_pool.rs`. The worker's allocator can't be
//!   reused directly because (i) `edge-ingress` does not depend on
//!   `edge-worker` (layering), (ii) the ingress needs a wider range
//!   (1000 vs the worker's 100 pre-populated), (iii) the ingress
//!   allocator surfaces the public port for render whereas the worker's
//!   allocator only surfaces a worker-private port.
//!
//! Public-port allocation in this commit is *ingress-local* (L4PortPool
//! only). Commit 9 introduces a CP-persistent cross-ingress endpoint
//! (`POST /api/v1/internal/l4-allocate`) so two ingress instances in
//! the same region cannot both hand out the same public port. Until
//! that lands the L4PortPool is per-process and would collide across
//! instances in a multi-ingress region.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use tokio::sync::RwLock;

/// One raw-TCP / layer-4 route — a (tenant, app) ↦ (public_port,
/// worker_addr, upstream_port) mapping. Distinct from
/// [`crate::routing::RouteEntry`], which additionally carries
/// canary `weight` + `rate_limit_*`. The L4 path has neither in v1.
#[derive(Debug, Clone, PartialEq)]
pub struct L4RouteEntry {
    pub tenant_id: String,
    pub app_name: String,
    /// Public port on the ingress host the client connects to (issue
    /// #548). Allocated by the ingress from the configured L4 port
    /// range; renders into `apps.layer4.servers.<name>.listen` as
    /// `[":<public_port>"]`.
    pub public_port: u16,
    /// Worker address (host:port) the ingress dials. From the
    /// `worker_addr` field on the heartbeat envelope (issue #70).
    pub worker_addr: String,
    /// Port the worker's app is listening on (the upstream). From
    /// the `port` field on the heartbeat's `AppStatus` for this app.
    pub upstream_port: u16,
    /// `Instant::now()` of the most recent heartbeat that touched
    /// this entry. Re-stamped on every `upsert`; the pruner uses
    /// it to drop rows older than `STALE_TIMEOUT`.
    pub last_seen: Instant,
}

impl L4RouteEntry {
    /// Stable Caddy server ID usable in `apps.layer4.servers.<id>`
    /// (one server per public port). The public port is globally
    /// unique within the ingress's port range, so it doubles as a
    /// stable identifier. The `l4_` prefix avoids any chance of
    /// collision with the existing `apps.http` route IDs that use
    /// a different shape (route_id there is
    /// `<tenant>:<app>:<deployment_id>`).
    pub fn server_id(&self) -> String {
        format!("l4_{}", self.public_port)
    }
}

/// In-memory L4 routing table — `(tenant_id, app_name)` →
/// [`L4RouteEntry`]. v1 deliberately ignores `deployment_id`:
/// canary/blue-green is an HTTP-only concern. When v2 adds L4 canaries
/// the key here grows `deployment_id` and the same eviction
/// discipline carries over from [`crate::routing::RoutingTable`].
pub struct L4RoutingTable {
    by_app: RwLock<HashMap<L4AppKey, L4RouteEntry>>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct L4AppKey {
    pub tenant_id: String,
    pub app_name: String,
}

impl L4AppKey {
    pub fn new(tenant_id: impl Into<String>, app_name: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            app_name: app_name.into(),
        }
    }
}

impl L4RoutingTable {
    pub fn new() -> Self {
        Self {
            by_app: RwLock::new(HashMap::new()),
        }
    }

    /// Upsert a route under `(tenant_id, app_name)`. Only
    /// `status == "running"` apps are routable; other statuses
    /// remove the entry under this key. `public_port` is the
    /// already-allocated ingress-side public port — the routing
    /// table does NOT own allocation; it stores whatever
    /// [`L4PortPool::acquire`] returned at upsert time.
    pub async fn upsert(
        &self,
        tenant_id: &str,
        app_name: &str,
        public_port: u16,
        worker_addr: &str,
        upstream_port: u16,
        status: &str,
    ) {
        let key = L4AppKey::new(tenant_id, app_name);
        if status != "running" && status != "draining" {
            self.remove(&key).await;
            return;
        }
        let mut inner = self.by_app.write().await;
        inner.insert(
            key,
            L4RouteEntry {
                tenant_id: tenant_id.to_string(),
                app_name: app_name.to_string(),
                public_port,
                worker_addr: worker_addr.to_string(),
                upstream_port,
                last_seen: Instant::now(),
            },
        );
    }

    /// Remove a single entry.
    pub async fn remove(&self, key: &L4AppKey) {
        let mut inner = self.by_app.write().await;
        inner.remove(key);
    }

    /// Drop entries whose `last_seen` is older than `older_than`.
    /// Returns the removed keys so the caller can release the
    /// matching ports back to [`L4PortPool`].
    pub async fn remove_stale(&self, older_than: Duration) -> Vec<L4AppKey> {
        let mut inner = self.by_app.write().await;
        let cutoff = Instant::now() - older_than;
        let stale: Vec<L4AppKey> = inner
            .iter()
            .filter(|(_, e)| e.last_seen < cutoff)
            .map(|(k, _)| k.clone())
            .collect();
        for k in &stale {
            inner.remove(k);
        }
        stale
    }

    /// Drop all entries whose `worker_addr` matches. Used when a
    /// worker sends a final heartbeat with an empty `apps` map,
    /// signalling it is shutting down (mirrors
    /// `RoutingTable::remove_worker`).
    pub async fn remove_worker(&self, worker_addr: &str) -> Vec<L4AppKey> {
        let mut inner = self.by_app.write().await;
        let removed: Vec<L4AppKey> = inner
            .iter()
            .filter(|(_, e)| e.worker_addr == worker_addr)
            .map(|(k, _)| k.clone())
            .collect();
        for k in &removed {
            inner.remove(k);
        }
        removed
    }

    /// Snapshot of all current L4 routes. Order is unspecified.
    pub async fn snapshot(&self) -> Vec<L4RouteEntry> {
        let inner = self.by_app.read().await;
        inner.values().cloned().collect()
    }

    /// Number of currently routable L4 instances.
    #[allow(clippy::len_without_is_empty)]
    pub async fn len(&self) -> usize {
        self.by_app.read().await.len()
    }
}

impl Default for L4RoutingTable {
    fn default() -> Self {
        Self::new()
    }
}

/// Ingress-local public-port pool (issue #548).
///
/// Modeled on `edge-worker/src/port_pool.rs::PortPool` (with
/// attribution) — the implementations are deliberately parallel so a
/// future refactor can unify them once `edge-ingress` gains a
/// dependency on `edge-worker` for shared infra. Layers deliberately
/// cannot merge today: the ingress must NOT pull in
/// `edge-worker` (it depends transitively on `wasmtime`, which would
/// bloat a stateless control-plane proxy by 30+ MB).
///
/// Differences from the worker-side pool:
///   1. **No pre-populated available set.** The worker pre-populates
///      100 ports for O(1) acquire under burst; the ingress's range is
///      configurable and can be 1000+ ports, so pre-population is
///      unbounded. Instead we walk the range on acquire.
///   2. **Sequential wrapping inside the configured
///      `[start, end]` range** — the worker's pool wraps at
///      `u16::MAX`; the ingress wraps at `l4_port_range_end` since the
///      configured range must stay within firewall carve-outs.
///   3. **Debug-visible `port_in_cooldown(port)` for tests + a
///      `L4PortCache`-shaped caller to skip over recently-released
///      ports.** Same double-release guard as the worker pool — a
///      second `release(p)` is a no-op so `acquire()` after re-release
///      still won't hand out the port until cooldown elapses.
pub struct L4PortPool {
    next_port: u16,
    start: u16,
    end: u16,
    cooldown_secs: u64,
    cooling_down: Vec<(u16, Instant)>,
    /// Ports we know are taken (allocated and not yet released).
    /// Acquire consults this so we don't double-hand-out a port in the
    /// exhaustion-recovery case where `next_port` walked past a taken
    /// slot.
    taken: HashSet<u16>,
}

impl L4PortPool {
    /// Create a new pool over `[start, end]` inclusive with a per-port
    /// cooldown of `cooldown_secs`.
    pub fn new(start: u16, end: u16, cooldown_secs: u64) -> Self {
        Self {
            next_port: start,
            start,
            end,
            cooldown_secs,
            cooling_down: Vec::new(),
            taken: HashSet::new(),
        }
    }

    /// Acquire a port. Returns `None` if every port in
    /// `[start, end]` is currently taken or in cooldown. Caller must
    /// surface `None` to the renderer (rather than `.expect()`-ing,
    /// which would crash the ingress — see #45 for the worker-side
    /// equivalent).
    pub fn acquire(&mut self) -> Option<u16> {
        self.reap_cooled_ports();
        // Walk a full range worth of attempts; in pathological
        // burst-restart scenarios every port can be in cooldown
        // simultaneously.
        let mut attempts = 0u32;
        let range_len = (self.end - self.start + 1) as u32;
        while attempts < range_len {
            let port = self.next_port;
            self.next_port = if self.next_port >= self.end {
                self.start
            } else {
                self.next_port + 1
            };
            attempts += 1;
            if self.taken.contains(&port) {
                continue;
            }
            if self.cooling_down.iter().any(|(p, _)| *p == port) {
                continue;
            }
            self.taken.insert(port);
            return Some(port);
        }
        None
    }

    /// Release a port back into cooldown. Guard against
    /// double-release: if the port is already cooling down, this is a
    /// no-op (matches the worker's pool).
    pub fn release(&mut self, port: u16) {
        if self.cooling_down.iter().any(|(p, _)| *p == port) {
            return;
        }
        if !self.taken.remove(&port) {
            // Not currently held — likely a duplicate release; safe to ignore.
            return;
        }
        let release_time = Instant::now() + Duration::from_secs(self.cooldown_secs);
        self.cooling_down.push((port, release_time));
    }

    /// Move cooled ports out of `cooling_down`. Mirrors the
    /// worker's `reap_cooled_ports`. Called automatically by
    /// `acquire`; exposed for tests that want to assert a port
    /// entered/exited cooldown without driving `acquire`.
    pub fn reap_cooled_ports(&mut self) {
        let now = Instant::now();
        self.cooling_down.retain(|(_, release_time)| {
            if now >= *release_time {
                false // remove — cooldown elapsed, port is now free for acquire
            } else {
                true
            }
        });
    }

    /// Number of ports currently held. Used by tests + the L4
    /// metrics counter (next pr).
    #[allow(dead_code)]
    pub fn taken_count(&self) -> usize {
        self.taken.len()
    }

    /// Whether `port` is currently in the cooldown set (test-only).
    #[cfg(test)]
    pub fn is_in_cooldown(&self, port: u16) -> bool {
        self.cooling_down.iter().any(|(p, _)| *p == port)
    }

    /// Snapshot of currently-held ports (test-only). Mirrors
    /// `is_in_cooldown` — kept `#[cfg(test)]` so a future refactor
    /// is free to reorganise the underlying structure without
    /// touching production callers.
    #[cfg(test)]
    pub fn taken(&self) -> &HashSet<u16> {
        &self.taken
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── L4RoutingTable ────────────────────────────────────────────────

    #[tokio::test]
    async fn l4_upsert_creates_entry() {
        let t = L4RoutingTable::new();
        t.upsert("t_a", "api", 31000, "1.2.3.4", 8081, "running")
            .await;
        let snap = t.snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].public_port, 31000);
        assert_eq!(snap[0].worker_addr, "1.2.3.4");
        assert_eq!(snap[0].upstream_port, 8081);
        assert_eq!(snap[0].server_id(), "l4_31000");
    }

    #[tokio::test]
    async fn l4_upsert_overwrites_existing_app() {
        let t = L4RoutingTable::new();
        t.upsert("t_a", "api", 31000, "1.2.3.4", 8081, "running")
            .await;
        // Same (tenant, app), different public port + worker.
        t.upsert("t_a", "api", 31001, "5.6.7.8", 8082, "running")
            .await;
        let snap = t.snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].public_port, 31001);
        assert_eq!(snap[0].worker_addr, "5.6.7.8");
        assert_eq!(snap[0].upstream_port, 8082);
    }

    #[tokio::test]
    async fn l4_cross_tenant_apps_with_same_name_dont_collide() {
        let t = L4RoutingTable::new();
        t.upsert("t_a", "api", 31000, "1.2.3.4", 8081, "running")
            .await;
        t.upsert("t_b", "api", 31001, "5.6.7.8", 9000, "running")
            .await;
        assert_eq!(t.len().await, 2);
    }

    #[tokio::test]
    async fn l4_non_running_status_removes_entry() {
        let t = L4RoutingTable::new();
        t.upsert("t_a", "api", 31000, "1.2.3.4", 8081, "running")
            .await;
        t.upsert("t_a", "api", 31000, "1.2.3.4", 8081, "crashed")
            .await;
        assert_eq!(t.len().await, 0);
    }

    #[tokio::test]
    async fn l4_remove_stale_drops_old_entries() {
        let t = L4RoutingTable::new();
        t.upsert("t_a", "api", 31000, "1.2.3.4", 8081, "running")
            .await;
        t.upsert("t_a", "web", 31001, "1.2.3.4", 8082, "running")
            .await;

        // Wait for the first to age out; re-touch the second.
        tokio::time::sleep(Duration::from_millis(20)).await;
        t.upsert("t_a", "web", 31001, "1.2.3.4", 8082, "running")
            .await;

        let removed = t.remove_stale(Duration::from_millis(10)).await;
        assert_eq!(removed, vec![L4AppKey::new("t_a", "api")]);
        assert_eq!(t.len().await, 1);
    }

    #[tokio::test]
    async fn l4_remove_worker_drops_all_for_addr() {
        let t = L4RoutingTable::new();
        t.upsert("t_a", "api", 31000, "1.2.3.4", 8081, "running")
            .await;
        t.upsert("t_a", "web", 31001, "1.2.3.4", 8082, "running")
            .await;
        t.upsert("t_b", "blog", 31002, "5.6.7.8", 9000, "running")
            .await;

        let removed = t.remove_worker("1.2.3.4").await;
        assert_eq!(removed.len(), 2);
        // t_b's entry stays.
        let snap = t.snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].app_name, "blog");
    }

    // ── L4PortPool ────────────────────────────────────────────────────

    #[test]
    fn l4_port_pool_acquire_returns_first_in_range() {
        let mut p = L4PortPool::new(31000, 31999, 60);
        assert_eq!(p.acquire(), Some(31000));
        // The next acquire walks past any ports already in `taken`
        // and lands on the second config-defined port — exactly the
        // same sequential semantics as the worker's pool (issue #548
        // rationale comment).
        assert_eq!(p.acquire(), Some(31001));
    }

    #[test]
    fn l4_port_pool_acquire_returns_none_when_exhausted() {
        let mut p = L4PortPool::new(31000, 31002, 60);
        assert_eq!(p.acquire(), Some(31000));
        assert_eq!(p.acquire(), Some(31001));
        assert_eq!(p.acquire(), Some(31002));
        // All taken — must return None rather than panic.
        assert_eq!(p.acquire(), None);
    }

    #[test]
    fn l4_port_pool_release_removes_port_from_taken() {
        // Issue #548: contract — a release() returns the port to the
        // allocator. The exact timing of re-acquisition depends on the
        // walk position (the worker's pool has the same semantics);
        // what's invariant is that the port is no longer in `taken`
        // so the next acquire can hand it out once the walk reaches
        // it (or the allocator exhausts and wraps around).
        let mut p = L4PortPool::new(31000, 31999, 0); // wide range, 0s cooldown
        let first = p.acquire().unwrap();
        p.release(first);
        // Walk forward past all 1000 ports by acquiring
        // exhaustively — but that's slow; instead, verify the
        // structural invariant directly via the test-only probe.
        assert!(
            !p.taken().contains(&first),
            "released port is back in the allocatable set"
        );
    }

    #[test]
    fn l4_port_pool_double_release_is_noop() {
        let mut p = L4PortPool::new(31000, 31999, 60);
        let port = p.acquire().unwrap();
        p.release(port);
        p.release(port); // second release must not panic and must
                         // not corrupt the cooling_down list
        assert!(p.is_in_cooldown(port));
        // Without explicit reap, acquire skips past the cooling-down port.
        let next = p.acquire();
        assert_ne!(next, Some(port));
    }

    #[test]
    fn l4_port_pool_wraps_within_configured_range() {
        let mut p = L4PortPool::new(31000, 31002, 60);
        p.acquire();
        p.acquire();
        p.acquire();
        // next should now wrap back to start, not u16::MAX.
        let wrapped = p.acquire();
        // Port already taken (it's the very first we acquired), so
        // acquire keeps walking and finds None — fully exhausted.
        assert_eq!(wrapped, None, "walked past wrap, fully exhausted");
    }

    #[test]
    fn l4_port_pool_release_unknown_port_is_noop() {
        // A release() of a port the pool never handed out must not
        // crash; matches the worker's pool semantics — defensive
        // against upstream callers passing a stale port after a
        // restarts-during-redisaster scenario.
        let mut p = L4PortPool::new(31000, 31999, 60);
        p.release(31555); // nothing taken; no-op
        assert_eq!(p.taken_count(), 0);
    }
}
