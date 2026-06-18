//! In-memory routing table: app_name → {worker_addr, port}.
//!
//! The table is updated on every heartbeat and pruned periodically to drop
//! entries that haven't been refreshed within `stale_after`. v1 keeps a single
//! entry per app_name (the freshest one seen). Multi-worker-per-app is a v2
//! concern (#85 autoscale) and will turn this into `Vec<RouteEntry>` per key.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;

#[derive(Debug, Clone, PartialEq)]
pub struct RouteEntry {
    pub tenant_id: String,
    pub app_name: String,
    pub worker_addr: String,
    pub port: u16,
    pub last_seen: Instant,
}

pub struct RoutingTable {
    inner: RwLock<HashMap<String, RouteEntry>>,
}

impl RoutingTable {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for RoutingTable {
    fn default() -> Self {
        Self::new()
    }
}

impl RoutingTable {
    /// Upsert a route. Per `app_name`, the freshest entry wins — if a stale
    /// entry exists, it's overwritten by the new one. Only `status == "running"`
    /// apps are routable; other statuses are dropped.
    pub async fn upsert(
        &self,
        tenant_id: &str,
        app_name: &str,
        worker_addr: &str,
        port: u16,
        status: &str,
    ) {
        if status != "running" {
            self.remove(app_name).await;
            return;
        }
        let mut inner = self.inner.write().await;
        inner.insert(
            app_name.to_string(),
            RouteEntry {
                tenant_id: tenant_id.to_string(),
                app_name: app_name.to_string(),
                worker_addr: worker_addr.to_string(),
                port,
                last_seen: Instant::now(),
            },
        );
    }

    /// Remove a single app_name (called when a heartbeat reports it
    /// non-running or stopped).
    pub async fn remove(&self, app_name: &str) {
        let mut inner = self.inner.write().await;
        inner.remove(app_name);
    }

    /// Drop entries whose `last_seen` is older than `older_than`. Returns the
    /// list of removed app_names so the caller can log them.
    pub async fn remove_stale(&self, older_than: Duration) -> Vec<String> {
        let mut inner = self.inner.write().await;
        let cutoff = Instant::now() - older_than;
        let stale: Vec<String> = inner
            .iter()
            .filter(|(_, e)| e.last_seen < cutoff)
            .map(|(k, _)| k.clone())
            .collect();
        for k in &stale {
            inner.remove(k);
        }
        stale
    }

    /// Snapshot of all current routes. Order is unspecified.
    pub async fn snapshot(&self) -> Vec<RouteEntry> {
        let inner = self.inner.read().await;
        inner.values().cloned().collect()
    }

    /// Number of currently routable apps.
    #[allow(dead_code, clippy::len_without_is_empty)]
    pub async fn len(&self) -> usize {
        self.inner.read().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn upsert_creates_entry() {
        let t = RoutingTable::new();
        t.upsert("t_a", "api", "1.2.3.4", 8081, "running").await;
        let snap = t.snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].app_name, "api");
        assert_eq!(snap[0].tenant_id, "t_a");
        assert_eq!(snap[0].worker_addr, "1.2.3.4");
        assert_eq!(snap[0].port, 8081);
    }

    #[tokio::test]
    async fn upsert_overwrites_existing_with_freshest() {
        let t = RoutingTable::new();
        t.upsert("t_a", "api", "1.2.3.4", 8081, "running").await;
        // A heartbeat for the same app from a different worker (e.g. after a
        // worker restart and a re-assignment) replaces the prior entry.
        t.upsert("t_a", "api", "5.6.7.8", 8082, "running").await;
        let snap = t.snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].worker_addr, "5.6.7.8");
        assert_eq!(snap[0].port, 8082);
    }

    #[tokio::test]
    async fn non_running_status_removes_entry() {
        let t = RoutingTable::new();
        t.upsert("t_a", "api", "1.2.3.4", 8081, "running").await;
        t.upsert("t_a", "api", "1.2.3.4", 8081, "crashed").await;
        assert_eq!(t.len().await, 0);
    }

    #[tokio::test]
    async fn remove_stale_drops_old_entries() {
        let t = RoutingTable::new();
        t.upsert("t_a", "api", "1.2.3.4", 8081, "running").await;
        t.upsert("t_a", "web", "1.2.3.4", 8082, "running").await;

        // Wait long enough for the first entry to age out. The second
        // entry will be re-touched right before the check so only the
        // first should be considered stale.
        tokio::time::sleep(Duration::from_millis(20)).await;
        t.upsert("t_a", "web", "1.2.3.4", 8082, "running").await;

        let removed = t.remove_stale(Duration::from_millis(10)).await;
        assert_eq!(removed, vec!["api".to_string()]);
        assert_eq!(t.len().await, 1);
    }
}
