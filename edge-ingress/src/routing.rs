//! In-memory routing table: `(tenant_id, app_name, deployment_id)` → RouteEntry.
//!
//! The table is updated on every heartbeat and pruned periodically to drop
//! entries that haven't been refreshed within `stale_after`. The composite key
//! allows multiple deployment IDs for the same `(tenant, app)` to coexist,
//! enabling canary/blue-green deployments where v1 and v2 run concurrently.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;

#[derive(Debug, Clone, PartialEq)]
pub struct RouteEntry {
    pub tenant_id: String,
    pub app_name: String,
    /// Deployment identifier. `None` means "legacy single-deployment" and
    /// is used for backward-compatible entries where the heartbeat did not
    /// carry a deployment_id in the key.
    pub deployment_id: Option<String>,
    /// Traffic weight 0-100 for this deployment. The ingress rendering layer
    /// uses this to build weighted upstream pools. Default is 100 when only
    /// a single deployment is active (legacy behavior).
    pub weight: u8,
    pub worker_addr: String,
    pub port: u16,
    pub last_seen: Instant,
}

/// Composite key. Two tenants may share an `app_name`; their routes must
/// not collide. The `deployment_id` field distinguishes multiple concurrent
/// deployments of the same app for the same tenant (canary/blue-green).
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct AppKey {
    pub tenant_id: String,
    pub app_name: String,
    pub deployment_id: Option<String>,
}

impl AppKey {
    pub fn new(tenant_id: impl Into<String>, app_name: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            app_name: app_name.into(),
            deployment_id: None,
        }
    }

    pub fn with_deployment(
        tenant_id: impl Into<String>,
        app_name: impl Into<String>,
        deployment_id: impl Into<String>,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            app_name: app_name.into(),
            deployment_id: Some(deployment_id.into()),
        }
    }
}

pub struct RoutingTable {
    inner: RwLock<HashMap<AppKey, RouteEntry>>,
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
    /// Upsert a route under `(tenant_id, app_name, deployment_id)`. Only
    /// `status == "running"` apps are routable; other statuses remove the
    /// entry under this key.
    pub async fn upsert(
        &self,
        tenant_id: &str,
        app_name: &str,
        deployment_id: Option<&str>,
        weight: u8,
        worker_addr: &str,
        port: u16,
        status: &str,
    ) {
        let key = match deployment_id {
            Some(id) => AppKey::with_deployment(tenant_id, app_name, id),
            None => AppKey::new(tenant_id, app_name),
        };
        if status != "running" {
            self.remove(&key).await;
            return;
        }
        let mut inner = self.inner.write().await;
        inner.insert(
            key.clone(),
            RouteEntry {
                tenant_id: tenant_id.to_string(),
                app_name: app_name.to_string(),
                deployment_id: deployment_id.map(|s| s.to_string()),
                weight,
                worker_addr: worker_addr.to_string(),
                port,
                last_seen: Instant::now(),
            },
        );
    }

    /// Remove the entry under a single `(tenant_id, app_name, deployment_id)` key.
    pub async fn remove(&self, key: &AppKey) {
        let mut inner = self.inner.write().await;
        inner.remove(key);
    }

    /// Drop entries whose `last_seen` is older than `older_than`. Returns
    /// the list of removed keys so the caller can log them.
    pub async fn remove_stale(&self, older_than: Duration) -> Vec<AppKey> {
        let mut inner = self.inner.write().await;
        let cutoff = Instant::now() - older_than;
        let stale: Vec<AppKey> = inner
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

    /// Number of currently routable app instances.
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
        t.upsert("t_a", "api", Some("d_v1"), 100, "1.2.3.4", 8081, "running")
            .await;
        let snap = t.snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].app_name, "api");
        assert_eq!(snap[0].tenant_id, "t_a");
        assert_eq!(snap[0].worker_addr, "1.2.3.4");
        assert_eq!(snap[0].port, 8081);
        assert_eq!(snap[0].deployment_id.as_deref(), Some("d_v1"));
        assert_eq!(snap[0].weight, 100);
    }

    #[tokio::test]
    async fn upsert_overwrites_existing_same_deployment() {
        let t = RoutingTable::new();
        t.upsert("t_a", "api", Some("d_v1"), 100, "1.2.3.4", 8081, "running")
            .await;
        // Same deployment on a different worker replaces the prior entry.
        t.upsert("t_a", "api", Some("d_v1"), 100, "5.6.7.8", 8082, "running")
            .await;
        let snap = t.snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].worker_addr, "5.6.7.8");
        assert_eq!(snap[0].port, 8082);
    }

    /// Two different deployment IDs for the same (tenant, app) must coexist
    /// — the core of canary support.
    #[tokio::test]
    async fn canary_two_deployments_coexist() {
        let t = RoutingTable::new();
        t.upsert("t_a", "api", Some("d_v1"), 95, "1.2.3.4", 8081, "running")
            .await;
        t.upsert("t_a", "api", Some("d_v2"), 5, "1.2.3.5", 8082, "running")
            .await;
        let snap = t.snapshot().await;
        assert_eq!(snap.len(), 2);

        let by_dep: std::collections::HashMap<&str, &RouteEntry> =
            snap.iter()
                .flat_map(|e| e.deployment_id.as_deref().map(|d| (d, e)))
                .collect();
        assert_eq!(by_dep["d_v1"].worker_addr, "1.2.3.4");
        assert_eq!(by_dep["d_v1"].weight, 95);
        assert_eq!(by_dep["d_v2"].worker_addr, "1.2.3.5");
        assert_eq!(by_dep["d_v2"].weight, 5);
    }

    /// The composite key: two tenants with the same `app_name` keep their
    /// routes independent. Without the composite key, one tenant's entry
    /// would silently clobber the other.
    #[tokio::test]
    async fn cross_tenant_apps_with_same_name_dont_collide() {
        let t = RoutingTable::new();
        t.upsert("t_a", "api", Some("d_v1"), 100, "1.2.3.4", 8081, "running")
            .await;
        t.upsert("t_b", "api", Some("d_v1"), 100, "5.6.7.8", 9000, "running")
            .await;

        let snap = t.snapshot().await;
        assert_eq!(snap.len(), 2, "both tenants' routes must coexist");

        let by_tenant: std::collections::HashMap<&str, &RouteEntry> =
            snap.iter().map(|e| (e.tenant_id.as_str(), e)).collect();
        assert_eq!(by_tenant["t_a"].worker_addr, "1.2.3.4");
        assert_eq!(by_tenant["t_a"].port, 8081);
        assert_eq!(by_tenant["t_b"].worker_addr, "5.6.7.8");
        assert_eq!(by_tenant["t_b"].port, 9000);
    }

    /// Non-running status removes the entry for that specific deployment.
    #[tokio::test]
    async fn non_running_status_removes_entry() {
        let t = RoutingTable::new();
        t.upsert("t_a", "api", Some("d_v1"), 100, "1.2.3.4", 8081, "running")
            .await;
        t.upsert("t_a", "api", Some("d_v1"), 100, "1.2.3.4", 8081, "crashed")
            .await;
        assert_eq!(t.len().await, 0);
    }

    /// Canarying: stopping d_v1 leaves d_v2 untouched.
    #[tokio::test]
    async fn stopping_one_deployment_keeps_other() {
        let t = RoutingTable::new();
        t.upsert("t_a", "api", Some("d_v1"), 95, "1.2.3.4", 8081, "running")
            .await;
        t.upsert("t_a", "api", Some("d_v2"), 5, "1.2.3.5", 8082, "running")
            .await;

        t.upsert("t_a", "api", Some("d_v1"), 95, "1.2.3.4", 8081, "stopping")
            .await;
        assert_eq!(t.len().await, 1);

        let snap = t.snapshot().await;
        assert_eq!(snap[0].deployment_id.as_deref(), Some("d_v2"));
    }

    #[tokio::test]
    async fn remove_stale_drops_old_entries() {
        let t = RoutingTable::new();
        t.upsert("t_a", "api", Some("d_v1"), 100, "1.2.3.4", 8081, "running")
            .await;
        t.upsert("t_a", "web", Some("d_v1"), 100, "1.2.3.4", 8082, "running")
            .await;

        // Wait long enough for the first entry to age out. The second
        // entry will be re-touched right before the check so only the
        // first should be considered stale.
        tokio::time::sleep(Duration::from_millis(20)).await;
        t.upsert("t_a", "web", Some("d_v1"), 100, "1.2.3.4", 8082, "running")
            .await;

        let removed = t.remove_stale(Duration::from_millis(10)).await;
        assert_eq!(
            removed,
            vec![AppKey::with_deployment("t_a", "api", "d_v1")],
            "only the (t_a, api, d_v1) key should be removed"
        );
        assert_eq!(t.len().await, 1);
    }

    /// Legacy entry (no deployment_id) still works.
    #[tokio::test]
    async fn legacy_entry_without_deployment_id() {
        let t = RoutingTable::new();
        t.upsert("t_a", "api", None, 100, "1.2.3.4", 8081, "running")
            .await;
        let snap = t.snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].deployment_id, None);
        assert_eq!(snap[0].weight, 100);
    }
}
