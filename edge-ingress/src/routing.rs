//! In-memory routing table: `(tenant_id, app_name)` → RouteEntry
//! + `fqdn` → (tenant_id, app_name).
//!
//! The composite key `(tenant_id, app_name)` lets multiple deployment IDs
//! for the same `(tenant, app)` coexist, enabling canary/blue-green
//! deployments where v1 and v2 run concurrently. v1 keeps a single entry
//! per `(tenant, app, deployment_id)` key (the freshest one seen), so two
//! tenants deploying the same app name never collide on the routing
//! table — their routes are kept under separate keys even though they
//! share the same `app_name`.
//!
//! The FQDN map (`by_fqdn`) is an INTERNAL detail of the routing table
//! (issue #83). External callers never see it — they call
//! `register_fqdn` / `deregister_fqdn` / `apply_poll_snapshot` to keep it
//! in sync with the control plane's DB, and the renderer reads
//! `fqdn_snapshot()` to know which FQDNs to render. The `RouteEntry`
//! struct does NOT carry upstream info on the FQDN side — at render time
//! the renderer looks up the upstream via the (tenant, app) key in
//! `by_app`. A FQDN whose app has no upstream is silently skipped.
//!
//! Why two maps instead of one? Because the renderer needs to answer
//! "what upstream should I use for this FQDN?" at every render. If the
//! FQDN map stored upstreams itself, we'd have to walk the FQDN map and
//! update upstream info on every heartbeat — but the heartbeat path
//! doesn't know which FQDNs exist for a given (tenant, app). Composing
//! at render time keeps the heartbeat pipeline simple: upsert the
//! upstream; that's it. The 30s poller handles FQDN membership; the
//! heartbeat handles upstream freshness.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
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
    /// Per-app rate limit in requests per second. `None` = use global default.
    pub rate_limit_rps: Option<u32>,
    /// Per-app burst size. `None` = use global default.
    pub rate_limit_burst: Option<u32>,
    pub last_seen: Instant,
}

/// Binding from a custom FQDN to a (tenant, app) tuple. Carries NO
/// upstream info — the renderer looks that up from `by_app` at render
/// time. This decouples heartbeat-driven upstream churn from the slower
/// FQDN membership changes driven by the 30s control-plane poll.
#[derive(Debug, Clone, PartialEq)]
pub struct FqdnBinding {
    pub tenant_id: String,
    pub app_name: String,
    pub fqdn: String,
}

/// Domain row shape matching the Go control plane's `domain.Domain`.
/// Wire-compatible with the JSON emitted by `GET /api/internal/domains`.
/// `status` and `last_error` / `verified_at` are intentionally NOT in
/// this struct: the ingress only cares about FQDN membership, not
/// per-domain state. The control plane's status transitions are
/// reflected in Caddy's view via the `tls-allowed` endpoint, not via
/// this snapshot.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Domain {
    pub id: String,
    pub tenant_id: String,
    pub app_name: String,
    pub fqdn: String,
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
    by_app: RwLock<HashMap<AppKey, RouteEntry>>,
    by_fqdn: RwLock<HashMap<String, FqdnBinding>>,
}

impl RoutingTable {
    pub fn new() -> Self {
        Self {
            by_app: RwLock::new(HashMap::new()),
            by_fqdn: RwLock::new(HashMap::new()),
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
    #[allow(clippy::too_many_arguments)]
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
        self.upsert_with_rate_limit(
            tenant_id,
            app_name,
            deployment_id,
            weight,
            worker_addr,
            port,
            status,
            None,
            None,
        )
        .await;
    }

    /// Upsert a route with optional per-app rate limits. When
    /// `rate_limit_rps` and `rate_limit_burst` are `None`, the global
    /// defaults from `Config` are used at render time.
    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_with_rate_limit(
        &self,
        tenant_id: &str,
        app_name: &str,
        deployment_id: Option<&str>,
        weight: u8,
        worker_addr: &str,
        port: u16,
        status: &str,
        rate_limit_rps: Option<u32>,
        rate_limit_burst: Option<u32>,
    ) {
        let key = match deployment_id {
            Some(id) => AppKey::with_deployment(tenant_id, app_name, id),
            None => AppKey::new(tenant_id, app_name),
        };
        if status != "running" {
            self.remove(&key).await;
            return;
        }
        let mut inner = self.by_app.write().await;
        inner.insert(
            key.clone(),
            RouteEntry {
                tenant_id: tenant_id.to_string(),
                app_name: app_name.to_string(),
                deployment_id: deployment_id.map(|s| s.to_string()),
                weight,
                worker_addr: worker_addr.to_string(),
                port,
                rate_limit_rps,
                rate_limit_burst,
                last_seen: Instant::now(),
            },
        );
    }

    /// Remove the entry under a single `(tenant_id, app_name, deployment_id)` key.
    pub async fn remove(&self, key: &AppKey) {
        let mut inner = self.by_app.write().await;
        inner.remove(key);
    }

    /// Drop entries whose `last_seen` is older than `older_than`. Returns
    /// the list of removed keys so the caller can log them.
    pub async fn remove_stale(&self, older_than: Duration) -> Vec<AppKey> {
        let mut inner = self.by_app.write().await;
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
        let inner = self.by_app.read().await;
        inner.values().cloned().collect()
    }

    /// Snapshot of all current FQDN bindings. Used by the Caddy renderer
    /// to emit per-FQDN routes (with per-route `tls.on_demand`).
    pub async fn fqdn_snapshot(&self) -> Vec<FqdnBinding> {
        let inner = self.by_fqdn.read().await;
        inner.values().cloned().collect()
    }

    /// Number of currently routable app instances.
    #[allow(dead_code, clippy::len_without_is_empty)]
    pub async fn len(&self) -> usize {
        self.by_app.read().await.len()
    }

    /// Register or overwrite a single FQDN → (tenant, app) binding.
    /// Called by the 30s poller as part of `apply_poll_snapshot` (or
    /// exercised directly by unit tests in this module). No upstream
    /// info stored — see the module-level doc comment for why.
    ///
    /// Visibility is `pub(crate)` rather than `pub` so the production
    /// poller path (`apply_poll_snapshot`) is the only way external
    /// code mutates the FQDN table. A future contributor who calls
    /// `register_fqdn` from outside the poller would break the
    /// diff-based `apply_poll_snapshot` invariant (the next 30s poll
    /// would mark the manually-inserted FQDN as `removed`).
    #[allow(dead_code)] // exercised by unit tests in this module; production goes through apply_poll_snapshot
    pub(crate) async fn register_fqdn(&self, fqdn: &str, tenant_id: &str, app_name: &str) {
        let mut inner = self.by_fqdn.write().await;
        inner.insert(
            fqdn.to_string(),
            FqdnBinding {
                tenant_id: tenant_id.to_string(),
                app_name: app_name.to_string(),
                fqdn: fqdn.to_string(),
            },
        );
    }

    /// Remove a single FQDN binding. Used by `apply_poll_snapshot` when
    /// a previously-seen FQDN is absent from the new poll response.
    #[allow(dead_code)]
    pub(crate) async fn deregister_fqdn(&self, fqdn: &str) {
        let mut inner = self.by_fqdn.write().await;
        inner.remove(fqdn);
    }

    /// Apply a fresh poll snapshot atomically: union of (old, new) minus
    /// (old minus new). Returns `(added, removed)` so the caller can
    /// log the churn without re-snapshotting.
    ///
    /// Single-write-lock invariant (PR #133 review finding #2): the
    /// diff and the mutation are computed and applied under ONE
    /// `by_fqdn.write()` guard. Two concurrent callers — e.g. a poll
    /// interleaving with a `register_fqdn` call, or two overlapping
    /// polls at startup — would otherwise both observe the same
    /// baseline, each compute a diff against that baseline, and on
    /// sequential application lose entries that one of them had
    /// intended to keep. Holding the write lock for the full
    /// diff-then-apply window eliminates the lost-update race.
    ///
    /// The lock is held for O(|new| + |current|) HashMap lookups —
    /// microseconds even at 50,000 entries — well below the 30s poll
    /// cadence, so blocking `fqdn_snapshot()` (the renderer's reader)
    /// for that long is acceptable.
    pub async fn apply_poll_snapshot(&self, domains: Vec<Domain>) -> (Vec<String>, Vec<String>) {
        let new_fqdns: HashMap<String, Domain> =
            domains.into_iter().map(|d| (d.fqdn.clone(), d)).collect();

        // Compute diff AND apply under a single write lock.
        let mut current = self.by_fqdn.write().await;
        let added: Vec<String> = new_fqdns
            .keys()
            .filter(|fqdn| !current.contains_key(*fqdn))
            .cloned()
            .collect();
        let removed: Vec<String> = current
            .keys()
            .filter(|fqdn| !new_fqdns.contains_key(*fqdn))
            .cloned()
            .collect();
        for fqdn in &removed {
            current.remove(fqdn);
        }
        for fqdn in &added {
            if let Some(d) = new_fqdns.get(fqdn) {
                current.insert(
                    fqdn.clone(),
                    FqdnBinding {
                        tenant_id: d.tenant_id.clone(),
                        app_name: d.app_name.clone(),
                        fqdn: fqdn.clone(),
                    },
                );
            }
        }
        (added, removed)
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

        let by_dep: std::collections::HashMap<&str, &RouteEntry> = snap
            .iter()
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

    /// FQDN bindings live in their own map (`by_fqdn`) and do NOT
    /// share keys with `by_app`. Two tenants can have the same FQDN
    /// only in pathological cases (the control plane should reject
    /// duplicates before they reach us), but the table itself is
    /// keyed by FQDN string.
    #[tokio::test]
    async fn fqdn_snapshot_returns_registered_bindings() {
        let t = RoutingTable::new();
        t.register_fqdn("api.acme.com", "t_a", "api").await;
        t.register_fqdn("blog.acme.com", "t_b", "blog").await;
        let snap = t.fqdn_snapshot().await;
        assert_eq!(snap.len(), 2);
        let fqdns: std::collections::HashSet<String> =
            snap.iter().map(|b| b.fqdn.clone()).collect();
        assert!(fqdns.contains("api.acme.com"));
        assert!(fqdns.contains("blog.acme.com"));
    }

    /// `deregister_fqdn` removes a single binding; others are kept.
    #[tokio::test]
    async fn deregister_fqdn_removes_only_target() {
        let t = RoutingTable::new();
        t.register_fqdn("a.example.com", "t_a", "api").await;
        t.register_fqdn("b.example.com", "t_a", "api").await;
        t.deregister_fqdn("a.example.com").await;
        let snap = t.fqdn_snapshot().await;
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].fqdn, "b.example.com");
    }

    /// `apply_poll_snapshot` is the production diff path. First
    /// poll seeds two FQDNs; second poll removes one and adds a new
    /// one; the diff returned is exactly `{added: [new], removed: [gone]}`.
    #[tokio::test]
    async fn apply_poll_snapshot_diff() {
        let t = RoutingTable::new();
        // First poll: two bindings.
        let (added, removed) = t
            .apply_poll_snapshot(vec![
                Domain {
                    id: "d_1".into(),
                    tenant_id: "t_a".into(),
                    app_name: "api".into(),
                    fqdn: "api.acme.com".into(),
                },
                Domain {
                    id: "d_2".into(),
                    tenant_id: "t_b".into(),
                    app_name: "web".into(),
                    fqdn: "web.acme.com".into(),
                },
            ])
            .await;
        assert_eq!(added.len(), 2);
        assert!(removed.is_empty());

        // Second poll: drop d_1, add d_3.
        let (added, removed) = t
            .apply_poll_snapshot(vec![
                Domain {
                    id: "d_2".into(),
                    tenant_id: "t_b".into(),
                    app_name: "web".into(),
                    fqdn: "web.acme.com".into(),
                },
                Domain {
                    id: "d_3".into(),
                    tenant_id: "t_c".into(),
                    app_name: "blog".into(),
                    fqdn: "blog.acme.com".into(),
                },
            ])
            .await;
        assert_eq!(added, vec!["blog.acme.com".to_string()]);
        assert_eq!(removed, vec!["api.acme.com".to_string()]);

        // Final state: 2 bindings, the survivors.
        let snap = t.fqdn_snapshot().await;
        assert_eq!(snap.len(), 2);
    }

    /// Regression test for PR #133 review finding #2: the two-phase
    /// read/drop/write pattern in `apply_poll_snapshot` could lose
    /// entries when two polls ran concurrently and both computed
    /// their diffs against the same baseline. With the single-write-
    /// lock fix, the polls serialize: whichever runs second observes
    /// the first's mutations and computes its diff against the
    /// post-first state. So the final state must be one of the two
    /// valid SEQUENTIAL outcomes — not the lost-update outcome that
    /// the two-phase bug produces.
    ///
    /// Scenario: pre-populate `{a, b, c, d}` via `apply_poll_snapshot`
    /// itself (so the test exercises the same path). Then drive two
    /// concurrent polls:
    ///   - Poll A: keep `{a, b}` (wants to remove `c, d`)
    ///   - Poll B: keep `{b, c}` (wants to remove `a, d`)
    ///
    /// Valid sequential outcomes:
    ///   A then B: A → `{a, b}`, B → `{b, c}`. Final: `{b, c}`.
    ///   B then A: B → `{b, c}`, A → `{a, b}`. Final: `{a, b}`.
    ///
    /// Two-phase-bug outcome (both reads see `{a, b, c, d}`, each
    /// computes its own diff against that baseline, sequential apply):
    ///   A removes `c, d` → `{a, b}`; B removes `a, d` (d no-op) →
    ///   `{b}`. Entries `a` AND `c` are lost — neither valid.
    ///
    /// `tokio::join!` does not guarantee which poll runs first, so
    /// the test asserts the final state is one of the two valid
    /// outcomes — never the lost-update `{b}` outcome.
    #[tokio::test]
    async fn apply_poll_snapshot_concurrent_polls_dont_lose_entries() {
        use std::collections::HashSet;

        let t = std::sync::Arc::new(RoutingTable::new());

        // Seed: {a, b, c, d}
        t.apply_poll_snapshot(vec![
            Domain {
                id: "d_a".into(),
                tenant_id: "t_a".into(),
                app_name: "api".into(),
                fqdn: "a.acme.com".into(),
            },
            Domain {
                id: "d_b".into(),
                tenant_id: "t_a".into(),
                app_name: "api".into(),
                fqdn: "b.acme.com".into(),
            },
            Domain {
                id: "d_c".into(),
                tenant_id: "t_a".into(),
                app_name: "api".into(),
                fqdn: "c.acme.com".into(),
            },
            Domain {
                id: "d_d".into(),
                tenant_id: "t_a".into(),
                app_name: "api".into(),
                fqdn: "d.acme.com".into(),
            },
        ])
        .await;

        let snap_before: HashSet<String> = t
            .fqdn_snapshot()
            .await
            .into_iter()
            .map(|b| b.fqdn)
            .collect();
        assert_eq!(
            snap_before,
            HashSet::from([
                "a.acme.com".to_string(),
                "b.acme.com".to_string(),
                "c.acme.com".to_string(),
                "d.acme.com".to_string(),
            ]),
            "test setup: seed must contain {{a, b, c, d}}"
        );

        // Poll A: keep {a, b}
        let t_a = t.clone();
        let poll_a = tokio::spawn(async move {
            t_a.apply_poll_snapshot(vec![
                Domain {
                    id: "d_a".into(),
                    tenant_id: "t_a".into(),
                    app_name: "api".into(),
                    fqdn: "a.acme.com".into(),
                },
                Domain {
                    id: "d_b".into(),
                    tenant_id: "t_a".into(),
                    app_name: "api".into(),
                    fqdn: "b.acme.com".into(),
                },
            ])
            .await
        });

        // Poll B: keep {b, c}
        let t_b = t.clone();
        let poll_b = tokio::spawn(async move {
            t_b.apply_poll_snapshot(vec![
                Domain {
                    id: "d_b".into(),
                    tenant_id: "t_a".into(),
                    app_name: "api".into(),
                    fqdn: "b.acme.com".into(),
                },
                Domain {
                    id: "d_c".into(),
                    tenant_id: "t_a".into(),
                    app_name: "api".into(),
                    fqdn: "c.acme.com".into(),
                },
            ])
            .await
        });

        let (res_a, res_b) = tokio::join!(poll_a, poll_b);
        res_a.expect("poll A task panicked");
        res_b.expect("poll B task panicked");

        // The single-write-lock fix serializes the two polls. The
        // final state must match one of the two valid sequential
        // outcomes — not the lost-update `{b}` outcome that the
        // two-phase bug would produce.
        let snap_after: HashSet<String> = t
            .fqdn_snapshot()
            .await
            .into_iter()
            .map(|b| b.fqdn)
            .collect();
        let valid_a_then_b = HashSet::from(["b.acme.com".to_string(), "c.acme.com".to_string()]);
        let valid_b_then_a = HashSet::from(["a.acme.com".to_string(), "b.acme.com".to_string()]);
        let lost_update = HashSet::from(["b.acme.com".to_string()]);
        assert_ne!(
            snap_after, lost_update,
            "concurrent polls lost entries that one of them intended to keep \
             — this is the read/drop/write race from PR #133 review finding #2"
        );
        assert!(
            snap_after == valid_a_then_b || snap_after == valid_b_then_a,
            "final state {snap_after:?} is neither of the two valid sequential \
             outcomes {{a, b}} / {{b, c}}; the single-write-lock fix has regressed"
        );
    }
}
