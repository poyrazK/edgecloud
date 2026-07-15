//! Cross-tenant persistence isolation (issue #620).
//!
//! Verifies that two `RuntimeState` instances with **different**
//! `tenant_id`s pointed at the same `EDGE_*_PATH` root get
//! disjoint views of the KV / cache / scheduling stores, and that
//! the on-disk persistence files (`store.json`, `cache.json`,
//! `schedule.json`) live under disjoint per-tenant subdirectories.
//!
//! The persistence helpers already re-validate `tenant_id` before
//! any `Path::join` (kv_store.rs:222, cache.rs:256, scheduling.rs:
//! 387), and the worker boundary at
//! `edge-worker/src/supervisor.rs:2074` and `:2137` calls
//! `is_safe_tenant_id` before any store allocation. These tests
//! formalize the contract by exercising the cross-tenant boundary
//! from end to end: two `RuntimeState`s, same env, different
//! tenants, distinct views.
//!
//! The closest existing analogues live at
//! `edge-worker/tests/layer_integration.rs:1778`
//! (`l35_tenant_isolation_kv_store`) and `:1810`
//! (`l36_tenant_isolation_cache`) - those drive the WIT surface
//! via the worker. The tests below exercise the same isolation
//! guarantee at the persistence boundary instead, which is what
//! issue #620 calls out as the unverified gap.
//!
//! Each integration test file under `tests/` is its own cargo
//! binary, so the `LazyLock<RwLock<HashMap<...>>>` registries for
//! `KV_STORES` / `CACHE_STORES` / `SCHEDULERS` (runtime.rs:80-85)
//! are fresh here. Within this file, every test uses a unique
//! UUID-suffixed tenant id so concurrent insertion under the
//! registries' write lock is collision-free.
//!
//! PERSISTENCE FLUSH: `KvStore::set`, `Cache::set`, and
//! `Scheduler::schedule_once` all call `flush_if_persistent`,
//! which is a no-op outside a Tokio runtime handle
//! (kv_store.rs:274, cache.rs:282, scheduling.rs:405). The
//! in-memory `set`/`get` tests therefore don't need a Tokio
//! runtime, but the on-disk tests do - they wrap the writes in
//! `tokio::task::spawn_blocking` so `rt.block_on` has a handle.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use edge_runtime::interfaces::observe::{AppLogContext, LogRecord, LogSink};
use edge_runtime::{EgressPolicy, RuntimeState};

/// `NoopSink` swallows guest `emit_log` calls.
struct NoopSink;
impl LogSink for NoopSink {
    fn push(&self, _r: LogRecord, _c: AppLogContext) {}
}

fn build_state(tenant_id: &str, app_name: &str) -> RuntimeState {
    RuntimeState::with_env_and_meter(
        HashMap::new(),
        None,
        tenant_id.to_string(),
        app_name,
        Arc::new(EgressPolicy::allow_all()),
        Arc::new(NoopSink) as Arc<dyn LogSink>,
        AppLogContext {
            app_name: app_name.to_string(),
            tenant_id: tenant_id.to_string(),
            deployment_id: "isolation-test".to_string(),
        },
        None,
        edge_runtime::socket_egress::SocketEgressPolicy::default(),
        Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
    )
}

fn fresh_tenant(label: &str) -> String {
    format!("iso-{}-{}", label, uuid::Uuid::new_v4())
}

// --- KV store: in-memory view isolation ------------------------------

#[test]
fn kv_store_isolation_two_tenants_get_disjoint_views() {
    let kv_dir = tempfile::tempdir().expect("kv tempdir");
    std::env::set_var("EDGE_KV_STORE_PATH", kv_dir.path());

    let tenant_a = fresh_tenant("kv-a");
    let tenant_b = fresh_tenant("kv-b");

    let state_a = build_state(&tenant_a, "app-a");
    let state_b = build_state(&tenant_b, "app-b");

    state_a
        .kv_store
        .set("k".into(), b"v-a".to_vec(), None)
        .expect("A set");
    state_b
        .kv_store
        .set("k".into(), b"v-b".to_vec(), None)
        .expect("B set");

    let got_a = state_a.kv_store.get("k").expect("A get");
    let got_b = state_b.kv_store.get("k").expect("B get");
    assert_eq!(
        got_a.as_deref(),
        Some(b"v-a".as_slice()),
        "tenant A must see its own write (issue #620)"
    );
    assert_eq!(
        got_b.as_deref(),
        Some(b"v-b".as_slice()),
        "tenant B must NOT see tenant A's write (issue #620)"
    );

    let cross_key = "cross-tenant-key";
    state_a
        .kv_store
        .set(cross_key.into(), b"a-only".to_vec(), None)
        .expect("A cross set");
    assert_eq!(
        state_b.kv_store.get(cross_key).expect("B cross get"),
        None,
        "tenant B must not see tenant A's cross-key write (issue #620)"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn kv_store_isolation_on_disk_paths_disjoint() {
    let kv_dir = tempfile::tempdir().expect("kv tempdir");
    let base: PathBuf = kv_dir.path().to_path_buf();
    let kv_str = base.to_string_lossy().to_string();
    let tenant_a = fresh_tenant("kv-disk-a");
    let tenant_b = fresh_tenant("kv-disk-b");
    let tenant_a_in = tenant_a.clone();
    let tenant_b_in = tenant_b.clone();

    tokio::task::spawn_blocking(move || {
        temp_env::with_var("EDGE_KV_STORE_PATH", Some(&kv_str), || {
            let state_a = build_state(&tenant_a_in, "app-a");
            state_a
                .kv_store
                .set("k".into(), b"v-a".to_vec(), None)
                .expect("A set");

            let state_b = build_state(&tenant_b_in, "app-b");
            state_b
                .kv_store
                .set("k".into(), b"v-b".to_vec(), None)
                .expect("B set");
        })
    })
    .await
    .expect("spawn_blocking panicked");

    let path_a = base.join(&tenant_a).join("store.json");
    let path_b = base.join(&tenant_b).join("store.json");

    assert!(
        path_a.exists(),
        "tenant A's store.json must exist at {path_a:?}"
    );
    assert!(
        path_b.exists(),
        "tenant B's store.json must exist at {path_b:?}"
    );
    assert_ne!(path_a, path_b, "store.json paths must be disjoint");

    let raw_a = std::fs::read_to_string(&path_a).expect("read A");
    let raw_b = std::fs::read_to_string(&path_b).expect("read B");
    // Values are base64-encoded by KvStorePersistence::flush. base64
    // of "v-a" (3 bytes) is "di1h"; base64 of "v-b" is "di1i".
    assert!(
        raw_a.contains("\"di1h\"") && !raw_a.contains("\"di1i\""),
        "tenant A's store.json must contain base64(v-a) and not base64(v-b) (got: {raw_a})"
    );
    assert!(
        raw_b.contains("\"di1i\"") && !raw_b.contains("\"di1h\""),
        "tenant B's store.json must contain base64(v-b) and not base64(v-a) (got: {raw_b})"
    );
}

// --- Cache: in-memory view isolation ---------------------------------

#[test]
fn cache_isolation_two_tenants_get_disjoint_views() {
    let cache_dir = tempfile::tempdir().expect("cache tempdir");
    std::env::set_var("EDGE_CACHE_PATH", cache_dir.path());

    let tenant_a = fresh_tenant("cache-a");
    let tenant_b = fresh_tenant("cache-b");

    let state_a = build_state(&tenant_a, "app-a");
    let state_b = build_state(&tenant_b, "app-b");

    state_a
        .cache
        .set("ck".into(), b"cv-a".to_vec(), None)
        .expect("A set");
    state_b
        .cache
        .set("ck".into(), b"cv-b".to_vec(), None)
        .expect("B set");

    let got_a = state_a.cache.get("ck").expect("A get");
    let got_b = state_b.cache.get("ck").expect("B get");
    assert_eq!(
        got_a.as_deref(),
        Some(b"cv-a".as_slice()),
        "tenant A must see its own cache write (issue #620)"
    );
    assert_eq!(
        got_b.as_deref(),
        Some(b"cv-b".as_slice()),
        "tenant B must NOT see tenant A's cache write (issue #620)"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn cache_isolation_on_disk_paths_disjoint() {
    let cache_dir = tempfile::tempdir().expect("cache tempdir");
    let base: PathBuf = cache_dir.path().to_path_buf();
    let cache_str = base.to_string_lossy().to_string();
    let tenant_a = fresh_tenant("cache-disk-a");
    let tenant_b = fresh_tenant("cache-disk-b");
    let tenant_a_in = tenant_a.clone();
    let tenant_b_in = tenant_b.clone();

    tokio::task::spawn_blocking(move || {
        temp_env::with_var("EDGE_CACHE_PATH", Some(&cache_str), || {
            let state_a = build_state(&tenant_a_in, "app-a");
            state_a
                .cache
                .set("ck".into(), b"cv-a".to_vec(), None)
                .expect("A set");

            let state_b = build_state(&tenant_b_in, "app-b");
            state_b
                .cache
                .set("ck".into(), b"cv-b".to_vec(), None)
                .expect("B set");
        })
    })
    .await
    .expect("spawn_blocking panicked");

    let path_a = base.join(&tenant_a).join("cache.json");
    let path_b = base.join(&tenant_b).join("cache.json");

    assert!(
        path_a.exists(),
        "tenant A's cache.json must exist at {path_a:?}"
    );
    assert!(
        path_b.exists(),
        "tenant B's cache.json must exist at {path_b:?}"
    );
    assert_ne!(path_a, path_b, "cache.json paths must be disjoint");

    let raw_a = std::fs::read_to_string(&path_a).expect("read A");
    let raw_b = std::fs::read_to_string(&path_b).expect("read B");
    assert!(
        raw_a.contains("Y3YtYQ") && !raw_a.contains("Y3YtYg"),
        "tenant A's cache.json must contain base64(cv-a) and not base64(cv-b) (got: {raw_a})"
    );
    assert!(
        raw_b.contains("Y3YtYg") && !raw_b.contains("Y3YtYQ"),
        "tenant B's cache.json must contain base64(cv-b) and not base64(cv-a) (got: {raw_b})"
    );
}

// --- Scheduling isolation --------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn scheduling_isolation_two_tenants_get_disjoint_views() {
    let sched_dir = tempfile::tempdir().expect("sched tempdir");
    let sched_str = sched_dir.path().to_string_lossy().to_string();
    let tenant_a = fresh_tenant("sched-a");
    let tenant_b = fresh_tenant("sched-b");

    let result = tokio::task::spawn_blocking(move || {
        temp_env::with_var("EDGE_SCHEDULING_PATH", Some(&sched_str), || {
            let state_a = build_state(&tenant_a, "app-a");
            let task_a = state_a
                .scheduling
                .schedule_once(60_000, b"payload-a".to_vec())
                .expect("A schedule");

            let state_b = build_state(&tenant_b, "app-b");
            let task_b = state_b
                .scheduling
                .schedule_once(60_000, b"payload-b".to_vec())
                .expect("B schedule");

            state_a.scheduling.cancel(&task_a).expect("A cancel");
            state_b.scheduling.cancel(&task_b).expect("B cancel");

            (task_a, task_b)
        })
    })
    .await
    .expect("spawn_blocking panicked");

    let (task_a, task_b) = result;
    assert_ne!(task_a, task_b, "scheduler task ids must be distinct");
}

#[tokio::test(flavor = "current_thread")]
async fn scheduling_isolation_on_disk_paths_disjoint() {
    let sched_dir = tempfile::tempdir().expect("sched tempdir");
    let base: PathBuf = sched_dir.path().to_path_buf();
    let sched_str = base.to_string_lossy().to_string();
    let tenant_a = fresh_tenant("sched-disk-a");
    let tenant_b = fresh_tenant("sched-disk-b");
    let tenant_a_in = tenant_a.clone();
    let tenant_b_in = tenant_b.clone();

    tokio::task::spawn_blocking(move || {
        temp_env::with_var("EDGE_SCHEDULING_PATH", Some(&sched_str), || {
            let state_a = build_state(&tenant_a_in, "app-a");
            let task_a = state_a
                .scheduling
                .schedule_once(60_000, b"payload-a".to_vec())
                .expect("A schedule");
            state_a.scheduling.cancel(&task_a).expect("A cancel");

            let state_b = build_state(&tenant_b_in, "app-b");
            let task_b = state_b
                .scheduling
                .schedule_once(60_000, b"payload-b".to_vec())
                .expect("B schedule");
            state_b.scheduling.cancel(&task_b).expect("B cancel");
        })
    })
    .await
    .expect("spawn_blocking panicked");

    let path_a = base.join(&tenant_a).join("schedule.json");
    let path_b = base.join(&tenant_b).join("schedule.json");

    assert!(
        path_a.exists(),
        "tenant A's schedule.json must exist at {path_a:?}"
    );
    assert!(
        path_b.exists(),
        "tenant B's schedule.json must exist at {path_b:?}"
    );
    assert_ne!(path_a, path_b, "schedule.json paths must be disjoint");
}
