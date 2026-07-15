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

/// `KvStorePersistence::flush` (kv_store.rs:750) and
/// `CachePersistence::flush` (cache.rs) base64-encode the value
/// bytes before serializing. Computing the expected encoding
/// ourselves means a refactor of the encoding function breaks the
/// on-disk tests loudly, rather than the assertion silently
/// degrading to "neither substring is present."
fn b64(v: &[u8]) -> String {
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, v)
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
    // Values are base64-encoded by KvStorePersistence::flush.
    // Compute the expected encoding via the helper so a refactor
    // of the persistence layer's encoding breaks this test
    // loudly — the quoted JSON value pins the wire format.
    let v_a = format!("\"{}\"", b64(b"v-a"));
    let v_b = format!("\"{}\"", b64(b"v-b"));
    assert!(
        raw_a.contains(&v_a) && !raw_a.contains(&v_b),
        "tenant A's store.json must contain {v_a:?} and not {v_b:?} (got: {raw_a})"
    );
    assert!(
        raw_b.contains(&v_b) && !raw_b.contains(&v_a),
        "tenant B's store.json must contain {v_b:?} and not {v_a:?} (got: {raw_b})"
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
    // Values are base64-encoded by CachePersistence::flush.
    // Compute the expected encoding via the helper so a refactor
    // of the persistence layer's encoding breaks this test
    // loudly — the quoted JSON value pins the wire format.
    let v_a = format!("\"{}\"", b64(b"cv-a"));
    let v_b = format!("\"{}\"", b64(b"cv-b"));
    assert!(
        raw_a.contains(&v_a) && !raw_a.contains(&v_b),
        "tenant A's cache.json must contain {v_a:?} and not {v_b:?} (got: {raw_a})"
    );
    assert!(
        raw_b.contains(&v_b) && !raw_b.contains(&v_a),
        "tenant B's cache.json must contain {v_b:?} and not {v_a:?} (got: {raw_b})"
    );
}

// --- Scheduling isolation --------------------------------------------

// `Scheduler::schedule_once` returns a UUID for each task, so
// `task_a != task_b` would be true even in a shared-namespace
// scheduler. The real isolation check is "tenant B's scheduler
// refuses to cancel a tenant-A task" — `cancel` errors with
// "task not found" if the id isn't in this scheduler's task
// map. We schedule under tenant A, then assert tenant B's
// scheduler rejects the cancel — proof the two task maps are
// disjoint.
#[tokio::test(flavor = "current_thread")]
async fn scheduling_isolation_two_tenants_get_disjoint_views() {
    let sched_dir = tempfile::tempdir().expect("sched tempdir");
    let sched_str = sched_dir.path().to_string_lossy().to_string();
    let tenant_a = fresh_tenant("sched-a");
    let tenant_b = fresh_tenant("sched-b");
    let tenant_a_in = tenant_a.clone();
    let tenant_b_in = tenant_b.clone();

    let result = tokio::task::spawn_blocking(move || {
        temp_env::with_var("EDGE_SCHEDULING_PATH", Some(&sched_str), || {
            let state_a = build_state(&tenant_a_in, "app-a");
            let task_a = state_a
                .scheduling
                .schedule_once(60_000, b"payload-a".to_vec())
                .expect("A schedule");

            let state_b = build_state(&tenant_b_in, "app-b");
            let task_b = state_b
                .scheduling
                .schedule_once(60_000, b"payload-b".to_vec())
                .expect("B schedule");

            // Task ids differ — but this would be true even in a
            // shared scheduler. The real check is below: tenant B
            // cannot see tenant A's task in its map.
            assert_ne!(task_a, task_b, "task ids must be distinct UUIDs");

            // Tenant B's scheduler must NOT find tenant A's task.
            // `cancel` returns `Err("task not found: ...")` if the
            // id isn't in this scheduler's task map, so a
            // successful cancel would prove a shared map.
            let cross_cancel = state_b.scheduling.cancel(&task_a);
            assert!(
                cross_cancel.is_err(),
                "tenant B's scheduler must not see tenant A's task {task_a}; \
                 got {cross_cancel:?} (issue #620)"
            );

            // Both schedulers' own tasks must still be cancellable
            // (sanity check that the cross-cancel didn't disturb them).
            state_a.scheduling.cancel(&task_a).expect("A cancel");
            state_b.scheduling.cancel(&task_b).expect("B cancel");

            (task_a, task_b)
        })
    })
    .await
    .expect("spawn_blocking panicked");

    let (_task_a, _task_b) = result;
}

// `schedule_once` calls `flush_if_persistent` immediately
// (scheduling.rs:427), so `schedule.json` is on disk by the time
// the closure returns — no need to wait for Drop or explicit
// flush. The wrapping `spawn_blocking` is still required so
// `flush_if_persistent` has a Tokio runtime handle to `block_on`.
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
