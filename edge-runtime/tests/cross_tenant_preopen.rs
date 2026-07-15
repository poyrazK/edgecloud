//! Cross-tenant filesystem preopen isolation (issue #620).
//!
//! Revives the deferred L9 placeholder at
//! `edge-worker/tests/layer_integration.rs:24`. Verifies that two
//! `RuntimeState` instances with **different** `tenant_id`s create
//! disjoint per-app preopen subdirectories under `EDGE_FS_PATH`, so
//! a file written into tenant A's preopen cannot be reached from
//! tenant B's preopen.
//!
//! This is a host-side path-construction check. Exercising the
//! actual `wasi:filesystem/types::open-at("/", ...)` surface
//! requires loading a real guest component; that's covered
//! end-to-end by `tests/handler_fixture_load.rs` and
//! `tests/js_fixture_load.rs` (which use the per-app constructor).
//! A guest-side end-to-end that *also* proves tenant A's
//! filesystem calls cannot escape into tenant B's mount remains a
//! follow-up — out of scope for this issue's host-side guarantee.
//!
//! Each integration test file under `tests/` is its own cargo
//! binary, so the `OnceLock<Option<PathBuf>>` inside
//! `edge_runtime::runtime::EDGE_FS_PATH` is fresh here and reads
//! the env var exactly once. The two preopen tests in this file
//! therefore MUST set `EDGE_FS_PATH` BEFORE the first
//! `RuntimeState` is constructed (see `preopen_per_app_isolation.rs`
//! for the same pattern).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use edge_runtime::interfaces::observe::{AppLogContext, LogRecord, LogSink};
use edge_runtime::{EgressPolicy, RuntimeState};

/// `NoopSink` swallows guest `emit_log` calls. Mirrors the test
/// helper inside `runtime.rs::with_env_and_meter_tests`.
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

/// Host-side path-construction check: confirms that two tenants
/// pointed at the same `EDGE_FS_PATH` base land in disjoint
/// per-app subdirectories. NOT a guest-side escape check —
/// proving `wasi:filesystem/types::open-at("/", ...)` cannot
/// cross the boundary requires loading a real component (covered
/// for the per-app constructor by `tests/handler_fixture_load.rs`
/// and `tests/js_fixture_load.rs`); a tenant A → tenant B escape
/// end-to-end remains a follow-up.
#[test]
fn preopen_isolation_two_tenants_get_distinct_subdirs() {
    // `tempfile::tempdir()` gives us a fresh base. We set
    // `EDGE_FS_PATH` to it BEFORE constructing the first
    // RuntimeState — once the `OnceLock` is initialized, the value
    // is locked for the rest of the test process.
    let base = tempfile::tempdir().expect("tempdir");
    let base_path: PathBuf = base.path().to_path_buf();
    std::env::set_var("EDGE_FS_PATH", &base_path);

    let tenant_a = "tenant-a-preopen";
    let tenant_b = "tenant-b-preopen";
    let _state_a = build_state(tenant_a, "app-x");
    let _state_b = build_state(tenant_b, "app-y");

    // Each tenant gets its own per-app subdirectory under the
    // shared base. The two subdirs are disjoint.
    let app_a_dir = base_path.join(tenant_a).join("app-x");
    let app_b_dir = base_path.join(tenant_b).join("app-y");

    assert!(
        app_a_dir.exists(),
        "expected per-app preopen dir for tenant A at {app_a_dir:?} (issue #620)"
    );
    assert!(
        app_b_dir.exists(),
        "expected per-app preopen dir for tenant B at {app_b_dir:?} (issue #620)"
    );
    assert_ne!(
        app_a_dir, app_b_dir,
        "tenant A and tenant B must not share the same preopen subdir"
    );
}

/// Host-side check: a file written directly into tenant A's
/// per-app preopen subdirectory is not visible inside tenant B's
/// per-app preopen subdirectory (no symlinks, no cross-mount,
/// no shared parent). NOT a guest-side escape check — see the
/// module docstring for the follow-up.
#[test]
fn preopen_isolation_other_tenants_files_not_visible() {
    // Host-side check that a file written into tenant A's preopen
    // subdirectory is NOT visible from tenant B's preopen
    // subdirectory. Mirrors the cross-tenant on-disk disjointness
    // guarantee the persistence helpers (kv_store/cache/
    // scheduling) already provide, but for the filesystem layer.
    let base = tempfile::tempdir().expect("tempdir");
    let base_path: PathBuf = base.path().to_path_buf();
    std::env::set_var("EDGE_FS_PATH", &base_path);

    let tenant_a = "tenant-a-files";
    let tenant_b = "tenant-b-files";

    // Construct state A first — this causes
    // `build_wasi_ctx_for_tenant` to `create_dir_all` the per-app
    // subdirectory for tenant A.
    let _state_a = build_state(tenant_a, "app-x");

    // Write a file directly into tenant A's per-app subdir.
    let app_a_dir = base_path.join(tenant_a).join("app-x");
    assert!(
        app_a_dir.exists(),
        "tenant A's preopen subdir must exist after build_state: {app_a_dir:?}"
    );
    let foo_path = app_a_dir.join("foo.txt");
    std::fs::write(&foo_path, b"tenant-a secret payload").expect("write foo.txt");

    // Now construct state B with a different tenant id. State B
    // gets its own per-app subdirectory under the same base.
    let _state_b = build_state(tenant_b, "app-y");
    let app_b_dir = base_path.join(tenant_b).join("app-y");
    assert!(
        app_b_dir.exists(),
        "tenant B's preopen subdir must exist after build_state: {app_b_dir:?}"
    );

    // The file written into tenant A's subdir must NOT appear
    // anywhere reachable from tenant B's subdir. We assert this
    // by walking tenant B's subdir and confirming no `foo.txt`
    // exists.
    let mut found_in_b = false;
    for entry in std::fs::read_dir(&app_b_dir).expect("read tenant B dir") {
        let entry = entry.expect("dir entry");
        if entry.file_name() == "foo.txt" {
            found_in_b = true;
            break;
        }
    }
    assert!(
        !found_in_b,
        "tenant B's preopen subdir must not contain tenant A's foo.txt ({foo_path:?})"
    );

    // Sanity check: the file still exists in tenant A's subdir.
    assert!(
        foo_path.exists(),
        "tenant A's foo.txt must remain on disk after tenant B is constructed"
    );

    // And tenant A's subdir does NOT contain tenant B's preopen
    // path (no symlinks, no cross-mount).
    let app_b_symlink = app_a_dir.join("app-y");
    assert!(
        !app_b_symlink.exists(),
        "tenant A's preopen must not reach into tenant B's per-app subdir"
    );
}
