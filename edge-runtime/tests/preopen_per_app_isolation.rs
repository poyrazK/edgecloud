//! Per-app preopen isolation (issue #558).
//!
//! Verifies that two `RuntimeState` instances for the **same** tenant
//! but **different** app names create separate on-disk preopen
//! subdirectories under `EDGE_FS_PATH`, so an app's files cannot leak
//! into another app of the same tenant.
//!
//! Pre-#558, both apps would `create_dir_all({base}/{tenant_id}/)` and
//! share one root — the bug this test guards against. Post-#558, each
//! app gets `{base}/{tenant_id}/{app_name}/` and stays isolated.
//!
//! This is a host-side path-construction check. Exercising the actual
//! `wasi:filesystem/types::open-at("/", ...)` surface requires loading
//! a real guest component; that's covered by the existing
//! `tests/handler_fixture_load.rs` and `tests/js_fixture_load.rs`
//! (which use the per-app constructor) end-to-end.
//!
//! Each integration test file under `tests/` is its own cargo binary,
//! so the `OnceLock<Option<PathBuf>>` inside
//! `edge_runtime::runtime::EDGE_FS_PATH` is fresh here and reads the
//! env var exactly once.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use edge_runtime::interfaces::observe::{AppLogContext, LogRecord, LogSink};
use edge_runtime::{EgressPolicy, RuntimeState};

/// `NoopSink` swallows guest `emit_log` calls. Mirrors the test helper
/// inside `runtime.rs::with_env_and_meter_tests`.
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

#[test]
fn two_apps_same_tenant_get_distinct_preopen_subdirs() {
    // `tempfile::tempdir()` gives us a fresh base. We set `EDGE_FS_PATH`
    // to it BEFORE constructing the first RuntimeState — once the
    // `OnceLock` is initialized, the value is locked for the rest of
    // the test process.
    let base = tempfile::tempdir().expect("tempdir");
    let base_path: PathBuf = base.path().to_path_buf();
    std::env::set_var("EDGE_FS_PATH", &base_path);

    let tenant_id = "tenant-shared";
    let _app_a = build_state(tenant_id, "app-a");
    let _app_b = build_state(tenant_id, "app-b");

    // After construction, both per-app subdirectories must exist.
    let app_a_dir = base_path.join(tenant_id).join("app-a");
    let app_b_dir = base_path.join(tenant_id).join("app-b");

    assert!(
        app_a_dir.exists(),
        "expected per-app preopen dir for app-a at {:?} (issue #558)",
        app_a_dir
    );
    assert!(
        app_b_dir.exists(),
        "expected per-app preopen dir for app-b at {:?} (issue #558)",
        app_b_dir
    );

    // The two apps MUST live in distinct directories — pre-#558 they
    // shared `base/{tenant_id}/` and one could clobber the other.
    assert_ne!(
        app_a_dir, app_b_dir,
        "per-app preopen directories must be distinct (issue #558)"
    );
}
