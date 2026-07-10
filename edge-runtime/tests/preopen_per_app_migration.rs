//! Preopen migration behavior (issue #558).
//!
//! When `EDGE_FS_PATH` is set and the per-tenant parent directory
//! already contains files but the per-app subdirectory does not yet
//! exist, the first mount of the app starts with an empty per-app
//! subdirectory and emits a WARN log. Pre-existing tenant-root files
//! are NOT auto-migrated — that's a clean break (see the doc comment
//! on `build_wasi_ctx_for_tenant` in `runtime.rs`).
//!
//! This test only asserts the on-disk shape: the per-app subdir is
//! created alongside any pre-existing tenant-root files.
//!
//! Lives in a separate cargo `[[test]]` binary from
//! `preopen_per_app_isolation.rs` because `EDGE_FS_PATH` is read into
//! a process-static `OnceLock` in `runtime.rs`. Each `tests/*.rs`
//! integration binary is built separately, so each gets its own
//! `OnceLock` initialization.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use edge_runtime::interfaces::observe::{AppLogContext, LogRecord, LogSink};
use edge_runtime::{EgressPolicy, RuntimeState};

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
            deployment_id: "migration-test".to_string(),
        },
        None,
        edge_runtime::socket_egress::SocketEgressPolicy::default(),
        Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
    )
}

#[test]
fn tenant_parent_with_legacy_files_does_not_block_app_subdir_creation() {
    let base = tempfile::tempdir().expect("tempdir");
    let base_path: PathBuf = base.path().to_path_buf();
    std::env::set_var("EDGE_FS_PATH", &base_path);

    let tenant_id = "tenant-migration";
    let tenant_root = base_path.join(tenant_id);
    std::fs::create_dir_all(&tenant_root).expect("create tenant root");
    std::fs::write(tenant_root.join("legacy.txt"), b"pre-558 file").expect("write legacy");

    let app = build_state(tenant_id, "fresh-app");
    let app_dir = base_path.join(tenant_id).join("fresh-app");

    assert!(
        app_dir.exists(),
        "per-app subdir must be created even when tenant root has legacy files (issue #558)"
    );
    assert!(
        tenant_root.join("legacy.txt").exists(),
        "legacy file preserved"
    );

    let _ = app;
}
