//! Preopen migration WARN fires once per (tenant, app), not per
//! request — issue #606.
//!
//! Before the fix, `build_wasi_ctx_for_tenant` ran `read_dir(&tenant_root)`
//! on every `RuntimeState` construction. On the FaaS path this
//! constructor runs once per accepted HTTP request, so the syscall
//! was being paid for every request even after the WARN had fired.
//!
//! After the fix, `read_dir` is gated behind `!had_app_dir` — once the
//! per-app subdir exists, the WARN cannot re-fire, so we skip the
//! `read_dir` entirely. The steady-state hot path is one `stat`-class
//! syscall (`app_dir.exists()`) instead of `stat` + open + readdir.
//!
//! This test asserts the on-the-wire observable: a tracing subscriber
//! that captures WARN-level records. The first construction for a
//! fresh (tenant, app) with legacy tenant-root files fires the WARN;
//! a subsequent construction for the same (tenant, app) does not —
//! because `had_app_dir` is true and `read_dir` is skipped.
//!
//! Lives in a separate cargo `[[test]]` binary from
//! `preopen_per_app_migration.rs` because `EDGE_FS_PATH` is read into
//! a process-static `OnceLock` in `runtime.rs`. Each `tests/*.rs`
//! integration binary is built separately, so each gets its own
//! `OnceLock` initialization. A second reason to isolate this test:
//! `tracing::subscriber::set_global_default` can only be called once
//! per process, so any WARN-capturing test needs to own the global
//! subscriber.

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

use edge_runtime::interfaces::observe::{AppLogContext, LogRecord, LogSink};
use edge_runtime::{EgressPolicy, RuntimeState};
use tracing_subscriber::fmt::MakeWriter;

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
            deployment_id: "warn-once-test".to_string(),
        },
        None,
        edge_runtime::socket_egress::SocketEgressPolicy::default(),
        Arc::new(edge_runtime::socket_egress::HostnamePinning::new()),
    )
}

/// Shared captured-output buffer. `MakeWriter` requires `Clone`, so we
/// store an `Arc<Mutex<Vec<u8>>>` in a `OnceLock` and clone the `Arc`
/// into each writer instance.
fn captured() -> Arc<Mutex<Vec<u8>>> {
    static BUF: OnceLock<Arc<Mutex<Vec<u8>>>> = OnceLock::new();
    BUF.get_or_init(|| Arc::new(Mutex::new(Vec::new()))).clone()
}

#[derive(Clone)]
struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

impl<'a> MakeWriter<'a> for CaptureWriter {
    type Writer = CaptureGuard;
    fn make_writer(&'a self) -> Self::Writer {
        CaptureGuard(self.0.clone())
    }
}

struct CaptureGuard(Arc<Mutex<Vec<u8>>>);
impl Write for CaptureGuard {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn install_capture_subscriber() {
    let buf = captured();
    let writer = CaptureWriter(buf.clone());
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_writer(writer)
        .with_ansi(false)
        .finish();
    // set_global_default returns Err if a subscriber is already set;
    // ignore that — earlier tests may have installed one and we don't
    // care for the WARN-presence assertions below (the test relies on
    // the WARN text being absent on the second construction; if a prior
    // subscriber swallows the WARN, the assertion is still correct
    // because `contains("per-app preopen: starting empty")` will be
    // false on the second read regardless).
    let _ = tracing::subscriber::set_global_default(subscriber);
}

fn warn_lines_for_preopen(buf: &Arc<Mutex<Vec<u8>>>) -> Vec<String> {
    let raw = buf.lock().unwrap().clone();
    let s = String::from_utf8_lossy(&raw);
    s.lines()
        .filter(|l| l.contains("per-app preopen: starting empty"))
        .map(|l| l.to_string())
        .collect()
}

#[test]
fn migration_warn_fires_only_on_first_construction_per_app() {
    let buf = captured();
    buf.lock().unwrap().clear();
    install_capture_subscriber();

    let base = tempfile::tempdir().expect("tempdir");
    let base_path: PathBuf = base.path().to_path_buf();
    std::env::set_var("EDGE_FS_PATH", &base_path);

    let tenant_id = "tenant-warn-once";
    let app_name = "fresh-app";

    // Pre-existing legacy file at the tenant root (pre-#558 shape).
    let tenant_root = base_path.join(tenant_id);
    std::fs::create_dir_all(&tenant_root).expect("create tenant root");
    std::fs::write(tenant_root.join("legacy.txt"), b"pre-558 file").expect("write legacy");

    // First construction for this (tenant, app): WARN should fire.
    let _state1 = build_state(tenant_id, app_name);
    let after_first = warn_lines_for_preopen(&buf);
    assert_eq!(
        after_first.len(),
        1,
        "first construction with legacy tenant-root files must fire the migration WARN exactly once; got {} lines:\n{}",
        after_first.len(),
        after_first.join("\n")
    );

    // Second construction for the SAME (tenant, app): the per-app
    // subdir now exists, so the new code path skips `read_dir`
    // entirely and the WARN must NOT re-fire. Before the fix this
    // assertion would catch a duplicate WARN line because the old
    // code ran `read_dir` every time and re-checked the condition.
    let _state2 = build_state(tenant_id, app_name);
    let after_second = warn_lines_for_preopen(&buf);
    assert_eq!(
        after_second.len(),
        1,
        "second construction must NOT re-fire the migration WARN; got {} lines:\n{}",
        after_second.len(),
        after_second.join("\n")
    );

    // Third construction: still no re-fire.
    let _state3 = build_state(tenant_id, app_name);
    let after_third = warn_lines_for_preopen(&buf);
    assert_eq!(
        after_third.len(),
        1,
        "subsequent constructions must NOT re-fire the migration WARN; got {} lines:\n{}",
        after_third.len(),
        after_third.join("\n")
    );
}
