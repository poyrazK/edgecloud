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
//! `app_dir.exists()` runs BEFORE `create_dir_all` so the WARN can
//! observe "dir does not yet exist" — issue #558 shipped the check
//! after the create, which made `had_app_dir` structurally always-true
//! and the WARN silently dead code.
//!
//! This test asserts the on-the-wire observable: a tracing subscriber
//! scoped via `tracing::subscriber::with_default` to the test body,
//! capturing WARN-level records into a shared `Arc<Mutex<Vec<u8>>>`
//! buffer. We avoid `set_global_default` because cargo runs tests in
//! parallel within a single binary — a global subscriber would
//! cross-pollinate buffers between sibling tests. `with_default`
//! scopes the subscriber to a closure, so the test sees only its own
//! WARNs.
//!
//! The single `#[test]` body covers both branches:
//!
//!   - Positive: legacy tenant-root file present → WARN fires on the
//!     first construction for a fresh (tenant, app), never again.
//!   - Negative: empty tenant root → WARN does NOT fire, even on the
//!     first construction.
//!
//! Both `(tenant, app)` pairs run in the same `EDGE_FS_PATH` tempdir
//! because `EDGE_FS_PATH` is read into a process-static `OnceLock` in
//! `runtime.rs` — once initialized, subsequent `set_var` calls are
//! ignored. Running positive and negative in the same test body
//! ensures they share the same OnceLock value.
//!
//! Lives in a separate cargo `[[test]]` binary from
//! `preopen_per_app_migration.rs` for the same reason.

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

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

/// Writer that appends each `write` call to a shared `Vec<u8>`.
/// `MakeWriter` requires `Clone`, so we wrap the buffer in an `Arc`.
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

/// Build a fresh subscriber writing into `buf`. Caller is expected to
/// invoke this subscriber via `tracing::subscriber::with_default(...)`
/// so it stays scoped to the test body.
fn make_subscriber(buf: Arc<Mutex<Vec<u8>>>) -> impl tracing::Subscriber + Send + Sync + 'static {
    let writer = CaptureWriter(buf);
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_writer(writer)
        .with_ansi(false)
        .finish()
}

fn warn_lines_for_preopen(
    buf: &Arc<Mutex<Vec<u8>>>,
    tenant_id: &str,
    app_name: &str,
) -> Vec<String> {
    let raw = buf.lock().unwrap().clone();
    let s = String::from_utf8_lossy(&raw);
    // Match WARN lines for the specific (tenant_id, app_name) pair so
    // earlier-positive or earlier-negative constructions don't leak
    // across assertions.
    let tag = format!("tenant_id=\"{}\"", tenant_id);
    s.lines()
        .filter(|l| l.contains("per-app preopen: starting empty") && l.contains(&tag))
        .filter(|l| l.contains(&format!("app_name=\"{}\"", app_name)))
        .map(|l| l.to_string())
        .collect()
}

#[test]
fn migration_warn_fires_once_with_legacy_and_never_without() {
    let buf = Arc::new(Mutex::new(Vec::new()));
    let subscriber = make_subscriber(buf.clone());

    tracing::subscriber::with_default(subscriber, || {
        let base = tempfile::tempdir().expect("tempdir");
        let base_path: PathBuf = base.path().to_path_buf();
        std::env::set_var("EDGE_FS_PATH", &base_path);

        // ── POSITIVE: legacy file present ──────────────────────────
        // Pre-#558 shape: tenant root has files but no per-app subdir.
        // First construction for a fresh (tenant, app) fires the WARN
        // exactly once; subsequent constructions must NOT re-fire.
        let pos_tenant = "tenant-positive";
        let pos_app = "fresh-app";
        let pos_root = base_path.join(pos_tenant);
        std::fs::create_dir_all(&pos_root).expect("create pos tenant root");
        std::fs::write(pos_root.join("legacy.txt"), b"pre-558 file").expect("write legacy");

        let _state1 = build_state(pos_tenant, pos_app);
        let pos_after_first = warn_lines_for_preopen(&buf, pos_tenant, pos_app);
        assert_eq!(
            pos_after_first.len(),
            1,
            "positive: first construction with legacy tenant-root files must fire the migration WARN exactly once; got {} lines:\n{}",
            pos_after_first.len(),
            pos_after_first.join("\n")
        );

        let _state2 = build_state(pos_tenant, pos_app);
        let pos_after_second = warn_lines_for_preopen(&buf, pos_tenant, pos_app);
        assert_eq!(
            pos_after_second.len(),
            1,
            "positive: second construction must NOT re-fire the migration WARN; got {} lines:\n{}",
            pos_after_second.len(),
            pos_after_second.join("\n")
        );

        let _state3 = build_state(pos_tenant, pos_app);
        let pos_after_third = warn_lines_for_preopen(&buf, pos_tenant, pos_app);
        assert_eq!(
            pos_after_third.len(),
            1,
            "positive: subsequent constructions must NOT re-fire the migration WARN; got {} lines:\n{}",
            pos_after_third.len(),
            pos_after_third.join("\n")
        );

        // ── NEGATIVE: empty tenant root ───────────────────────────
        // The inner guard `if had_tenant_files { warn!() }` must
        // suppress the WARN when the tenant root has no legacy files.
        // The per-app subdir does not exist yet, so the outer
        // `!had_app_dir` branch IS entered — but with no files, no
        // WARN fires.
        let neg_tenant = "tenant-negative";
        let neg_app = "fresh-app";
        let neg_root = base_path.join(neg_tenant);
        std::fs::create_dir_all(&neg_root).expect("create neg tenant root");
        // No legacy file written — tenant root is empty.

        let _state_neg1 = build_state(neg_tenant, neg_app);
        let neg_after_first = warn_lines_for_preopen(&buf, neg_tenant, neg_app);
        assert_eq!(
            neg_after_first.len(),
            0,
            "negative: first construction with EMPTY tenant root must NOT fire the migration WARN; got {} lines:\n{}",
            neg_after_first.len(),
            neg_after_first.join("\n")
        );

        let _state_neg2 = build_state(neg_tenant, neg_app);
        let neg_after_second = warn_lines_for_preopen(&buf, neg_tenant, neg_app);
        assert_eq!(
            neg_after_second.len(),
            0,
            "negative: subsequent construction with empty tenant root must NOT fire the migration WARN; got {} lines:\n{}",
            neg_after_second.len(),
            neg_after_second.join("\n")
        );
    });
}
