//! Cross-language NATS wire-contract tests (issue #610).
//!
//! These tests deserialize the same golden JSON fixtures committed under
//! `edge-control-plane/internal/nats/testdata/` that the Go-side round-trip
//! tests at `edge-control-plane/internal/nats/wire_contract_test.go`
//! decode. A wire-shape drift on either side (field rename, `omitempty`
//! flip, enum-tag change) turns a test red on whichever side introduced
//! the change. The Rust side is stricter on closed enums (see the
//! `task_purge_unknown_reason_fails_to_parse` test below) so the
//! failure mode surfaces immediately there.
//!
//! The fixtures are referenced via `include_str!` relative to the
//! `edge-worker/` manifest dir, so the path is
//! `../../edge-control-plane/internal/nats/testdata/<name>`.

use std::collections::HashMap;

use edge_runtime::socket_egress::SocketEgressPolicy;
use edge_worker::messages::{HeartbeatMessage, PurgeReason, TaskMessage};

fn task_update_full() -> &'static str {
    include_str!("../../edge-control-plane/internal/nats/testdata/task_update.json")
}

fn task_update_minimal() -> &'static str {
    include_str!("../../edge-control-plane/internal/nats/testdata/task_update_minimal.json")
}

fn full_sync_fixture() -> &'static str {
    include_str!("../../edge-control-plane/internal/nats/testdata/full_sync.json")
}

fn task_purge_per_app() -> &'static str {
    include_str!("../../edge-control-plane/internal/nats/testdata/task_purge_per_app.json")
}

fn task_purge_tenant_wide() -> &'static str {
    include_str!("../../edge-control-plane/internal/nats/testdata/task_purge_tenant_wide.json")
}

fn task_purge_unknown_reason() -> &'static str {
    include_str!("../../edge-control-plane/internal/nats/testdata/task_purge_unknown_reason.json")
}

fn heartbeat_full() -> &'static str {
    include_str!("../../edge-control-plane/internal/nats/testdata/heartbeat.json")
}

fn heartbeat_minimal() -> &'static str {
    include_str!("../../edge-control-plane/internal/nats/testdata/heartbeat_minimal.json")
}

#[test]
fn task_update_full_matches_go_fixture() {
    let msg: TaskMessage = serde_json::from_str(task_update_full())
        .expect("task_update.json must deserialize into TaskMessage::TaskUpdate");

    let TaskMessage::TaskUpdate {
        tenant_id, apps, ..
    } = msg
    else {
        panic!("expected TaskUpdate variant");
    };

    assert_eq!(tenant_id, "t_acme");

    let app = apps.get("myapp").expect("apps.myapp must be present");
    assert_eq!(app.deployment_id, "d_abc123");
    assert_eq!(
        app.deployment_hash,
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    );
    assert_eq!(
        app.deployment_signature.as_deref(),
        Some("kZSd4kQb8zMqOH8Lbq5F7j9W6o4yYfE2tPmI1nVcA3w")
    );
    assert_eq!(app.signing_key_id.as_deref(), Some("k1"));
    assert_eq!(app.preview_id.as_deref(), Some("prev_abc123"));
    assert_eq!(app.preview_pr_number, Some(42));

    let routes = app.routes.as_ref().expect("routes must be present");
    assert_eq!(routes.len(), 2);
    assert_eq!(routes[0].deployment_id, "d_abc123");
    assert_eq!(routes[0].weight, 80);
    assert_eq!(routes[1].deployment_id, "d_def456");
    assert_eq!(routes[1].weight, 20);
    // Route #1 omits deployment_signature + signing_key_id; both should
    // deserialize to None via #[serde(default)].
    assert!(routes[1].deployment_signature.is_none());
    assert!(routes[1].signing_key_id.is_none());
    assert!(routes[1].deployment_hash.starts_with("fedcba"));

    let env: HashMap<String, String> = [
        ("API_KEY".to_string(), "redacted".to_string()),
        ("LOG_LEVEL".to_string(), "info".to_string()),
    ]
    .into_iter()
    .collect();
    assert_eq!(app.env, env);

    let allowlist = app.allowlist.as_ref().expect("allowlist must be present");
    assert_eq!(
        allowlist,
        &vec!["api.stripe.com".to_string(), "*.sendgrid.net".to_string()]
    );

    assert_eq!(app.socket_mode, Some(SocketEgressPolicy::HostnamePinned));
    assert_eq!(app.max_memory_mb, 256);
    assert_eq!(app.cpu_budget_ms, Some(500));
}

#[test]
fn task_update_minimal_parses_with_optionals_none() {
    let msg: TaskMessage = serde_json::from_str(task_update_minimal())
        .expect("task_update_minimal.json must deserialize");

    let TaskMessage::TaskUpdate {
        tenant_id, apps, ..
    } = msg
    else {
        panic!("expected TaskUpdate variant");
    };
    assert_eq!(tenant_id, "t_acme");

    let app = apps.get("myapp").expect("apps.myapp");
    assert_eq!(app.deployment_id, "d_minimal");
    assert_eq!(app.deployment_signature, None);
    assert_eq!(app.signing_key_id, None);
    assert_eq!(app.preview_id, None);
    assert_eq!(app.preview_pr_number, None);
    assert!(app.routes.is_none());
    assert_eq!(app.allowlist, None);
    assert_eq!(app.socket_mode, None);
    assert_eq!(app.cpu_budget_ms, Some(250));
    assert_eq!(app.max_memory_mb, 128);
}

#[test]
fn full_sync_parses_with_multiple_apps() {
    let msg: TaskMessage = serde_json::from_str(full_sync_fixture()).expect("full_sync.json");

    let TaskMessage::FullSync {
        tenant_id, apps, ..
    } = msg
    else {
        panic!("expected FullSync variant");
    };

    assert_eq!(tenant_id, "t_acme");
    assert_eq!(apps.len(), 2);
    assert!(apps.contains_key("myapp"));
    assert!(apps.contains_key("other"));
    // The `other` app omits every `Option` field — they must all be None.
    let other = &apps["other"];
    assert_eq!(other.deployment_signature, None);
    assert_eq!(other.signing_key_id, None);
    assert!(other.routes.is_none());
    assert_eq!(other.allowlist, None);
    assert_eq!(other.socket_mode, None);
    assert_eq!(other.cpu_budget_ms, Some(100));
}

#[test]
fn task_purge_per_app_parses_with_reason_app_deleted() {
    let msg: TaskMessage =
        serde_json::from_str(task_purge_per_app()).expect("task_purge_per_app.json");

    let TaskMessage::TaskPurge {
        tenant_id,
        app_name,
        reason,
        ..
    } = msg
    else {
        panic!("expected TaskPurge variant");
    };

    assert_eq!(tenant_id, "t_acme");
    assert_eq!(app_name, "myapp");
    assert_eq!(reason, PurgeReason::AppDeleted);
}

#[test]
fn task_purge_tenant_wide_parses_with_empty_app_name() {
    let msg: TaskMessage =
        serde_json::from_str(task_purge_tenant_wide()).expect("task_purge_tenant_wide.json");

    let TaskMessage::TaskPurge {
        tenant_id,
        app_name,
        reason,
        ..
    } = msg
    else {
        panic!("expected TaskPurge variant");
    };

    assert_eq!(tenant_id, "t_acme");
    assert_eq!(app_name, ""); // absent -> empty -> tenant-wide
    assert_eq!(reason, PurgeReason::TenantOffboarded);
}

/// The Go side decodes unknown reasons (PurgeReason is a string alias).
/// The Rust side is closed via `#[serde(rename_all = "snake_case")]` —
/// no `#[serde(other)]` arm — so unknown values must error. This is
/// intentional (issue #569): the worker must reject tombstones it
/// doesn't know how to handle rather than silently logging an
/// unrecognized audit value.
/// See edge-worker/src/messages.rs `task_purge_unknown_reason_fails_to_deserialize`.
#[test]
fn task_purge_unknown_reason_fails_to_parse() {
    let result: Result<TaskMessage, _> = serde_json::from_str(task_purge_unknown_reason());
    assert!(
        result.is_err(),
        "Rust PurgeReason is a closed enum; unknown reason must error. Got: {:?}",
        result.map(|m| match m {
            TaskMessage::TaskPurge { reason, .. } => format!("{:?}", reason),
            other => format!("{:?}", other),
        })
    );
}

#[test]
fn heartbeat_full_parses_with_dedupe_id_and_observer_metrics() {
    let hb: HeartbeatMessage = serde_json::from_str(heartbeat_full()).expect("heartbeat.json");

    assert_eq!(hb.msg_type, "heartbeat");
    assert_eq!(hb.worker_id, "w_global_dev");
    assert_eq!(hb.region, "global");

    let app = hb.apps.get("myapp").expect("apps.myapp");
    assert_eq!(app.status, "running");
    assert_eq!(app.deployment_id, "d_abc123");
    assert_eq!(app.request_count, 1024);
    assert_eq!(app.outbound_bytes, 4096);
    assert_eq!(app.tenant_id, "t_acme");
    assert_eq!(app.port, 8081);

    assert_eq!(
        app.dedupe_id.as_deref(),
        Some("w_global_dev:d_abc123:2026-07-11T10:30:00Z")
    );
    assert_eq!(app.resident_seconds, Some(90));
    assert_eq!(app.duration_ms_total, 1500);

    assert_eq!(app.observer_metrics.len(), 3);
    assert_eq!(app.observer_metrics[0].name, "hits");
    assert_eq!(app.observer_metrics[1].name, "memory_bytes");
    assert_eq!(app.observer_metrics[2].name, "latency_ms");

    let headroom = hb.cluster_headroom.as_ref().expect("cluster_headroom");
    assert_eq!(headroom.app_slots, 42);
    assert_eq!(headroom.cpu_pct, Some(0.5));
    assert_eq!(headroom.mem_pct, Some(0.3));

    // NOTE: `HeartbeatMessage::worker_addr` / `tenant_id` (issue #297) are
    // part of the Rust-side envelope but NOT modeled on the Go-side
    // `HeartbeatMessage` envelope (`internal/nats/publisher.go:178`).
    // They flow on the wire; the CP derives them from `apps[*].tenant_id`
    // and the worker's separate register call. They're intentionally
    // absent from the shared fixtures because adding them would force
    // the Go envelope to grow to keep the structural round-trip — a
    // production-code change outside #610's scope. If the Go side ever
    // gains these fields, add them to the fixtures AND extend both
    // round-trip tests.
    assert_eq!(hb.worker_addr, None);
    assert_eq!(hb.tenant_id, None);
}

#[test]
fn heartbeat_minimal_parses_without_optionals() {
    let hb: HeartbeatMessage =
        serde_json::from_str(heartbeat_minimal()).expect("heartbeat_minimal.json");

    assert_eq!(hb.msg_type, "heartbeat");
    assert_eq!(hb.worker_id, "w_global_dev");

    let app = hb.apps.get("myapp").expect("apps.myapp");
    assert_eq!(app.status, "running");
    assert_eq!(app.deployment_id, "d_abc123");
    assert_eq!(app.request_count, 0);
    assert_eq!(app.outbound_bytes, 0);
    assert_eq!(app.tenant_id, "t_acme");
    assert_eq!(app.port, 8081);

    // Pre-#418 / pre-#484 / pre-#555 shape — these fields are absent.
    assert_eq!(app.dedupe_id, None);
    assert_eq!(app.resident_seconds, None);
    assert_eq!(app.duration_ms_total, 0); // u64 default, not Option
    assert_eq!(app.observer_metrics.len(), 0);
    assert!(hb.cluster_headroom.is_none());

    // See note in `heartbeat_full_parses_with_dedupe_id_and_observer_metrics`
    // for why worker_addr / tenant_id are not asserted here.
    assert_eq!(hb.worker_addr, None);
    assert_eq!(hb.tenant_id, None);
}
