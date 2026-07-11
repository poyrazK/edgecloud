//! Issue #448 — JS WebSocket end-to-end test.
//!
//! Boots a real `edge-worker` Supervisor against the long-running
//! `hello-js-ws` sample fixture
//! (`samples/hello-js-ws/.edge/hello-js-ws.component.wasm`, committed
//! to `edge-worker/tests/fixtures/js_websocket_handler.wasm` and
//! pinned by `test_fixtures_match_source.rs`). The fixture exports
//! `start: func()` (canonical `edge-runtime` world); the supervisor's
//! `run_app_loop` calls `start`, the shim builds a QuickJS runtime,
//! evaluates `handler.js`, and the JS calls `websocket.listen(wsPort)`
//! followed by an `accept`/`receive`/`send`/`close` echo loop.
//!
//! The test then:
//!  1. TCP-probes the bound port (via `ws_port` in the heartbeat).
//!  2. Performs an RFC 6455 §4.1 Upgrade handshake.
//!  3. Sends a masked text frame and asserts the echo round-trips
//!     byte-for-byte.
//!
//! Skipped when Docker is unavailable or under `SKIP_INTEGRATION_TESTS`
//! — matches the 9 existing supervisor integration tests via
//! `should_skip_integration_tests()` from `edge-test-helpers`.

use std::collections::HashMap;
use std::time::Duration;

use base64::Engine as _;
use sha1::{Digest as _, Sha1};
use sha2::Sha256;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use edge_runtime::socket_egress::SocketEgressPolicy;
use edge_test_helpers::{build_supervisor_with, should_skip_integration_tests};
use edge_worker::config::Config;
use edge_worker::messages::{AppSpec, HeartbeatMessage, TaskMessage};

// ── Fixtures ──────────────────────────────────────────────────────

const HELLO_JS_WS_WASM: &[u8] = include_bytes!("fixtures/js_websocket_handler.wasm");

const WS_GUID: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

fn hello_js_ws_hash() -> String {
    let digest = Sha256::digest(HELLO_JS_WS_WASM);
    let mut out = String::with_capacity(64);
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

// ── WS frame helpers (RFC 6455) ───────────────────────────────────

/// `Sec-WebSocket-Accept` = base64( sha1( client_key || WS_GUID ) ).
/// Mirrors `edge-runtime/src/interfaces/websocket.rs::compute_accept_key`.
fn compute_accept_key(client_key: &str) -> String {
    let mut h = Sha1::new();
    h.update(client_key.as_bytes());
    h.update(WS_GUID);
    base64::engine::general_purpose::STANDARD.encode(h.finalize())
}

/// Build a client→server masked text frame (RFC 6455 §5.3).
fn encode_client_text(text: &str) -> Vec<u8> {
    let payload = text.as_bytes();
    let mask = [0x37u8, 0xfa, 0x19, 0x80];
    let mut frame = Vec::with_capacity(2 + 4 + payload.len());
    frame.push(0x81); // FIN=1, opcode=1 (text)
    frame.push(0x80 | (payload.len() as u8 & 0x7F)); // MASK=1, len <= 125
    frame.extend_from_slice(&mask);
    for (i, b) in payload.iter().enumerate() {
        frame.push(b ^ mask[i & 3]);
    }
    frame
}

/// Parse the opcode + payload out of a server→client (unmasked) frame.
fn parse_server_frame(buf: &[u8]) -> (u8, Vec<u8>) {
    let opcode = buf[0] & 0x0F;
    let len = (buf[1] & 0x7F) as usize;
    let payload = if len < 126 {
        buf[2..2 + len].to_vec()
    } else if len == 126 {
        let n = u16::from_be_bytes([buf[2], buf[3]]) as usize;
        buf[4..4 + n].to_vec()
    } else {
        let n = u64::from_be_bytes([
            buf[2], buf[3], buf[4], buf[5], buf[6], buf[7], buf[8], buf[9],
        ]) as usize;
        buf[10..10 + n].to_vec()
    };
    (opcode, payload)
}

async fn tcp_connect_retry(addr: &str, attempts: u32) -> tokio::net::TcpStream {
    for _ in 0..attempts {
        if let Ok(s) = tokio::net::TcpStream::connect(addr).await {
            return s;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("could not TCP-connect to {addr} after {attempts} attempts");
}

// ── Test ──────────────────────────────────────────────────────────

// Multi-threaded flavor is required: the host's websocket::accept impl
// calls `tokio::task::block_in_place` (see edge-runtime's websocket.rs),
// which panics on a single-threaded runtime. Matches production, which
// always runs under `#[tokio::main]`'s multi-threaded default.
//
// Quarantined (issue #602): the host's websocket accept() parks an
// uncancellable blocking thread in std TcpListener::accept inside
// spawn_blocking; tokio's Runtime::drop waits for blocking threads, so
// even when the outer 60s timeout below fires and panics, the test
// process cannot exit — nextest reports SLOW forever and the CI job
// wedges until its 30-minute timeout. Un-ignore once #602 lands a
// cancellable accept path.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "issue #602: websocket accept() parks an uncancellable spawn_blocking thread; runtime teardown hangs the test process even after the 60s timeout panics"]
async fn js_websocket_round_trip() {
    if should_skip_integration_tests() {
        eprintln!("SKIPPED: integration tests skipped (Docker unavailable or CI)");
        return;
    }

    // Hard outer timeout: defense-in-depth against a repeat of the
    // "Cannot start a runtime from within a runtime" panic (fixed in
    // edge-runtime's websocket.rs) or any future regression that hangs
    // the guest's websocket.accept — a single stuck test must not be
    // able to wedge the whole CI job.
    tokio::time::timeout(Duration::from_secs(60), js_websocket_round_trip_inner())
        .await
        .expect("js_websocket_round_trip timed out after 60s");
}

async fn js_websocket_round_trip_inner() {
    // 1. Spin up the mock control plane + a Supervisor with AllowAll
    //    egress so the shim's `websocket.listen(wsPort)` can bind
    //    freely. Production runs with the default BlockAll
    //    (`config.socket_mode`); this test opts in via
    //    `socket_mode: AllowAll` because the long-running WS shim
    //    opens a server socket the host's egress policy would
    //    otherwise deny.
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/internal/download/d_hello_js_ws_001"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(HELLO_JS_WS_WASM))
        .mount(&mock)
        .await;

    let cache_dir = tempfile::TempDir::new().expect("cache dir");
    let config = Config {
        worker_id: "test-worker".to_string(),
        region: "test-region".to_string(),
        worker_addr: "test-host:0".to_string(),
        nats_url: String::new(), // overwritten by build_supervisor_with
        control_plane_url: mock.uri(),
        cache_dir: cache_dir.path().to_path_buf(),
        heartbeat_interval_secs: 30,
        worker_sync_threshold_secs: 60,
        health_check_timeout_secs: 60,
        port_cooldown_secs: 60,
        starting_port: 21_000, // avoid clashing with handler.wasm tests (18_000)
        max_memory_mb: 256,
        epoch_tick_ms: 10,
        epoch_deadline_ticks: 100,
        consumer_name: "test-js-ws-consumer".to_string(),
        queue_group: String::new(),
        worker_jwt_secret: String::from_utf8(b"test-jwt-secret-for-js-ws-e2e".to_vec()).unwrap(),
        worker_jwt_kid: None,
        worker_jwt_issuer: "edgecloud".to_string(),
        worker_tenant_id: "t_test".to_string(),
        handler_request_budget_ms: 1000,
        handler_max_request_body_bytes: 10 * 1024 * 1024,
        task_stream_replicas: 1,
        tls_cert_path: None,
        tls_key_path: None,
        worker_bootstrap_secret: String::new(),
        socket_mode: SocketEgressPolicy::AllowAll,
        hostname_pinning_enabled: false,
        standby_pool_size: 10,
        require_signature: false,
        signing_keyring: None,
        signing_keyring_path: None,
    };
    let guard = build_supervisor_with(config, None).await;
    let supervisor = guard.supervisor.clone();

    // 2. Send TaskMessage with the fixture. EDGE_WS_PORT="0" is the
    //    sentinel the worker recognizes and replaces with the real
    //    PortPool-allocated port (supervisor.rs:1217-1218).
    let spec = AppSpec {
        deployment_id: "d_hello_js_ws_001".to_string(),
        deployment_hash: hello_js_ws_hash(),
        deployment_signature: None,
        signing_key_id: None,
        env: HashMap::from([("EDGE_WS_PORT".to_string(), "0".to_string())]),
        allowlist: Some(vec!["*".to_string()]),
        socket_mode: Some(SocketEgressPolicy::AllowAll),
        max_memory_mb: 256,
        cpu_budget_ms: None,
        routes: None,
        preview_id: None,
        preview_pr_number: None,
    };
    let msg = TaskMessage::TaskUpdate {
        timestamp: "2026-07-09T00:00:00Z".to_string(),
        tenant_id: "t_test".to_string(),
        apps: HashMap::from([("hello-js-ws".to_string(), spec)]),
    };
    supervisor
        .handle_task_message(msg)
        .await
        .expect("handle_task_message");

    // 3. Poll the heartbeat for `ws_port`. Up to 30s — QuickJS
    //    initialization + bundle eval + `start()` can take a few
    //    seconds on cold cache.
    let mut ws_port: Option<u16> = None;
    for _ in 0..300 {
        // 300 * 100ms = 30s
        let hb: HeartbeatMessage = supervisor.build_heartbeat().await;
        if let Some(app) = hb.apps.get("hello-js-ws") {
            assert_eq!(app.status, "running", "app must be running before WS probe");
            if let Some(p) = app.ws_port {
                ws_port = Some(p);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let port = ws_port.expect("ws_port must surface in heartbeat within 30s");

    // 4. TCP probe + RFC 6455 §4.1 Upgrade handshake.
    let addr = format!("127.0.0.1:{port}");
    let mut sock = tcp_connect_retry(&addr, 40).await;
    let key = "dGhlIHNhbXBsZSBub25jZQ=="; // RFC 6455 §1.3 example
    let req = format!(
        "GET / HTTP/1.1\r\n\
         Host: {addr}\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Key: {key}\r\n\
         Sec-WebSocket-Version: 13\r\n\r\n"
    );
    sock.write_all(req.as_bytes()).await.expect("write Upgrade");

    let mut resp = Vec::new();
    let mut tmp = [0u8; 4096];
    let n = sock.read(&mut tmp).await.expect("read 101 response");
    resp.extend_from_slice(&tmp[..n]);
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(
        resp_str.contains("101 Switching Protocols"),
        "expected 101 Switching Protocols, got: {resp_str}"
    );
    let expected_accept = compute_accept_key(key);
    assert!(
        resp_str.contains(&expected_accept),
        "Sec-WebSocket-Accept mismatch: expected {expected_accept}, got: {resp_str}"
    );

    // 5. Send a masked text frame; assert the echo round-trips
    //    byte-for-byte.
    let payload = "hello, edgeCloud JS!";
    let frame = encode_client_text(payload);
    sock.write_all(&frame).await.expect("write text frame");

    let mut echo = [0u8; 4096];
    let n = sock.read(&mut echo).await.expect("read echo frame");
    let (opcode, body) = parse_server_frame(&echo[..n]);
    assert_eq!(opcode, 0x1, "echo must be a text frame (opcode 1)");
    assert_eq!(
        body,
        payload.as_bytes(),
        "echoed bytes must equal sent bytes byte-for-byte"
    );
}
