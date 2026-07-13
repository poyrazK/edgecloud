//! `hello-tcp` — minimal edgeCloud L4/TCP long-running guest.
//!
//! For any inbound TCP connection, reads RESP frames until it sees a
//! complete `PING\r\n` (the simplest possible RESP command) and writes
//! back `+PONG\r\n`, then keeps the connection open for further
//! commands. The point of the sample is to be the smallest possible
//! end-to-end-deployable raw-TCP guest component — issue #548's
//! `wasi:sockets/tcp` wire path through `edge-ingress`'s L4 routing
//! table and Caddy's `apps.layer4` gets exercised byte-for-byte with
//! every `nc localhost <port> <<< "PING"` a developer runs against it.
//!
//! ## Why a long-running (FaaS-shaped) world and not a handler?
//!
//! A raw-TCP listener has to own its socket for the duration of the
//! process. The `edge-runtime-handler` (FaaS) world has no
//! `wasi:sockets/tcp` access from the guest — the host owns the
//! socket and the guest is invoked once per HTTP request.
//! `edge-runtime` exposes the full `wasi:cli/command@0.2.1` family
//! (the workspace `wit/edge-cloud.wit` `include`s it), so the guest
//! can bind + accept itself the same way a stdlib `TcpListener`
//! would on a Linux box.
//!
//! ## Build
//!
//! The CLI does the two-step build (cargo + wasm-tools wrap) for you:
//!
//! ```sh
//! cd samples/hello-tcp
//! ../../target/release/edge build
//! ```
//!
//! See `README.md` for why the `wasm32-wasip2` target alone is
//! insufficient (wasi:http@0.2.4 vs 0.2.1 mismatch with wasmtime 45.0.3).
//!
//! ## Listening port
//!
//! The worker stamps the guest's private upstream port into
//! `EDGE_HTTP_SERVER_PORT` at start time (see
//! `edge-worker/src/supervisor.rs::start_app` line ~2144). The name
//! is mildly wrong for TCP but the semantics — "the worker port your
//! server should listen on" — are identical, and the env-var rename
//! would have broken every existing guest. So hello-tcp just reads
//! the same env var the HTTP sample does.
//!
//! After the worker publishes the heartbeat, `edge-ingress` learns
//! about the new app via `apps.layer4` and the operator-routed public
//! port is reachable via the CP `GET /api/v1/apps/{appName}/l4-port`
//! response.

#![no_main]

wit_bindgen::generate!({
    world: "edge-runtime",
    // Canonical wit-bindgen-compatible WIT lives at the repo root
    // (`wit/edge-cloud.wit` + `wit/deps/*`). See samples/hello/src/lib.rs
    // header for the full rationale on the repo-root vs runtime-tree
    // separation.
    path: "../../wit",
    generate_all,
});

use crate::edge::cloud::observe;
use crate::edge::cloud::process;
use crate::wasi::io::streams::{InputStream, OutputStream, StreamError};
use crate::wasi::sockets::instance_network;
use crate::wasi::sockets::network::{IpAddressFamily, IpSocketAddress, Ipv4SocketAddress, Network};
use crate::wasi::sockets::tcp::TcpSocket;
use crate::wasi::sockets::tcp_create_socket;

/// Well-known RESP reply to `PING`. Pulled out of the hot path so the
/// per-write loop is allocation-free.
const PONG_REPLY: &[u8] = b"+PONG\r\n";

/// Catch-all RESP error reply. Sent when the client sends anything
/// that isn't the single `PING\r\n` command this sample supports.
const ERR_REPLY: &[u8] = b"-ERR unknown command\r\n";

/// Max bytes we'll buffer waiting for a complete `\r\n`-terminated
/// line. PING's wire form is 6 bytes; 64 leaves room for the first
/// few bytes of any longer command without an unbounded growth
/// vector attack from a malicious or buggy client.
const MAX_LINE_BYTES: usize = 64;

/// Tag every `edge:cloud/observe` log line with a `target=hello-tcp`
/// label so the operator can `grep` for the sample's logs in the
/// control-plane's log forwarder without grep -v'ing the rest of
/// the platform.
const LOG_TARGET: &str = "hello-tcp";

/// Convenience wrapper around `observe::emit_log` that prefixes the
/// structured fields every record here uses. The guest doesn't have
/// a stdout — `tracing` won't help — so the canonical
/// guest-to-control-plane log channel is `edge:cloud/observe` (which
/// the host records via the `LogForwarder` → `POST /api/internal/logs`
/// pipeline described in `CLAUDE.md`'s edge-worker section).
fn log_error(message: &str) {
    observe::emit_log("error", message, &[(LOG_TARGET.into(), LOG_TARGET.into())]);
}
fn log_warn(message: &str) {
    observe::emit_log("warn", message, &[(LOG_TARGET.into(), LOG_TARGET.into())]);
}

/// Drain `bytes` into `out` one chunk at a time. `blocking_write_and_flush`
/// accepts up to 4096 bytes per call and returns `result<_, stream-error>`
/// — the success arm carries no payload, so we just check the `Result`.
fn write_all(out: &mut OutputStream, mut bytes: &[u8]) -> Result<(), StreamError> {
    while !bytes.is_empty() {
        // The wasi:io `OutputStream::blocking_write_and_flush` is
        // permitted to return a short write under backpressure — drop
        // the leading N bytes and keep looping. The 4096-byte chunk
        // limit keeps a single slow consumer from monopolizing the
        // thread.
        let chunk_len = bytes.len().min(4096);
        let chunk = &bytes[..chunk_len];
        out.blocking_write_and_flush(chunk)?;
        bytes = &bytes[chunk_len..];
    }
    Ok(())
}

/// Per-connection handler. Reads bytes off `input` until either a
/// full line (`\r\n`-terminated) matches `PING\r\n` — in which case
/// it responds with `+PONG\r\n` — or the client closes the stream
/// (a benign half-close scenario).
///
/// The accept loop is single-threaded per the README — the sample's
/// job is to be readable, not to demonstrate `tokio::spawn` against
/// `wasi:sockets`. A production listener would lift this into a pool
/// of wasi pollables and dispatch each accept onto its own wasi-socket
/// stream pair, with a per-connection `tokio::task` driving the I/O.
fn handle_connection(input: InputStream, mut output: OutputStream) {
    let mut line: [u8; MAX_LINE_BYTES] = [0; MAX_LINE_BYTES];
    let mut len = 0usize;

    loop {
        // `InputStream::read(len)` returns up to `len` bytes worth of
        // data the host currently has buffered for this connection;
        // it does not block waiting for more. We loop until either the
        // client closes the stream or we see a newline. `read` (as
        // opposed to `blocking_read`) is the right primitive here —
        // the supervisor's epoch ticker is what bounds execution.
        let chunk = match input.read(1) {
            Ok(c) => c,
            Err(StreamError::Closed) => break,
            Err(e) => {
                log_error(&format!("hello-tcp read error: {e:?}"));
                break;
            }
        };
        if chunk.is_empty() {
            // Empty chunk with no error — peer closed. Benign.
            break;
        }
        if len + chunk.len() > MAX_LINE_BYTES {
            // Defensive cap. We don't try to recover the connection
            // because we can't push the read pointer back; just send
            // an ERR reply and let the client close.
            let _ = write_all(&mut output, ERR_REPLY);
            let _ = output.flush();
            return;
        }
        line[len..len + chunk.len()].copy_from_slice(&chunk);
        len += chunk.len();

        // Look for a complete `\r\n` terminator.
        if let Some(term_idx) = line[..len].windows(2).position(|w| w == b"\r\n") {
            // Examine the line up to (but not including) the CRLF.
            let cmd = &line[..term_idx];
            let reply: &[u8] = if cmd.eq_ignore_ascii_case(b"PING") {
                PONG_REPLY
            } else {
                ERR_REPLY
            };
            if write_all(&mut output, reply).is_err() {
                break;
            }
            // Reset the buffer for the next command on this connection.
            line = [0; MAX_LINE_BYTES];
            len = 0;
        }
    }

    // Best-effort final flush so the client's last PONG isn't sitting
    // in a buffer that the `InputStream`/`OutputStream` shutdown
    // might or might not push out, depending on the host's
    // teardown order.
    let _ = output.flush();
}

/// Bind a TCP socket on the worker-supplied port (read from
/// `EDGE_HTTP_SERVER_PORT`), put it into the listening state, and
/// hand the listener back. The caller owns the accept loop.
fn listen_on_worker_port() -> Result<TcpSocket, String> {
    let port_str = process::get_env("EDGE_HTTP_SERVER_PORT")
        .ok_or_else(|| "EDGE_HTTP_SERVER_PORT not stamped by the worker".to_string())?;
    let port: u16 = port_str
        .parse()
        .map_err(|e| format!("EDGE_HTTP_SERVER_PORT='{port_str}' is not a u16: {e}"))?;

    // The world's `wasi:sockets/imports` exposes `instance-network`
    // — a 0-argument function returning the host's default Network
    // capability handle. wasmtime 45.0.3 wires this via
    // `wasmtime_wasi::p2::add_to_linker_async` to give the guest a
    // borrow of the singleton network.
    let network: Network = instance_network::instance_network();

    let socket = tcp_create_socket::create_tcp_socket(IpAddressFamily::Ipv4)
        .map_err(|e| format!("create_tcp_socket failed: {e:?}"))?;

    // WASI sockets use an explicit two-phase bind for permission
    // gating (start-bind/finish-bind). The wasmtime host collapses
    // both phases into a single syscall under the hood, so
    // start-bind returning `ok` on the first try is the common path.
    // The finish-bind `would-block` branch matters for an interactive
    // host that wants to inject a permission prompt before the
    // bind becomes visible — wasmtime 45.0.3 is not that host.
    let addr = IpSocketAddress::Ipv4(Ipv4SocketAddress {
        // (0, 0, 0, 0) — leave it to the kernel to pick the
        // interface.
        address: (0, 0, 0, 0),
        port,
    });
    socket
        .start_bind(&network, addr)
        .map_err(|e| format!("start_bind failed: {e:?}"))?;
    socket
        .finish_bind()
        .map_err(|e| format!("finish_bind failed: {e:?}"))?;
    socket
        .start_listen()
        .map_err(|e| format!("start_listen failed: {e:?}"))?;
    socket
        .finish_listen()
        .map_err(|e| format!("finish_listen failed: {e:?}"))?;
    Ok(socket)
}

/// The world entry point. The supervisor calls this once and never
/// again — returning from `start` is the guest's signal that it has
/// exited cleanly (or that it wants a restart).
fn start() -> Result<(), String> {
    let socket = match listen_on_worker_port() {
        Ok(s) => s,
        Err(e) => {
            log_error(&format!("hello-tcp: bind failed: {e}"));
            // Returning `Err` here is the long-running world's "I
            // couldn't start" signal — the supervisor logs it and
            // either restarts us (within the backoff budget) or marks
            // the app `Crashed`. Same shape every long-running
            // component uses.
            return Err(e);
        }
    };

    loop {
        match socket.accept() {
            Ok((_client_socket, input, output)) => {
                handle_connection(input, output);
                // `client_socket` is dropped here; the streams are
                // consumed by `handle_connection`. `wasi:sockets`
                // closes the underlying fd on drop.
            }
            Err(e) => {
                log_warn(&format!("hello-tcp: accept error: {e:?}"));
                // Loop continues; without this the supervisor could
                // trip its runaway-task detector if `accept` returns
                // the same error in a hot loop.
            }
        }
    }
}

struct HelloTcp;

impl Guest for HelloTcp {
    fn start() {
        // We never return to the supervisor — `start` is a permanent
        // listener loop — so the `Result` we get from our internal
        // `start()` helper is consumed with `let _ =` rather than `?`.
        // A production service would either wrap this in a fresh
        // supervisor-friendly `start: func() -> Result<(), ...>` or
        // accept that "no error path" is a property of the listener
        // design rather than something to bubble up.
        let _ = start();
    }
}

// `wasi:cli/run` is part of the `edge-runtime` world (it pulls in
// `wasi:cli/command`), but the supervisor's long-running path
// never calls it — `edge-worker/src/supervisor.rs::execute_app`
// dispatches per-app via `instance.get_typed_func("start")` and
// awaits the result. We still have to provide a stub so wit-bindgen
// generates the run export and the linker resolves it at component
// load.
impl crate::exports::wasi::cli::run::Guest for HelloTcp {
    fn run() -> Result<(), ()> {
        // Unreachable: the long-running supervisor path goes through
        // `start`. If a misconfigured host does call `run` instead,
        // returning `Err` makes the failure visible in the supervisor
        // logs rather than silently exiting 0 (which would look like
        // a clean shutdown to the supervisor's backoff machinery).
        Err(())
    }
}

export!(HelloTcp);
