//! `redis-lite` — minimal RESP server for edgeCloud's L4/TCP long-running
//! guest path. Speaks RESP2 over raw TCP via `wasi:sockets/tcp` and
//! persists SET/GET/DEL state through `edge:cloud/kv-store`, so values
//! survive a worker restart (per #495's restart-persistence requirement).
//!
//! ## Commands
//!
//! - `PING`                       → `+PONG\r\n`
//! - `ECHO <msg>`                 → bulk-string reply carrying `<msg>`
//! - `SET <key> <value>`          → `+OK\r\n` (persistent, no TTL)
//! - `GET <key>`                  → bulk-string reply or `$-1\r\n` for missing
//! - `DEL <key>`                  → `:1\r\n` if it existed, `:0\r\n` if not
//!
//! Anything else → `-ERR unknown command\r\n`. Wrong arity → `-ERR wrong arity\r\n`.
//!
//! ## Build
//!
//! ```sh
//! cd samples/redis-lite
//! ../../target/release/edge build
//! ```
//!
//! That command runs `cargo build --target wasm32-unknown-unknown --release`
//! and then `wasm-tools component new <core> -o target/component.wasm`. The
//! wrapped `target/component.wasm` is what `edge deploy` uploads.
//!
//! ## Listening port
//!
//! Same as `samples/hello-tcp`: the worker stamps `EDGE_HTTP_SERVER_PORT`
//! at start time (see `edge-worker/src/supervisor.rs::start_app`). The
//! env-var name is mildly wrong for TCP but the semantics — "the worker
//! port your server should listen on" — are identical. See the matching
//! long-form explanation in `samples/hello-tcp/src/lib.rs`.
//!
//! ## Why a long-running (FaaS-shaped) world and not a handler?
//!
//! A raw-TCP listener has to own its socket for the duration of the
//! process. The `edge-runtime-handler` (FaaS) world has no
//! `wasi:sockets/tcp` access from the guest — the host owns the socket
//! and the guest is invoked once per HTTP request. `edge-runtime`
//! exposes the full `wasi:cli/command@0.2.1` family (the workspace
//! `wit/edge-cloud.wit` `include`s it), so the guest can bind + accept
//! itself.
//!
//! ## Persistence note
//!
//! `edge:cloud/kv-store` is scoped per-tenant — values written by this
//! guest live in `<EDGE_KV_STORE_PATH>/<tenant_id>/store.json` on the
//! worker. Two apps of the same tenant share the same key namespace
//! (issue #558). Values are base64-encoded JSON on disk; the host's
//! `KvStore` is responsible for the atomic-rename flush.

#![no_main]

wit_bindgen::generate!({
    world: "edge-runtime",
    // Canonical wit-bindgen-compatible WIT lives at the repo root
    // (`wit/edge-cloud.wit` + `wit/deps/*`). See samples/hello-tcp/src/lib.rs
    // header for the full rationale on the repo-root vs runtime-tree
    // separation.
    path: "../../wit",
    generate_all,
});

// RESP parser lives in its own sibling crate (`samples/redis-lite/resp-parser/`)
// so the unit tests can run on the host — the parent crate is
// `#![no_main]` for the WASM cdylib, which prevents `cargo test` from
// linking a test binary. Implementation is duplicated byte-for-byte
// across the two files; keep them in sync.
use redis_lite_resp as resp;

use crate::edge::cloud::kv_store;
use crate::edge::cloud::observe;
use crate::edge::cloud::process;
use crate::wasi::io::streams::{InputStream, OutputStream, StreamError};
use crate::wasi::sockets::instance_network;
use crate::wasi::sockets::network::{
    IpAddressFamily, IpSocketAddress, Ipv4SocketAddress, Network,
};
use crate::wasi::sockets::tcp::TcpSocket;
use crate::wasi::sockets::tcp_create_socket;

/// Tag every `edge:cloud/observe` log line with `target=redis-lite` so the
/// operator can `grep` for the sample's logs in the control-plane's log
/// forwarder without grep -v'ing the rest of the platform.
const LOG_TARGET: &str = "redis-lite";

/// Catch-all RESP error reply. Sent when the client sends a command name
/// we don't recognize.
const ERR_UNKNOWN_CMD: &[u8] = b"-ERR unknown command\r\n";

/// Wrong-arity reply.
const ERR_WRONG_ARITY: &[u8] = b"-ERR wrong number of arguments\r\n";

/// Desync reply — the wire bytes stopped being valid RESP mid-stream.
const ERR_BAD_PROTOCOL: &[u8] = b"-ERR bad protocol\r\n";

/// Maximum bytes we'll buffer while waiting for a complete RESP frame.
/// A SET with a 16 MiB value is plausible; 64 MiB leaves room without
/// giving a malicious client an unbounded growth vector. Bulk reads
/// larger than this trigger an error reply and a connection close.
const MAX_BULK_BYTES: usize = 64 * 1024 * 1024;

/// Convenience wrapper around `observe::emit_log` — the guest doesn't
/// have a stdout (the supervisor's epoch ticker bounds execution),
/// so `edge:cloud/observe` is the canonical guest-to-control-plane log
/// channel (`LogForwarder` → `POST /api/internal/logs`).
fn log_warn(message: &str) {
    observe::emit_log("warn", message, &[(LOG_TARGET.into(), LOG_TARGET.into())]);
}
fn log_error(message: &str) {
    observe::emit_log("error", message, &[(LOG_TARGET.into(), LOG_TARGET.into())]);
}

/// Drain `bytes` into `out` one chunk at a time. `blocking_write_and_flush`
/// accepts up to 4096 bytes per call and returns `result<_, stream-error>`.
/// Under backpressure the implementation is permitted to short-write —
/// we just keep dropping the leading N bytes until the buffer is empty.
fn write_all(out: &mut OutputStream, mut bytes: &[u8]) -> Result<(), StreamError> {
    while !bytes.is_empty() {
        let chunk_len = bytes.len().min(4096);
        let chunk = &bytes[..chunk_len];
        out.blocking_write_and_flush(chunk)?;
        bytes = &bytes[chunk_len..];
    }
    Ok(())
}

/// Read bytes off `input` until either (a) `predicate` returns true on
/// the current buffer or (b) the peer closes / errors. Used by
/// `handle_connection` to keep feeding the parser between frames.
fn read_until<F: Fn(&[u8]) -> bool>(
    input: &InputStream,
    buf: &mut Vec<u8>,
    predicate: F,
) -> Result<(), ()> {
    loop {
        if predicate(buf) {
            return Ok(());
        }
        if buf.len() > MAX_BULK_BYTES {
            log_error(&format!(
                "redis-lite: client exceeded MAX_BULK_BYTES={MAX_BULK_BYTES}, closing"
            ));
            return Err(());
        }
        // `read(len: u64)` returns `result<list<u8>, stream-error>` per
        // `wit/deps/io/streams.wit` — the bindgen expands it to
        // `Result<Vec<u8>, StreamError>`. Ask for up to 4096 bytes; the
        // host may short-read (returns a `Vec<u8>` shorter than the
        // requested `len`).
        let chunk = [0u8; 4096];
        let chunk = match input.read(chunk.len() as u64) {
            Ok(c) => c,
            Err(StreamError::Closed) => return Err(()),
            Err(e) => {
                log_error(&format!("redis-lite read error: {e:?}"));
                return Err(());
            }
        };
        if chunk.is_empty() {
            // Empty chunk with no error — peer closed. Benign.
            return Err(());
        }
        buf.extend_from_slice(&chunk);
    }
}

/// Per-connection handler. Reads bytes off `input`, feeds them to the
/// RESP parser, dispatches each command to the kv-store-backed handler,
/// and writes the reply to `output`. Loops until the peer closes or the
/// buffer cap is exceeded.
fn handle_connection(input: InputStream, mut output: OutputStream) {
    let mut buf: Vec<u8> = Vec::with_capacity(4096);

    loop {
        // Pull bytes until the parser can extract at least one frame.
        if let Err(()) = read_until(&input, &mut buf, |b| {
            matches!(resp::parse(b), Ok(_))
        }) {
            // Peer closed or buffer overflow. Either way, exit cleanly.
            let _ = output.flush();
            return;
        }

        let (frame, rest) = match resp::parse(&buf) {
            Ok(pair) => pair,
            Err(resp::Error::BadProtocol) => {
                let _ = write_all(&mut output, ERR_BAD_PROTOCOL);
                let _ = output.flush();
                return;
            }
            Err(resp::Error::Incomplete) => {
                // Shouldn't happen — `read_until` gated on the parser
                // succeeding. Treat as a desync and close.
                log_error("redis-lite: parser reported Incomplete after read_until gate");
                return;
            }
        };
        buf = rest.to_vec();

        match frame {
            resp::Frame::Array(args) if !args.is_empty() => {
                dispatch_command(args, &mut output);
            }
            _ => {
                let _ = write_all(&mut output, ERR_BAD_PROTOCOL);
            }
        }
    }
}

/// Resolve a single argument from a command array as `&str`. Returns
/// `Err(())` if the arg isn't a bulk string (the only RESP type that
/// makes sense for a command or key word).
fn arg_as_str<'a>(frame: &'a resp::Frame) -> Result<&'a str, ()> {
    match frame {
        resp::Frame::Bulk(Some(bytes)) => std::str::from_utf8(bytes).map_err(|_| ()),
        _ => Err(()),
    }
}

/// Dispatch a single command to its handler. Each branch is responsible
/// for writing exactly one RESP reply (or `ERR_WRONG_ARITY` /
/// `ERR_BAD_PROTOCOL` on the failure path). The connection handler
/// loops back to read the next frame after this returns.
fn dispatch_command(args: Vec<resp::Frame>, output: &mut OutputStream) {
    let cmd = match arg_as_str(&args[0]) {
        Ok(s) => s.to_ascii_uppercase(),
        Err(()) => {
            let _ = write_all(output, ERR_BAD_PROTOCOL);
            return;
        }
    };

    match cmd.as_str() {
        "PING" => {
            let _ = write_all(output, b"+PONG\r\n");
        }
        "ECHO" => {
            if args.len() != 2 {
                let _ = write_all(output, ERR_WRONG_ARITY);
                return;
            }
            match &args[1] {
                resp::Frame::Bulk(Some(b)) => {
                    let mut reply = Vec::with_capacity(b.len() + 16);
                    reply.extend_from_slice(format!("${}\r\n", b.len()).as_bytes());
                    reply.extend_from_slice(b);
                    reply.extend_from_slice(b"\r\n");
                    let _ = write_all(output, &reply);
                }
                _ => {
                    let _ = write_all(output, ERR_BAD_PROTOCOL);
                }
            }
        }
        "SET" => {
            if args.len() != 3 {
                let _ = write_all(output, ERR_WRONG_ARITY);
                return;
            }
            let (k, v) = match (&args[1], &args[2]) {
                (resp::Frame::Bulk(Some(k)), resp::Frame::Bulk(Some(v))) => (k, v),
                _ => {
                    let _ = write_all(output, ERR_BAD_PROTOCOL);
                    return;
                }
            };
            let key = match std::str::from_utf8(k) {
                Ok(s) => s,
                Err(_) => {
                    let _ = write_all(output, ERR_BAD_PROTOCOL);
                    return;
                }
            };
            // `kv_store::set` takes `&str` + `&[u8]` + `Option<u32>` —
            // pass `None` for persistent (per #495 restart persistence
            // requirement).
            kv_store::set(key, v, None);
            let _ = write_all(output, b"+OK\r\n");
        }
        "GET" => {
            if args.len() != 2 {
                let _ = write_all(output, ERR_WRONG_ARITY);
                return;
            }
            let key_bytes = match &args[1] {
                resp::Frame::Bulk(Some(b)) => b,
                _ => {
                    let _ = write_all(output, ERR_BAD_PROTOCOL);
                    return;
                }
            };
            let key = match std::str::from_utf8(key_bytes) {
                Ok(s) => s,
                Err(_) => {
                    let _ = write_all(output, ERR_BAD_PROTOCOL);
                    return;
                }
            };
            match kv_store::get(key) {
                Some(v) => {
                    let mut reply = Vec::with_capacity(v.len() + 16);
                    reply.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
                    reply.extend_from_slice(&v);
                    reply.extend_from_slice(b"\r\n");
                    let _ = write_all(output, &reply);
                }
                None => {
                    let _ = write_all(output, b"$-1\r\n");
                }
            }
        }
        "DEL" => {
            if args.len() != 2 {
                let _ = write_all(output, ERR_WRONG_ARITY);
                return;
            }
            let key_bytes = match &args[1] {
                resp::Frame::Bulk(Some(b)) => b,
                _ => {
                    let _ = write_all(output, ERR_BAD_PROTOCOL);
                    return;
                }
            };
            let key = match std::str::from_utf8(key_bytes) {
                Ok(s) => s,
                Err(_) => {
                    let _ = write_all(output, ERR_BAD_PROTOCOL);
                    return;
                }
            };
            // `kv_store::delete` doesn't tell us whether the key existed,
            // so we check `exists()` first to return the correct `:1` / `:0`.
            let existed = kv_store::exists(key);
            kv_store::delete(key);
            let _ = write_all(output, if existed { b":1\r\n" } else { b":0\r\n" });
        }
        _ => {
            let _ = write_all(output, ERR_UNKNOWN_CMD);
        }
    }
}

/// Bind a TCP socket on the worker-supplied port (read from
/// `EDGE_HTTP_SERVER_PORT`), put it into the listening state, and hand
/// the listener back. The caller owns the accept loop.
fn listen_on_worker_port() -> Result<TcpSocket, String> {
    let port_str = process::get_env("EDGE_HTTP_SERVER_PORT")
        .ok_or_else(|| "EDGE_HTTP_SERVER_PORT not stamped by the worker".to_string())?;
    let port: u16 = port_str
        .parse()
        .map_err(|e| format!("EDGE_HTTP_SERVER_PORT='{port_str}' is not a u16: {e}"))?;

    let network: Network = instance_network::instance_network();

    let socket = tcp_create_socket::create_tcp_socket(IpAddressFamily::Ipv4)
        .map_err(|e| format!("create_tcp_socket failed: {e:?}"))?;

    let addr = IpSocketAddress::Ipv4(Ipv4SocketAddress {
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
            log_error(&format!("redis-lite: bind failed: {e}"));
            return Err(e);
        }
    };

    loop {
        match socket.accept() {
            // `socket.accept()` returns `(client_socket, input, output)`.
            // `client_socket` is dropped here; the streams are consumed
            // by `handle_connection`. `wasi:sockets` closes the underlying
            // fd on drop.
            Ok((_client_socket, input, output)) => {
                handle_connection(input, output);
            }
            Err(e) => {
                log_warn(&format!("redis-lite: accept error: {e:?}"));
                // Loop continues; without this the supervisor could
                // trip its runaway-task detector if `accept` returns the
                // same error in a hot loop.
            }
        }
    }
}

struct RedisLite;

impl Guest for RedisLite {
    fn start() {
        // We never return to the supervisor — `start` is a permanent
        // listener loop — so the `Result` we get from our internal
        // `start()` helper is consumed with `let _ =` rather than `?`.
        let _ = start();
    }
}

// `wasi:cli/run` is part of the `edge-runtime` world (it pulls in
// `wasi:cli/command`), but the supervisor's long-running path never
// calls it — `edge-worker/src/supervisor.rs::execute_app` dispatches
// per-app via `instance.get_typed_func("start")` and awaits the result.
// We still have to provide a stub so wit-bindgen generates the run
// export and the linker resolves it at component load.
impl crate::exports::wasi::cli::run::Guest for RedisLite {
    fn run() -> Result<(), ()> {
        // Unreachable: the long-running supervisor path goes through
        // `start`. If a misconfigured host does call `run` instead,
        // returning `Err` makes the failure visible in the supervisor
        // logs rather than silently exiting 0.
        Err(())
    }
}

export!(RedisLite);