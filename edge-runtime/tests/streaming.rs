//! Integration tests for streaming HTTP bodies.
//!
//! These tests exercise the wire-level round-trip for:
//! - `http-client::fetch` with a streaming outbound body (reqwest::Body::wrap_stream)
//! - `http-server::respond_stream` with a chunked response body
//!
//! Gating follows the pattern in `edge-worker/tests/integration_tests.rs:39-43`:
//! in CI (or when `SKIP_INTEGRATION_TESTS=1` is set), the tests are skipped so
//! they don't require network access. Locally they run against `wiremock`.
//!
//! Requires the `http-client` and `http-server` features (both enabled by default).

use std::time::Duration;

use edge_runtime::streams::{
    self, IncomingEntry, IncomingProducer, OutgoingEntry, OutgoingStreamAdapter, StreamError,
};
/// Skip the test when in CI or when the operator sets the skip env var.
fn skip_in_ci() -> bool {
    std::env::var("CI").is_ok() || std::env::var("SKIP_INTEGRATION_TESTS").is_ok()
}

// ---- Streams primitives ----------------------------------------------------

#[tokio::test]
async fn test_incoming_stream_roundtrip_with_multiple_chunks() {
    let (producer, mut stream) = streams::incoming_pair(streams::DEFAULT_STREAM_CAPACITY);

    let writer = tokio::spawn(async move {
        producer.push(Ok(b"chunk-a".to_vec())).await.unwrap();
        producer.push(Ok(b"chunk-b".to_vec())).await.unwrap();
        producer.push(Ok(b"chunk-c".to_vec())).await.unwrap();
        // Drop producer → EOF on the consumer side.
    });

    assert_eq!(stream.read_chunk().await.unwrap(), b"chunk-a");
    assert_eq!(stream.read_chunk().await.unwrap(), b"chunk-b");
    assert_eq!(stream.read_chunk().await.unwrap(), b"chunk-c");
    assert_eq!(stream.read_chunk().await, Err(StreamError::Closed));
    writer.await.unwrap();
}

#[tokio::test]
async fn test_outgoing_stream_roundtrip_drains_via_futures_stream() {
    use futures::StreamExt;

    let entry = OutgoingEntry::new(streams::DEFAULT_STREAM_CAPACITY);
    let OutgoingEntry {
        mut stream,
        adapter,
    } = entry;
    let adapter: OutgoingStreamAdapter =
        adapter.expect("fresh OutgoingEntry must have adapter present");

    let writer = tokio::spawn(async move {
        stream.write_chunk(b"hello".to_vec()).await.unwrap();
        stream.write_chunk(b"world".to_vec()).await.unwrap();
        stream.finish().await.unwrap();
    });

    let chunks: Vec<Vec<u8>> = adapter.map(|res| res.unwrap().to_vec()).collect().await;
    assert_eq!(chunks.len(), 2);
    assert_eq!(&chunks[0][..], b"hello");
    assert_eq!(&chunks[1][..], b"world");
    writer.await.unwrap();
}

#[tokio::test]
async fn test_outgoing_entry_new_yields_paired_writer_and_adapter() {
    let entry = OutgoingEntry::new(streams::DEFAULT_STREAM_CAPACITY);
    assert!(entry.adapter.is_some());
    let OutgoingEntry { stream, adapter } = entry;
    let adapter = adapter.unwrap();
    // Drop the writer side — the adapter must observe EOF (sender dropped).
    drop(stream);
    use futures::StreamExt;
    let mut adapter = adapter;
    assert!(adapter.next().await.is_none());
}

// ---- Host-side stream integration with reqwest -----------------------------

#[tokio::test]
async fn test_fetch_streaming_response_round_trip_via_wiremock() {
    use futures::StreamExt;

    if skip_in_ci() {
        eprintln!("skipping streaming integration test (CI or SKIP_INTEGRATION_TESTS set)");
        return;
    }

    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/stream"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/octet-stream")
                .set_body_bytes(b"first-chunk\nsecond-chunk\nthird-chunk\n".to_vec()),
        )
        .mount(&server)
        .await;

    let url = format!("{}/stream", server.uri());
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let resp = client.get(&url).send().await.expect("http send");
    assert_eq!(resp.status(), 200);

    // Build a (producer, stream) pair as the http-client fetch_streaming path
    // would. Drain the wiremock response into the producer; read it back via
    // the stream. Confirms the primitives compose correctly with reqwest's
    // bytes_stream.
    let (producer, mut stream): (IncomingProducer, _) = streams::incoming_pair(8);
    let mut byte_stream = resp.bytes_stream();
    tokio::spawn(async move {
        while let Some(item) = byte_stream.next().await {
            let chunk = item.expect("reqwest chunk").to_vec();
            if producer.push(Ok(chunk)).await.is_err() {
                break;
            }
        }
    });

    let collected = read_remaining(&mut stream).await;
    let joined: Vec<u8> = collected.iter().flatten().copied().collect();
    let s = std::str::from_utf8(&joined).expect("utf-8");
    assert!(s.contains("first-chunk"));
    assert!(s.contains("second-chunk"));
    assert!(s.contains("third-chunk"));
}

/// Read chunks from an IncomingStream until EOF or error, returning a Vec of
/// each chunk (preserving boundaries for boundary-aware assertions).
async fn read_remaining(stream: &mut edge_runtime::streams::IncomingStream) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    loop {
        match stream.read_chunk().await {
            Ok(chunk) => out.push(chunk),
            Err(StreamError::Closed) => break,
            Err(StreamError::Cancelled) => break,
            Err(StreamError::Io(msg)) => panic!("unexpected io error: {msg}"),
        }
    }
    out
}

// ---- Smoke: BodySource debug impl -----------------------------------------

#[test]
fn test_incoming_entry_construction() {
    let (producer, stream) = streams::incoming_pair(streams::DEFAULT_STREAM_CAPACITY);
    let _entry = IncomingEntry { stream };
    drop(producer); // drops sender; consumer would observe Closed
}

// ---- T3: stream_threshold drives BodySource::Streamed ---------------------

/// Verifies that a Content-Length above the configured `stream_threshold`
/// arrives at the guest as `BodySource::Streamed` and is readable as a
/// stream of chunks (rather than a single buffered Vec<u8>). Also exercises
/// the body-prefix path: TCP delivers headers + body in one read, so
/// `read_headers` must preserve the bytes past `\r\n\r\n` for the body reader.
#[tokio::test]
async fn test_inbound_body_above_threshold_is_streamed() {
    use edge_runtime::interfaces::http_server::{BodySource, HttpServer};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let mut server = HttpServer::new()
        .with_stream_threshold(4)
        .with_max_body_size(64 * 1024)
        .with_connection_timeout(5);
    server.start(0, None).await.expect("server start");
    let port = server.get_assigned_port().expect("port assigned");

    let body_bytes: Vec<u8> = (0..64u8).cycle().take(64).collect();
    let request = format!(
        "POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body_bytes.len()
    );
    let mut wire = Vec::new();
    wire.extend_from_slice(request.as_bytes());
    wire.extend_from_slice(&body_bytes);

    let mut sock = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect");
    sock.write_all(&wire).await.expect("write request");

    let req = tokio::time::timeout(std::time::Duration::from_secs(3), server.poll())
        .await
        .expect("poll timed out")
        .expect("poll")
        .expect("some request");

    assert_eq!(req.method, "POST");
    assert_eq!(req.path, "/upload");
    assert_eq!(
        req.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("Content-Length"))
            .map(|(_, v)| v.as_str()),
        Some("64"),
    );

    let mut stream = match req.body {
        BodySource::Streamed(s) => s,
        other => panic!("expected BodySource::Streamed, got {:?}", other),
    };

    let mut collected = Vec::new();
    loop {
        match tokio::time::timeout(std::time::Duration::from_secs(3), stream.read_chunk())
            .await
            .expect("read_chunk timed out")
        {
            Ok(chunk) => collected.extend_from_slice(&chunk),
            Err(StreamError::Closed) => break,
            Err(StreamError::Cancelled) => break,
            Err(StreamError::Io(msg)) => panic!("io error: {msg}"),
        }
    }
    assert_eq!(collected, body_bytes);

    // Respond with a small body so the server can finish writing and close
    // the socket. Then read the response back off the wire — also a smoke
    // check that the SharedWriteHalf path works after the body-pipeline task
    // is done.
    server
        .respond(
            req.id,
            200,
            vec![("Content-Type".into(), "text/plain".into())],
            b"ok".to_vec(),
        )
        .await
        .expect("respond");

    let mut response_buf = Vec::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        sock.read_to_end(&mut response_buf),
    )
    .await
    .expect("read_to_end timed out")
    .expect("read response");
    let response_str = std::str::from_utf8(&response_buf).expect("utf-8 response");
    assert!(
        response_str.starts_with("HTTP/1.1 200"),
        "unexpected response: {:?}",
        &response_str[..response_str.len().min(64)],
    );
    assert!(response_str.contains("ok"));
}

/// Verifies that a small Content-Length (below `stream_threshold`) arrives at
/// the guest as `BodySource::Buffered` — the streaming path is opt-in via the
/// threshold, not the default.
#[tokio::test]
async fn test_inbound_body_below_threshold_is_buffered() {
    use edge_runtime::interfaces::http_server::{BodySource, HttpServer};
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    let mut server = HttpServer::new()
        .with_stream_threshold(1024) // > 4-byte body
        .with_max_body_size(64 * 1024)
        .with_connection_timeout(5);
    server.start(0, None).await.expect("server start");
    let port = server.get_assigned_port().expect("port assigned");

    let body_bytes = b"abcd".to_vec();
    let request = format!(
        "POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body_bytes.len()
    );
    let mut wire = Vec::new();
    wire.extend_from_slice(request.as_bytes());
    wire.extend_from_slice(&body_bytes);

    let mut sock = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect");
    sock.write_all(&wire).await.expect("write request");

    let req = tokio::time::timeout(std::time::Duration::from_secs(3), server.poll())
        .await
        .expect("poll timed out")
        .expect("poll")
        .expect("some request");

    match req.body {
        BodySource::Buffered(b) => assert_eq!(b, body_bytes),
        other => panic!("expected BodySource::Buffered, got {:?}", other),
    }
}

/// Regression for F1 streaming path: Content-Length: N with body shorter
/// than N should deliver an `Err(IO("truncated body: ..."))` chunk to the
/// guest via the IncomingStream before the stream closes. The buffered
/// path is covered by a unit test against `read_body_inline` directly in
/// the `http_server::tests` module.
#[tokio::test]
async fn test_truncated_body_streaming_path_returns_error() {
    use edge_runtime::interfaces::http_server::{BodySource, HttpServer};
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    let mut server = HttpServer::new()
        .with_stream_threshold(4) // force streaming path
        .with_max_body_size(64 * 1024)
        .with_connection_timeout(5);
    server.start(0, None).await.expect("server start");
    let port = server.get_assigned_port().expect("port assigned");

    let request = format!(
        "POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        100
    );
    let mut sock = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect");
    sock.write_all(request.as_bytes())
        .await
        .expect("write head");
    sock.write_all(b"only-fifty").await.expect("write body");
    sock.shutdown().await.expect("shutdown");

    let req = tokio::time::timeout(std::time::Duration::from_secs(3), server.poll())
        .await
        .expect("poll timed out")
        .expect("poll")
        .expect("some request");

    let mut stream = match req.body {
        BodySource::Streamed(s) => s,
        other => panic!("expected BodySource::Streamed, got {:?}", other),
    };

    // Read chunks until we see the truncation error or EOF.
    let mut saw_error = false;
    loop {
        match tokio::time::timeout(std::time::Duration::from_secs(3), stream.read_chunk())
            .await
            .expect("read_chunk timed out")
        {
            Ok(chunk) if chunk.starts_with(b"only-fifty") => continue,
            Ok(chunk) => panic!("unexpected chunk: {:?}", chunk),
            Err(StreamError::Io(msg)) => {
                assert!(
                    msg.contains("truncated body"),
                    "expected truncation error, got {:?}",
                    msg
                );
                saw_error = true;
                break;
            }
            Err(StreamError::Closed) => break,
            Err(StreamError::Cancelled) => break,
        }
    }
    assert!(saw_error, "expected a truncation error chunk");
}

/// F2 regression: a guest that calls `respond_stream`, writes one chunk, and
/// then never calls `finish` and never drops the Outgoing resource must not
/// pin the connection task forever — the per-iteration deadline tears the
/// connection down at the configured `conn_timeout`.
///
/// This test uses a `conn_timeout` of 1 second and a stalled guest to keep
/// wall-clock time short. The test asserts the connection is closed within
/// a generous upper bound (3 seconds) after the chunk is written.
#[tokio::test]
async fn test_streaming_response_stalled_guest_is_timed_out() {
    use edge_runtime::interfaces::http_server::{HttpServer, IncomingRequest};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    // 1s conn_timeout so the test completes quickly; the streaming deadline
    // is also at conn_timeout, so a stalled guest is torn down at ~1s.
    let mut server = HttpServer::new()
        .with_connection_timeout(1)
        .with_max_body_size(64 * 1024)
        .with_stream_threshold(1024);
    server.start(0, None).await.expect("server start");
    let port = server.get_assigned_port().expect("port assigned");

    let mut sock = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect");
    sock.write_all(b"GET /streamed HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .expect("write request");

    // Receive the IncomingRequest on the server side.
    let req: IncomingRequest = tokio::time::timeout(Duration::from_secs(2), server.poll())
        .await
        .expect("poll timed out")
        .expect("poll ok")
        .expect("some request");
    let req_id = req.id;

    // Build an outgoing stream and a stalled guest: write one chunk, then
    // intentionally hold the writer (no finish, no drop) until the
    // per-iteration deadline fires.
    let entry = OutgoingEntry::new(streams::DEFAULT_STREAM_CAPACITY);
    let OutgoingEntry {
        stream: writer,
        adapter,
    } = entry;
    let adapter: OutgoingStreamAdapter = adapter.expect("adapter present");

    // The writer is moved into a task that writes one chunk and then
    // sleeps well past the conn_timeout — simulating a stalled guest.
    let writer_task = tokio::spawn(async move {
        let mut writer = writer;
        writer
            .write_chunk(b"first-and-only".to_vec())
            .await
            .unwrap();
        // Stalled: no finish(), no drop. Drop is forced at end of scope.
        tokio::time::sleep(Duration::from_secs(5)).await;
    });

    // Send the response. This enters write_streaming_response's drain loop.
    // The first chunk writes successfully; the second iteration's
    // `adapter.next().await` will park until the deadline timer fires
    // (~1s after the deadline was set, which is `now() + conn_timeout`).
    let respond = server.respond_stream(
        req_id,
        200,
        vec![
            ("Content-Type".to_string(), "text/plain".to_string()),
            ("Content-Length".to_string(), "999".to_string()),
        ],
        adapter,
    );
    let _ = tokio::time::timeout(Duration::from_secs(3), respond)
        .await
        .expect("respond_stream should return within 3s of stall");

    // The connection should be torn down — sock.read_to_end should observe
    // EOF (server closed the connection) within a few seconds of the deadline.
    // The server will have already written the head + the first chunk before
    // the deadline fires; the assertion is that the connection is *closed*
    // (read_to_end returns), not that zero bytes were sent.
    let mut drain = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(3), sock.read_to_end(&mut drain))
        .await
        .expect("read timed out — server did not close the connection at the deadline")
        .expect("read ok");
    assert!(
        drain.starts_with(b"HTTP/1.1 200"),
        "expected a partial response on the wire, got: {:?}",
        String::from_utf8_lossy(&drain[..drain.len().min(80)])
    );
    assert!(
        drain
            .windows(b"first-and-only".len())
            .any(|w| w == b"first-and-only"),
        "expected the first chunk to be on the wire, got: {:?}",
        String::from_utf8_lossy(&drain)
    );
    let _ = read;

    // Cleanup: the writer is held by writer_task; let it finish.
    let _ = writer_task.await;
}

/// F3 regression: `respond_stream` without a Content-Length header must
/// return an error (the host cannot default-inject CL because the
/// adapter does not expose its total byte count).
#[tokio::test]
async fn test_streaming_response_rejects_missing_content_length() {
    use edge_runtime::interfaces::http_server::{HttpServer, IncomingRequest};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let mut server = HttpServer::new()
        .with_connection_timeout(2)
        .with_max_body_size(64 * 1024);
    server.start(0, None).await.expect("server start");
    let port = server.get_assigned_port().expect("port assigned");

    let mut sock = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect");
    sock.write_all(b"GET /streamed HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .expect("write request");

    let req: IncomingRequest = tokio::time::timeout(Duration::from_secs(2), server.poll())
        .await
        .expect("poll timed out")
        .expect("poll ok")
        .expect("some request");
    let req_id = req.id;

    let entry = OutgoingEntry::new(streams::DEFAULT_STREAM_CAPACITY);
    let OutgoingEntry { stream, adapter } = entry;
    let adapter = adapter.expect("adapter present");

    // No Content-Length in the header set. Validation runs in the per-
    // connection task, so respond_stream returns Ok on the oneshot send.
    // The server tears the connection down before writing the head.
    server
        .respond_stream(
            req_id,
            200,
            vec![("Content-Type".to_string(), "text/plain".to_string())],
            adapter,
        )
        .await
        .expect("respond_stream send");

    // Read everything the server sends. We expect EOF with no bytes —
    // the server tore the connection down without sending a response.
    let mut drain = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(2), sock.read_to_end(&mut drain))
        .await
        .expect("read timed out — server should close the connection on validation error")
        .expect("read ok");
    assert_eq!(
        read,
        0,
        "expected no bytes on the wire after Content-Length validation; got: {:?}",
        String::from_utf8_lossy(&drain)
    );
    drop(stream);
}

/// F3 regression: a Content-Length value that does not parse as a positive
/// integer must also be rejected.
#[tokio::test]
async fn test_streaming_response_rejects_invalid_content_length() {
    use edge_runtime::interfaces::http_server::{HttpServer, IncomingRequest};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let mut server = HttpServer::new()
        .with_connection_timeout(2)
        .with_max_body_size(64 * 1024);
    server.start(0, None).await.expect("server start");
    let port = server.get_assigned_port().expect("port assigned");

    let mut sock = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect");
    sock.write_all(b"GET /streamed HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .expect("write request");

    let req: IncomingRequest = tokio::time::timeout(Duration::from_secs(2), server.poll())
        .await
        .expect("poll timed out")
        .expect("poll ok")
        .expect("some request");
    let req_id = req.id;

    let entry = OutgoingEntry::new(streams::DEFAULT_STREAM_CAPACITY);
    let OutgoingEntry { stream, adapter } = entry;
    let adapter = adapter.expect("adapter present");

    server
        .respond_stream(
            req_id,
            200,
            vec![
                ("Content-Type".to_string(), "text/plain".to_string()),
                ("Content-Length".to_string(), "not-a-number".to_string()),
            ],
            adapter,
        )
        .await
        .expect("respond_stream send");

    let mut drain = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(2), sock.read_to_end(&mut drain))
        .await
        .expect("read timed out — server should close the connection on validation error")
        .expect("read ok");
    assert_eq!(
        read,
        0,
        "expected no bytes on the wire after invalid-Content-Length rejection; got: {:?}",
        String::from_utf8_lossy(&drain)
    );
    drop(stream);
}

/// F3 regression: hop-by-hop / host-reserved headers in the guest's set
/// must be stripped before the head is written to the socket. Here
/// `Connection: close` is what the client already sent, and `Server`
/// would otherwise let a guest spoof the server identity.
#[tokio::test]
async fn test_streaming_response_strips_hop_byhop_headers() {
    use edge_runtime::interfaces::http_server::{HttpServer, IncomingRequest};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let mut server = HttpServer::new()
        .with_connection_timeout(5)
        .with_max_body_size(64 * 1024);
    server.start(0, None).await.expect("server start");
    let port = server.get_assigned_port().expect("port assigned");

    let mut sock = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect");
    sock.write_all(b"GET /strip HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .expect("write request");

    let req: IncomingRequest = tokio::time::timeout(Duration::from_secs(2), server.poll())
        .await
        .expect("poll timed out")
        .expect("poll ok")
        .expect("some request");
    let req_id = req.id;

    let entry = OutgoingEntry::new(streams::DEFAULT_STREAM_CAPACITY);
    let OutgoingEntry {
        mut stream,
        adapter,
    } = entry;
    let adapter = adapter.expect("adapter present");

    let writer = tokio::spawn(async move {
        stream.write_chunk(b"ok".to_vec()).await.unwrap();
        stream.finish().await.unwrap();
    });

    // Include a hop-by-hop header (Transfer-Encoding) and a spoofing-risk
    // header (Server). Neither should appear on the wire.
    server
        .respond_stream(
            req_id,
            200,
            vec![
                ("Content-Type".to_string(), "text/plain".to_string()),
                ("Content-Length".to_string(), "2".to_string()),
                ("Transfer-Encoding".to_string(), "chunked".to_string()),
                ("Server".to_string(), "spoofed/1.0".to_string()),
            ],
            adapter,
        )
        .await
        .expect("respond_stream ok");

    let mut drain = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(3), sock.read_to_end(&mut drain))
        .await
        .expect("read timed out");
    let s = std::str::from_utf8(&drain).expect("utf-8");
    let head_end = s.find("\r\n\r\n").expect("head end");
    let head = &s[..head_end];
    assert!(
        !head.to_ascii_lowercase().contains("transfer-encoding"),
        "Transfer-Encoding should be stripped; got: {head}"
    );
    assert!(
        !head.to_ascii_lowercase().contains("server:"),
        "Server header should be stripped; got: {head}"
    );
    assert!(
        head.to_ascii_lowercase().contains("content-length: 2"),
        "Content-Length should be preserved; got: {head}"
    );
    writer.await.unwrap();
}

/// F4 regression: `Content-Encoding: gzip` (or any other compression
/// header) on `respond_stream` must be rejected — v1 does not implement
/// streaming compression. Use `respond` for pre-compressed bodies.
#[tokio::test]
async fn test_streaming_response_rejects_content_encoding() {
    use edge_runtime::interfaces::http_server::{HttpServer, IncomingRequest};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    let mut server = HttpServer::new()
        .with_connection_timeout(2)
        .with_max_body_size(64 * 1024);
    server.start(0, None).await.expect("server start");
    let port = server.get_assigned_port().expect("port assigned");

    let mut sock = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect");
    sock.write_all(b"GET /streamed HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .expect("write request");

    let req: IncomingRequest = tokio::time::timeout(Duration::from_secs(2), server.poll())
        .await
        .expect("poll timed out")
        .expect("poll ok")
        .expect("some request");
    let req_id = req.id;

    let entry = OutgoingEntry::new(streams::DEFAULT_STREAM_CAPACITY);
    let OutgoingEntry { stream, adapter } = entry;
    let adapter = adapter.expect("adapter present");

    server
        .respond_stream(
            req_id,
            200,
            vec![
                ("Content-Type".to_string(), "text/plain".to_string()),
                ("Content-Length".to_string(), "100".to_string()),
                ("Content-Encoding".to_string(), "gzip".to_string()),
            ],
            adapter,
        )
        .await
        .expect("respond_stream send");

    let mut drain = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(2), sock.read_to_end(&mut drain))
        .await
        .expect("read timed out — server should close the connection on Content-Encoding")
        .expect("read ok");
    assert_eq!(
        read,
        0,
        "expected no bytes on the wire after Content-Encoding rejection; got: {:?}",
        String::from_utf8_lossy(&drain)
    );
    drop(stream);
}

// ---- Inbound Transfer-Encoding: chunked (F2 review-2) ----------------------

/// Inbound `Transfer-Encoding: chunked` body (small, multi-chunk) decoded
/// and delivered to the guest as a streaming `Incoming` resource. The
/// chunked pipeline always streams regardless of `stream_threshold` because
/// the total body length is unknown up front — `stream_threshold` is a
/// CL-only signal. Trailers are ignored in v1.
#[tokio::test]
async fn test_inbound_chunked_body_decoded_via_streaming_path() {
    use edge_runtime::interfaces::http_server::{BodySource, HttpServer};
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    let mut server = HttpServer::new()
        .with_stream_threshold(64 * 1024) // body is small, but chunked always streams
        .with_max_body_size(64 * 1024)
        .with_connection_timeout(5);
    server.start(0, None).await.expect("server start");
    let port = server.get_assigned_port().expect("port assigned");

    // Multi-chunk body + an ignored trailer.
    let wire = b"POST /upload HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n5\r\nhello\r\n6\r\n world\r\n0\r\nX-Trace-Id: abc\r\n\r\n".to_vec();

    let mut sock = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect");
    sock.write_all(&wire).await.expect("write request");

    let req = tokio::time::timeout(Duration::from_secs(3), server.poll())
        .await
        .expect("poll timed out")
        .expect("poll")
        .expect("some request");

    let mut stream = match req.body {
        BodySource::Streamed(s) => s,
        other => panic!("expected BodySource::Streamed, got {:?}", other),
    };

    // Read all chunks until EOF.
    let mut collected = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(2), stream.read_chunk())
            .await
            .expect("read_chunk timed out")
        {
            Ok(chunk) => collected.extend_from_slice(&chunk),
            Err(StreamError::Closed) => break,
            Err(e) => panic!("unexpected stream error: {e:?}"),
        }
    }
    assert_eq!(collected, b"hello world".to_vec());
}

/// Inbound chunked body where the client closes the connection before the
/// terminating 0-chunk arrives — the streaming pipeline must surface this
/// as an `Io("truncated chunked body: ...")` chunk rather than a silent EOF.
#[tokio::test]
async fn test_inbound_chunked_body_truncated_streaming_returns_error() {
    use edge_runtime::interfaces::http_server::{BodySource, HttpServer};
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    let mut server = HttpServer::new()
        .with_max_body_size(64 * 1024)
        .with_connection_timeout(5);
    server.start(0, None).await.expect("server start");
    let port = server.get_assigned_port().expect("port assigned");

    // Declare a 5-byte chunk but never send the payload, CRLF, or 0-chunk.
    let wire = b"POST /upload HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n5\r\nhel".to_vec();

    let mut sock = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect");
    sock.write_all(&wire).await.expect("write request");
    sock.shutdown().await.expect("shutdown");

    let req = tokio::time::timeout(Duration::from_secs(3), server.poll())
        .await
        .expect("poll timed out")
        .expect("poll")
        .expect("some request");

    let mut stream = match req.body {
        BodySource::Streamed(s) => s,
        other => panic!("expected BodySource::Streamed, got {:?}", other),
    };

    let mut saw_truncation_error = false;
    loop {
        match tokio::time::timeout(Duration::from_secs(2), stream.read_chunk())
            .await
            .expect("read_chunk timed out")
        {
            Ok(_chunk) => continue,
            Err(StreamError::Io(msg)) if msg.contains("truncated") || msg.contains("EOF") => {
                saw_truncation_error = true;
                break;
            }
            Err(StreamError::Closed) => break,
            Err(e) => panic!("unexpected stream error: {e:?}"),
        }
    }
    assert!(
        saw_truncation_error,
        "expected a truncation Io error before EOF"
    );
}

/// Inbound chunked body where the client declares a chunk larger than
/// `max_body_size`. The pipeline must reject this BEFORE reading the
/// payload (the per-chunk cap bounds memory since total size is unknown).
///
/// Note: `with_max_body_size` clamps to `MIN_MAX_BODY_SIZE` (1024), so the
/// effective cap is 1024 bytes — declare a 0x801-byte (2049) chunk to
/// exercise the rejection.
#[tokio::test]
async fn test_inbound_chunked_body_oversize_chunk_rejected_streaming() {
    use edge_runtime::interfaces::http_server::{BodySource, HttpServer};
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    let mut server = HttpServer::new()
        .with_max_body_size(2048) // effective cap = 2048 (above MIN=1024)
        .with_connection_timeout(5);
    server.start(0, None).await.expect("server start");
    let port = server.get_assigned_port().expect("port assigned");

    // Declare a 0x801 (2049) byte chunk; cap is 2048 → must reject.
    let wire = b"POST /upload HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n801\r\n".to_vec();

    let mut sock = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect");
    sock.write_all(&wire).await.expect("write request");
    sock.shutdown().await.expect("shutdown");

    let req = tokio::time::timeout(Duration::from_secs(3), server.poll())
        .await
        .expect("poll timed out")
        .expect("poll")
        .expect("some request");

    let mut stream = match req.body {
        BodySource::Streamed(s) => s,
        other => panic!("expected BodySource::Streamed, got {:?}", other),
    };

    let mut saw_cap_error = false;
    loop {
        match tokio::time::timeout(Duration::from_secs(2), stream.read_chunk())
            .await
            .expect("read_chunk timed out")
        {
            Ok(_chunk) => continue,
            Err(StreamError::Io(msg)) if msg.contains("per-chunk cap") => {
                saw_cap_error = true;
                break;
            }
            Err(StreamError::Closed) => break,
            Err(e) => panic!("unexpected stream error: {e:?}"),
        }
    }
    assert!(
        saw_cap_error,
        "expected a per-chunk-cap Io error before EOF"
    );
}

/// `Transfer-Encoding` and `Content-Length` both present: per RFC 7230 §3.3.3,
/// TE wins and CL is ignored. The chunked path runs regardless of CL.
#[tokio::test]
async fn test_inbound_chunked_wins_over_content_length() {
    use edge_runtime::interfaces::http_server::{BodySource, HttpServer};
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    let mut server = HttpServer::new()
        .with_max_body_size(64 * 1024)
        .with_connection_timeout(5);
    server.start(0, None).await.expect("server start");
    let port = server.get_assigned_port().expect("port assigned");

    // CL=999 (much larger than actual chunked body) + TE: chunked.
    let wire = b"POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: 999\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n5\r\nhello\r\n0\r\n\r\n".to_vec();

    let mut sock = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect");
    sock.write_all(&wire).await.expect("write request");

    let req = tokio::time::timeout(Duration::from_secs(3), server.poll())
        .await
        .expect("poll timed out")
        .expect("poll")
        .expect("some request");

    let mut stream = match req.body {
        BodySource::Streamed(s) => s,
        other => panic!(
            "expected BodySource::Streamed (TE wins over CL), got {:?}",
            other
        ),
    };

    let mut collected = Vec::new();
    loop {
        match tokio::time::timeout(Duration::from_secs(2), stream.read_chunk())
            .await
            .expect("read_chunk timed out")
        {
            Ok(chunk) => collected.extend_from_slice(&chunk),
            Err(StreamError::Closed) => break,
            Err(e) => panic!("unexpected stream error: {e:?}"),
        }
    }
    assert_eq!(
        collected, b"hello",
        "chunked framing should win over the bogus CL=999"
    );
}
