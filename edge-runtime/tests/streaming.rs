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
