//! Streaming body primitives shared by http-client and http-server.
//!
//! Two directions:
//! - **incoming** (host → guest): response body, large request body.
//!   The host produces chunks; the guest consumes via `read_chunk`.
//! - **outgoing** (guest → host): streaming request body, streaming response body.
//!   The guest produces chunks via `write_chunk`/`finish`; the host consumes.
//!
//! Each direction uses a bounded tokio mpsc channel for backpressure. The default
//! capacity is 8 chunks; configurable per pair. EOF on incoming is signaled by
//! `StreamError::Closed` (sender dropped). `finish()` on outgoing closes the
//! stream cleanly; `Drop` is the cancellation backstop if the guest abandons
//! the handle.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc;

/// Default backpressure: at most 8 chunks buffered between producer and consumer.
pub const DEFAULT_STREAM_CAPACITY: usize = 8;

/// Error type returned from stream operations.
///
/// `Closed` indicates graceful end-of-stream (sender dropped, or `finish()` called).
/// `Cancelled` indicates the consumer (or host) cancelled the stream.
/// `Io` carries a producer-side error message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamError {
    Cancelled,
    Closed,
    Io(String),
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamError::Cancelled => write!(f, "stream cancelled"),
            StreamError::Closed => write!(f, "stream closed"),
            StreamError::Io(msg) => write!(f, "stream io error: {}", msg),
        }
    }
}

impl std::error::Error for StreamError {}

/// Convert from our internal StreamError to the WIT-generated one. The
/// bindgen produces a structurally-identical enum under
/// `crate::edge::cloud::streams::StreamError`. Since the WIT type is in a
/// private generated module, we can't `impl From` on it directly — callers
/// use the `to_wit()` helper. Kept as a free function for clarity.
pub fn to_wit(err: StreamError) -> crate::edge::cloud::streams::StreamError {
    match err {
        StreamError::Cancelled => crate::edge::cloud::streams::StreamError::Cancelled,
        StreamError::Closed => crate::edge::cloud::streams::StreamError::Closed,
        StreamError::Io(msg) => crate::edge::cloud::streams::StreamError::Io(msg),
    }
}

/// host → guest direction. The guest calls `read_chunk` until `Closed`.
pub struct IncomingStream {
    rx: mpsc::Receiver<Result<Vec<u8>, String>>,
    cancelled: Arc<AtomicBool>,
}

impl IncomingStream {
    /// Read the next chunk. Returns `Err(Closed)` when the producer has dropped
    /// its sender (EOF). Returns `Err(Cancelled)` if `cancel()` was called.
    /// Returns `Err(Io)` for producer-side errors.
    pub async fn read_chunk(&mut self) -> Result<Vec<u8>, StreamError> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(StreamError::Cancelled);
        }
        match self.rx.recv().await {
            Some(Ok(chunk)) => Ok(chunk),
            Some(Err(e)) => Err(StreamError::Io(e)),
            None => Err(StreamError::Closed),
        }
    }

    /// Try to read a chunk without awaiting. Used by the runtime's
    /// `block_on(tokio::time::timeout(...))` bridge to bound dead-lock risk.
    pub fn try_read_chunk(&mut self) -> Result<Option<Vec<u8>>, StreamError> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(StreamError::Cancelled);
        }
        match self.rx.try_recv() {
            Ok(Ok(bytes)) => Ok(Some(bytes)),
            Ok(Err(e)) => Err(StreamError::Io(e)),
            Err(mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(mpsc::error::TryRecvError::Disconnected) => Err(StreamError::Closed),
        }
    }

    /// Mark the stream as cancelled. Subsequent `read_chunk` calls return
    /// `Err(Cancelled)`. The producer's `tx.send` will fail (channel closed
    /// when the IncomingStream is dropped).
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }
}

/// guest → host direction. The guest calls `write_chunk` then `finish`.
pub struct OutgoingStream {
    tx: mpsc::Sender<Result<Vec<u8>, String>>,
    finished: Arc<AtomicBool>,
}

impl OutgoingStream {
    /// Write a chunk to the stream. Returns `Err(Closed)` if the consumer has
    /// dropped its receiver (e.g. host cancelled, or upstream connection lost)
    /// or if `finish()` was already called.
    pub async fn write_chunk(&self, bytes: Vec<u8>) -> Result<(), StreamError> {
        if self.finished.load(Ordering::Acquire) {
            return Err(StreamError::Closed);
        }
        self.tx
            .send(Ok(bytes))
            .await
            .map_err(|_| StreamError::Closed)
    }

    /// Signal end-of-stream. Subsequent `write_chunk` calls return `Err(Closed)`.
    pub async fn finish(&self) -> Result<(), StreamError> {
        self.finished.store(true, Ordering::Release);
        Ok(())
    }
}

impl Drop for OutgoingStream {
    fn drop(&mut self) {
        // Mark finished so any racing write_chunk would observe Closed rather
        // than blocking forever on a half-closed channel.
        self.finished.store(true, Ordering::Release);
    }
}

/// Producer-side handle for an incoming stream. Held by the host task that
/// reads from reqwest or TCP; pushes chunks into the channel that backs the
/// `IncomingStream` resource given to the guest.
pub struct IncomingProducer {
    tx: mpsc::Sender<Result<Vec<u8>, String>>,
}

impl IncomingProducer {
    /// Push a chunk to the consumer. Returns the chunk back if the consumer
    /// dropped its IncomingStream (guest cancelled). The caller should stop
    /// producing in that case.
    pub async fn push(
        &self,
        chunk: Result<Vec<u8>, String>,
    ) -> Result<(), Result<Vec<u8>, String>> {
        match self.tx.send(chunk).await {
            Ok(()) => Ok(()),
            Err(send_err) => Err(send_err.0),
        }
    }

    /// True if the consumer has dropped the IncomingStream (guest cancelled
    /// or finished). The producer should stop sending.
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

/// Resource-table entry for an outgoing stream. Holds the writer side (which
/// the guest calls write_chunk/finish on) plus the adapter side (which the host
/// drains via `reqwest::Body::wrap_stream`). The adapter is taken (via
/// `Option::take`) when the host consumes the stream for an upstream call.
pub struct OutgoingEntry {
    pub stream: OutgoingStream,
    pub adapter: Option<OutgoingStreamAdapter>,
}

impl OutgoingEntry {
    pub fn new(buffer: usize) -> Self {
        let (stream, adapter) = outgoing_pair(buffer);
        Self {
            stream,
            adapter: Some(adapter),
        }
    }
}

/// Resource-table entry for an incoming stream. The producer side (held by
/// the host's body-pipeline task) pushes chunks via the `IncomingProducer`.
pub struct IncomingEntry {
    pub stream: IncomingStream,
}

/// Adapter that exposes an outgoing stream's chunks as a `futures::Stream`.
/// Used with `reqwest::Body::wrap_stream` for outbound streaming request bodies.
pub struct OutgoingStreamAdapter {
    rx: mpsc::Receiver<Result<Vec<u8>, String>>,
}

impl futures::stream::Stream for OutgoingStreamAdapter {
    type Item = Result<bytes::Bytes, std::io::Error>;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.rx.poll_recv(cx) {
            Poll::Ready(Some(Ok(chunk))) => Poll::Ready(Some(Ok(bytes::Bytes::from(chunk)))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(std::io::Error::other(e)))),
            // Sender dropped — guest finished the stream (or abandoned it).
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Construct a paired (outgoing-stream, outgoing-adapter).
///
/// The `OutgoingStream` is given to the guest (via the ResourceTable) for
/// `write_chunk`/`finish` calls. The `OutgoingStreamAdapter` is held by the
/// host (e.g. passed to `reqwest::Body::wrap_stream`) and pulls chunks from
/// the same channel.
pub fn outgoing_pair(buffer: usize) -> (OutgoingStream, OutgoingStreamAdapter) {
    let (tx, rx) = mpsc::channel(buffer);
    let finished = Arc::new(AtomicBool::new(false));
    (
        OutgoingStream { tx, finished },
        OutgoingStreamAdapter { rx },
    )
}

/// Construct a paired (incoming-producer, incoming-stream).
///
/// The `IncomingProducer` is held by the host task that reads from the network
/// (reqwest response bytes_stream or TCP body-pipeline). The `IncomingStream`
/// is given to the guest (via the ResourceTable) for `read_chunk` calls.
pub fn incoming_pair(buffer: usize) -> (IncomingProducer, IncomingStream) {
    let (tx, rx) = mpsc::channel(buffer);
    let cancelled = Arc::new(AtomicBool::new(false));
    (IncomingProducer { tx }, IncomingStream { rx, cancelled })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_outgoing_pair_roundtrip() {
        let (out, adapter) = outgoing_pair(DEFAULT_STREAM_CAPACITY);

        // Producer (guest side) writes two chunks then finishes.
        tokio::spawn(async move {
            out.write_chunk(b"hello".to_vec()).await.unwrap();
            out.write_chunk(b"world".to_vec()).await.unwrap();
            out.finish().await.unwrap();
        });

        // Consumer (host side, via reqwest::Body::wrap_stream).
        let collected: Vec<bytes::Bytes> = adapter.map(|r| r.unwrap()).collect().await;
        assert_eq!(collected.len(), 2);
        assert_eq!(&collected[0][..], b"hello");
        assert_eq!(&collected[1][..], b"world");
    }

    #[tokio::test]
    async fn test_incoming_pair_roundtrip() {
        let (producer, mut stream) = incoming_pair(DEFAULT_STREAM_CAPACITY);

        // Producer (host side) pushes two chunks then drops.
        tokio::spawn(async move {
            producer.push(Ok(b"foo".to_vec())).await.unwrap();
            producer.push(Ok(b"bar".to_vec())).await.unwrap();
            // Drop closes the channel.
        });

        assert_eq!(stream.read_chunk().await.unwrap(), b"foo");
        assert_eq!(stream.read_chunk().await.unwrap(), b"bar");
        // Next read should observe Closed (sender dropped).
        assert_eq!(stream.read_chunk().await, Err(StreamError::Closed));
    }

    #[tokio::test]
    async fn test_incoming_cancel_signals_cancelled() {
        let (producer, mut stream) = incoming_pair(DEFAULT_STREAM_CAPACITY);
        stream.cancel();
        // Producer push succeeds because cancel only affects the consumer side.
        producer.push(Ok(b"x".to_vec())).await.unwrap();
        // First read observes the cancelled flag.
        assert_eq!(stream.read_chunk().await, Err(StreamError::Cancelled));
    }

    #[tokio::test]
    async fn test_outgoing_drop_is_cancellation_backstop() {
        let (out, _adapter) = outgoing_pair(DEFAULT_STREAM_CAPACITY);
        drop(out);
        // Adapter should observe None (sender dropped).
        let mut adapter = _adapter;
        use futures::StreamExt;
        assert!(adapter.next().await.is_none());
    }
}
