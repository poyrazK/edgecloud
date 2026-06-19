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
use tokio::sync::{mpsc, Mutex as TokioMutex};

/// Default backpressure: at most 8 chunks buffered between producer and consumer.
pub const DEFAULT_STREAM_CAPACITY: usize = 8;

/// Type aliases for the clone-able channel handles. Keeps the struct
/// declarations readable and avoids `clippy::type_complexity` warnings.
type Chunk = Result<Vec<u8>, String>;
type SharedReceiver = Arc<TokioMutex<mpsc::Receiver<Chunk>>>;
type SharedSender = Arc<TokioMutex<Option<mpsc::Sender<Chunk>>>>;

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
///
/// Clone-able: the channel Receiver is shared via `Arc<TokioMutex<…>>` so the
/// runtime can clone the stream out of a resource map, drop the map guard,
/// then await on the clone — without holding a `StdMutex` across `.await`.
///
/// Note on `TokioMutex` vs `StdMutex`: `mpsc::Receiver::recv` returns a future
/// that borrows `&mut self` from the receiver, so the receiver (and the lock
/// guard over it) must live for the entire `.await`. `StdMutex` guards can't
/// be held across `.await` (Rust borrow checker + executor deadlock risk), so
/// we use `tokio::sync::Mutex` — specifically designed to be held across
/// `.await`. The lock is per-stream, not per-resource-map-entry, so it only
/// contends with concurrent ops on the same stream.
pub struct IncomingStream {
    rx: SharedReceiver,
    cancelled: Arc<AtomicBool>,
}

impl Clone for IncomingStream {
    fn clone(&self) -> Self {
        Self {
            rx: Arc::clone(&self.rx),
            cancelled: Arc::clone(&self.cancelled),
        }
    }
}

impl IncomingStream {
    /// Read the next chunk. Returns `Err(Closed)` when the producer has dropped
    /// its sender (EOF). Returns `Err(Cancelled)` if `cancel()` was called.
    /// Returns `Err(Io)` for producer-side errors.
    pub async fn read_chunk(&mut self) -> Result<Vec<u8>, StreamError> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(StreamError::Cancelled);
        }
        let mut guard = self.rx.lock().await;
        match guard.recv().await {
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
        // TokioMutex has no sync `lock()` — use try_lock. If another task
        // holds the lock (mid-read), return Cancelled (best-effort advisory).
        let mut guard = self.rx.try_lock().map_err(|_| StreamError::Cancelled)?;
        match guard.try_recv() {
            Ok(Ok(bytes)) => Ok(Some(bytes)),
            Ok(Err(e)) => Err(StreamError::Io(e)),
            Err(mpsc::error::TryRecvError::Empty) => Ok(None),
            Err(mpsc::error::TryRecvError::Disconnected) => Err(StreamError::Closed),
        }
    }

    /// Mark the stream as cancelled. Subsequent `read_chunk` calls return
    /// `Err(Cancelled)`. The producer's `tx.send` will fail (channel closed
    /// when the last IncomingStream clone is dropped).
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }
}

/// guest → host direction. The guest calls `write_chunk` then `finish`.
///
/// Clone-able: the sender is shared via `Arc<TokioMutex<Option<Sender>>>`
/// so the runtime can clone the stream out of a resource map, drop the map
/// guard, then await on the clone — without holding a `StdMutex` across
/// `.await`. See `IncomingStream` for the TokioMutex vs StdMutex rationale.
///
/// Channel-closure semantics: the mpsc channel closes via one of two paths:
/// - **Explicit close:** `finish()` takes the `Sender` out of the `Option`
///   and drops it. With no senders left, the channel closes. All clones
///   observe `None` on subsequent `write_chunk` and return `Closed`.
/// - **Implicit close:** when the last `Arc<OutgoingStream>` clone drops,
///   the `Arc<TokioMutex<Option<Sender>>>` drops, which drops the inner
///   `Option`, which drops the inner `Sender` (if not already taken). mpsc
///   closes when all senders drop.
///
/// Drop on individual clones does NOT take the Sender — that would close
/// the channel after every Host trait call (the streams_impl clone-out
/// pattern would close after one write). The "Option<Sender> + Drop takes
/// it" anti-pattern from the original design is avoided: Drop is a no-op,
/// and the natural Arc drop cascade handles closure on the last clone.
pub struct OutgoingStream {
    tx: SharedSender,
}

impl Clone for OutgoingStream {
    fn clone(&self) -> Self {
        Self {
            tx: Arc::clone(&self.tx),
        }
    }
}

impl OutgoingStream {
    /// Write a chunk to the stream. Returns `Err(Closed)` if the consumer has
    /// dropped its receiver (e.g. host cancelled, or upstream connection lost)
    /// or if `finish()` was already called on any clone.
    pub async fn write_chunk(&mut self, bytes: Vec<u8>) -> Result<(), StreamError> {
        let mut guard = self.tx.lock().await;
        match guard.as_mut() {
            Some(tx) => tx.send(Ok(bytes)).await.map_err(|_| StreamError::Closed),
            None => Err(StreamError::Closed),
        }
    }

    /// Signal end-of-stream. Takes the sender out of the `Option`, dropping
    /// it and closing the channel so the adapter's next `poll_next` returns
    /// `None` (EOF) without waiting for the resource handle itself to be
    /// released. All clones observe the closure; subsequent `write_chunk`
    /// calls on any clone return `Err(Closed)`.
    pub async fn finish(&mut self) -> Result<(), StreamError> {
        let _ = self.tx.lock().await.take();
        Ok(())
    }
}

/// Producer-side handle for an incoming stream. Held by the host task that
/// reads from reqwest or TCP; pushes chunks into the channel that backs the
/// `IncomingStream` resource given to the guest.
pub struct IncomingProducer {
    tx: mpsc::Sender<Chunk>,
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
    rx: mpsc::Receiver<Chunk>,
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
/// the same channel. Both the stream and the adapter are Clone-able; cloning
/// the stream shares the underlying sender.
pub fn outgoing_pair(buffer: usize) -> (OutgoingStream, OutgoingStreamAdapter) {
    let (tx, rx) = mpsc::channel(buffer);
    (
        OutgoingStream {
            tx: Arc::new(TokioMutex::new(Some(tx))),
        },
        OutgoingStreamAdapter { rx },
    )
}

/// Construct a paired (incoming-producer, incoming-stream).
///
/// The `IncomingProducer` is held by the host task that reads from the network
/// (reqwest response bytes_stream or TCP body-pipeline). The `IncomingStream`
/// is given to the guest (via the ResourceTable) for `read_chunk` calls. The
/// stream is Clone-able; clones share the underlying receiver.
pub fn incoming_pair(buffer: usize) -> (IncomingProducer, IncomingStream) {
    let (tx, rx) = mpsc::channel(buffer);
    let cancelled = Arc::new(AtomicBool::new(false));
    (
        IncomingProducer { tx },
        IncomingStream {
            rx: Arc::new(TokioMutex::new(rx)),
            cancelled,
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    #[tokio::test]
    async fn test_outgoing_pair_roundtrip() {
        let (mut out, adapter) = outgoing_pair(DEFAULT_STREAM_CAPACITY);

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
        // Adapter should observe None (sender dropped). With Clone semantics,
        // the channel closes when the LAST Arc drops — which is what we have
        // here (single OutgoingStream, no clones).
        let mut adapter = _adapter;
        use futures::StreamExt;
        assert!(adapter.next().await.is_none());
    }

    #[tokio::test]
    async fn test_outgoing_finish_closes_channel_without_writer_drop() {
        // Regression for C1 (review of PR #90): `finish()` must close the
        // channel so the adapter observes EOF immediately, rather than waiting
        // for the OutgoingStream to be dropped.
        use futures::StreamExt;

        let (mut out, mut adapter) = outgoing_pair(DEFAULT_STREAM_CAPACITY);

        out.write_chunk(b"hello".to_vec()).await.unwrap();
        out.finish().await.unwrap();

        // First pull returns the chunk written before finish.
        let first = adapter.next().await.expect("chunk present");
        assert_eq!(first.unwrap(), bytes::Bytes::from_static(b"hello"));

        // Second pull MUST observe EOF from finish() — not stall.
        // (A 100ms timeout protects against regressions where the channel
        // only closes on Drop.)
        let eof = tokio::time::timeout(std::time::Duration::from_millis(100), adapter.next())
            .await
            .expect("adapter should observe EOF promptly after finish, not stall");
        assert!(eof.is_none(), "expected EOF after finish, got {:?}", eof);

        // OutgoingStream is still alive at this point — that's the point.
        // A second write_chunk must return Closed.
        let write_after_finish = out.write_chunk(b"after".to_vec()).await;
        assert_eq!(write_after_finish, Err(StreamError::Closed));
    }

    #[tokio::test]
    async fn test_outgoing_write_chunk_after_finish_returns_closed() {
        // Defensive: write after finish must be rejected immediately.
        let (mut out, _adapter) = outgoing_pair(DEFAULT_STREAM_CAPACITY);
        out.finish().await.unwrap();
        let err = out.write_chunk(b"x".to_vec()).await;
        assert_eq!(err, Err(StreamError::Closed));
    }

    // ---- Clone semantics (F3 review-2: lock-across-await fix) ----------------

    #[tokio::test]
    async fn test_incoming_stream_clone_shares_channel() {
        // Clone the IncomingStream, drop the original — the clone still
        // receives chunks. The channel is owned by the shared Arc<TokioMutex>,
        // so dropping one handle does not close it.
        let (producer, stream) = incoming_pair(DEFAULT_STREAM_CAPACITY);
        let mut clone = stream.clone();
        drop(stream);

        producer.push(Ok(b"survives-drop".to_vec())).await.unwrap();
        drop(producer);

        assert_eq!(clone.read_chunk().await.unwrap(), b"survives-drop");
        // EOF when the producer drops.
        assert_eq!(clone.read_chunk().await, Err(StreamError::Closed));
    }

    #[tokio::test]
    async fn test_outgoing_clones_share_finish_semantics() {
        // Finish on one clone closes the channel for ALL clones.
        use futures::StreamExt;

        let (out_a, mut adapter) = outgoing_pair(DEFAULT_STREAM_CAPACITY);
        let mut out_b = out_a.clone();
        drop(out_a); // Original dropped — but out_b is still alive.

        // Finish on the surviving clone closes the channel.
        out_b.finish().await.unwrap();

        // Adapter should observe EOF (channel closed by finish on a clone).
        let eof = tokio::time::timeout(std::time::Duration::from_millis(100), adapter.next())
            .await
            .expect("adapter should observe EOF promptly");
        assert!(
            eof.is_none(),
            "expected EOF after finish on clone, got {:?}",
            eof
        );

        // A new clone should also see Closed on write_chunk.
        let mut out_c = out_b.clone();
        let err = out_c.write_chunk(b"after".to_vec()).await;
        assert_eq!(err, Err(StreamError::Closed));
    }

    #[tokio::test]
    async fn test_outgoing_drop_all_clones_closes_channel() {
        // Drop all clones — the channel must close (cancellation backstop).
        use futures::StreamExt;

        let (out_a, mut adapter) = outgoing_pair(DEFAULT_STREAM_CAPACITY);
        let out_b = out_a.clone();
        drop(out_a);
        drop(out_b);
        // Both clones dropped — channel should close.

        let eof = tokio::time::timeout(std::time::Duration::from_millis(100), adapter.next())
            .await
            .expect("adapter should observe EOF after all clones dropped");
        assert!(eof.is_none(), "expected EOF, got {:?}", eof);
    }

    #[tokio::test]
    async fn test_outgoing_drop_one_clone_keeps_channel_open() {
        // With multiple clones, dropping one (without finish) keeps the channel
        // open for the surviving clones. This is the new behavior under Clone.
        use futures::StreamExt;

        let (out_a, mut adapter) = outgoing_pair(DEFAULT_STREAM_CAPACITY);
        let mut out_b = out_a.clone();
        drop(out_a); // Drop one clone — channel should stay open.

        // The surviving clone can still write.
        out_b.write_chunk(b"still-alive".to_vec()).await.unwrap();

        // Adapter observes the chunk.
        let first = tokio::time::timeout(std::time::Duration::from_millis(100), adapter.next())
            .await
            .expect("adapter should receive chunk — channel must be open")
            .expect("some item");
        assert_eq!(first.unwrap(), bytes::Bytes::from_static(b"still-alive"));

        out_b.finish().await.unwrap();
    }
}
