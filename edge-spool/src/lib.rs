//! Append-only JSONL disk spool for durable log-batch buffering.
//!
//! `Spool` is the worker-side durability layer between `LogForwarder`'s
//! in-memory buffer and the control plane's `POST /api/internal/logs`.
//! Failed batches (5xx, network timeout) are persisted to disk so they
//! survive worker restarts and are retried on the next flush tick.
//!
//! # On-disk format
//!
//! A single JSONL file at `<dir>/spool.jsonl`. Each line is a
//! `serde_json::Value` representing one `IngestLogsRequest` (the
//! worker's wire-format body, with all entries bundled together).
//! One line = one batch.
//!
//! # Durability semantics
//!
//! - `append` writes to the active file and `flush`es the OS write
//!   buffer — no `fsync`. A worker crash between the OS write and the
//!   disk commit can lose a single batch. This matches
//!   `edge-runtime/src/interfaces/kv_store.rs:111-141` (which also
//!   doesn't fsync) and is a documented known gap; the follow-up is a
//!   per-record fsync policy. The trade-off is throughput: a per-record
//!   fsync would dominate the 1s flush interval.
//! - `drain` uses `spool.jsonl` → `spool.draining` atomic rename so a
//!   concurrent `append` either sees the old file (and lands a new line
//!   after the rename returns) or a missing file (and creates a new
//!   one). The drained batches are read into memory, parsed, and the
//!   `.draining` file is unlinked.
//! - `rotate_when_over` reads all lines, drops from the head until
//!   under cap, writes the survivors to a `.tmp` sibling, and renames
//!   over the active file. Atomic.
//!
//! # Concurrency
//!
//! A single `tokio::sync::Mutex` serializes `append`, `drain`, and
//! `rotate_when_over`. The lock is not held across the HTTP POST — only
//! across the on-disk I/O. A flush pipeline is therefore:
//!
//! 1. `drain()` → read spool contents (under lock)
//! 2. merge with in-memory buffer
//! 3. POST
//! 4. on failure: `append(batch)` (under lock)
//! 5. on overflow: `rotate_when_over(cap)` (under lock)
//!
//! Steps 4 and 5 are short disk writes; step 3 (the HTTP RTT) dominates
//! the cost.
//!
//! # `size()`
//!
//! Returns the current file size in bytes. Used by `flush_now` to
//! decide whether `rotate_when_over` is needed before the next
//! append. Synchronous (`std::fs::metadata`) — fine, it's just a stat.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

/// Append-only JSONL disk spool.
///
/// One instance per worker process. Shareable across async tasks via
/// `Arc<Spool>` (the API takes `&self`, so `Arc<Spool>` is the natural
/// handle).
#[derive(Clone)]
pub struct Spool {
    inner: Arc<SpoolInner>,
}

struct SpoolInner {
    /// Directory that holds `spool.jsonl` (and transient `*.draining` /
    /// `*.tmp` siblings during drain/rotation).
    dir: PathBuf,
    /// `<dir>/spool.jsonl` — the active file. New `append` calls write
    /// here. `drain` renames it to `<dir>/spool.draining`.
    path: PathBuf,
    /// Serializes `append`, `drain`, and `rotate_when_over`. The
    /// `flush_in_flight` guard in the caller wraps the entire
    /// drain+POST+append cycle, so contention is bounded by the HTTP RTT
    /// between flushes, not by `append` itself.
    lock: Mutex<()>,
}

impl Spool {
    /// Open or create a spool rooted at `dir`. Creates the directory
    /// (recursively) if it doesn't exist. Idempotent — calling `open` on
    /// an existing spool is a no-op for the data file.
    ///
    /// The spool is intentionally *not* drained here. The first call
    /// site's `drain()` (typically during `LogForwarder::new`'s
    /// `replay_spool`) decides what to do with any pending entries —
    /// that keeps the boundary clear between "spool is open" and
    /// "buffer contains the replayed contents".
    pub async fn open(dir: &Path) -> Result<Self> {
        tokio::fs::create_dir_all(dir)
            .await
            .with_context(|| format!("create spool dir {}", dir.display()))?;

        let path = dir.join("spool.jsonl");

        Ok(Self {
            inner: Arc::new(SpoolInner {
                dir: dir.to_path_buf(),
                path,
                lock: Mutex::new(()),
            }),
        })
    }

    /// Append one batch to the spool. The batch is serialized as a
    /// single JSONL line and written to the active file. The file is
    /// created on first append.
    ///
    /// Returns an error if the write fails (disk full, permission
    /// denied, etc.). The caller is expected to handle this by
    /// re-injecting the batch into the in-memory buffer so the next
    /// flush can retry — silently dropping on disk failure would
    /// violate the durability contract.
    pub async fn append(&self, batch: &serde_json::Value) -> Result<()> {
        // Serialize outside the lock to keep the critical section
        // short. A large batch (up to 1 MiB at the worker side per
        // log_forwarder.rs::BYTE_NOTIFY_THRESHOLD) would otherwise
        // hold the lock during JSON encoding.
        let mut line = serde_json::to_vec(batch).context("serialize batch for spool")?;
        line.push(b'\n');

        let _guard = self.inner.lock.lock().await;

        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.inner.path)
            .await
            .with_context(|| {
                format!("open spool file for append: {}", self.inner.path.display())
            })?;

        file.write_all(&line)
            .await
            .with_context(|| format!("write to spool file: {}", self.inner.path.display()))?;

        // Flush the OS write buffer. Does NOT fsync — the durability
        // gap is documented at the module level.
        file.flush()
            .await
            .with_context(|| format!("flush spool file: {}", self.inner.path.display()))?;

        Ok(())
    }

    /// Atomically move all pending batches out of the spool and return
    /// them. Subsequent `append` calls land in a fresh active file.
    ///
    /// Returns `Ok(Vec::new())` if the active file is missing (the
    /// first `drain` on a brand-new spool is a no-op).
    pub async fn drain(&self) -> Result<Vec<serde_json::Value>> {
        let _guard = self.inner.lock.lock().await;

        if !self.inner.path.exists() {
            return Ok(Vec::new());
        }

        let draining = self.inner.dir.join("spool.draining");

        // Atomic rename on POSIX filesystems. After this returns, no
        // concurrent `append` can write into the draining file (they
        // see the missing active file, `OpenOptions::create(true)`
        // makes a new one).
        tokio::fs::rename(&self.inner.path, &draining)
            .await
            .with_context(|| {
                format!(
                    "rename spool {} -> {}",
                    self.inner.path.display(),
                    draining.display()
                )
            })?;

        let raw = match tokio::fs::read(&draining).await {
            Ok(b) => b,
            // If the read fails, try to put the file back so the next
            // drain can retry — silently losing the contents is worse
            // than a noisy log line.
            Err(e) => {
                let _ = tokio::fs::rename(&draining, &self.inner.path).await;
                return Err(e)
                    .with_context(|| format!("read draining spool file: {}", draining.display()));
            }
        };

        // Best-effort unlink. If this fails (e.g. transient I/O), the
        // file is still consumed and the next drain sees a missing
        // draining file; the data is in the parsed Vec. A leftover
        // file would be re-drained on the next call and re-parsed —
        // duplicates at the application level, not a data loss.
        let _ = tokio::fs::remove_file(&draining).await;

        if raw.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for (i, line) in raw.split(|b| *b == b'\n').enumerate() {
            if line.is_empty() {
                // Trailing newline; ignore.
                continue;
            }
            // A corrupt line is a real bug (we wrote every line with
            // `\n` appended and no partial writes), so surface it
            // rather than silently dropping. The caller can decide to
            // log + continue or fail the drain.
            let value: serde_json::Value =
                serde_json::from_slice(line).with_context(|| format!("parse spool line {i}"))?;
            out.push(value);
        }
        Ok(out)
    }

    /// If the active spool file exceeds `cap_bytes`, drop the oldest
    /// lines (FIFO) until under cap. Returns the number of lines
    /// dropped.
    ///
    /// No-op (returns 0) if the file is missing or already under cap.
    pub async fn rotate_when_over(&self, cap_bytes: u64) -> Result<usize> {
        let _guard = self.inner.lock.lock().await;

        if !self.inner.path.exists() {
            return Ok(0);
        }

        let raw = match tokio::fs::read(&self.inner.path).await {
            Ok(b) => b,
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("read spool for rotation: {}", self.inner.path.display())
                });
            }
        };

        if raw.len() as u64 <= cap_bytes {
            return Ok(0);
        }

        // Walk lines from the front, accumulating byte counts. Drop
        // the prefix until the remainder is under cap. We track the
        // *start byte* of each line so the survivors can be written
        // contiguously without re-serializing JSON.
        let mut line_starts: Vec<usize> = Vec::new();
        line_starts.push(0);
        for (i, b) in raw.iter().enumerate() {
            if *b == b'\n' && i + 1 < raw.len() {
                line_starts.push(i + 1);
            }
        }
        // Total bytes including the trailing newline of the last line.
        // (If the file doesn't end with a newline, we still treat the
        // last chunk as a line — defensive, the writer always appends
        // `\n`, so this only fires on a manual edit.)
        let total = raw.len();

        // Find the smallest prefix length whose removal puts the
        // remainder under cap. Equivalent to: walk line_starts from
        // the front, dropping complete lines until
        // `total - drop_prefix_bytes <= cap_bytes`.
        //
        // We always drop the candidate line first, then check the
        // resulting survivor length. (Checking "would dropping this
        // line be enough?" *before* dropping leaves a single boundary
        // case: when the line being checked is exactly the one that
        // would bring us under cap, we want to drop it, not skip it.)
        let mut dropped = 0usize;
        let mut drop_prefix_bytes = 0usize;
        for (i, &start) in line_starts.iter().enumerate() {
            // Bytes for line i = next line's start - this line's start.
            // For the last line, the bytes go to `total`.
            let end = line_starts.get(i + 1).copied().unwrap_or(total);
            let line_len = end - start;

            let survivor_len = (total - drop_prefix_bytes - line_len) as u64;
            drop_prefix_bytes += line_len;
            dropped = i + 1;
            if survivor_len <= cap_bytes {
                break;
            }
        }

        if dropped == 0 {
            // No line drops; even the first line alone fits. But we
            // already checked total > cap, so this branch should be
            // unreachable unless a single line is > cap. Treat as a
            // no-op and let the caller log.
            return Ok(0);
        }

        // Write the survivors to a tmp sibling, then atomic rename.
        let survivors = &raw[drop_prefix_bytes..];
        let tmp = self.inner.dir.join("spool.jsonl.tmp");
        tokio::fs::write(&tmp, survivors)
            .await
            .with_context(|| format!("write tmp spool: {}", tmp.display()))?;
        tokio::fs::rename(&tmp, &self.inner.path)
            .await
            .with_context(|| {
                format!(
                    "rename tmp spool {} -> {}",
                    tmp.display(),
                    self.inner.path.display()
                )
            })?;

        Ok(dropped)
    }

    /// Current size of the active spool file in bytes. Returns 0 if
    /// the file doesn't exist.
    ///
    /// Synchronous — a `stat` is cheap and the value is a hint, not a
    /// synchronization primitive. The check + rotation cycle in
    /// `flush_now` is racy by design: a concurrent `append` between
    /// the `size` and the `rotate_when_over` is fine, the next
    /// rotation will catch it.
    pub fn size(&self) -> u64 {
        std::fs::metadata(&self.inner.path)
            .map(|m| m.len())
            .unwrap_or(0)
    }

    /// Returns the path to the active spool file. Used by tests for
    /// assertion; not part of the public contract.
    #[cfg(test)]
    fn path(&self) -> &Path {
        &self.inner.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    /// Convenience: open a spool in a fresh tempdir, return both.
    async fn fresh_spool() -> (TempDir, Spool) {
        let dir = TempDir::new().expect("tempdir");
        let spool = Spool::open(dir.path()).await.expect("open");
        (dir, spool)
    }

    #[tokio::test]
    async fn append_then_drain_round_trips() {
        let (_dir, spool) = fresh_spool().await;

        let b1 = json!({"entries": [{"id": 1}]});
        let b2 = json!({"entries": [{"id": 2}]});
        let b3 = json!({"entries": [{"id": 3}]});

        spool.append(&b1).await.expect("append 1");
        spool.append(&b2).await.expect("append 2");
        spool.append(&b3).await.expect("append 3");

        let drained = spool.drain().await.expect("drain");
        assert_eq!(drained.len(), 3, "all three batches round-trip");
        assert_eq!(drained[0], b1);
        assert_eq!(drained[1], b2);
        assert_eq!(drained[2], b3);

        // After drain, the file is gone — a second drain is a no-op.
        let drained2 = spool.drain().await.expect("drain 2");
        assert!(
            drained2.is_empty(),
            "second drain on empty spool returns empty vec"
        );
    }

    #[tokio::test]
    async fn drain_on_empty_returns_empty_vec() {
        let (_dir, spool) = fresh_spool().await;
        // Brand-new spool — no file yet.
        assert!(!spool.path().exists(), "no file before any append");

        let drained = spool.drain().await.expect("drain empty");
        assert!(drained.is_empty());

        // After drain, still no file (drain is read-only when empty).
        assert!(!spool.path().exists());
        assert_eq!(spool.size(), 0);
    }

    #[tokio::test]
    async fn rotate_drops_oldest_when_over_cap() {
        let (_dir, spool) = fresh_spool().await;

        // Append 10 batches. Each ~50 bytes of JSON → ~500 bytes total.
        for i in 0..10 {
            spool
                .append(&json!({"i": i, "padding": "x".repeat(20)}))
                .await
                .expect("append");
        }
        assert!(spool.size() > 200, "spool should be > 200 bytes");

        // Cap at 200 bytes — must drop the oldest lines.
        let dropped = spool.rotate_when_over(200).await.expect("rotate");
        assert!(dropped > 0, "rotation must drop at least one line");
        assert!(dropped < 10, "must not drop everything");

        assert!(
            spool.size() <= 200,
            "spool must be under cap after rotation; got {}",
            spool.size()
        );

        // Survivors are the most recent entries.
        let survivors = spool.drain().await.expect("drain");
        assert_eq!(survivors.len(), 10 - dropped);
        // The first survivor's `i` should be `dropped` (we dropped
        // lines [0..dropped]).
        assert_eq!(
            survivors[0]["i"].as_i64().unwrap(),
            dropped as i64,
            "first survivor is the (dropped)th line"
        );
    }

    #[tokio::test]
    async fn rotate_is_noop_when_under_cap() {
        let (_dir, spool) = fresh_spool().await;
        spool
            .append(&json!({"only": "line"}))
            .await
            .expect("append");

        let dropped = spool.rotate_when_over(1_000_000).await.expect("rotate");
        assert_eq!(dropped, 0, "no drops when under cap");
        // The line is still there.
        let survivors = spool.drain().await.expect("drain");
        assert_eq!(survivors.len(), 1);
    }

    #[tokio::test]
    async fn rotate_on_missing_file_is_noop() {
        let (_dir, spool) = fresh_spool().await;
        // Never appended — no file.
        let dropped = spool.rotate_when_over(1).await.expect("rotate");
        assert_eq!(dropped, 0);
    }

    #[tokio::test]
    async fn reappend_after_partial_failure() {
        // Simulate the worker's failure path: drain, then re-append
        // only the batches that failed. A subsequent drain returns
        // exactly those.
        let (_dir, spool) = fresh_spool().await;

        let good = json!({"status": "good"});
        let bad = json!({"status": "bad"});
        spool.append(&good).await.expect("append good");
        spool.append(&bad).await.expect("append bad");

        // First drain: the worker attempts to POST both. The "good"
        // one succeeds (worker drops it); the "bad" one fails (worker
        // re-appends).
        let drained = spool.drain().await.expect("drain");
        assert_eq!(drained.len(), 2);

        // Simulate the worker's re-append of the failed batch.
        spool.append(&drained[1]).await.expect("re-append bad");

        // Second drain: only the failed batch remains.
        let drained2 = spool.drain().await.expect("drain 2");
        assert_eq!(drained2.len(), 1, "only the re-appended batch remains");
        assert_eq!(drained2[0], bad);
    }

    #[tokio::test]
    async fn concurrent_appends_dont_corrupt_lines() {
        let (_dir, spool) = fresh_spool().await;

        // 10 tasks × 100 batches each, with distinct content per task.
        // If the lock is missing, a concurrent writer will split a
        // line and the drain's `from_slice` will fail on the
        // half-line.
        let mut handles = Vec::new();
        for task_id in 0..10 {
            let spool = spool.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..100 {
                    let batch = json!({
                        "task": task_id,
                        "i": i,
                        "marker": "x".repeat(50),
                    });
                    spool.append(&batch).await.expect("append");
                }
            }));
        }
        for h in handles {
            h.await.expect("task join");
        }

        let drained = spool.drain().await.expect("drain");
        assert_eq!(
            drained.len(),
            1000,
            "all 1000 batches must be present after concurrent appends"
        );

        // Verify per-task ordering: within each task_id, the `i` values
        // are 0..=99 in order. A split line would have caused a parse
        // error before this loop.
        let mut counts = [0usize; 10];
        for batch in &drained {
            let t = batch["task"].as_i64().unwrap() as usize;
            let i = batch["i"].as_i64().unwrap() as usize;
            assert_eq!(i, counts[t], "task {t} must see i in order");
            counts[t] += 1;
        }
        assert_eq!(counts, [100; 10], "each task appended 100 batches");
    }

    #[tokio::test]
    async fn drain_then_immediate_append_creates_new_file() {
        // The atomic-rename contract: after drain, the next append
        // creates a fresh file (the old one is renamed away).
        let (_dir, spool) = fresh_spool().await;
        spool
            .append(&json!({"first": true}))
            .await
            .expect("append 1");
        let drained = spool.drain().await.expect("drain 1");
        assert_eq!(drained.len(), 1);

        spool
            .append(&json!({"second": true}))
            .await
            .expect("append 2");
        let drained2 = spool.drain().await.expect("drain 2");
        assert_eq!(drained2.len(), 1);
        assert_eq!(drained2[0], json!({"second": true}));
    }
}
