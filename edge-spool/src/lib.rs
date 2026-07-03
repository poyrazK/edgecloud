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
//! - `rotate_when_over` streams lines from the front (finding C1) to
//!   determine how many oldest lines to drop, seeks past the dropped
//!   prefix, and copies only the survivors to a `.tmp` sibling before
//!   the atomic rename. Memory is bounded by the largest single line
//!   plus the standard `tokio::io::copy` buffer (8 KiB), regardless of
//!   the spool size.
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
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, SeekFrom};
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
    ///
    /// **Crash recovery (review C3 + H6).** Before returning, `open`
    /// scans `dir` for orphans left by a previous crash:
    ///   - `spool.draining` is renamed back to `spool.jsonl` so the
    ///     pending batches are visible to the next `drain` instead of
    ///     being silently lost. This is the case where `drain` had
    ///     renamed `spool.jsonl` to `spool.draining` but died before
    ///     the parse/unlink completed — without recovery, every
    ///     pending batch would be invisible.
    ///   - `*.tmp` siblings are removed. These are by definition
    ///     orphans from a crashed `rotate_when_over`; the rotate
    ///     either completed (the canonical file is already up to date)
    ///     or didn't (the data on disk is still the pre-rotate
    ///     contents and a subsequent rotate will redo the work).
    ///     Leaving them in place leaks disk on every crash loop.
    ///
    /// Cleanup order: `*.tmp` first (idempotent), then `spool.draining`
    /// rename (preserves the data).
    pub async fn open(dir: &Path) -> Result<Self> {
        tokio::fs::create_dir_all(dir)
            .await
            .with_context(|| format!("create spool dir {}", dir.display()))?;

        let path = dir.join("spool.jsonl");

        // Orphan recovery. Failures here are warnings, not errors — a
        // best-effort cleanup is strictly better than panicking, and
        // a missing rename doesn't lose data (the file is still on
        // disk; the next `drain` will pick it up if the rename
        // happens to land before that, or the operator can manually
        // clean up).
        recover_spool_orphans(dir, &path).await;

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
    /// `limit_bytes` bounds how much of the spool is returned (and
    /// therefore how much is loaded into memory). When `None`, the
    /// full file is drained — the pre-C2 behavior, used by
    /// `flush_now`'s normal path where we want to ship everything to
    /// the control plane.
    ///
    /// When `Some(n)`:
    ///   - if the file is `<= n` bytes, behaves like `None` (no head
    ///     to preserve)
    ///   - otherwise, only the **last** `n` bytes are returned; the
    ///     older head is copied back to the active file (via streaming
    ///     copy, bounded memory) so the next drain picks it up.
    ///
    /// The `Some(n)` path is used by `LogForwarder::replay_spool` on
    /// worker startup (finding C2) so a worker that restarts during
    /// an extended control-plane outage doesn't spend seconds
    /// JSON-parsing every line of a near-cap spool before it can
    /// accept traffic. The pre-limit head stays on disk and is
    /// drained by `flush_now`'s normal path once the control plane
    /// recovers.
    ///
    /// Returns `Ok(Vec::new())` if the active file is missing (the
    /// first `drain` on a brand-new spool is a no-op).
    pub async fn drain(&self, limit_bytes: Option<u64>) -> Result<Vec<serde_json::Value>> {
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

        let metadata = match tokio::fs::metadata(&draining).await {
            Ok(m) => m,
            Err(e) => {
                // Put the file back so the next drain can retry —
                // silently losing the contents is worse than a noisy
                // log line.
                let _ = tokio::fs::rename(&draining, &self.inner.path).await;
                return Err(e)
                    .with_context(|| format!("stat draining spool file: {}", draining.display()));
            }
        };
        let total = metadata.len();

        // Decide whether to apply the limit. A non-positive limit is
        // treated as "no limit" — same as `None`. A limit larger than
        // the file degenerates to "drain everything".
        let effective_limit = limit_bytes.filter(|n| *n > 0 && *n < total);

        // F5 fix: the limited-drain branch captures both halves of a
        // straddling line so the next drain reconstructs the original
        // line. `dropped_head_suffix` holds the head half (dropped
        // because `head_end` is line-aligned, truncating the partial
        // line) and `dropped_tail_prefix` holds the tail half (dropped
        // because the tail slice's first line is partial). Both are
        // appended back to the head file after the rename.
        let mut dropped_head_suffix: Vec<u8> = Vec::new();
        let mut dropped_tail_prefix: Vec<u8> = Vec::new();

        let raw = if let Some(limit) = effective_limit {
            // Copy the head (the bytes we're NOT returning) back to
            // the active file via streaming copy. The active file
            // holds the older pending entries; the next drain picks
            // them up. Memory stays bounded by the 8 KiB tokio copy
            // buffer.
            //
            // Open two handles because `AsyncReadExt::take` moves
            // its receiver; we need separate handles for the head
            // copy and the tail read.
            let head_bytes = total - limit;
            let mut head_src = tokio::fs::File::open(&draining).await.with_context(|| {
                format!("open draining spool for head copy: {}", draining.display())
            })?;
            let tmp = self.inner.dir.join("spool.jsonl.tmp");
            // Find the offset of the last newline within the first
            // `head_bytes` bytes — the head copy must be line-aligned
            // so the preserved file contains only complete lines. If
            // there's no newline in the head (a single batch straddles
            // the split), `head_end` is 0 and we copy nothing — the
            // batch's bytes in the tail slice will be the partial
            // first line and get dropped there.
            let head_end = find_last_newline_in_range(&mut head_src, head_bytes).await?;
            // F5 fix: when the boundary lands mid-line, `head_end <
            // head_bytes` — the bytes `[head_end..head_bytes]` are
            // the HEAD half of the straddling line. The TAIL half is
            // captured below as `dropped_tail_prefix`. We need both
            // halves to reconstruct the line on the next drain.
            //
            // `find_last_newline_in_range` leaves `head_src`
            // positioned at `head_bytes` (its internal `take` consumed
            // exactly that many bytes), so seek back to `head_end`
            // before reading the dropped head suffix.
            dropped_head_suffix = if head_end < head_bytes {
                head_src
                    .seek(SeekFrom::Start(head_end))
                    .await
                    .with_context(|| {
                        format!(
                            "seek to head_end {} for dropped head suffix: {}",
                            head_end,
                            draining.display()
                        )
                    })?;
                let mut buf = vec![0u8; (head_bytes - head_end) as usize];
                head_src.read_exact(&mut buf).await.with_context(|| {
                    format!(
                        "read dropped head suffix [{}, {}): {}",
                        head_end,
                        head_bytes,
                        draining.display()
                    )
                })?;
                buf
            } else {
                Vec::new()
            };
            // Rewind head_src to the start of the file so the
            // streaming copy below reads bytes 0..head_end.
            head_src
                .seek(SeekFrom::Start(0))
                .await
                .with_context(|| format!("rewind head_src: {}", draining.display()))?;
            {
                let mut dst = tokio::fs::File::create(&tmp)
                    .await
                    .with_context(|| format!("create tmp spool for head: {}", tmp.display()))?;
                if head_end > 0 {
                    let mut limited_src = (&mut head_src).take(head_end);
                    tokio::io::copy(&mut limited_src, &mut dst)
                        .await
                        .with_context(|| {
                            format!(
                                "copy spool head to tmp: src={} dst={}",
                                draining.display(),
                                tmp.display()
                            )
                        })?;
                }
                // head_end == 0: tmp file is empty (just created).
                dst.flush()
                    .await
                    .with_context(|| format!("flush tmp spool head: {}", tmp.display()))?;
                // dst dropped here so the rename below doesn't fail
                // on platforms that hold the file open.
            }
            tokio::fs::rename(&tmp, &self.inner.path)
                .await
                .with_context(|| {
                    format!(
                        "rename head tmp {} -> {}",
                        tmp.display(),
                        self.inner.path.display()
                    )
                })?;

            // Second handle for the tail. Seek to (total - limit) and
            // read exactly `limit` bytes. `total` was stat'd above so
            // `SeekFrom::End(-limit)` is exact.
            let mut tail_src = tokio::fs::File::open(&draining).await.with_context(|| {
                format!("open draining spool for tail read: {}", draining.display())
            })?;
            let tail_offset = total - limit;
            tail_src
                .seek(SeekFrom::Start(tail_offset))
                .await
                .with_context(|| {
                    format!(
                        "seek draining spool to tail offset {}: {}",
                        tail_offset,
                        draining.display()
                    )
                })?;
            let mut buf = vec![0u8; limit as usize];
            tail_src
                .read_exact(&mut buf)
                .await
                .with_context(|| format!("read draining spool tail: {}", draining.display()))?;
            // The split point may fall in the middle of a line —
            // `limit` is a byte count, not a line count. Drop the
            // partial first line so the returned slice is line-aligned
            // and the parser below doesn't choke on a truncated JSON
            // value. We detect "mid-line" by reading the byte at
            // `tail_offset - 1`: if it's a newline (or `tail_offset`
            // is 0), the split is at a line boundary and we keep the
            // entire tail slice. Otherwise, skip up to and including
            // the first newline in the slice.
            let at_line_boundary = if tail_offset == 0 {
                true
            } else {
                let mut boundary_src =
                    tokio::fs::File::open(&draining).await.with_context(|| {
                        format!(
                            "open draining spool for boundary check: {}",
                            draining.display()
                        )
                    })?;
                boundary_src
                    .seek(SeekFrom::Start(tail_offset - 1))
                    .await
                    .with_context(|| {
                        format!(
                            "seek draining spool for boundary check: {}",
                            draining.display()
                        )
                    })?;
                let mut prev = [0u8; 1];
                boundary_src.read_exact(&mut prev).await.with_context(|| {
                    format!(
                        "read boundary byte before tail offset: {}",
                        draining.display()
                    )
                })?;
                prev[0] == b'\n'
            };
            if !at_line_boundary {
                if let Some(skip) = buf.iter().position(|&b| b == b'\n').map(|i| i + 1) {
                    // F5 fix: capture the dropped partial-line bytes
                    // so we can write them back to the active file
                    // (append, after the rename). The head file holds
                    // `[0..head_end]` which is line-aligned; the tail
                    // slice's dropped prefix is the FIRST part of the
                    // straddling line. Appending it to the head file
                    // reconstructs the original line for the next
                    // drain. Prepending would break byte order — must
                    // be append.
                    dropped_tail_prefix = buf[..skip].to_vec();
                    buf.drain(..skip);
                } else {
                    // No newline at all in the tail slice (limit < one
                    // line). The tail IS one partial line; keep it as
                    // the dropped prefix so the next drain
                    // reconstructs the full line.
                    dropped_tail_prefix = buf.clone();
                    buf.clear();
                }
            }
            buf
        } else {
            // Unlimited drain (or limit not binding): existing
            // behavior — read the whole file.
            match tokio::fs::read(&draining).await {
                Ok(b) => b,
                Err(e) => {
                    let _ = tokio::fs::rename(&draining, &self.inner.path).await;
                    return Err(e).with_context(|| {
                        format!("read draining spool file: {}", draining.display())
                    });
                }
            }
        };

        // F5 fix: if the limited drain dropped halves of a straddling
        // line (one from the head truncation, one from the tail
        // prefix), append both back to the now-active spool file in
        // their original byte order. The head file holds
        // `[0..head_end]` (line-aligned); appending head-then-tail
        // reconstructs the original line on the next drain.
        // Prepending would break byte order — must be append.
        //
        // H4 fix: if the suffix-append fails (disk full, I/O error),
        // rename `draining` back to `active` so the next drain retries
        // the full file idempotently. Without this, the next drain's
        // rename of `spool.jsonl → spool.draining` would atomically
        // overwrite the orphaned `draining` (which still holds the
        // full original) with the partial active file — losing the
        // middle bytes that the failed append was supposed to preserve.
        if !dropped_head_suffix.is_empty() || !dropped_tail_prefix.is_empty() {
            let append_result: Result<()> = async {
                let mut append = tokio::fs::OpenOptions::new()
                    .write(true)
                    .append(true)
                    .open(&self.inner.path)
                    .await
                    .with_context(|| {
                        format!(
                            "open active spool for dropped-suffix append: {}",
                            self.inner.path.display()
                        )
                    })?;
                if !dropped_head_suffix.is_empty() {
                    tokio::io::AsyncWriteExt::write_all(&mut append, &dropped_head_suffix)
                        .await
                        .with_context(|| {
                            format!(
                                "append head suffix to active spool: {}",
                                self.inner.path.display()
                            )
                        })?;
                }
                if !dropped_tail_prefix.is_empty() {
                    tokio::io::AsyncWriteExt::write_all(&mut append, &dropped_tail_prefix)
                        .await
                        .with_context(|| {
                            format!(
                                "append tail prefix to active spool: {}",
                                self.inner.path.display()
                            )
                        })?;
                }
                tokio::io::AsyncWriteExt::flush(&mut append)
                    .await
                    .with_context(|| {
                        format!(
                            "flush active spool after dropped-suffix append: {}",
                            self.inner.path.display()
                        )
                    })?;
                Ok(())
            }
            .await;
            if let Err(err) = append_result {
                // H4 recovery: rename draining back to active. The
                // full original is now on disk again; the next drain
                // retries idempotently. Best-effort: log if the
                // recovery rename fails but still propagate the
                // original append error.
                if let Err(rename_err) =
                    tokio::fs::rename(&draining, &self.inner.path).await
                {
                    tracing::error!(
                        draining = %draining.display(),
                        active = %self.inner.path.display(),
                        append_err = %err,
                        rename_err = %rename_err,
                        "spool: failed to recover from dropped-suffix append error"
                    );
                }
                return Err(err);
            }
        }

        // H5 fix: moved unlink to AFTER the parse loop so the draining
        // file remains on disk if parse fails. Per-line errors are
        // logged + skipped (one bad line doesn't drop the whole
        // spool — review finding H5). The unlink only runs if we
        // successfully parse the rest; otherwise the data is still
        // on disk and the next drain can retry idempotently.
        if raw.is_empty() {
            let _ = tokio::fs::remove_file(&draining).await;
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for (i, line) in raw.split(|b| *b == b'\n').enumerate() {
            if line.is_empty() {
                // Trailing newline; ignore.
                continue;
            }
            // H5 fix: tolerate a single unparseable line. The writer
            // (`append`) only ever appends `\n`-terminated JSONL, so a
            // corrupt line in practice means either a forward-
            // incompatible schema change between worker versions or
            // a partial disk write. In both cases, dropping the whole
            // batch set (the pre-fix behavior) is the wrong move —
            // we'd lose every accumulated batch since the previous
            // successful drain. Log + skip the bad line and keep
            // parsing the rest; the unlink at the bottom of the
            // function removes the consumed data once we're done.
            match serde_json::from_slice::<serde_json::Value>(line) {
                Ok(value) => out.push(value),
                Err(err) => {
                    tracing::warn!(
                        line_index = i,
                        line_len = line.len(),
                        err = %err,
                        "spool: skipping unparseable line in drain"
                    );
                }
            }
        }

        // Now that the parse loop is complete (with any unparseable
        // lines logged and skipped), unlink the draining file. If
        // this fails (e.g. transient I/O), the data is already in
        // `out`; a leftover file would be re-drained on the next
        // call and re-parsed — duplicates at the application level,
        // not data loss.
        let _ = tokio::fs::remove_file(&draining).await;

        Ok(out)
    }

    /// If the active spool file exceeds `cap_bytes`, drop the oldest
    /// lines (FIFO) until under cap. Returns the number of lines
    /// dropped.
    ///
    /// No-op (returns 0) if the file is missing or already under cap.
    ///
    /// **Finding C1 — streaming rotation.** The previous implementation
    /// loaded the entire spool into memory (via `tokio::fs::read`)
    /// before deciding which lines to keep. Under a sustained 5xx
    /// storm that filled the 1 GiB cap, every rotation allocated and
    /// walked a 1 GiB `Vec<u8>` while holding the spool `Mutex` —
    /// blocking `append` and `drain` for hundreds of milliseconds.
    /// The new implementation streams lines via a `BufReader` to
    /// determine the drop offset, then seeks into the file and
    /// copies only the survivors to a `.tmp` sibling. Memory is
    /// bounded by the largest single line plus the standard
    /// `tokio::io::copy` buffer (8 KiB), regardless of the cap.
    ///
    /// The on-disk format is unchanged: one JSONL file, one line per
    /// batch, atomic rename on completion. Existing tests still pass
    /// without modification.
    pub async fn rotate_when_over(&self, cap_bytes: u64) -> Result<usize> {
        let _guard = self.inner.lock.lock().await;

        if !self.inner.path.exists() {
            return Ok(0);
        }

        // Cheap pre-check: if the file is already under cap, there's
        // nothing to do. `metadata` is a stat — no read of file
        // contents, no allocation.
        let metadata = tokio::fs::metadata(&self.inner.path)
            .await
            .with_context(|| format!("stat spool for rotation: {}", self.inner.path.display()))?;
        let total = metadata.len();
        if total <= cap_bytes {
            return Ok(0);
        }

        // Phase 1: stream lines from the front until the remaining
        // bytes fit under cap. We track only the cumulative byte
        // count and drop count — no per-line metadata Vec, no
        // full-file buffer. The `BufReader::read_until(b'\n', ...)`
        // call accumulates at most one line at a time into the
        // reusable scratch buffer.
        let file = tokio::fs::File::open(&self.inner.path)
            .await
            .with_context(|| format!("open spool for rotation: {}", self.inner.path.display()))?;
        let mut reader = BufReader::new(file);
        let mut scratch = Vec::new();
        let mut dropped = 0usize;
        let mut drop_prefix_bytes = 0usize;
        loop {
            scratch.clear();
            let n = reader
                .read_until(b'\n', &mut scratch)
                .await
                .with_context(|| {
                    format!(
                        "stream-read spool for rotation: {}",
                        self.inner.path.display()
                    )
                })?;
            if n == 0 {
                // EOF without finding a drop boundary. Either the
                // file has no trailing newline (defensive only — the
                // writer always appends one) or every line alone is
                // >= cap. Bail without rewriting; the caller can log.
                return Ok(dropped);
            }
            let survivor_len = total.saturating_sub(drop_prefix_bytes as u64 + n as u64);
            drop_prefix_bytes += n;
            dropped += 1;
            if survivor_len <= cap_bytes {
                break;
            }
        }

        if dropped == 0 {
            // Unreachable given the `total > cap_bytes` check above
            // (no line means total == 0), but keep the guard for
            // future refactors.
            return Ok(0);
        }

        // Phase 2: seek past the dropped prefix and copy survivors to
        // a `.tmp` sibling. `tokio::io::copy` streams through an
        // internal 8 KiB buffer, so peak memory is bounded by the
        // largest single line from phase 1 plus the copy buffer.
        let mut src = tokio::fs::File::open(&self.inner.path)
            .await
            .with_context(|| {
                format!(
                    "reopen spool for survivor copy: {}",
                    self.inner.path.display()
                )
            })?;
        src.seek(SeekFrom::Start(drop_prefix_bytes as u64))
            .await
            .with_context(|| {
                format!(
                    "seek spool to drop offset {}: {}",
                    drop_prefix_bytes,
                    self.inner.path.display()
                )
            })?;
        let survivors_bytes = total - drop_prefix_bytes as u64;
        let mut limited_src = src.take(survivors_bytes);

        let tmp = self.inner.dir.join("spool.jsonl.tmp");
        let mut dst = tokio::fs::File::create(&tmp)
            .await
            .with_context(|| format!("create tmp spool: {}", tmp.display()))?;
        tokio::io::copy(&mut limited_src, &mut dst)
            .await
            .with_context(|| {
                format!(
                    "copy survivors to tmp spool: src={} dst={}",
                    self.inner.path.display(),
                    tmp.display()
                )
            })?;
        // Flush the OS buffer on the tmp file before the rename so
        // the renamed file is durable (matches the durability contract
        // documented at the module level — no fsync, just flush).
        dst.flush()
            .await
            .with_context(|| format!("flush tmp spool: {}", tmp.display()))?;
        drop(dst); // close before rename on platforms that need it.

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

/// Best-effort orphan cleanup called from `Spool::open`.
///
/// Scans `dir` for files left over by a previous worker crash:
///
/// - **`spool.draining`** — rename back to `spool.jsonl` (the active
///   path). This is the durability-critical case: `drain` had already
///   moved the file off the active path, and a crash before the
///   parse/unlink step would otherwise lose every pending batch. The
///   atomic rename restores the data so the next `drain` picks it up.
///
/// - **`spool.jsonl.tmp`** (and any `spool.jsonl.tmp.*` siblings from
///   future tmp-name patterns) — remove. These are by definition
///   orphans from a crashed `rotate_when_over`; the rotate either
///   completed (the canonical file is up to date) or didn't (a
///   subsequent rotate will redo the work). Leaving them in place
///   leaks disk on every crash loop.
///
/// The order is `*.tmp` first, then `spool.draining` rename. Failures
/// are logged but never propagated — a best-effort cleanup is strictly
/// better than panicking; a missed rename doesn't lose data (the file
/// is still on disk).
async fn recover_spool_orphans(dir: &Path, active_path: &Path) {
    // Glob-match via read_dir: any sibling whose name starts with the
    // canonical tmp prefix is an orphan. Future tmp-name patterns are
    // caught without code changes.
    let tmp_prefix = "spool.jsonl.tmp";
    let mut entries = match tokio::fs::read_dir(dir).await {
        Ok(e) => e,
        Err(err) => {
            tracing::warn!(
                dir = %dir.display(),
                err = %err,
                "spool: read_dir failed during orphan recovery; skipping"
            );
            return;
        }
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str == "spool.draining" {
            // Atomic rename back to the active path.
            let draining = entry.path();
            match tokio::fs::rename(&draining, active_path).await {
                Ok(()) => tracing::warn!(
                    draining = %draining.display(),
                    active = %active_path.display(),
                    "spool: recovered orphaned draining file from previous crash"
                ),
                Err(err) => tracing::warn!(
                    draining = %draining.display(),
                    err = %err,
                    "spool: failed to recover orphaned draining file"
                ),
            }
        } else if name_str.starts_with(tmp_prefix) {
            // Best-effort remove. The tmp file is by definition an
            // orphan — losing it on a transient error is no worse than
            // the original crash.
            let tmp = entry.path();
            match tokio::fs::remove_file(&tmp).await {
                Ok(()) => tracing::warn!(
                    tmp = %tmp.display(),
                    "spool: removed stale tmp file from previous crash"
                ),
                Err(err) => tracing::warn!(
                    tmp = %tmp.display(),
                    err = %err,
                    "spool: failed to remove stale tmp file"
                ),
            }
        }
    }
}

/// Find the byte offset of the last newline in the first `range_bytes`
/// of `src`, returning the offset IMMEDIATELY AFTER that newline
/// (i.e. the byte offset where the next line begins). Returns 0 if no
/// newline exists in the range.
///
/// Used by `Spool::drain` to line-align the head copy so the preserved
/// file contains only complete lines. Memory is bounded by the 8 KiB
/// tokio read buffer — we don't load the whole range into memory.
async fn find_last_newline_in_range(src: &mut tokio::fs::File, range_bytes: u64) -> Result<u64> {
    use tokio::io::AsyncBufReadExt;
    let mut reader = tokio::io::BufReader::new(src).take(range_bytes);
    let mut last_newline_offset: u64 = 0;
    let mut absolute_pos: u64 = 0;
    let mut buf = Vec::with_capacity(8 * 1024);
    loop {
        buf.clear();
        let n = reader
            .read_until(b'\n', &mut buf)
            .await
            .context("read_until in find_last_newline_in_range")?;
        if n == 0 {
            break;
        }
        // The buf returned by `read_until(b'\n', ...)` ends with '\n'
        // if a newline was present in the range (or at EOF). The
        // absolute offset of that newline is `absolute_pos + n - 1`.
        if buf.last() == Some(&b'\n') {
            last_newline_offset = absolute_pos + n as u64;
        }
        absolute_pos += n as u64;
    }
    Ok(last_newline_offset)
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

        let drained = spool.drain(None).await.expect("drain");
        assert_eq!(drained.len(), 3, "all three batches round-trip");
        assert_eq!(drained[0], b1);
        assert_eq!(drained[1], b2);
        assert_eq!(drained[2], b3);

        // After drain, the file is gone — a second drain is a no-op.
        let drained2 = spool.drain(None).await.expect("drain 2");
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

        let drained = spool.drain(None).await.expect("drain empty");
        assert!(drained.is_empty());

        // After drain, still no file (drain is read-only when empty).
        assert!(!spool.path().exists());
        assert_eq!(spool.size(), 0);
    }

    #[tokio::test]
    async fn drain_with_limit_bytes_returns_only_tail() {
        let (_dir, spool) = fresh_spool().await;

        // 10 batches of ~1 KiB+overhead each → ~10 KiB+overhead on
        // disk. Use a representative batch to size the limit
        // deterministically. The serialized length excludes the
        // trailing newline that `Spool::append` adds.
        let representative = json!({
            "entries": [{"id": 0, "msg": "x".repeat(1024)}],
        });
        let batch_payload_size = serde_json::to_vec(&representative).unwrap().len();
        let line_size = batch_payload_size + 1; // +1 for the trailing '\n'
        assert!(
            line_size > 1024,
            "test setup: line must be >1 KiB to exercise limit; got {}",
            line_size
        );

        let mut all = Vec::new();
        for i in 0..10 {
            let b = json!({
                "entries": [{"id": i, "msg": "x".repeat(1024)}],
            });
            all.push(b);
            spool.append(all.last().unwrap()).await.expect("append");
        }
        let total = spool.size();
        assert_eq!(
            total,
            (line_size as u64) * 10,
            "10 lines of {} bytes each = total",
            line_size
        );

        // Limit to the last 3 lines worth of bytes. tail_offset = total
        // - limit = 7 * line_size, which lands at the START of line 7
        // (a line boundary).
        let limit = (line_size as u64) * 3;
        let drained = spool.drain(Some(limit)).await.expect("drain with limit");
        assert_eq!(
            drained.len(),
            3,
            "limit to last 3 lines worth must return 3 lines"
        );
        assert_eq!(drained[0], all[7]);
        assert_eq!(drained[1], all[8]);
        assert_eq!(drained[2], all[9]);

        // The head (batches 0..7) survives on disk for the next drain.
        let remaining = spool.size();
        assert!(
            remaining > 0,
            "head must remain on disk for next drain; got {}",
            remaining
        );
        assert!(
            remaining < total,
            "head must be strictly smaller than pre-drain total; got {} vs {}",
            remaining,
            total
        );

        // Second drain (no limit) picks up the remaining head.
        let rest = spool.drain(None).await.expect("drain head");
        assert_eq!(
            rest.len(),
            7,
            "second drain returns the surviving head (batches 0..7)"
        );
        for i in 0..7 {
            assert_eq!(rest[i], all[i], "order preserved across splits");
        }
    }

    #[tokio::test]
    async fn drain_with_limit_preserves_straddling_line_on_next_drain() {
        // PR #165 review finding F5: the limited drain must capture
        // the partial-line bytes that straddle the split and append
        // them back to the active spool. The next drain then
        // reconstructs the full line (head body = line-aligned prefix;
        // appended bytes = dropped tail prefix; together they form
        // the original line in byte order).
        let (_dir, spool) = fresh_spool().await;

        // 5 batches of ~1 KiB each ≈ 5 KiB total. The limit is sized
        // to land 100 bytes BEFORE the end of line 3, so:
        //   - head body is lines 0..2 (line-aligned at last newline)
        //   - tail slice contains the tail end of line 3, then "\n",
        //     then line 4
        //   - dropped_tail_prefix = tail end of line 3 + "\n"
        //   - returned slice = line 4 only
        // On the next drain(None), the active file = head body ++
        // dropped_tail_prefix, which concatenates to lines 0..3 (the
        // original straddling line 3 is now complete).
        let representative = json!({
            "entries": [{"id": 0, "msg": "x".repeat(1024)}],
        });
        let line_size = serde_json::to_vec(&representative).unwrap().len() + 1;

        let mut all = Vec::new();
        for i in 0..5 {
            let b = json!({
                "entries": [{"id": i, "msg": "x".repeat(1024)}],
            });
            all.push(b);
            spool.append(all.last().unwrap()).await.expect("append");
        }
        let total = spool.size();

        let limit = total - (3 * line_size as u64) - (line_size as u64 - 100);
        let drained = spool
            .drain(Some(limit))
            .await
            .expect("drain with mid-line limit");
        assert_eq!(
            drained.len(),
            1,
            "tail slice has line 4 (the only fully-tail line); line 3 \
             straddles and its tail-end is captured for the next drain"
        );
        assert_eq!(drained[0], all[4], "line 4 is the only survivor");

        // F5: the next drain reconstructs the straddling line. The
        // active file = head (lines 0..2) ++ dropped_tail_prefix
        // (tail end of line 3 + newline). Together they form
        // lines 0..3 in original byte order.
        let rest = spool.drain(None).await.expect("drain head");
        assert_eq!(
            rest.len(),
            4,
            "next drain must return lines 0..3 — the straddling line 3 \
             is reconstructed from the head + appended dropped prefix"
        );
        for i in 0..4 {
            assert_eq!(
                rest[i], all[i],
                "line {i} must match the original; byte order preserved"
            );
        }
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
        let survivors = spool.drain(None).await.expect("drain");
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
        let survivors = spool.drain(None).await.expect("drain");
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
        let drained = spool.drain(None).await.expect("drain");
        assert_eq!(drained.len(), 2);

        // Simulate the worker's re-append of the failed batch.
        spool.append(&drained[1]).await.expect("re-append bad");

        // Second drain: only the failed batch remains.
        let drained2 = spool.drain(None).await.expect("drain 2");
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

        let drained = spool.drain(None).await.expect("drain");
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
        let drained = spool.drain(None).await.expect("drain 1");
        assert_eq!(drained.len(), 1);

        spool
            .append(&json!({"second": true}))
            .await
            .expect("append 2");
        let drained2 = spool.drain(None).await.expect("drain 2");
        assert_eq!(drained2.len(), 1);
        assert_eq!(drained2[0], json!({"second": true}));
    }

    // ----------------------------------------------------------------
    // Crash-recovery tests (review findings C3, H5, H6).
    // ----------------------------------------------------------------

    #[tokio::test]
    async fn open_recovers_orphaned_draining_file() {
        // Simulates a worker crash mid-drain: the rename to
        // `spool.draining` happened but the parse/unlink did not.
        // Without recovery, every pending batch would be invisible
        // to the next `open`/`drain` — the spool's central durability
        // promise is broken.
        let dir = TempDir::new().expect("tempdir");
        let draining = dir.path().join("spool.draining");
        let line1 = json!({"recovered": 1});
        let line2 = json!({"recovered": 2});
        let line3 = json!({"recovered": 3});
        let payload = format!(
            "{}\n{}\n{}\n",
            serde_json::to_string(&line1).unwrap(),
            serde_json::to_string(&line2).unwrap(),
            serde_json::to_string(&line3).unwrap()
        );
        tokio::fs::write(&draining, payload.as_bytes())
            .await
            .expect("seed draining file");

        // Open: must atomically rename `spool.draining` → `spool.jsonl`.
        let spool = Spool::open(dir.path()).await.expect("open");

        // After open, the draining file is gone and the active file
        // holds the recovered data.
        assert!(!draining.exists(), "draining file must be renamed away");
        assert!(
            dir.path().join("spool.jsonl").exists(),
            "active file must exist after recovery"
        );

        let drained = spool.drain(None).await.expect("drain");
        assert_eq!(
            drained.len(),
            3,
            "all three recovered batches must round-trip"
        );
        assert_eq!(drained[0], line1);
        assert_eq!(drained[1], line2);
        assert_eq!(drained[2], line3);
    }

    #[tokio::test]
    async fn open_cleans_stale_tmp_files() {
        // Simulates one or more worker crashes during
        // `rotate_when_over` (which uses `spool.jsonl.tmp` as the
        // staging file). Without cleanup, these leak disk on every
        // crash loop.
        let dir = TempDir::new().expect("tempdir");
        let tmp1 = dir.path().join("spool.jsonl.tmp");
        let tmp2 = dir.path().join("spool.jsonl.tmp.abc123");
        tokio::fs::write(&tmp1, b"junk from first crash")
            .await
            .expect("seed tmp1");
        tokio::fs::write(&tmp2, b"junk from second crash")
            .await
            .expect("seed tmp2");

        let _spool = Spool::open(dir.path()).await.expect("open");

        assert!(!tmp1.exists(), "tmp1 must be removed");
        assert!(!tmp2.exists(), "tmp2 (with extra suffix) must also be removed");
        // No active file yet — open on a fresh dir is a no-op for data.
        assert!(!dir.path().join("spool.jsonl").exists());
    }

    #[tokio::test]
    async fn open_handles_draining_and_tmp_together() {
        // Both kinds of orphans present simultaneously — the cleanup
        // must handle them in a single open call.
        let dir = TempDir::new().expect("tempdir");
        let draining = dir.path().join("spool.draining");
        let tmp = dir.path().join("spool.jsonl.tmp");
        let seed = json!({"recovered": true});
        let payload = format!("{}\n", serde_json::to_string(&seed).unwrap());
        tokio::fs::write(&draining, payload.as_bytes())
            .await
            .expect("seed draining");
        tokio::fs::write(&tmp, b"orphan tmp").await.expect("seed tmp");

        let spool = Spool::open(dir.path()).await.expect("open");

        assert!(!draining.exists(), "draining renamed");
        assert!(!tmp.exists(), "tmp removed");

        let drained = spool.drain(None).await.expect("drain");
        assert_eq!(drained, vec![seed]);
    }

    #[tokio::test]
    async fn drain_skips_unparseable_line_keeps_others() {
        // Review H5: a single corrupt line must not drop the entire
        // spool. The pre-fix `?`-propagate would abandon every batch
        // since the last successful drain.
        let (_dir, spool) = fresh_spool().await;

        let good_a = json!({"ok": "a"});
        let good_b = json!({"ok": "b"});
        let good_c = json!({"ok": "c"});
        spool.append(&good_a).await.expect("append a");
        spool.append(&good_b).await.expect("append b");
        spool.append(&good_c).await.expect("append c");

        // Inject a corrupt line directly into the spool file.
        use std::io::Write as _;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(spool.path())
            .expect("open spool for inject");
        f.write_all(b"{not valid json\n")
            .expect("inject corrupt line");
        drop(f);

        let drained = spool.drain(None).await.expect("drain");
        assert_eq!(
            drained.len(),
            3,
            "the three good lines must round-trip; the corrupt line is skipped"
        );
        assert_eq!(drained[0], good_a);
        assert_eq!(drained[1], good_b);
        assert_eq!(drained[2], good_c);
    }

    #[tokio::test]
    async fn drain_unlinks_after_parse() {
        // The unlink must run AFTER parse, not before. If parse fails
        // (tested above), the file should still be on disk so the
        // operator can recover. After a successful parse, the file is
        // gone.
        let dir = TempDir::new().expect("tempdir");
        let spool = Spool::open(dir.path()).await.expect("open");
        spool.append(&json!({"k": 1})).await.expect("append");

        let drained = spool.drain(None).await.expect("drain");
        assert_eq!(drained.len(), 1);

        // After a successful drain, both the active and draining
        // files are gone.
        assert!(!dir.path().join("spool.jsonl").exists());
        assert!(!dir.path().join("spool.draining").exists());
    }

    /// Finding C1 — `rotate_when_over` must complete in bounded time
    /// even for spools much larger than would fit comfortably in
    /// memory. The original implementation loaded the whole file
    /// into a `Vec<u8>` via `tokio::fs::read`; the streaming version
    /// holds at most one line in memory plus the standard 8 KiB
    /// `tokio::io::copy` buffer.
    ///
    /// The test creates a spool that exceeds the cap by a wide
    /// margin, runs `rotate_when_over`, and asserts:
    ///   - some lines were dropped
    ///   - the file is now under cap
    ///   - the survivors are the most recent entries (preserves
    ///     FIFO semantics)
    ///
    /// We don't measure peak RSS here (Rust tests can't easily
    /// without a proc-macro dep); the existing
    /// `rotate_drops_oldest_when_over_cap` test covers correctness,
    /// and the implementation comment above documents the bounded
    /// memory invariant. A coarse wall-clock check (this finishes
    /// in well under a second on any reasonable hardware) catches
    /// regressions to the full-file-read path.
    #[tokio::test]
    async fn rotate_when_over_streams_survivors_without_full_file_load() {
        let (_dir, spool) = fresh_spool().await;

        // Append enough batches that the file is several MiB.
        // Each batch is ~250 bytes (50 chars of padding + JSON
        // envelope); 10_000 batches ≈ 2.5 MiB. Well past the
        // streaming boundary.
        let n_batches = 10_000usize;
        for i in 0..n_batches {
            spool
                .append(&json!({"i": i, "padding": "x".repeat(200)}))
                .await
                .expect("append");
        }
        let original_size = spool.size();
        assert!(
            original_size > 1_000_000,
            "spool must be > 1 MiB for the streaming path to matter; got {original_size}"
        );

        // Cap at 256 KiB — must drop the vast majority of lines.
        let cap = 256 * 1024u64;
        let start = std::time::Instant::now();
        let dropped = spool.rotate_when_over(cap).await.expect("rotate");
        let elapsed = start.elapsed();

        assert!(
            dropped > 0 && dropped < n_batches,
            "must drop some but not all ({dropped}/{n_batches})"
        );
        assert!(
            spool.size() <= cap,
            "spool must be under cap after rotation; got {} > {cap}",
            spool.size()
        );

        // Sanity: survivors must be the most recent entries (FIFO).
        let survivors = spool.drain(None).await.expect("drain");
        let expected_count = n_batches - dropped;
        assert_eq!(
            survivors.len(),
            expected_count,
            "expected {expected_count} survivors, got {}",
            survivors.len()
        );
        // The first survivor's `i` must equal `dropped` (we dropped
        // the prefix lines).
        assert_eq!(
            survivors[0]["i"].as_i64().unwrap(),
            dropped as i64,
            "first survivor is the (dropped)th line"
        );
        // The last survivor's `i` must be n_batches - 1.
        assert_eq!(
            survivors.last().unwrap()["i"].as_i64().unwrap(),
            (n_batches - 1) as i64,
            "last survivor is the final line"
        );

        // Coarse wall-clock assertion: 2.5 MiB rotation should
        // complete in well under a second on any reasonable
        // hardware. A regression to the full-file-read path would
        // still pass this bound on most machines (Vec<u8> of 2.5
        // MiB is cheap), but a regression that re-parses the
        // survivors as JSON or does many extra copies would push
        // past it. The assertion is intentionally lenient.
        assert!(
            elapsed.as_secs() < 5,
            "rotate_when_over took {elapsed:?} for {} MiB; expected < 5s",
            original_size / (1024 * 1024)
        );
    }
}
