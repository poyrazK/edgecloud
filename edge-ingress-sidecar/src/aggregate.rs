//! Sliding-window RPS aggregation (issue #665, PR B).
//!
//! The sidecar consumes per-replica deltas from the JetStream stream
//! `edgecloud.rate-limit.global.delta.<replica_id>`. The consumer pushes
//! each parsed [`DeltaMsg`] into a tokio mpsc channel; this module owns
//! the receiving end and answers one question on every tick:
//!
//!   "What is the sum of every replica's latest delta inside the last
//!   `window` (default 1s), grouped by replica so the renderer can
//!   compute `others_share` (issue #665 plan: each replica caps at
//!   `max(1, configured − others_share)`)?"
//!
//! ## Windowing
//!
//! The window is a sliding 1-second bucket. On every tick the
//! aggregator:
//!   1. Drains the channel of every [`DeltaMsg`] that arrived since
//!      the last tick.
//!   2. Merges them into the per-replica latest-delta view
//!      ([`WindowState::latest_by_replica`]).
//!   3. Prunes entries whose timestamp is older than `now − window` —
//!      a replica that hasn't published in >1s is no longer counted.
//!      This is the "missed second tolerated" invariant: a sidecar that
//!      misses one tick (network blip, brief GC pause) is dropped from
//!      the platform total on the next tick, which is the right
//!      behavior — a stale count would inflate `others_share` and
//!      suppress legitimate load elsewhere.
//!
//! ## Per-replica latest-wins
//!
//! Within the window, only the freshest message per `replica_id` is
//! kept. If a single replica publishes twice in the window (e.g. a
//! briefly-delayed first message arrived late), the older one is
//! discarded. This matches Caddy's "rate over the last second"
//! intuition and avoids double-counting.
//!
//! ## [`Snapshot`]
//!
//! The aggregator hands out immutable [`Snapshot`]s to the renderer
//! task (PR D reads the snapshot to populate the cache; the same
//! snapshot drives the UDS writer in PR B's [`expose`] module). The
//! snapshot carries everything the renderer needs to compute
//! `per_replica_cap`:
//!   - `configured_cap` — the operator's configured platform cap
//!     (passed through unchanged; the aggregator doesn't know what
//!     "the cap" is, only what each replica contributed).
//!   - `platform_total` — sum of every replica's latest delta in the window.
//!   - `this_replica_rps` — the local replica's contribution.
//!   - `replicas_seen` — number of distinct replicas that published
//!     in the window.
//!
//! The renderer computes `others_share` and `per_replica_cap` from
//! these four fields; the aggregator deliberately does NOT bake the
//! arithmetic in so the same module can serve a future "different
//! enforcement model" without a refactor.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;

use crate::caddy_metrics::DeltaMsg;

/// Default sliding-window size. 1s matches the plan: a per-second
/// RPS measurement is the natural unit for a rate limiter, and the
/// JetStream stream's `MaxAge=60s` (issue #665 plan) gives ample
/// headroom for late messages.
pub const DEFAULT_WINDOW: Duration = Duration::from_secs(1);

/// Channel buffer between the consumer task and the aggregator.
/// Sized for one full window of burst + headroom: at 1 Hz the
/// consumer pushes one message per replica per tick, but a reconnect
/// or backfill can dump many messages at once. 256 is generous for
/// the expected 3-replica deployment and stays small enough that a
/// stuck consumer can't accumulate unbounded memory.
#[cfg(test)] // referenced from the test module; `main.rs` uses mpsc::channel(256) directly.
const CHANNEL_BUFFER: usize = 256;

/// Output of [`Aggregator::snapshot_at`] — what the renderer task
/// reads.
///
/// Mirrors the `GlobalRPSCache::Entry` shape documented in the issue
/// #665 plan. The aggregator produces this without knowing how the
/// renderer will use it; the load-bearing arithmetic (`others_share`,
/// `per_replica_cap`) lives at the call site so this module can serve
/// future enforcement models without a refactor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// Operator's configured platform cap (passed through unchanged
    /// from `Config::global_rate_limit_rps`).
    pub configured_cap: u32,
    /// Sum of every replica's latest delta inside the window.
    pub platform_total: u32,
    /// This replica's latest delta inside the window (0 if this
    /// replica hasn't published yet).
    pub this_replica_rps: u32,
    /// Number of distinct replicas that published in the window.
    pub replicas_seen: u32,
}

impl Snapshot {
    /// The load-bearing arithmetic. Each replica caps at
    /// `max(1, configured − others_share)` where
    /// `others_share = (platform_total − this_replica_rps) / (replicas_seen − 1)`.
    ///
    /// Worked example: 3 replicas × 10k RPS at cap=10k →
    /// `others_share = (30000 − 10000) / 2 = 10000`,
    /// `per_replica_cap = max(1, 10000 − 10000) = 1` — Caddy
    /// token-buckets almost everything, ~2/3 of platform load
    /// 429'd. This is the verification target the issue body asks
    /// for.
    ///
    /// `None` ⇒ the caller should emit NO global route (fail-closed).
    /// Currently only triggered when `replicas_seen == 0` (no traffic
    /// has been seen in the window).
    pub fn per_replica_cap(&self) -> Option<u32> {
        if self.replicas_seen == 0 {
            return None;
        }
        let others_share = if self.replicas_seen > 1 {
            (self.platform_total.saturating_sub(self.this_replica_rps)) / (self.replicas_seen - 1)
        } else {
            0
        };
        Some(self.configured_cap.saturating_sub(others_share).max(1))
    }
}

/// Per-replica latest-delta + age. Kept in a `VecDeque` so we can
/// prune by timestamp without a full scan.
#[derive(Debug, Clone)]
struct Entry {
    replica_id: String,
    rps: u32,
    ts: Instant,
}

/// Internal window state. Lives behind an `Arc<RwLock<_>>` so the
/// consumer can drain the channel concurrently with the renderer
/// reading a snapshot.
///
/// `latest_by_replica` is a `HashMap` — the contract is "freshest
/// wins", and a `VecDeque<Entry>` per replica is unnecessary because
/// stale entries get pruned on every tick.
#[derive(Debug)]
struct WindowState {
    /// Per-replica latest-delta, keyed by `replica_id`. Updated in
    /// place when a newer message arrives.
    latest_by_replica: HashMap<String, Entry>,
    /// Insertion-order history of every entry currently in
    /// `latest_by_replica`. Lets us prune by timestamp in O(pruned)
    /// instead of O(replicas × window).
    order: VecDeque<Entry>,
    /// Window size. Tunable for tests; the production caller always
    /// uses [`DEFAULT_WINDOW`].
    window: Duration,
}

impl WindowState {
    fn new(window: Duration) -> Self {
        Self {
            latest_by_replica: HashMap::new(),
            order: VecDeque::new(),
            window,
        }
    }

    /// Insert a fresh delta. If a delta for `msg.replica_id` already
    /// exists, replace it (latest-wins) and remove the stale
    /// `order` entry. Otherwise append.
    fn insert(&mut self, msg: DeltaMsg, now: Instant) {
        let entry = Entry {
            replica_id: msg.replica_id,
            rps: msg.rps,
            ts: now,
        };
        if let Some(existing) = self
            .latest_by_replica
            .insert(entry.replica_id.clone(), entry.clone())
        {
            // Drop the stale entry from `order`. VecDeque::remove
            // is O(n) but n is bounded by the live replica count
            // (typically 3-10); this is the standard
            // "small-bounded-window" trade-off.
            if let Some(pos) = self
                .order
                .iter()
                .position(|e| e.replica_id == existing.replica_id && e.ts == existing.ts)
            {
                self.order.remove(pos);
            }
        }
        self.order.push_back(entry);
    }

    /// Drop every entry whose timestamp is older than `now − window`.
    fn prune(&mut self, now: Instant) {
        let cutoff = now.checked_sub(self.window).unwrap_or_else(Instant::now);
        while let Some(front) = self.order.front() {
            if front.ts < cutoff {
                let removed = self.order.pop_front().expect("front exists");
                // Only drop the `latest_by_replica` mapping if it
                // still points at THIS entry. A newer message for
                // the same replica may have replaced it
                // (latest-wins), and we must not zero out the
                // fresher value.
                if let Some(current) = self.latest_by_replica.get(&removed.replica_id) {
                    if current.ts == removed.ts {
                        self.latest_by_replica.remove(&removed.replica_id);
                    }
                }
            } else {
                break;
            }
        }
    }
}

/// Aggregator: receives `DeltaMsg` from the consumer, holds the
/// sliding-window state, hands out [`Snapshot`]s to the renderer.
#[derive(Clone)]
pub struct Aggregator {
    state: Arc<RwLock<WindowState>>,
    /// The replica_id of THIS sidecar, captured at startup. Used to
    /// populate `Snapshot::this_replica_rps` so the renderer can
    /// compute `others_share` without knowing the operator's view of
    /// the cluster.
    this_replica_id: String,
    /// Configured cap (passed through to every snapshot).
    configured_cap: u32,
}

impl Aggregator {
    /// Construct a fresh aggregator.
    ///
    /// `configured_cap` is the operator's `SIDECAR_GLOBAL_RATE_LIMIT_RPS`.
    /// It threads through every snapshot unchanged — the aggregator
    /// does NOT bake it into the arithmetic.
    pub fn new(this_replica_id: String, configured_cap: u32) -> Self {
        Self {
            state: Arc::new(RwLock::new(WindowState::new(DEFAULT_WINDOW))),
            this_replica_id,
            configured_cap,
        }
    }

    /// Construct an aggregator with a custom window size. Test-only;
    /// production callers use [`Aggregator::new`].
    #[cfg(test)]
    pub fn with_window(this_replica_id: String, configured_cap: u32, window: Duration) -> Self {
        Self {
            state: Arc::new(RwLock::new(WindowState::new(window))),
            this_replica_id,
            configured_cap,
        }
    }

    /// Drain every pending [`DeltaMsg`] from the channel, prune, and
    /// return a fresh [`Snapshot`].
    ///
    /// Called once per tick from `main.rs` (and once from tests).
    /// `now` is parameterized so tests can drive the clock without
    /// `tokio::time::sleep`.
    pub async fn tick(&self, rx: &mut mpsc::Receiver<DeltaMsg>, now: Instant) -> Snapshot {
        // Drain everything queued since the last tick. Bounded by
        // CHANNEL_BUFFER; if the consumer is running >CHANNEL_BUFFER
        // ahead, the channel recv() returns None and we'd block —
        // which is the correct backpressure signal.
        while let Ok(msg) = rx.try_recv() {
            let mut state = self.state.write().await;
            state.insert(msg, now);
        }
        {
            let mut state = self.state.write().await;
            state.prune(now);
        }
        self.snapshot_at(now).await
    }

    /// Snapshot the current window without draining. Public so tests
    /// can inspect intermediate state; production callers should use
    /// [`Aggregator::tick`] which drains + snapshots atomically.
    ///
    /// `_now` is accepted for parity with [`Aggregator::tick`] (so
    /// the same call shape works whether or not we drive the clock
    /// manually) but is unused — pruning already happened during
    /// `tick` and the read path doesn't consult the clock.
    #[allow(unused_variables)]
    pub async fn snapshot_at(&self, _now: Instant) -> Snapshot {
        let state = self.state.read().await;
        let platform_total: u32 = state.latest_by_replica.values().map(|e| e.rps).sum();
        let replicas_seen = state.latest_by_replica.len() as u32;
        let this_replica_rps = state
            .latest_by_replica
            .get(&self.this_replica_id)
            .map(|e| e.rps)
            .unwrap_or(0);
        Snapshot {
            configured_cap: self.configured_cap,
            platform_total,
            this_replica_rps,
            replicas_seen,
        }
    }
}

/// Spawn the background drainer task. It owns the receiver end of
/// the consumer→aggregator channel and ticks at 1 Hz.
///
/// `on_snapshot` is called once per tick with the freshest snapshot;
/// the caller wires it to the UDS writer (PR B's [`expose`] module
/// and PR D's cache write).
///
/// Returns the join handle so tests can `.await` it for clean
/// shutdown.
pub fn spawn_aggregator(
    aggregator: Aggregator,
    mut rx: mpsc::Receiver<DeltaMsg>,
    on_snapshot: Arc<dyn Fn(Snapshot) + Send + Sync>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(DEFAULT_WINDOW);
        // Skip missed ticks (don't burst-drain after a long stall —
        // the window will be empty on resume).
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!("aggregator: shutdown received");
                    return;
                }
                _ = ticker.tick() => {
                    let snap = aggregator.tick(&mut rx, Instant::now()).await;
                    on_snapshot(snap);
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn delta(replica_id: &str, rps: u32) -> DeltaMsg {
        DeltaMsg {
            replica_id: replica_id.to_string(),
            rps,
        }
    }

    // ── Snapshot::per_replica_cap tests ─────────────────────────────

    /// Pin the load-bearing arithmetic from the issue #665 plan.
    /// 3 replicas × 10k RPS at cap=10k ⇒ cap=1 per replica.
    #[test]
    fn snapshot_cap_at_verification_target_three_replicas() {
        let snap = Snapshot {
            configured_cap: 10_000,
            platform_total: 30_000,
            this_replica_rps: 10_000,
            replicas_seen: 3,
        };
        assert_eq!(snap.per_replica_cap(), Some(1));
    }

    /// 3 replicas × 5k RPS at cap=10k ⇒ cap=5k (no overage).
    #[test]
    fn snapshot_cap_three_replicas_under_cap() {
        let snap = Snapshot {
            configured_cap: 10_000,
            platform_total: 15_000,
            this_replica_rps: 5_000,
            replicas_seen: 3,
        };
        assert_eq!(snap.per_replica_cap(), Some(5_000));
    }

    /// Single-replica deployment — `others_share=0` ⇒ cap unchanged.
    #[test]
    fn snapshot_cap_single_replica() {
        let snap = Snapshot {
            configured_cap: 10_000,
            platform_total: 12_000,
            this_replica_rps: 12_000,
            replicas_seen: 1,
        };
        assert_eq!(snap.per_replica_cap(), Some(10_000));
    }

    /// Zero replicas in the window ⇒ None (fail-closed: emit NO
    /// global route).
    #[test]
    fn snapshot_cap_zero_replicas_yields_none() {
        let snap = Snapshot {
            configured_cap: 10_000,
            platform_total: 0,
            this_replica_rps: 0,
            replicas_seen: 0,
        };
        assert_eq!(snap.per_replica_cap(), None);
    }

    /// Saturation guard: if `others_share > configured`, the floor
    /// is 1, not 0. Caddy's token bucket would choke on `rps=0`.
    #[test]
    fn snapshot_cap_floor_is_one_not_zero() {
        let snap = Snapshot {
            configured_cap: 1_000,
            platform_total: 100_000,
            this_replica_rps: 50_000,
            replicas_seen: 2,
        };
        assert_eq!(snap.per_replica_cap(), Some(1));
    }

    // ── Aggregator tests (drive the clock by hand) ──────────────────

    #[tokio::test]
    async fn insert_replaces_stale_entry_for_same_replica() {
        // Two messages from the same replica within the window —
        // the newer one wins.
        let agg = Aggregator::with_window("A".into(), 10_000, Duration::from_secs(1));
        let (tx, mut rx) = mpsc::channel(CHANNEL_BUFFER);
        tx.send(delta("A", 5_000)).await.unwrap();
        tx.send(delta("A", 7_000)).await.unwrap();
        let t0 = Instant::now();
        let snap = agg.tick(&mut rx, t0).await;
        assert_eq!(snap.this_replica_rps, 7_000, "latest wins");
        assert_eq!(snap.platform_total, 7_000);
        assert_eq!(snap.replicas_seen, 1);
    }

    #[tokio::test]
    async fn prune_drops_entries_older_than_window() {
        // Insert three replicas at t0, then advance the clock past
        // the window. All three should be pruned.
        let agg = Aggregator::with_window("A".into(), 10_000, Duration::from_millis(100));
        let (tx, mut rx) = mpsc::channel(CHANNEL_BUFFER);
        tx.send(delta("A", 5_000)).await.unwrap();
        tx.send(delta("B", 3_000)).await.unwrap();
        tx.send(delta("C", 2_000)).await.unwrap();
        let t0 = Instant::now();
        let snap = agg.tick(&mut rx, t0).await;
        assert_eq!(snap.replicas_seen, 3);
        assert_eq!(snap.platform_total, 10_000);
        // Advance 200ms — well past the 100ms window.
        let t1 = t0 + Duration::from_millis(200);
        let snap = agg.tick(&mut rx, t1).await;
        assert_eq!(snap.replicas_seen, 0, "all entries pruned");
        assert_eq!(snap.platform_total, 0);
        assert_eq!(snap.this_replica_rps, 0);
    }

    #[tokio::test]
    async fn miss_one_second_is_tolerated() {
        // Replica A publishes at t0, no publishes between t0 and t1,
        // then B publishes at t1. The t0 entry should have been
        // pruned by t1, so the platform total reflects only the
        // newer publish.
        let agg = Aggregator::with_window("A".into(), 10_000, Duration::from_millis(100));
        let (tx, mut rx) = mpsc::channel(CHANNEL_BUFFER);
        let t0 = Instant::now();
        tx.send(delta("A", 1_000)).await.unwrap();
        let snap0 = agg.tick(&mut rx, t0).await;
        assert_eq!(snap0.platform_total, 1_000);
        let t1 = t0 + Duration::from_millis(150);
        tx.send(delta("B", 2_000)).await.unwrap();
        let snap1 = agg.tick(&mut rx, t1).await;
        // A's t0 entry pruned (150ms > 100ms window); B's t1 entry fresh.
        assert_eq!(snap1.platform_total, 2_000);
        assert_eq!(snap1.replicas_seen, 1, "A pruned, B fresh");
    }

    #[tokio::test]
    async fn three_replica_load_balances_to_per_replica_cap_one() {
        // Mirror the verification target: 3 replicas at 10k RPS
        // each ⇒ per_replica_cap == 1.
        let agg = Aggregator::with_window("A".into(), 10_000, Duration::from_secs(1));
        let (tx, mut rx) = mpsc::channel(CHANNEL_BUFFER);
        for rid in ["A", "B", "C"] {
            tx.send(delta(rid, 10_000)).await.unwrap();
        }
        let snap = agg.tick(&mut rx, Instant::now()).await;
        assert_eq!(snap.replicas_seen, 3);
        assert_eq!(snap.platform_total, 30_000);
        assert_eq!(snap.this_replica_rps, 10_000);
        assert_eq!(snap.per_replica_cap(), Some(1));
    }

    #[tokio::test]
    async fn this_replica_rps_distinguished_from_others() {
        // A is the local replica, B and C are remote. Snapshot
        // must report A's contribution as this_replica_rps and the
        // total as platform_total — these are the two inputs to
        // per_replica_cap().
        let agg = Aggregator::with_window("A".into(), 10_000, Duration::from_secs(1));
        let (tx, mut rx) = mpsc::channel(CHANNEL_BUFFER);
        tx.send(delta("A", 4_000)).await.unwrap();
        tx.send(delta("B", 6_000)).await.unwrap();
        tx.send(delta("C", 10_000)).await.unwrap();
        let snap = agg.tick(&mut rx, Instant::now()).await;
        assert_eq!(snap.this_replica_rps, 4_000);
        assert_eq!(snap.platform_total, 20_000);
        assert_eq!(snap.replicas_seen, 3);
        // others_share = (20000 - 4000) / 2 = 8000;
        // per_replica_cap = max(1, 10000 - 8000) = 2000.
        assert_eq!(snap.per_replica_cap(), Some(2_000));
    }
}
