//! Sliding-window RPS sum. Filled in by PR B (issue #665).
//!
//! Holds a `VecDeque<(Instant, u32)>` of (per-replica, timestamped) delta
//! entries and answers two questions on every tick:
//!   1. `current_total()` — sum of all entries inside the 1-second window
//!   2. `prune()` — drop entries whose timestamp is older than
//!      `now - window_ms` (a replica that hasn't published in >1s is no
//!      longer counted; matches the "missed second tolerated" invariant).
//!
//! The actual sliding-window implementation lands in PR B (tests 8.a/8.b
//! in the issue #665 plan).

#![allow(dead_code)]
