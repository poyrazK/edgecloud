//! Port allocation with sequential assignment and cooldown.

use std::collections::HashSet;
use std::time::{Duration, Instant};

/// Port pool for allocating TCP ports to apps.
///
/// Ports are allocated sequentially starting at `starting_port`.
/// When a port is released, it enters a cooldown period before being
/// re-available, preventing address reuse conflicts with TIME_WAIT connections.
pub struct PortPool {
    next_port: u16,
    starting_port: u16,
    cooldown_secs: u64,
    /// Ports available for immediate allocation (populated as ports leave cooldown).
    available: HashSet<u16>,
    /// Ports currently in cooldown: (port, release_time).
    cooling_down: Vec<(u16, Instant)>,
}

impl PortPool {
    /// Create a new port pool.
    ///
    /// - `starting_port`: first port to allocate (e.g., 8081)
    /// - `cooldown_secs`: seconds before a released port is re-available
    pub fn new(starting_port: u16, cooldown_secs: u64) -> Self {
        let mut pool = Self {
            next_port: starting_port,
            starting_port,
            cooldown_secs,
            available: HashSet::new(),
            cooling_down: Vec::new(),
        };
        // Pre-populate with a range of available ports for fast O(1) allocation.
        for port in starting_port..(starting_port + 100) {
            pool.available.insert(port);
        }
        pool
    }

    /// Acquire a port for an app. Returns `None` if the pool is exhausted.
    pub fn acquire(&mut self) -> Option<u16> {
        self.reap_cooled_ports();

        // Fast path: try pre-populated available ports.
        if let Some(port) = self.available.iter().copied().next() {
            self.available.remove(&port);
            return Some(port);
        }

        // Sequential fallback: find the next port not currently in cooldown.
        // Caps at 1000 iterations to prevent infinite loops if all ports are in
        // cooldown (e.g., during a burst of restarts).
        let mut attempts = 0u32;
        while attempts < 1000 {
            let port = self.next_port;
            self.next_port = self.next_port.saturating_add(1);
            if self.next_port == u16::MAX {
                self.next_port = self.starting_port;
            }
            attempts += 1;

            // Skip ports currently in cooldown.
            if !self.cooling_down.iter().any(|(p, _)| *p == port) {
                return Some(port);
            }
        }

        // Exhausted: all ports are in cooldown.
        None
    }

    /// Number of immediately allocatable ports (the autoscaler's capacity
    /// signal). Does not count the sequential fallback range — when
    /// `available` drops to zero, the worker is effectively at capacity.
    pub fn free_slots(&mut self) -> u32 {
        self.reap_cooled_ports();
        self.available.len() as u32
    }

    /// Release a port back into cooldown.
    /// Guard against double-release: if the port is already cooling down, this
    /// is a no-op.
    pub fn release(&mut self, port: u16) {
        if self.cooling_down.iter().any(|(p, _)| *p == port) {
            return; // already cooling down
        }
        let release_time = Instant::now() + Duration::from_secs(self.cooldown_secs);
        self.cooling_down.push((port, release_time));
    }

    /// Move cooled ports back into the available set.
    fn reap_cooled_ports(&mut self) {
        let now = Instant::now();
        self.cooling_down.retain(|(port, release_time)| {
            if now >= *release_time {
                self.available.insert(*port);
                false // remove from cooling_down
            } else {
                true // keep in cooling_down
            }
        });
    }

    /// Whether `port` is currently in the cooldown set (test-only).
    /// Used by `stop_app_releases_ws_port` (#448 regression) to
    /// assert that `release(ws_port)` actually ran after a
    /// `stop_app` — without exposing the internal `cooling_down`
    /// Vec to the test crate.
    #[cfg(test)]
    pub fn is_in_cooldown(&self, port: u16) -> bool {
        self.cooling_down.iter().any(|(p, _)| *p == port)
    }

    /// Drain the pool, leaving exactly `available_remaining` ports
    /// allocatable (test-only). Both the pre-populated `available`
    /// set and the next 1000 sequential-fallback ports are pushed
    /// into `cooling_down` so `acquire()` only succeeds up to
    /// `available_remaining` times. `next_port` is intentionally
    /// not advanced.
    ///
    /// `available_remaining` must be ≤ the pre-populated size (100);
    /// values > 100 are clamped to 100. After this runs, the next
    /// `available_remaining` calls to `acquire()` return `Some(_)`
    /// (consuming the remaining `available` set), and any further
    /// `acquire()` walks the 1000-iter sequential-fallback cap,
    /// finds every port in cooldown, returns `None`.
    ///
    /// Used by the two #641 regression tests:
    /// - `start_app_returns_err_when_port_pool_exhausted` calls
    ///   with `available_remaining=0` to fully exhaust the pool.
    /// - `start_app_releases_raw_port_when_ws_port_acquire_fails`
    ///   calls with `available_remaining=1` so the HTTP acquire
    ///   succeeds and the WS acquire fails.
    ///
    /// Assumes `next_port` is far enough from `u16::MAX` that the
    /// 1000-iteration push loop doesn't wrap. The regression tests
    /// use `starting_port=10000` so the loop ends at ~11000.
    #[cfg(test)]
    pub fn drain_leaving_n_available(&mut self, available_remaining: usize) {
        let now = Instant::now();
        let release_time = now + Duration::from_secs(self.cooldown_secs);

        // Drain the pre-populated `available` set, then re-add
        // `available_remaining` ports at the tail so subsequent
        // `acquire()` calls can still consume them.
        let pre_populated: Vec<u16> = self.available.drain().collect();
        let keep = available_remaining.min(pre_populated.len());
        for port in &pre_populated[pre_populated.len() - keep..] {
            self.available.insert(*port);
        }
        for port in &pre_populated[..pre_populated.len() - keep] {
            self.cooling_down.push((*port, release_time));
        }

        // Push the next 1000 sequential-fallback ports into cooldown
        // without advancing the cursor. The next `acquire` that
        // exhausts the remaining `available` slots walks the 1000-iter
        // cap and returns None.
        let cursor_start = self.next_port;
        let mut p = cursor_start;
        let mut i = 0u32;
        while i < 1000 {
            self.cooling_down.push((p, release_time));
            p += 1;
            i += 1;
        }
    }

    /// Backwards-compatible alias for the existing
    /// `start_app_returns_err_when_port_pool_exhausted` test —
    /// drains the pool to fully exhausted.
    #[cfg(test)]
    pub fn drain_available_into_cooldown(&mut self) -> usize {
        let before = self.available.len();
        self.drain_leaving_n_available(0);
        before
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acquire_and_release() {
        let mut pool = PortPool::new(8081, 60);
        let port = pool.acquire();
        assert!(port.is_some());
        pool.release(port.unwrap());
    }

    #[test]
    fn test_cooldown() {
        let mut pool = PortPool::new(8081, 0); // 0-second cooldown for testing
        let port = pool.acquire().unwrap();
        pool.release(port);
        // With 0-second cooldown, port should be immediately available
        let next = pool.acquire().unwrap();
        assert_eq!(port, next);
    }

    #[test]
    fn test_double_release_ignored() {
        let mut pool = PortPool::new(8081, 60);
        let port = pool.acquire().unwrap();
        pool.release(port);
        pool.release(port); // second release should be a no-op
                            // Port should only be in cooling_down once; acquire returns it after cooldown.
                            // With 0 cooldown it would be immediately available, but with 60s it stays
                            // in cooling_down so acquire falls back to sequential.
                            // Verify by checking the port is NOT in available (since cooldown hasn't passed).
        let next = pool.acquire();
        assert_ne!(next, Some(port));
    }

    #[test]
    fn test_sequential_fallback_skips_cooldown() {
        let mut pool = PortPool::new(8081, 0); // 0-second cooldown so ports are immediately reusable
                                               // Exhaust the pre-populated ports
        let mut ports = Vec::new();
        for _ in 0..100 {
            ports.push(pool.acquire().unwrap());
        }
        // Now fallback kicks in — sequential should work
        let port = pool.acquire().unwrap();
        assert!(port >= 8081);
        // Release it and acquire again (cooldown is 0 so it's immediately available)
        pool.release(port);
        let next = pool.acquire().unwrap();
        assert_eq!(port, next);
    }
}
