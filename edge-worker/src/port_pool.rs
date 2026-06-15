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
    /// Ports available for immediate allocation.
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
        // Pre-populate with a range of available ports
        for port in starting_port..(starting_port + 100) {
            pool.available.insert(port);
        }
        pool
    }

    /// Acquire a port for an app. Returns `None` if the pool is exhausted.
    pub fn acquire(&mut self) -> Option<u16> {
        self.reap_cooled_ports();

        // First try pre-populated available ports
        if let Some(port) = self.available.iter().copied().next() {
            self.available.remove(&port);
            return Some(port);
        }

        // Fall back to sequential allocation (should rarely happen)
        let port = self.next_port;
        self.next_port = self.next_port.saturating_add(1);
        if self.next_port == u16::MAX {
            self.next_port = self.starting_port;
        }
        Some(port)
    }

    /// Release a port back into cooldown.
    pub fn release(&mut self, port: u16) {
        let release_time = Instant::now() + Duration::from_secs(self.cooldown_secs);
        self.cooling_down.push((port, release_time));
    }

    /// Move cooled ports back into the available set.
    fn reap_cooled_ports(&mut self) {
        let now = Instant::now();
        self.cooling_down.retain(|(port, release_time)| {
            if now >= *release_time {
                self.available.insert(*port);
                false
            } else {
                true
            }
        });
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
}
