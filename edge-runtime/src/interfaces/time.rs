//! `edge:time` — monotonic clock and sleep.

pub struct Clock;

impl Clock {
    pub fn new() -> Self {
        Self
    }

    pub fn now(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    pub fn sleep(&self, duration_ms: u64) {
        std::thread::sleep(std::time::Duration::from_millis(duration_ms));
    }

    pub fn resolution(&self) -> u64 {
        100 // nanoseconds
    }
}
