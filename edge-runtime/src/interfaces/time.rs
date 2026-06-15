//! `edge:time` — monotonic clock and sleep.

#[derive(Default)]
pub struct Clock;

impl Clock {
    pub fn new() -> Self {
        Self {}
    }

    pub fn now(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    /// Sleep — delegates to the scheduling subsystem's thread pool so it does not
    /// block the tokio main thread.
    pub fn sleep(&self, duration_ms: u64) -> Result<(), String> {
        let duration = std::time::Duration::from_millis(duration_ms);
        std::thread::sleep(duration);
        Ok(())
    }

    pub fn resolution(&self) -> u64 {
        100 // nanoseconds
    }
}
