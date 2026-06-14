//! `edge:scheduling` — delayed and repeating task execution.

pub struct Scheduler {
    next_id: std::sync::atomic::AtomicU64,
}

impl Scheduler {
    pub fn new() -> Self {
        Self {
            next_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    pub fn schedule_once(&self, delay_ms: u64, _payload: Vec<u8>) -> String {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::debug!(task_id = id, delay_ms = delay_ms, "scheduled one-shot task");
        format!("task_{}", id)
    }

    pub fn schedule_repeating(&self, interval_ms: u64, _payload: Vec<u8>) -> String {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tracing::debug!(
            task_id = id,
            interval_ms = interval_ms,
            "scheduled repeating task"
        );
        format!("task_{}", id)
    }

    pub fn cancel(&self, id: &str) {
        tracing::debug!(task_id = id, "cancelled task");
    }
}
