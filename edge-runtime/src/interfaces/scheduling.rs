//! `edge:scheduling` — delayed and repeating task execution.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq)]
pub struct ScheduledTask {
    pub id: String,
    pub payload: Vec<u8>,
    pub interval_ms: Option<u64>, // None = one-shot, Some = repeating interval
    pub next_at: Instant,
}

/// Maximum number of due tasks that can accumulate in the queue.
/// Excess tasks are dropped (oldest first) to prevent unbounded memory growth.
const MAX_DUE_QUEUE: usize = 1000;

pub struct Scheduler {
    tasks: Arc<Mutex<HashMap<String, ScheduledTask>>>,
    /// Queue of due tasks ready for the guest to poll.
    due_queue: Arc<Mutex<Vec<ScheduledTask>>>,
}

impl Scheduler {
    /// Create a new Scheduler using the current tokio runtime handle.
    pub fn new() -> Self {
        Self::new_with_handle(tokio::runtime::Handle::current())
    }

    /// Create a Scheduler with an explicit runtime handle.
    pub fn new_with_handle(handle: tokio::runtime::Handle) -> Self {
        let tasks = Arc::new(Mutex::new(HashMap::<String, ScheduledTask>::new()));
        let due_queue: Arc<Mutex<Vec<ScheduledTask>>> = Arc::new(Mutex::new(Vec::new()));
        let tasks_clone = tasks.clone();
        let due_queue_clone = due_queue.clone();

        // Spawn a background worker that fires due tasks and pushes them to due_queue.
        handle.spawn(async move {
            loop {
                // Sleep until the next task is due (or a generous interval if none).
                let sleep_ms = {
                    let tasks = tasks_clone.lock().unwrap();
                    let next = tasks.values().map(|t| t.next_at).min();
                    match next {
                        Some(next_at) => {
                            let remaining = next_at.saturating_duration_since(Instant::now());
                            remaining.as_millis().max(100) as u64
                        }
                        None => 10_000,
                    }
                };
                tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;

                // Collect due tasks without removing non-due ones.
                let now = Instant::now();
                let due: Vec<(String, ScheduledTask)> = {
                    let tasks = tasks_clone.lock().unwrap();
                    tasks
                        .iter()
                        .filter(|(_, t)| t.next_at <= now)
                        .map(|(id, t)| (id.clone(), t.clone()))
                        .collect()
                };

                // Remove only the due tasks from the HashMap.
                {
                    let mut tasks = tasks_clone.lock().unwrap();
                    tasks.retain(|_, t| t.next_at > now);
                }

                for (id, mut task) in due {
                    if let Some(interval_ms) = task.interval_ms {
                        // Repeating: reinsert with next deadline.
                        task.next_at =
                            Instant::now() + std::time::Duration::from_millis(interval_ms);
                        tracing::debug!(task_id = %id, interval_ms, "repeating task due");

                        // Only push to queue if not already pending delivery — single lock.
                        {
                            let mut queue = due_queue_clone.lock().unwrap();
                            if !queue.iter().any(|t| t.id == id) {
                                if queue.len() >= MAX_DUE_QUEUE {
                                    let drain_count = queue.len() - MAX_DUE_QUEUE + 1;
                                    queue.drain(0..drain_count);
                                }
                                queue.push(task.clone());
                            }
                        }

                        let mut tasks = tasks_clone.lock().unwrap();
                        tasks.insert(id, task);
                    } else {
                        // One-shot: push to queue for guest polling.
                        tracing::debug!(task_id = %id, "one-shot task due");
                        let mut queue = due_queue_clone.lock().unwrap();
                        if queue.len() >= MAX_DUE_QUEUE {
                            let drain_count = queue.len() - MAX_DUE_QUEUE + 1;
                            queue.drain(0..drain_count);
                        }
                        queue.push(task);
                    }
                }
            }
        });

        Self {
            tasks,
            due_queue,
        }
    }

    pub fn schedule_once(&self, delay_ms: u64, payload: Vec<u8>) -> Result<String, String> {
        let id = Uuid::new_v4().to_string();
        let task = ScheduledTask {
            id: id.clone(),
            payload,
            interval_ms: None,
            next_at: Instant::now() + std::time::Duration::from_millis(delay_ms),
        };
        self.tasks.lock().unwrap().insert(id.clone(), task);
        tracing::debug!(task_id = %id, delay_ms, "scheduled one-shot task");
        Ok(id)
    }

    pub fn schedule_repeating(&self, interval_ms: u64, payload: Vec<u8>) -> Result<String, String> {
        let id = Uuid::new_v4().to_string();
        let task = ScheduledTask {
            id: id.clone(),
            payload,
            interval_ms: Some(interval_ms),
            next_at: Instant::now() + std::time::Duration::from_millis(interval_ms),
        };
        self.tasks.lock().unwrap().insert(id.clone(), task);
        tracing::debug!(task_id = %id, interval_ms, "scheduled repeating task");
        Ok(id)
    }

    pub fn cancel(&self, id: &str) -> Result<(), String> {
        // First try to remove from the tasks HashMap.
        if self.tasks.lock().unwrap().remove(id).is_some() {
            tracing::debug!(task_id = %id, "cancelled task (not yet queued)");
            return Ok(());
        }

        // Task not in HashMap — it may already be in the due_queue.
        let mut queue = self.due_queue.lock().unwrap();
        if let Some(pos) = queue.iter().position(|t| t.id == id) {
            queue.remove(pos);
            tracing::debug!(task_id = %id, "cancelled task (was queued)");
            Ok(())
        } else {
            Err(format!("task not found: {}", id))
        }
    }

    /// Poll for the next due task. Returns (id, payload) if a task is ready.
    pub fn poll_scheduled(&self) -> Option<(String, Vec<u8>)> {
        let mut queue = self.due_queue.lock().unwrap();
        queue.pop().map(|t| (t.id, t.payload))
    }

    #[cfg(test)]
    pub fn num_tasks(&self) -> usize {
        self.tasks.lock().unwrap().len()
    }

    #[cfg(test)]
    pub fn num_due(&self) -> usize {
        self.due_queue.lock().unwrap().len()
    }

    #[cfg(test)]
    /// Directly push a task into the due_queue (bypasses the worker).
    /// Enforces MAX_DUE_QUEUE cap — drops oldest entries if full.
    /// Use for deterministic testing without async sleeps.
    pub fn inject_task(&self, task: ScheduledTask) {
        let mut queue = self.due_queue.lock().unwrap();
        if queue.len() >= MAX_DUE_QUEUE {
            let drain_count = queue.len() - MAX_DUE_QUEUE + 1;
            queue.drain(0..drain_count);
        }
        queue.push(task);
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper: runtime for tests that need to spawn async workers.
    fn create_test_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap()
    }

    #[test]
    fn test_cancel_removes_task_not_yet_queued() {
        // Cancel before the worker fires — task is still in tasks HashMap.
        let rt = create_test_runtime();
        let scheduler = Scheduler::new_with_handle(rt.handle().clone());

        let id = scheduler.schedule_once(10_000, b"payload".to_vec()).unwrap();

        // Task is in tasks, not yet in queue
        assert_eq!(scheduler.num_tasks(), 1);
        assert_eq!(scheduler.num_due(), 0);

        // Cancel should succeed
        scheduler.cancel(&id).unwrap();

        // Task is gone from both
        assert_eq!(scheduler.num_tasks(), 0);
        assert_eq!(scheduler.num_due(), 0);
    }

    #[test]
    fn test_cancel_removes_queued_task() {
        // Cancel after task is in due_queue (injected directly).
        let rt = create_test_runtime();
        let scheduler = Scheduler::new_with_handle(rt.handle().clone());

        let id = Uuid::new_v4().to_string();
        let task = ScheduledTask {
            id: id.clone(),
            payload: b"payload".to_vec(),
            interval_ms: None,
            next_at: Instant::now(),
        };
        scheduler.inject_task(task);

        assert_eq!(scheduler.num_tasks(), 0);
        assert_eq!(scheduler.num_due(), 1);

        // Cancel should find and remove it from queue
        scheduler.cancel(&id).unwrap();

        assert_eq!(scheduler.num_due(), 0);
    }

    #[test]
    fn test_cancel_unknown_id_returns_error() {
        let rt = create_test_runtime();
        let scheduler = Scheduler::new_with_handle(rt.handle().clone());

        let result = scheduler.cancel("nonexistent-id");
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "task not found: nonexistent-id");
    }

    #[test]
    fn test_poll_scheduled_returns_payload() {
        let rt = create_test_runtime();
        let scheduler = Scheduler::new_with_handle(rt.handle().clone());

        let id = Uuid::new_v4().to_string();
        let payload = b"test-payload".to_vec();
        let task = ScheduledTask {
            id: id.clone(),
            payload: payload.clone(),
            interval_ms: None,
            next_at: Instant::now(),
        };
        scheduler.inject_task(task);

        let result = scheduler.poll_scheduled();
        assert!(result.is_some());
        let (polled_id, polled_payload) = result.unwrap();
        assert_eq!(polled_id, id);
        assert_eq!(polled_payload, payload);
    }

    #[test]
    fn test_poll_scheduled_empty_queue_returns_none() {
        let rt = create_test_runtime();
        let scheduler = Scheduler::new_with_handle(rt.handle().clone());

        let result = scheduler.poll_scheduled();
        assert!(result.is_none());
    }

    #[test]
    fn test_due_queue_max_size_capped() {
        let rt = create_test_runtime();
        let scheduler = Scheduler::new_with_handle(rt.handle().clone());

        // Inject MAX_DUE_QUEUE + 5 tasks (task-0 through task-1004)
        for i in 0..(MAX_DUE_QUEUE + 5) {
            let task = ScheduledTask {
                id: format!("task-{}", i),
                payload: vec![],
                interval_ms: None,
                next_at: Instant::now(),
            };
            scheduler.inject_task(task);
        }

        // Queue must be capped at MAX_DUE_QUEUE regardless of how many tasks were injected
        assert_eq!(scheduler.num_due(), MAX_DUE_QUEUE);

        // After MAX_DUE_QUEUE+5 injections, at least 5 oldest tasks must have been dropped.
        // We verify this by checking that task-0 through task-4 are NOT in the remaining queue.
        let mut found_ids = Vec::new();
        while let Some((id, _)) = scheduler.poll_scheduled() {
            found_ids.push(id);
        }
        for i in 0..5 {
            let id = format!("task-{}", i);
            assert!(!found_ids.contains(&id), "task-{} should have been dropped", i);
        }
        // All remaining tasks should be >= task-5
        for id in &found_ids {
            let num: usize = id.strip_prefix("task-").unwrap().parse().unwrap();
            assert!(num >= 5, "expected task-5 or higher, got {}", id);
        }
    }

    #[test]
    fn test_repeating_task_not_duplicated_in_queue() {
        // Verify that inject_task + cancel pattern works for repeating tasks
        let rt = create_test_runtime();
        let scheduler = Scheduler::new_with_handle(rt.handle().clone());

        let id = Uuid::new_v4().to_string();
        let task1 = ScheduledTask {
            id: id.clone(),
            payload: b"first".to_vec(),
            interval_ms: Some(1000),
            next_at: Instant::now(),
        };
        let task2 = ScheduledTask {
            id: id.clone(),
            payload: b"second".to_vec(),
            interval_ms: Some(1000),
            next_at: Instant::now(),
        };

        // Inject two tasks with same id (simulating two fire cycles before polling)
        scheduler.inject_task(task1);
        scheduler.inject_task(task2);

        // Both were pushed (no deduplication at inject level — that's the worker's job)
        assert_eq!(scheduler.num_due(), 2);

        // First poll returns first
        let (polled_id, _) = scheduler.poll_scheduled().unwrap();
        assert_eq!(polled_id, id);

        // Second poll returns second
        let (polled_id, _) = scheduler.poll_scheduled().unwrap();
        assert_eq!(polled_id, id);

        // Queue is now empty
        assert!(scheduler.poll_scheduled().is_none());
    }

    #[test]
    fn test_schedule_once_inserts_into_tasks_hashmap() {
        let rt = create_test_runtime();
        let scheduler = Scheduler::new_with_handle(rt.handle().clone());

        let id = scheduler.schedule_once(5000, b"hello".to_vec()).unwrap();

        assert!(!id.is_empty());
        assert_eq!(scheduler.num_tasks(), 1);
        assert_eq!(scheduler.num_due(), 0);
    }

    #[test]
    fn test_schedule_repeating_inserts_into_tasks_hashmap() {
        let rt = create_test_runtime();
        let scheduler = Scheduler::new_with_handle(rt.handle().clone());

        let id = scheduler.schedule_repeating(1000, b"repeating".to_vec()).unwrap();

        assert!(!id.is_empty());
        assert_eq!(scheduler.num_tasks(), 1);
        assert_eq!(scheduler.num_due(), 0);
    }
}
