//! `edge:scheduling` — delayed and repeating task execution.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use uuid::Uuid;

#[derive(Clone)]
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

                        // Only push to queue if not already pending delivery.
                        let already_queued = {
                            let queue = due_queue_clone.lock().unwrap();
                            queue.iter().any(|t| t.id == id)
                        };
                        if !already_queued {
                            let mut queue = due_queue_clone.lock().unwrap();
                            // Enforce max queue size — drop oldest if full.
                            if queue.len() >= MAX_DUE_QUEUE {
                                let drain_count = queue.len() - MAX_DUE_QUEUE + 1;
                                queue.drain(0..drain_count);
                            }
                            queue.push(task.clone());
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
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}