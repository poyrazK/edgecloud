//! `edge:scheduling` — delayed and repeating task execution.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use uuid::Uuid;

pub struct ScheduledTask {
    pub id: String,
    pub payload: Vec<u8>,
    pub interval_ms: Option<u64>, // None = one-shot, Some = repeating interval
    pub next_at: Instant,
}

pub struct Scheduler {
    tasks: Arc<Mutex<HashMap<String, ScheduledTask>>>,
}

impl Scheduler {
    pub fn new() -> Self {
        let tasks = Arc::new(Mutex::new(HashMap::<String, ScheduledTask>::new()));
        let tasks_clone = tasks.clone();

        // Spawn a background worker that fires due tasks.
        // Task execution (wasm payload invocation) is left to the caller —
        // this worker only tracks scheduling state and removes completed one-shots.
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("scheduling: failed to spawn runtime");

            rt.block_on(async {
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

                    // Collect due tasks.
                    let now = Instant::now();
                    let mut tasks = tasks_clone.lock().unwrap();
                    let due: Vec<(String, ScheduledTask)> =
                        tasks.drain().filter(|(_, t)| t.next_at <= now).collect();

                    for (id, mut task) in due {
                        if let Some(interval_ms) = task.interval_ms {
                            // Repeating: reinsert with next deadline.
                            task.next_at =
                                Instant::now() + std::time::Duration::from_millis(interval_ms);
                            tracing::debug!(task_id = %id, interval_ms, "repeating task due");
                            tasks.insert(id, task);
                        } else {
                            // One-shot: removed, execution is caller responsibility.
                            tracing::debug!(task_id = %id, "one-shot task fired");
                        }
                    }
                }
            });
        });

        Self { tasks }
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
        let removed = self.tasks.lock().unwrap().remove(id).is_some();
        if removed {
            tracing::debug!(task_id = %id, "cancelled task");
            Ok(())
        } else {
            Err(format!("task not found: {}", id))
        }
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}
