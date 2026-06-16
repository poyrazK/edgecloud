//! `edge:scheduling` — delayed and repeating task execution.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

// --- Time reference for Instant <-> Unix timestamp conversion ---

static BOOT_TIME: OnceLock<u64> = OnceLock::new();
static BOOT_INSTANT: OnceLock<Instant> = OnceLock::new();

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn init_time_refs() {
    BOOT_TIME.get_or_init(now_secs);
    BOOT_INSTANT.get_or_init(Instant::now);
}

/// Convert an `Instant` to a Unix timestamp in seconds.
fn instant_to_secs(i: &Instant) -> u64 {
    let boot_instant = *BOOT_INSTANT.get().unwrap();
    let boot_time = *BOOT_TIME.get().unwrap();
    let elapsed = i.saturating_duration_since(boot_instant).as_secs();
    boot_time + elapsed
}

/// Convert a Unix timestamp in seconds to an `Instant`.
fn secs_to_instant(secs: u64) -> Instant {
    let boot_instant = *BOOT_INSTANT.get().unwrap();
    let boot_time = *BOOT_TIME.get().unwrap();
    let offset = secs.saturating_sub(boot_time);
    boot_instant + std::time::Duration::from_secs(offset)
}

// --- Persistence error type ---

#[derive(Debug, thiserror::Error)]
pub enum SchedulerError {
    #[error("IO error: {0}")]
    Io(String),
    #[error("serialization error: {0}")]
    Serialization(String),
}

// --- On-disk representation ---

const SCHEDULE_FILENAME: &str = "schedule.json";
const ENV_SCHEDULING_PATH: &str = "EDGE_SCHEDULING_PATH";

#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedScheduler {
    version: u32,
    tasks: Vec<PersistedTask>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedTask {
    id: String,
    payload: String, // base64-encoded
    interval_ms: Option<u64>,
    expires_at: u64, // Unix timestamp of next scheduled firing
}

// --- Persistence handle ---

struct SchedulerPersistence {
    path: PathBuf,
}

impl SchedulerPersistence {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// Load all non-expired tasks from the schedule file.
    /// Missing or corrupt files result in an empty list (no error).
    /// Repeating tasks are rescheduled relative to now.
    pub async fn load(&self) -> Result<Vec<ScheduledTask>, SchedulerError> {
        let contents = match tokio::fs::read_to_string(&self.path).await {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!("schedule file not found, starting empty");
                return Ok(Vec::new());
            }
            Err(e) => return Err(SchedulerError::Io(e.to_string())),
        };

        let state: PersistedScheduler = serde_json::from_str(&contents)
            .map_err(|e| SchedulerError::Io(format!("corrupt schedule file: {}", e)))?;

        let now = now_secs();
        let mut loaded = Vec::with_capacity(state.tasks.len());

        for p in state.tasks {
            if p.expires_at <= now {
                // Task already expired — skip.
                continue;
            }
            let payload = BASE64
                .decode(&p.payload)
                .map_err(|_| SchedulerError::Io("invalid base64 payload".into()))?;

            // Repeating: reschedule next_at relative to now.
            // One-shot: compute next_at from expires_at.
            let next_at = secs_to_instant(p.expires_at);

            loaded.push(ScheduledTask {
                id: p.id,
                payload,
                interval_ms: p.interval_ms,
                next_at,
            });
        }

        Ok(loaded)
    }

    /// Atomically flush the current task list to disk.
    /// Uses rename-to-replace: write to .tmp, then rename.
    pub async fn flush(
        &self,
        tasks: &Mutex<HashMap<String, ScheduledTask>>,
    ) -> Result<(), SchedulerError> {
        let task_list: Vec<PersistedTask> = {
            let tasks = tasks.lock().unwrap();
            tasks
                .values()
                .map(|t| PersistedTask {
                    id: t.id.clone(),
                    payload: BASE64.encode(&t.payload),
                    interval_ms: t.interval_ms,
                    expires_at: instant_to_secs(&t.next_at),
                })
                .collect()
        };

        let state = PersistedScheduler {
            version: 1,
            tasks: task_list,
        };

        let json = serde_json::to_string(&state)
            .map_err(|e| SchedulerError::Serialization(e.to_string()))?;

        let tmp_path = self.path.with_extension("json.tmp");
        if let Some(parent) = tmp_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| SchedulerError::Io(format!("failed to create directory: {}", e)))?;
        }
        tokio::fs::write(&tmp_path, json.as_bytes())
            .await
            .map_err(|e| SchedulerError::Io(e.to_string()))?;
        tokio::fs::rename(&tmp_path, &self.path)
            .await
            .map_err(|e| SchedulerError::Io(e.to_string()))?;
        Ok(())
    }
}

// --- Main types ---

pub struct ScheduledTask {
    pub id: String,
    pub payload: Vec<u8>,
    pub interval_ms: Option<u64>, // None = one-shot, Some = repeating interval
    pub next_at: Instant,
}

pub struct Scheduler {
    tasks: Arc<Mutex<HashMap<String, ScheduledTask>>>,
    persistence: Option<SchedulerPersistence>,
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl Scheduler {
    /// Ephemeral in-memory scheduler (backward compatible).
    pub fn new() -> Self {
        init_time_refs();
        let tasks = Arc::new(Mutex::new(HashMap::<String, ScheduledTask>::new()));
        let tasks_clone = tasks.clone();

        // Spawn a background worker that fires due tasks.
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("scheduling: failed to spawn runtime");

            rt.block_on(async {
                loop {
                    let sleep_ms = {
                        let tasks = tasks_clone.lock().unwrap();
                        let next = tasks.values().map(|t| t.next_at).min_by_key(|i| *i);
                        match next {
                            Some(next_at) => {
                                let remaining = next_at.saturating_duration_since(Instant::now());
                                remaining.as_millis().max(100) as u64
                            }
                            None => 10_000,
                        }
                    };
                    tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;

                    let now = Instant::now();
                    let mut tasks = tasks_clone.lock().unwrap();
                    let due: Vec<(String, ScheduledTask)> =
                        tasks.drain().filter(|(_, t)| t.next_at <= now).collect();

                    for (id, mut task) in due {
                        if let Some(interval_ms) = task.interval_ms {
                            task.next_at =
                                Instant::now() + std::time::Duration::from_millis(interval_ms);
                            tracing::debug!(task_id = %id, interval_ms, "repeating task due");
                            tasks.insert(id, task);
                        } else {
                            tracing::debug!(task_id = %id, "one-shot task fired");
                        }
                    }
                }
            });
        });

        Self {
            tasks,
            persistence: None,
        }
    }

    /// Persistent scheduler at the given directory path.
    /// The schedule file is `<path>/schedule.json`.
    pub fn with_persistence(path: &Path) -> Result<Self, SchedulerError> {
        init_time_refs();
        let schedule_path = path.join(SCHEDULE_FILENAME);
        let persistence = SchedulerPersistence::new(schedule_path);
        let rt = tokio::runtime::Handle::try_current()
            .map_err(|_| SchedulerError::Io("no Tokio runtime active".into()))?;
        let loaded = rt.block_on(persistence.load())?;

        let tasks = Arc::new(Mutex::new(HashMap::<String, ScheduledTask>::new()));
        let tasks_clone = tasks.clone();

        // Spawn the background worker (same as new()).
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("scheduling: failed to spawn runtime");

            rt.block_on(async {
                loop {
                    let sleep_ms = {
                        let tasks = tasks_clone.lock().unwrap();
                        let next = tasks.values().map(|t| t.next_at).min_by_key(|i| *i);
                        match next {
                            Some(next_at) => {
                                let remaining = next_at.saturating_duration_since(Instant::now());
                                remaining.as_millis().max(100) as u64
                            }
                            None => 10_000,
                        }
                    };
                    tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;

                    let now = Instant::now();
                    let mut tasks = tasks_clone.lock().unwrap();
                    let due: Vec<(String, ScheduledTask)> =
                        tasks.drain().filter(|(_, t)| t.next_at <= now).collect();

                    for (id, mut task) in due {
                        if let Some(interval_ms) = task.interval_ms {
                            task.next_at =
                                Instant::now() + std::time::Duration::from_millis(interval_ms);
                            tracing::debug!(task_id = %id, interval_ms, "repeating task due");
                            tasks.insert(id, task);
                        } else {
                            tracing::debug!(task_id = %id, "one-shot task fired");
                        }
                    }
                }
            });
        });

        // Populate tasks from loaded state.
        {
            let mut tasks_guard = tasks.lock().unwrap();
            for task in loaded {
                tasks_guard.insert(task.id.clone(), task);
            }
        }

        Ok(Self {
            tasks,
            persistence: Some(persistence),
        })
    }

    /// Persistent scheduler using the `EDGE_SCHEDULING_PATH` environment variable.
    /// Returns `Ok(None)` if the env var is not set (ephemeral mode).
    pub fn from_env() -> Result<Option<Self>, SchedulerError> {
        match std::env::var(ENV_SCHEDULING_PATH) {
            Ok(path) => Self::with_persistence(Path::new(&path)).map(Some),
            Err(_) => Ok(None),
        }
    }

    /// Internal helper: flush to disk if persistence is configured.
    fn flush_if_persistent(&self) {
        if self.persistence.is_none() {
            return;
        }
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            let _ = rt.block_on(self.flush_impl());
        }
    }

    async fn flush_impl(&self) -> Result<(), SchedulerError> {
        if let Some(ref p) = self.persistence {
            p.flush(&self.tasks).await?;
        }
        Ok(())
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
        self.flush_if_persistent();
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
        self.flush_if_persistent();
        Ok(id)
    }

    pub fn cancel(&self, id: &str) -> Result<(), String> {
        let removed = self.tasks.lock().unwrap().remove(id).is_some();
        if removed {
            tracing::debug!(task_id = %id, "cancelled task");
            self.flush_if_persistent();
            Ok(())
        } else {
            Err(format!("task not found: {}", id))
        }
    }
}
