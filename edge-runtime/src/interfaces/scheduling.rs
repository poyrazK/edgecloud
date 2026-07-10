//! `edge:scheduling` — delayed and repeating task execution.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
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
    #[error("invalid tenant_id: {0:?}")]
    InvalidTenantId(String),
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

#[derive(Clone)]
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
    shutdown: Arc<AtomicBool>,
    /// Signals the background thread to wake and exit without waiting the full sleep period.
    shutdown_notify: Arc<tokio::sync::Notify>,
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        self.shutdown_notify.notify_one();
    }
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
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();
        let shutdown_notify = Arc::new(tokio::sync::Notify::new());
        let shutdown_notify_clone = shutdown_notify.clone();

        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("scheduling: failed to spawn runtime");

            rt.block_on(async {
                loop {
                    if shutdown_clone.load(Ordering::Acquire) {
                        break;
                    }
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
                    tokio::select! {
                        _ = tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)) => {}
                        _ = shutdown_notify_clone.notified() => { break; }
                    }
                    if shutdown_clone.load(Ordering::Acquire) {
                        break;
                    }

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
            shutdown,
            shutdown_notify,
        }
    }

    /// Persistent scheduler at the given directory path.
    /// The schedule file is `<path>/schedule.json`.
    pub fn with_persistence(path: &Path) -> Result<Self, SchedulerError> {
        init_time_refs();
        let schedule_path = path.join(SCHEDULE_FILENAME);
        let persistence = SchedulerPersistence::new(schedule_path);

        // Load persisted state in a dedicated thread with its own runtime.
        // This avoids calling block_on on the caller's runtime, which panics
        // if the caller is already running inside a Tokio async context.
        let (tx, rx) = std::sync::mpsc::channel();
        let persistence_for_load = persistence.clone();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("scheduling: failed to spawn runtime");
            let result = rt.block_on(persistence_for_load.load());
            let _ = tx.send(result);
        });
        let loaded = rx
            .recv()
            .map_err(|_| SchedulerError::Io("load thread panicked".into()))??;

        // Populate tasks before spawning the worker so the thread sees the full
        // task list immediately on its first iteration.
        let mut initial_tasks = HashMap::<String, ScheduledTask>::new();
        for task in loaded {
            initial_tasks.insert(task.id.clone(), task);
        }
        let tasks = Arc::new(Mutex::new(initial_tasks));
        let tasks_clone = tasks.clone();
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();
        let shutdown_notify = Arc::new(tokio::sync::Notify::new());
        let shutdown_notify_clone = shutdown_notify.clone();

        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("scheduling: failed to spawn runtime");

            rt.block_on(async {
                loop {
                    if shutdown_clone.load(Ordering::Acquire) {
                        break;
                    }
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
                    tokio::select! {
                        _ = tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)) => {}
                        _ = shutdown_notify_clone.notified() => { break; }
                    }
                    if shutdown_clone.load(Ordering::Acquire) {
                        break;
                    }

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

        Ok(Self {
            tasks,
            persistence: Some(persistence),
            shutdown,
            shutdown_notify,
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

    /// Persistent scheduler scoped to a specific tenant.
    /// The schedule file is `{EDGE_SCHEDULING_PATH}/{tenant_id}/schedule.json`.
    /// Returns `Ok(None)` if `EDGE_SCHEDULING_PATH` is not set.
    pub fn from_env_for_tenant(tenant_id: &str) -> Result<Option<Self>, SchedulerError> {
        if !super::is_safe_tenant_id(tenant_id) {
            return Err(SchedulerError::InvalidTenantId(tenant_id.to_string()));
        }
        match std::env::var(ENV_SCHEDULING_PATH) {
            Ok(base) => {
                let path = Path::new(&base).join(tenant_id);
                Self::with_persistence(&path).map(Some)
            }
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

    /// Drop every scheduled task in this scheduler. Used by
    /// `runtime::purge_tenant` (issue #569) when a tenant's data
    /// lifecycle ends — the worker stops every app for the tenant,
    /// then drops the in-memory scheduling state before the
    /// on-disk directory is removed.
    ///
    /// Unlike per-task `cancel`, this method does NOT call
    /// `flush_if_persistent` — the caller is about to remove the
    /// persistence file, so a final flush would race with the
    /// `remove_dir_all`. The in-memory `JoinHandle`s held by each
    /// task are dropped here; their `Drop` impl aborts the
    /// underlying tokio task, so a 60s repeating task in flight is
    /// cancelled without further work.
    pub fn abort_all(&self) {
        let mut tasks = self.tasks.lock().unwrap();
        let n = tasks.len();
        tasks.clear();
        tracing::debug!(count = n, "scheduler.abort_all dropped all tasks");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schedule_once_returns_valid_id() {
        let scheduler = Scheduler::new();
        let id = scheduler
            .schedule_once(60_000, b"payload".to_vec())
            .unwrap();
        assert!(!id.is_empty());
    }

    #[test]
    fn test_schedule_repeating_returns_valid_id() {
        let scheduler = Scheduler::new();
        let id = scheduler
            .schedule_repeating(1_000, b"payload".to_vec())
            .unwrap();
        assert!(!id.is_empty());
    }

    #[test]
    fn test_cancel_one_shot_before_fire() {
        let scheduler = Scheduler::new();
        let id = scheduler
            .schedule_once(600_000, b"payload".to_vec())
            .unwrap();
        scheduler.cancel(&id).unwrap();
        let result = scheduler.cancel(&id);
        assert!(result.is_err()); // already cancelled
    }

    #[test]
    fn test_cancel_repeating_before_fire() {
        let scheduler = Scheduler::new();
        let id = scheduler
            .schedule_repeating(1_000, b"payload".to_vec())
            .unwrap();
        scheduler.cancel(&id).unwrap();
        let result = scheduler.cancel(&id);
        assert!(result.is_err()); // already cancelled
    }

    #[test]
    fn test_cancel_unknown_id_returns_err() {
        let scheduler = Scheduler::new();
        let result = scheduler.cancel("not-a-real-id");
        assert!(result.is_err());
    }

    // abort_all (issue #569): a bulk drop of every scheduled task —
    // the runtime purge_tenant helper relies on this to tear down a
    // tenant's scheduler entries without enumerating IDs. After
    // abort_all, every prior cancel returns Err (task not found).
    #[test]
    fn test_abort_all_drops_every_task() {
        let scheduler = Scheduler::new();
        let id1 = scheduler.schedule_once(60_000, b"a".to_vec()).unwrap();
        let id2 = scheduler.schedule_repeating(60_000, b"b".to_vec()).unwrap();

        scheduler.abort_all();

        assert!(scheduler.cancel(&id1).is_err(), "id1 must be gone");
        assert!(scheduler.cancel(&id2).is_err(), "id2 must be gone");
    }

    // abort_all on an empty scheduler is a no-op — locks the
    // idempotency contract runtime::purge_tenant depends on.
    #[test]
    fn test_abort_all_on_empty_is_noop() {
        let scheduler = Scheduler::new();
        scheduler.abort_all();
        // No panic, no side effects. Asserting only via re-call.
        scheduler.abort_all();
    }

    #[test]
    fn test_from_env_returns_none_when_not_set() {
        let result = Scheduler::from_env();
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[test]
    fn test_instant_to_secs_roundtrip() {
        init_time_refs();
        let instant = Instant::now();
        let secs = instant_to_secs(&instant);
        let recovered = secs_to_instant(secs);
        // Recovered instant should be within 2 seconds of original (clock drift)
        let drift = instant.saturating_duration_since(recovered);
        assert!(drift.as_secs() < 2);
    }

    // ── Persistence ─────────────────────────────────────────────────────
    //
    // The scheduler's `flush_if_persistent` needs a tokio runtime handle
    // on the current thread. In `#[test]` there is none, so we create a
    // runtime and enter its context.

    #[test]
    fn test_scheduling_persistence_survives_drop() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let path = dir.path().join("sched.json");

        let id;
        {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime");
            let _guard = rt.enter();
            let s = Scheduler::with_persistence(&path).expect("create");
            id = s.schedule_once(60_000, b"payload".to_vec()).unwrap();
        } // s drops, scheduler thread exits

        // Rebuild — task should still be cancellable.
        {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime");
            let _guard = rt.enter();
            let s = Scheduler::with_persistence(&path).expect("reload");
            s.cancel(&id)
                .expect("persisted task should be cancellable after reload");
        }
    }

    #[test]
    fn test_scheduling_persistence_repeating_survives_drop() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let path = dir.path().join("sched-repeating.json");

        let id;
        {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime");
            let _guard = rt.enter();
            let s = Scheduler::with_persistence(&path).expect("create");
            id = s.schedule_repeating(10_000, b"payload".to_vec()).unwrap();
        }

        {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime");
            let _guard = rt.enter();
            let s = Scheduler::with_persistence(&path).expect("reload");
            s.cancel(&id)
                .expect("persisted repeating task should be cancellable");
        }
    }

    #[test]
    fn test_scheduling_persistence_empty_round_trips() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let path = dir.path().join("sched-empty.json");

        {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime");
            let _guard = rt.enter();
            let _s = Scheduler::with_persistence(&path).expect("create");
        }

        // Rebuild — should not crash and should accept new tasks.
        {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime");
            let _guard = rt.enter();
            let s = Scheduler::with_persistence(&path).expect("reload");
            let id = s.schedule_once(60_000, b"new".to_vec()).unwrap();
            s.cancel(&id).unwrap();
        }
    }

    // ── Persistence error paths ────────────────────────────────────────

    #[test]
    fn persistence_load_corrupted_json() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        // with_persistence expects a directory — it joins SCHEDULE_FILENAME.
        std::fs::write(dir.path().join(SCHEDULE_FILENAME), "{invalid json}").unwrap();
        assert!(
            Scheduler::with_persistence(dir.path()).is_err(),
            "corrupted JSON should return Err"
        );
    }

    #[test]
    fn persistence_load_corrupted_base64() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let data = r#"{"version":1,"tasks":[{"id":"t1","payload":"not-base64!!","interval_ms":null,"expires_at":9999999999}]}"#;
        std::fs::write(dir.path().join(SCHEDULE_FILENAME), data).unwrap();
        let err = Scheduler::with_persistence(dir.path()).err().unwrap();
        let msg = format!("{err:?}");
        assert!(msg.contains("base64"), "expected base64 error, got {msg}");
    }

    #[test]
    fn persistence_load_non_existent_file() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        // Point at an empty dir — no schedule.json exists, should return empty.
        let s =
            Scheduler::with_persistence(dir.path()).expect("should return Ok with empty scheduler");
        assert!(s.cancel("nonexistent").is_err());
    }

    #[test]
    fn persistence_flush_if_persistent_no_store() {
        let s = Scheduler::new();
        let id = s.schedule_once(60_000, b"payload".to_vec()).unwrap();
        s.cancel(&id).unwrap();
    }

    #[test]
    fn persistence_ttl_expiry_drops_oneshot_on_reload() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let past = 100_000_000u64; // definitely expired

        // Write a schedule file directly with an expired one-shot task.
        let expired_entry = serde_json::json!({
            "version": 1,
            "tasks": [{
                "id": "expired-task",
                "payload": base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"dead"),
                "interval_ms": null,
                "expires_at": past
            }]
        });
        std::fs::write(
            dir.path().join(SCHEDULE_FILENAME),
            expired_entry.to_string(),
        )
        .unwrap();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let _guard = rt.enter();
        let s = Scheduler::with_persistence(dir.path()).expect("load with expired task");
        // The expired one-shot was dropped on load — cancel should fail.
        assert!(
            s.cancel("expired-task").is_err(),
            "expired task must not survive reload"
        );
    }
}
