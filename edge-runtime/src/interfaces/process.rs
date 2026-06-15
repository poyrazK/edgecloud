//! `edge:process` — environment variables, command-line args, and exit.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

/// Process state — holds per-app environment variables when used in the worker supervisor.
pub struct Process {
    env: Arc<HashMap<String, String>>,
    /// Atomic flag set when the guest calls process.exit. Non-zero = exit code requested.
    /// This allows execute_app to distinguish "guest called exit" from "wasm trap".
    exit_code: Arc<AtomicU32>,
}

impl Process {
    /// Create a new Process with no environment variables (uses host process env).
    pub fn new() -> Self {
        Self {
            env: Arc::new(std::env::vars().collect()),
            exit_code: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Create a Process with a specific per-app environment map.
    pub fn with_env(env: Arc<HashMap<String, String>>) -> Self {
        Self {
            env,
            exit_code: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Create a Process with a specific environment map and exit code flag.
    pub fn with_env_and_exit_code(
        env: Arc<HashMap<String, String>>,
        exit_code: Arc<AtomicU32>,
    ) -> Self {
        Self { env, exit_code }
    }

    pub fn get_env(&self, key: &str) -> Option<String> {
        self.env.get(key).cloned()
    }

    pub fn get_all_env(&self) -> Vec<(String, String)> {
        self.env.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    pub fn get_args(&self) -> Vec<String> {
        std::env::args().collect()
    }

    /// Called by the guest WASM component via the `exit` host function.
    /// Stores the exit code in an atomic flag and returns normally — the wasmtime
    /// trap that follows will cause `call()` to return Err, which we distinguish
    /// from a real error by checking this flag after a successful call return.
    pub fn exit(&self, code: u32) {
        self.exit_code.store(code, Ordering::SeqCst);
    }

    /// Returns `Some(code)` if the guest called process.exit, `None` otherwise.
    pub fn exit_requested(&self) -> Option<u32> {
        let code = self.exit_code.load(Ordering::SeqCst);
        if code == 0 {
            None
        } else {
            Some(code)
        }
    }
}

impl Default for Process {
    fn default() -> Self {
        Self::new()
    }
}
