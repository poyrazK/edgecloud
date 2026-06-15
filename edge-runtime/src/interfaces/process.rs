//! `edge:process` — environment variables, command-line args, and exit.

use std::collections::HashMap;
use std::sync::Arc;

/// Process state — holds per-app environment variables when used in the worker supervisor.
pub struct Process {
    env: Arc<HashMap<String, String>>,
}

impl Process {
    /// Create a new Process with no environment variables (uses host process env).
    pub fn new() -> Self {
        Self {
            env: Arc::new(std::env::vars().collect()),
        }
    }

    /// Create a Process with a specific per-app environment map.
    pub fn with_env(env: Arc<HashMap<String, String>>) -> Self {
        Self { env }
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

    pub fn exit(&self, code: u32) -> ! {
        std::process::exit(code as i32)
    }
}

impl Default for Process {
    fn default() -> Self {
        Self::new()
    }
}
