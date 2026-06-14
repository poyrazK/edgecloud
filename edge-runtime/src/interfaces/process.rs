//! `edge:process` — environment variables, command-line args, and exit.

pub struct Process;

impl Process {
    pub fn new() -> Self {
        Self
    }

    pub fn get_env(&self, key: &str) -> Option<String> {
        std::env::var(key).ok()
    }

    pub fn get_all_env(&self) -> Vec<(String, String)> {
        std::env::vars().collect()
    }

    pub fn get_args(&self) -> Vec<String> {
        std::env::args().collect()
    }

    pub fn exit(&self, code: u32) -> ! {
        std::process::exit(code as i32)
    }
}
