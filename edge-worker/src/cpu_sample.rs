//! Cached CPU% sampler for the heartbeat path (issue #85).
//!
//! `sysinfo::System::cpu_usage()` returns the cumulative CPU usage
//! since the last `refresh_cpu_usage()` call — so a single sample
//! taken at startup is always 0.0. To get a real reading we keep a
//! `System` alive on the supervisor and refresh it on every
//! heartbeat build; the reading reflects the interval between two
//! consecutive heartbeats (typically 30s).
//!
//! The cache lives behind a `Mutex` so multiple heartbeat tasks don't
//! race on the sample. The actual `sysinfo::System` API is not
//! thread-safe — only one thread at a time may call `refresh_*` and
//! read global CPU state.

use std::sync::Mutex;
use std::time::Instant;

use sysinfo::System;

/// Fraction of CPU in use on this worker, expressed as `[0.0, 100.0]`.
/// `None` when the sampler hasn't completed its first interval yet
/// (sysinfo returns 0% on the first sample, which we discard).
pub type CpuPct = Option<f64>;

pub struct CpuSample {
    inner: Mutex<Inner>,
}

struct Inner {
    sys: System,
    last_refresh: Option<Instant>,
    /// First CPU reading (discarded — sysinfo returns 0 on the
    /// initial sample, before any interval has elapsed).
    primed: bool,
}

impl CpuSample {
    pub fn new() -> Self {
        // `new()` doesn't read CPU; the first refresh + read happens
        // on the first call to `take()`. Using `new()` rather than
        // `new_all()` keeps startup fast — we don't need disk / memory
        // / network data, just CPU%.
        let mut sys = System::new();
        // Refresh CPU usage once at startup so the first take() has
        // a baseline. The actual reading from this call is discarded
        // (primed=false → take returns None).
        sys.refresh_cpu_usage();
        Self {
            inner: Mutex::new(Inner {
                sys,
                last_refresh: Some(Instant::now()),
                primed: false,
            }),
        }
    }

    /// Refresh the sample and return the CPU% since the previous
    /// call. Returns `None` on the first invocation after construction
    /// (no baseline interval yet).
    pub fn take(&self) -> CpuPct {
        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        inner.sys.refresh_cpu_usage();
        inner.last_refresh = Some(Instant::now());
        if !inner.primed {
            inner.primed = true;
            return None;
        }
        // global_cpu_usage returns `f32` in `[0.0, 100.0]`. Clamp
        // defensively — sysinfo can briefly report >100% on hot-plug
        // events. Coerce to f64 to match `CpuPct`.
        Some(f64::from(inner.sys.global_cpu_usage().clamp(0.0, 100.0)))
    }
}

impl Default for CpuSample {
    fn default() -> Self {
        Self::new()
    }
}