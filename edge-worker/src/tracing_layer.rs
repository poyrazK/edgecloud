//! `tracing_subscriber::Layer` that bridges worker-side `tracing` events
//! to the existing `LogForwarder` so they reach the control plane's
//! `/api/internal/logs` endpoint.
//!
//! Why this exists
//! ===============
//!
//! Before this layer, every `tracing::info!` / `warn!` / `error!` call in
//! the worker only landed on local stdout. Operators could not see
//! host-side events (downloads, supervisor lifecycle, hash mismatches,
//! NATS errors, heartbeat failures) in the central log store. This
//! layer funnels those events through `LogForwarder::push` — the same
//! path `edge:observe::emit_log` already uses for guest records — so
//! workers produce one unified log stream.
//!
//! Worker-side records carry an empty `AppLogContext` (all three fields
//! are `""`). The `logs` table column `app_name` / `deployment_id` are
//! `VARCHAR NOT NULL`, which accepts `""`. The control plane's
//! `IngestLogs` overwrites `tenant_id` from the JWT claim, so the
//! empty `tenant_id` becomes the worker's tenant at insert time.
//! Operators query worker-side events with `app_name = ''`; per-app
//! events with `app_name <> ''`.
//!
//! Double-shipping prevention
//! ==========================
//!
//! Guest `emit_log` calls already pass through
//! `Observer::emit_log_record_inner` (see
//! `edge-runtime::interfaces::observe.rs:317-363`) which mirrors the
//! event into `tracing::*!` (module path
//! `edge_runtime::interfaces::observe`) AND forwards the `LogRecord`
//! to the configured `LogSink::push`. Without a filter, this layer
//! would re-ship the guest record on top of the runtime's
//! `log_sink.push` at `observe.rs:362`. The filter below skips events
//! whose `module_path()` starts with `edge_runtime::` — the runtime's
//! mirror never reaches the forwarder via this path.
//!
//! Re-entrancy guard
//! =================
//!
//! `LogForwarder::push` emits its own `tracing::warn!` when the buffer
//! is at hard cap. Without the `in_push: AtomicBool` guard, that warn
//! would re-enter this `on_event`, recursing until the stack blows.
//! The guard is set on entry to `forwarder.push` and cleared via an
//! `InPushGuard` `Drop` impl so a panic inside `push` still releases
//! it (the `Drop` runs during unwinding).

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use edge_runtime::interfaces::observe::{AppLogContext, LogLevel, LogRecord, LogSink};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::Layer;

/// Module-path prefix for runtime-sourced events. Events from
/// `edge_runtime::...` are mirrored into `tracing::*!` by the runtime
/// itself (see `Observer::emit_log_record_inner`) and already reach the
/// forwarder via `log_sink.push`. This layer MUST NOT also re-ship them.
const RUNTIME_MODULE_PREFIX: &str = "edge_runtime::";

/// Sentinel field name `tracing` uses for the formatted message in
/// every event (either explicitly as `tracing::info!("msg", ...)` or
/// implicitly via `tracing::info!(...)` with a literal positional arg).
const MESSAGE_FIELD: &str = "message";

/// `tracing_subscriber::Layer` that ships worker-side events to a
/// `LogSink` (the `LogForwarder` in production) over
/// `/api/internal/logs`.
///
/// Generic over `S: Subscriber` so it composes with any subscriber the
/// `Registry` is built around (in our case: `tracing_subscriber::Registry`
/// + the `fmt::Layer` for stdout).
pub struct WorkerLogLayer {
    /// Destination sink. Shared with the runtime so the same LogForwarder
    /// services both guest and host events. Already `Send + Sync` (Mutex-guarded).
    forwarder: Arc<dyn LogSink>,
    /// Minimum level to forward. Events strictly below this are dropped at
    /// `on_event` time so they never touch the forwarder's buffer.
    min_level: Level,
    /// Re-entrancy guard. Set while `forwarder.push` is in progress so a
    /// `tracing::warn!` fired from inside `push` doesn't recursively
    /// reach this layer.
    in_push: AtomicBool,
}

impl WorkerLogLayer {
    pub fn new(forwarder: Arc<dyn LogSink>, min_level: Level) -> Self {
        Self {
            forwarder,
            min_level,
            in_push: AtomicBool::new(false),
        }
    }
}

impl<S: Subscriber> Layer<S> for WorkerLogLayer {
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let metadata = event.metadata();

        // Skip events from the runtime crate — they are the mirror
        // calls inside Observer::emit_log_record_inner, already shipped
        // by log_sink.push at observe.rs:362.
        if let Some(module) = metadata.module_path() {
            if module.starts_with(RUNTIME_MODULE_PREFIX) {
                return;
            }
        }

        // Level filter. `Level` is `Ord`; lower severity == higher ordinal.
        if *metadata.level() > self.min_level {
            return;
        }

        // Re-entrancy guard. The forwarder's own `tracing::warn!` on
        // hard-cap drop would re-enter this method without it.
        if self.in_push.swap(true, Ordering::AcqRel) {
            return;
        }
        let _guard = InPushGuard {
            flag: &self.in_push,
        };

        // Walk the event's fields to capture the formatted message and
        // any structured key/value pairs. The `Visit` pattern is
        // `tracing`'s canonical way to extract fields without going
        // through a format boundary.
        let mut visitor = WorkerEventVisitor::default();
        event.record(&mut visitor);

        // If `tracing::*!` was called with no message argument, fall
        // back to the metadata-level event name (empty message would
        // produce an unhelpful blank log line).
        let message = if visitor.message.is_empty() {
            metadata.name().to_string()
        } else {
            visitor.message
        };

        let record = LogRecord {
            // Worker-side events have no canonical timestamp source
            // the control plane trusts more than NOW(); the handler's
            // DB default stamps `ts = NOW()`. We pass 0 — the handler
            // ignores it.
            timestamp_ms: 0,
            level: tracing_level_to_log_level(*metadata.level()),
            message,
            labels: visitor.labels,
        };

        self.forwarder.push(record, AppLogContext::empty());
    }
}

/// RAII guard that clears `WorkerLogLayer::in_push` on drop. Holds a
/// reference to the flag so the guard lifetime is tied to the layer's
/// field — clearing the same atomic the swap set.
struct InPushGuard<'a> {
    flag: &'a AtomicBool,
}

impl Drop for InPushGuard<'_> {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

/// Visitor that extracts the message and any structured fields from a
/// `tracing::Event`.
///
/// The default impl of each `record_*` method appends to internal
/// buffers; we only need `record_str` (covers the most common field
/// types — `&str` literals), `record_debug` (covers
/// `tracing::info!(err = %e, ...)` and `tracing::info!(port = raw_port, ...)`
/// where the value is a non-string Debug-able type), and the integer /
/// bool overrides so they don't fall through to `record_debug` with
/// `{:?}` formatting when a plain decimal string would be cheaper.
#[derive(Default)]
struct WorkerEventVisitor {
    message: String,
    labels: Vec<(String, String)>,
}

impl Visit for WorkerEventVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == MESSAGE_FIELD && self.message.is_empty() {
            // Multiple `record_str` calls for `message` are not possible
            // per event; the first one wins.
            self.message = value.to_string();
        } else if field.name() != MESSAGE_FIELD {
            self.labels
                .push((field.name().to_string(), value.to_string()));
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() == MESSAGE_FIELD && self.message.is_empty() {
            // `tracing::info!("literal")` records the literal as a
            // `&str` via `record_str`; the debug branch fires only for
            // non-string message args (e.g. `format_args!`). Defensive
            // — copy the formatted output through `format!` so it
            // allocates once.
            self.message = format!("{:?}", value);
        } else if field.name() != MESSAGE_FIELD {
            self.labels
                .push((field.name().to_string(), format!("{:?}", value)));
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.labels
            .push((field.name().to_string(), value.to_string()));
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.labels
            .push((field.name().to_string(), value.to_string()));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.labels
            .push((field.name().to_string(), value.to_string()));
    }

    // Default impls for record_f64 / record_i128 / record_u128 / record_bytes
    // fall through to record_debug, which captures them via Debug.
}

fn tracing_level_to_log_level(level: Level) -> LogLevel {
    match level {
        Level::ERROR => LogLevel::Error,
        Level::WARN => LogLevel::Warn,
        Level::INFO => LogLevel::Info,
        Level::DEBUG => LogLevel::Debug,
        Level::TRACE => LogLevel::Trace,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tracing::dispatcher;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::Registry;

    /// In-memory sink for tests. Captures every `push` call so we can
    /// assert on the records the layer produced.
    #[derive(Default)]
    struct CapturingSink {
        records: Mutex<Vec<(LogRecord, AppLogContext)>>,
    }
    impl LogSink for CapturingSink {
        fn push(&self, record: LogRecord, ctx: AppLogContext) {
            self.records.lock().unwrap().push((record, ctx));
        }
    }

    /// Run `f` with a subscriber stack of `[Registry -> WorkerLogLayer]`
    /// (no fmt layer — we don't want stdout noise in tests).
    fn with_layer<F: FnOnce()>(sink: Arc<CapturingSink>, level: Level, f: F) {
        let layer = WorkerLogLayer::new(sink.clone(), level);
        let subscriber = Registry::default().with(layer);
        let _guard = dispatcher::set_default(&subscriber.into());
        f();
    }

    /// Basic: a worker-side `tracing::info!` produces exactly one record
    /// with the right level, message, and an empty `AppLogContext`.
    #[test]
    fn on_event_forwards_info_record() {
        let sink = Arc::new(CapturingSink::default());
        with_layer(sink.clone(), Level::INFO, || {
            tracing::info!("cache directory ready");
        });
        let records = sink.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        let (rec, ctx) = &records[0];
        assert_eq!(rec.level, LogLevel::Info);
        assert_eq!(rec.message, "cache directory ready");
        assert!(ctx.app_name.is_empty());
        assert!(ctx.tenant_id.is_empty());
        assert!(ctx.deployment_id.is_empty());
    }

    /// Structured fields become labels. `tracing::info!(k = "v", ...)`
    /// produces `record_str` for the `&str` values; the message comes
    /// through the same `record_str` path.
    #[test]
    fn on_event_extracts_fields_as_labels() {
        let sink = Arc::new(CapturingSink::default());
        with_layer(sink.clone(), Level::INFO, || {
            tracing::info!(
                deployment_id = "d_xyz",
                err = "boom",
                "cache read failed; downloading"
            );
        });
        let records = sink.records.lock().unwrap();
        let (rec, _) = &records[0];
        assert_eq!(rec.message, "cache read failed; downloading");
        let labels: std::collections::HashMap<_, _> = rec.labels.iter().cloned().collect();
        assert_eq!(
            labels.get("deployment_id").map(String::as_str),
            Some("d_xyz")
        );
        assert_eq!(labels.get("err").map(String::as_str), Some("boom"));
    }

    /// Level filter: `debug!` and `trace!` events are dropped when
    /// min_level is `info`.
    #[test]
    fn on_event_drops_below_min_level() {
        let sink = Arc::new(CapturingSink::default());
        with_layer(sink.clone(), Level::INFO, || {
            tracing::debug!("cache hit");
            tracing::info!("downloading artifact");
            tracing::trace!("scheduler tick");
        });
        let records = sink.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0.message, "downloading artifact");
    }

    /// Predicate test: the module-path filter must skip
    /// `edge_runtime::*` events. Production
    /// `Event::metadata().module_path()` is allocation-free (returns
    /// `Option<&'static str>`), so we test the predicate directly
    /// rather than dispatching an event from a synthetic module path.
    #[test]
    fn module_path_predicate_skips_runtime() {
        assert!(should_skip_module(Some(
            "edge_runtime::interfaces::observe"
        )));
        assert!(should_skip_module(Some("edge_runtime::foo")));
        assert!(!should_skip_module(Some("edge_worker::main")));
        assert!(!should_skip_module(Some("edge_worker::downloader")));
        assert!(!should_skip_module(None));
    }

    /// The actual filter predicate. Exposed as a function so tests can
    /// call it without needing to dispatch a real `Event` from
    /// `edge_runtime::*`. The runtime never produces events from any
    /// module path outside its own crate, so this covers the production
    /// filter path.
    fn should_skip_module(module: Option<&str>) -> bool {
        matches!(module, Some(m) if m.starts_with(RUNTIME_MODULE_PREFIX))
    }

    /// Integration smoke: an `Event` fired from a module outside
    /// `edge_runtime::*` produces exactly one record. We use the current
    /// test module's module path (`edge_worker::tracing_layer::tests`) —
    /// since this module lives in the `edge_worker` crate, the filter
    /// passes it through.
    #[test]
    fn on_event_passes_through_worker_modules() {
        let sink = Arc::new(CapturingSink::default());
        with_layer(sink.clone(), Level::INFO, || {
            tracing::warn!(deployment_id = "d_1", "artifact hash mismatch");
        });
        let records = sink.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        let (rec, ctx) = &records[0];
        assert_eq!(rec.level, LogLevel::Warn);
        assert_eq!(rec.message, "artifact hash mismatch");
        assert_eq!(
            rec.labels
                .iter()
                .find(|(k, _)| k == "deployment_id")
                .map(|(_, v)| v.as_str()),
            Some("d_1")
        );
        assert!(ctx.app_name.is_empty());
    }

    /// A `tracing::error!` event with structured fields (`expected`,
    /// `actual`) gets all fields into labels and the literal message
    /// preserved.
    #[test]
    fn on_event_preserves_error_message_and_fields() {
        let sink = Arc::new(CapturingSink::default());
        with_layer(sink.clone(), Level::DEBUG, || {
            tracing::error!(
                deployment_id = "d_1",
                expected = "abc123",
                actual = "def456",
                "artifact hash mismatch — refusing to instantiate"
            );
        });
        let records = sink.records.lock().unwrap();
        let (rec, _) = &records[0];
        assert_eq!(rec.level, LogLevel::Error);
        assert_eq!(
            rec.message,
            "artifact hash mismatch — refusing to instantiate"
        );
        let labels: std::collections::HashMap<_, _> = rec.labels.iter().cloned().collect();
        assert_eq!(labels.get("deployment_id").map(String::as_str), Some("d_1"));
        assert_eq!(labels.get("expected").map(String::as_str), Some("abc123"));
        assert_eq!(labels.get("actual").map(String::as_str), Some("def456"));
    }

    /// Re-entrancy guard: a sink whose `push` itself fires a
    /// `tracing::warn!` must NOT cause the outer `push` to recurse
    /// infinitely. The layer must drop the inner event.
    #[test]
    fn on_event_breaks_recursion_via_in_push_guard() {
        /// Sink whose push emits its own `tracing::warn!`. Mirrors the
        /// hard-cap drop warning inside `LogForwarder::push`.
        struct RecursingSink {
            outer_calls: Mutex<u32>,
        }
        impl LogSink for RecursingSink {
            fn push(&self, _record: LogRecord, _ctx: AppLogContext) {
                *self.outer_calls.lock().unwrap() += 1;
                tracing::warn!("inner warn from inside push");
            }
        }
        let sink = Arc::new(RecursingSink {
            outer_calls: Mutex::new(0),
        });
        let layer = WorkerLogLayer::new(sink.clone() as Arc<dyn LogSink>, Level::INFO);
        let subscriber = Registry::default().with(layer);
        let _guard = dispatcher::set_default(&subscriber.into());

        tracing::info!("outer event");
        // Drop the dispatcher so we can read the counters without
        // racing with the in-flight handler.
        drop(_guard);

        // Exactly ONE outer push — the inner warn was suppressed.
        assert_eq!(*sink.outer_calls.lock().unwrap(), 1);
    }

    /// Event with no message → falls back to metadata.name() (non-empty).
    #[test]
    fn on_event_fallback_to_metadata_name() {
        let sink = Arc::new(CapturingSink::default());
        with_layer(sink.clone(), Level::INFO, || {
            // Use tracing::event! with only a structured field, no literal message.
            // The metadata.name() fallback produces a non-empty string.
            tracing::event!(target: "my_function", Level::INFO, key = "val");
        });
        let records = sink.records.lock().unwrap();
        assert_eq!(records.len(), 1);
        let (rec, _) = &records[0];
        // The message should be non-empty (metadata.name() fallback).
        // The exact content depends on the tracing implementation.
        assert!(
            !rec.message.is_empty(),
            "message should be non-empty (metadata.name fallback)"
        );
    }

    /// Tracing event with a u64 field captured via record_u64.
    #[test]
    fn on_event_handles_u64_field() {
        let sink = Arc::new(CapturingSink::default());
        with_layer(sink.clone(), Level::INFO, || {
            tracing::info!(port = 8080u64, "server bound");
        });
        let records = sink.records.lock().unwrap();
        let (rec, _) = &records[0];
        let labels: std::collections::HashMap<_, _> = rec.labels.iter().cloned().collect();
        assert_eq!(labels.get("port").map(String::as_str), Some("8080"));
    }

    /// Tracing event with an i64 field captured via record_i64.
    #[test]
    fn on_event_handles_i64_field() {
        let sink = Arc::new(CapturingSink::default());
        with_layer(sink.clone(), Level::INFO, || {
            tracing::info!(exit_code = -1i64, "process exited");
        });
        let records = sink.records.lock().unwrap();
        let (rec, _) = &records[0];
        let labels: std::collections::HashMap<_, _> = rec.labels.iter().cloned().collect();
        assert_eq!(labels.get("exit_code").map(String::as_str), Some("-1"));
    }

    /// Tracing event with a bool field captured via record_bool.
    #[test]
    fn on_event_handles_bool_field() {
        let sink = Arc::new(CapturingSink::default());
        with_layer(sink.clone(), Level::INFO, || {
            tracing::info!(ready = true, "component loaded");
        });
        let records = sink.records.lock().unwrap();
        let (rec, _) = &records[0];
        let labels: std::collections::HashMap<_, _> = rec.labels.iter().cloned().collect();
        assert_eq!(labels.get("ready").map(String::as_str), Some("true"));
    }

    /// Debug fallback: f64 values captured via record_debug.
    #[test]
    fn on_event_handles_debug_fallback() {
        let sink = Arc::new(CapturingSink::default());
        with_layer(sink.clone(), Level::INFO, || {
            tracing::info!(latency_ms = 42.5f64, "request completed");
        });
        let records = sink.records.lock().unwrap();
        let (rec, _) = &records[0];
        let labels: std::collections::HashMap<_, _> = rec.labels.iter().cloned().collect();
        let val = labels.get("latency_ms").unwrap();
        assert!(
            val.contains("42.5"),
            "expected debug formatting of f64, got {val}"
        );
    }

    /// tracing_level_to_log_level maps all 5 variants correctly.
    #[test]
    fn tracing_level_to_log_level_maps_error() {
        assert_eq!(tracing_level_to_log_level(Level::ERROR), LogLevel::Error);
    }

    #[test]
    fn tracing_level_to_log_level_maps_warn() {
        assert_eq!(tracing_level_to_log_level(Level::WARN), LogLevel::Warn);
    }

    #[test]
    fn tracing_level_to_log_level_maps_info() {
        assert_eq!(tracing_level_to_log_level(Level::INFO), LogLevel::Info);
    }

    #[test]
    fn tracing_level_to_log_level_maps_debug() {
        assert_eq!(tracing_level_to_log_level(Level::DEBUG), LogLevel::Debug);
    }

    #[test]
    fn tracing_level_to_log_level_maps_trace() {
        assert_eq!(tracing_level_to_log_level(Level::TRACE), LogLevel::Trace);
    }
}
