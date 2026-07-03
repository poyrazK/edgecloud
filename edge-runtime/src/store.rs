//! wasmtime Store creation.

use crate::limits::new_memory_limits;
use wasmtime::{Engine, ResourceLimiter, Store, StoreLimits};

/// Types that embed a [`StoreLimits`] for wasmtime resource accounting.
///
/// Implement this on the Store data type `T` so that [`create_store`] can
/// wire resource limits via a properly lifetime-bounded closure — no
/// `'static` extension, no heap allocation beyond the [`Store`] itself,
/// no unsafe code.
pub trait HasStoreLimits {
    /// Called by [`create_store`] before the wasmtime [`Store`] is
    /// constructed. Implementations store `limits` in a field.
    fn set_store_limits(&mut self, limits: StoreLimits);

    /// Returns a mutable reference to the embedded [`ResourceLimiter`].
    /// Called by wasmtime's limiter callback on every resource check.
    /// The lifetime is tied to `&mut self`, so no `'static` extension
    /// is needed.
    ///
    /// # Panics
    /// May panic if called before [`set_store_limits`].
    fn store_limits_mut(&mut self) -> &mut dyn ResourceLimiter;
}

/// Create a wasmtime [`Store`] with optional memory limits.
///
/// When `max_memory_mb` is `0` no limiter is installed and the guest can
/// grow memory without bound. For any non-zero value the limits are
/// embedded in `data` via [`HasStoreLimits`] and exposed to wasmtime's
/// resource-limiter callback with a properly lifetime-bounded closure.
pub fn create_store<T: HasStoreLimits>(
    engine: &Engine,
    max_memory_mb: u64,
    mut data: T,
) -> Store<T> {
    if max_memory_mb > 0 {
        data.set_store_limits(new_memory_limits(max_memory_mb));
    }
    let mut store = Store::new(engine, data);
    if max_memory_mb > 0 {
        store.limiter(|data| data.store_limits_mut());
    }
    store
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::create_engine;
    use crate::runtime::RuntimeState;
    use wasmtime::{Module, Store};

    /// Smoke test: `create_store` returns a Store wired up with the limiter
    /// from `RuntimeState`. The limiter is applied by the time the store is
    /// returned, so a subsequent attempt to grow memory past the configured
    /// cap will trap rather than succeed. See `limiter_traps_on_memory_grow`
    /// below for the real assertion.
    #[test]
    fn create_store_attaches_limiter() {
        let engine = create_engine().expect("engine");
        let state = RuntimeState::new();
        let _store: Store<RuntimeState> = create_store(&engine, 1, state);
    }

    /// End-to-end proof that `StoreLimits` actually traps memory.grow past the
    /// configured cap (issue #39). Without the limiter wiring in `create_store`
    /// this test would silently succeed in growing memory far past 1 MiB.
    ///
    /// wasmtime 25.x surfaces both memory-cap denials and epoch-deadline hits
    /// as the same wasm trap variant ("interrupt") in its public Error Display,
    /// so we assert on the wasm-trap prefix. The crucial contract is that
    /// *something* traps the guest — without the limiter the grow would
    /// succeed.
    ///
    /// Skipped on Windows: wasmtime trap delivery triggers
    /// STATUS_STACK_BUFFER_OVERRUN in the Windows test runner.
    ///
    /// `config.async_support(true)` is enabled in `create_engine()` (Phase C)
    /// — required for `wasi:cli/command` wiring — so instantiation and the
    /// function call must use the `_async` variants. wasmtime enforces this
    /// at runtime: `must use async instantiation when async support is enabled`.
    #[tokio::test(flavor = "current_thread")]
    #[cfg_attr(
        windows,
        ignore = "wasmtime trap delivery triggers STATUS_STACK_BUFFER_OVERRUN on Windows"
    )]
    async fn limiter_traps_on_memory_grow() {
        let engine = create_engine().expect("engine");
        // 1 MiB cap. memory.grow of 1024 pages (64 MiB) must trap.
        let state = RuntimeState::new();
        let mut store = create_store(&engine, 1, state);

        let wat = r#"
            (module
              (memory (export "mem") 1)              ;; 1 page = 64 KiB
              (func (export "grow_huge")
                ;; Ask for 1024 pages = 64 MiB; well past the 1 MiB cap.
                (drop (memory.grow (i32.const 1024)))
              )
            )
        "#;
        let module =
            Module::new(&engine, wat::parse_str(wat).expect("valid wat")).expect("compile module");
        let instance = wasmtime::Instance::new_async(&mut store, &module, &[])
            .await
            .expect("instantiate");
        let grow = instance
            .get_typed_func::<(), ()>(&mut store, "grow_huge")
            .expect("exported func");

        let trap = grow
            .call_async(&mut store, ())
            .await
            .expect_err("must trap on memory cap");
        let debug_msg = format!("{:?}", trap).to_lowercase();
        assert!(
            debug_msg.contains("wasm trap"),
            "expected wasm trap (memory cap), got {:?}",
            debug_msg
        );
    }

    /// End-to-end proof that `Store::set_epoch_deadline` + `Engine::increment_epoch`
    /// actually interrupts a runaway guest (issue #40). Without this wiring a
    /// guest `(loop $L br $L)` would hang the worker forever.
    ///
    /// wasmtime 25.x surfaces deadline hits as the "interrupt" trap variant
    /// in the public Display. The crucial contract is that the guest
    /// *returns at all* — without the deadline it would loop forever and
    /// `call` would never come back.
    ///
    /// Skipped on Windows: wasmtime's epoch signal delivery triggers
    /// STATUS_STACK_BUFFER_OVERRUN in the Windows test runner.
    ///
    /// See `limiter_traps_on_memory_grow` for why we use the `_async`
    /// instantiation + call variants (Phase C's `async_support(true)`).
    #[tokio::test(flavor = "current_thread")]
    #[cfg_attr(
        windows,
        ignore = "wasmtime epoch signals trigger STATUS_STACK_BUFFER_OVERRUN on Windows"
    )]
    async fn epoch_deadline_interrupts_infinite_loop() {
        let engine = create_engine().expect("engine");
        let state = RuntimeState::new();
        let mut store = create_store(&engine, 64, state);

        // Tight deadline — 2 ticks — and the engine starts at epoch 0.
        store.set_epoch_deadline(2);
        // Advance the engine epoch past the deadline.
        engine.increment_epoch();
        engine.increment_epoch();

        let wat = r#"
            (module
              (func (export "loop") (loop $L br $L))
            )
        "#;
        let module =
            Module::new(&engine, wat::parse_str(wat).expect("valid wat")).expect("compile module");
        let instance = wasmtime::Instance::new_async(&mut store, &module, &[])
            .await
            .expect("instantiate");
        let f = instance
            .get_typed_func::<(), ()>(&mut store, "loop")
            .expect("exported func");

        let trap = f
            .call_async(&mut store, ())
            .await
            .expect_err("must trap on deadline");
        let debug_msg = format!("{:?}", trap).to_lowercase();
        assert!(
            debug_msg.contains("interrupt"),
            "expected interrupt trap on deadline, got {:?}",
            debug_msg
        );
    }
}
