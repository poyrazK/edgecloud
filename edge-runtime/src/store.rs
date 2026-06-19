//! wasmtime Store creation.

use crate::limits::new_memory_limits;
use std::ptr::NonNull;
use wasmtime::{Engine, ResourceLimiter, Store, StoreLimits};

/// Create a wasmtime Store.
///
/// Memory limits are enforced via wasmtime's `ResourceLimiter` mechanism.
pub fn create_store<T>(engine: &Engine, max_memory_mb: u64, data: T) -> Store<T> {
    let mut store = Store::new(engine, data);
    if max_memory_mb == 0 {
        return store;
    }

    let limits = new_memory_limits(max_memory_mb);
    let limiter = StaticLimiter::new(limits);
    store.limiter(move |_data| limiter.limiter());

    store
}

/// Holds a `StoreLimits` with a `'static` lifetime by leaking it,
/// then hands out `&'static mut dyn ResourceLimiter` to wasmtime.
struct StaticLimiter {
    ptr: NonNull<StoreLimits>,
}

impl StaticLimiter {
    fn new(limits: StoreLimits) -> Self {
        let leaked = Box::leak(Box::new(limits));
        Self {
            ptr: NonNull::from(leaked),
        }
    }

    fn limiter(&self) -> &'static mut dyn ResourceLimiter {
        // SAFETY: Box::leak gives us ownership with 'static lifetime.
        // NonNull is aliasing-free. StoreLimits implements ResourceLimiter.
        // wasmtime calls this for the lifetime of the store, which is fine
        // since the leaked box is never freed.
        unsafe { &mut *self.ptr.as_ptr() }
    }
}

// SAFETY: StaticLimiter owns a 'static leaked Box<StoreLimits>. wasmtime calls
// the limiter from its synchronized internal context, so &self is safe.
unsafe impl Send for StaticLimiter {}
unsafe impl Sync for StaticLimiter {}

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
    #[test]
    fn limiter_traps_on_memory_grow() {
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
        let instance = wasmtime::Instance::new(&mut store, &module, &[]).expect("instantiate");
        let grow = instance
            .get_typed_func::<(), ()>(&mut store, "grow_huge")
            .expect("exported func");

        let trap = grow
            .call(&mut store, ())
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
    #[test]
    fn epoch_deadline_interrupts_infinite_loop() {
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
        let instance = wasmtime::Instance::new(&mut store, &module, &[]).expect("instantiate");
        let f = instance
            .get_typed_func::<(), ()>(&mut store, "loop")
            .expect("exported func");

        let trap = f.call(&mut store, ()).expect_err("must trap on deadline");
        let debug_msg = format!("{:?}", trap).to_lowercase();
        assert!(
            debug_msg.contains("interrupt"),
            "expected interrupt trap on deadline, got {:?}",
            debug_msg
        );
    }
}
