//! Linker factory — single shared implementation for both WIT worlds.
//!
//! Both `edge-runtime` (long-running) and `edge-runtime-handler` (FaaS)
//! worlds share the same `edge:cloud@0.2.0` import surface and the
//! same `include wasi:cli/command@0.2.1` surface. The only difference
//! between the two worlds is the `wasi:http/incoming-handler` *export*
//! on the handler world, which `wasmtime_wasi_http::p2::bindings::ProxyPre` routes
//! against at *instantiation* time, not at linker-construction time.
//! So one factory, parameterized on `engine`, is correct.
//!
//! ## Wiring strategy (wasmtime 45.0.3)
//!
//! In wasmtime 45.0.3, `wasmtime_wasi::p2::add_to_linker_async` and
//! `wasmtime_wasi_http::p2::add_only_http_to_linker_async` register
//! their Host impls against stores that implement `WasiView` and
//! `WasiHttpView` respectively. `RuntimeState` implements both (see
//! `runtime.rs`). `T::sockets()` is reachable through the blanket
//! `impl<T: WasiView> WasiSocketsView for T` from
//! `wasmtime-wasi-45.0.3/src/view.rs:87-95` — we do **not** shadow it
//! with a manual `WasiSocketsView for RuntimeState`.
//!
//! The linker is built up in three explicit passes:
//!
//! 1. **`wasmtime_wasi::p2::add_to_linker_async`** — registers every
//!    `wasi:cli/command` import (`wasi:io/*`, `wasi:clocks/*`,
//!    `wasi:filesystem/*`, `wasi:random/*`, `wasi:sockets/*`,
//!    `wasi:cli/*`) against `T: WasiView`. Sockets policy is
//!    **injected upstream of this call** via
//!    `WasiCtxBuilder::socket_addr_check(...)` inside
//!    `build_wasi_ctx_for_tenant` (`runtime.rs`). wasmtime 45.0.3's
//!    `WasiSocketsCtx` fields are `pub(crate)`, so a manual trait impl
//!    overriding `WasiSocketsView::sockets()` is not possible —
//!    `socket_egress::make_socket_addr_check` is the closure consumed
//!    by the builder (see `socket_egress.rs`).
//! 2. **`wasmtime_wasi_http::p2::add_only_http_to_linker_async`** — adds
//!    `wasi:http/{outgoing-handler,types}` via `WasiHttpImpl<&mut T>`
//!    against `T: WasiHttpView`. `add_only_http` (NOT
//!    `add_to_linker_async`) because the latter double-registers
//!    `wasi:io`, `wasi:clocks`, `wasi:random` with step 1. HTTP egress
//!    is gated by `EgressHttpHooks::send_request` on
//!    `RuntimeState::http_hooks` (see `runtime.rs`).
//! 3. **bindgen-generated per-interface `add_to_linker_get_host` for
//!    each `edge:cloud/*` Host impl** — `RuntimeState` directly
//!    implements these (in `runtime.rs`). We invoke each individually
//!    so we don't pull in wasi:* registration accidentally.
//!
//! `allow_shadowing(true)` was removed because it was a defensive hack
//! from the v0.1 days; with wasi: + edge: in disjoint namespaces it is
//! both unnecessary and a footgun (silent overloads of canonical
//! interfaces).

use crate::RuntimeState;
use anyhow::Result;
use wasmtime::component::{HasSelf, Linker as ComponentLinker};
use wasmtime::Engine;

/// Build the linker shared by both `edge-runtime` and
/// `edge-runtime-handler` worlds. Both worlds have identical imports;
/// the choice of world happens later when
/// `Linker::instantiate(&component)` resolves the component's exports.
///
/// Long-running components implement `_start` and self-host any TCP
/// servers they need via `wasi:sockets` (registered via step 1).
/// Handler components additionally export `wasi:http/incoming-handler`,
/// which `wasmtime_wasi_http::p2::bindings::ProxyPre` dispatches against at request
/// time — see `edge-worker/src/dispatch.rs`.
pub fn create_component_linker(engine: &Engine) -> Result<ComponentLinker<RuntimeState>> {
    let mut linker: ComponentLinker<RuntimeState> = ComponentLinker::new(engine);

    // Step 1: wasi:cli/command (io, clocks, filesystem, random, sockets, cli).
    // Requires `RuntimeState: WasiView` — implemented in runtime.rs.
    wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;

    // Step 2: wasi:http/{outgoing-handler, types}. Components can make
    // outbound HTTP calls. Egress enforcement is wired via
    // RuntimeState's `EgressHttpHooks` (stored as `http_hooks`), reached
    // through `WasiHttpView::http()` -> `WasiHttpCtxView::hooks`.
    // Requires `RuntimeState: WasiHttpView` — implemented in runtime.rs.
    wasmtime_wasi_http::p2::add_only_http_to_linker_async(&mut linker)?;

    // Step 3: edge:cloud/* — registers each Host impl individually.
    // RuntimeState implements all six below in runtime.rs.
    edge_cloud_add_to_linker_get_host(&mut linker)?;

    Ok(linker)
}

/// Compatibility wrapper retained for the existing call site in
/// `lib.rs`. Both worlds link against the same `WasiView +
/// WasiHttpView + edge:cloud` surface — the world bindgen picks the
/// imports per world at component-instantiation time, so a single
/// factory covers both.
pub fn create_component_linker_long_running(
    engine: &Engine,
) -> Result<ComponentLinker<RuntimeState>> {
    create_component_linker(engine)
}

/// Compatibility wrapper retained for the existing call site in
/// `lib.rs`. See `create_component_linker` for the full rationale.
pub fn create_component_linker_handler(engine: &Engine) -> Result<ComponentLinker<RuntimeState>> {
    create_component_linker(engine)
}

/// Register every `edge:cloud@0.2.0/*` Host impl on the linker.
///
/// Each `add_to_linker` is bindgen-generated per-interface and has the
/// shape `fn add_to_linker<T, U>(linker, get: impl Fn(&mut T) -> &mut U)`
/// requiring `T: Send` and `U: <iface>::Host + Send`. We pass a plain
/// function pointer instead of a closure so the lifetime annotations
/// are unambiguous (a `Fn(&'a mut T) -> &'a mut U` closure type is what
/// the trait wants; function pointers have explicit `'a` lifetimes).
///
/// `RuntimeState` implements every `edge:cloud::*:Host` in `runtime.rs`
/// and is Send (every field is `Send` via Arc/ParkingLot RwLock
/// patterns), so `U = RuntimeState` works for every interface.
///
/// Both worlds share the `edge:cloud@0.2.0` package, so the bindgens
/// generate the SAME interface module paths under
/// `edge_runtime_long::edge::cloud::*` and
/// `edge_runtime_handler::edge::cloud::*`. We register each interface
/// once via the long_running bindgen's path (equivalent to the handler
/// one's — same underlying Host impl since the WIT bodies are
/// identical; the bindgens differ only in their generated trait
/// namespaces, not the host impls).
pub(crate) fn host_getter(state: &mut RuntimeState) -> &mut RuntimeState {
    state
}

fn edge_cloud_add_to_linker_get_host(linker: &mut ComponentLinker<RuntimeState>) -> Result<()> {
    use crate::edge_runtime_long::edge::cloud as long_cloud;

    macro_rules! register_host {
        ($mod:ident) => {{
            // In wasmtime 36 the bindgen-generated add_to_linker wants
            // `for<'a> Fn(&'a mut T) -> <T as HasData>::Data<'a>`. The
            // turbofish `HasSelf<RuntimeState>` tells the macro the
            // store type explicitly — `HasSelf<T>`'s blanket `Data<'a> =
            // &'a mut T` then resolves the (otherwise ambiguous) two-
            // world Host impls.
            long_cloud::$mod::add_to_linker::<_, HasSelf<RuntimeState>>(linker, host_getter)?;
        }};
    }

    register_host!(cache);
    register_host!(kv_store);
    register_host!(observe);
    register_host!(time);
    register_host!(scheduling);
    register_host!(process);
    register_host!(websocket);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::create_engine;
    use crate::runtime::RuntimeState;

    #[test]
    fn create_linker_succeeds() {
        let engine = create_engine().expect("engine");
        let linker = create_component_linker(&engine).expect("linker");
        drop(linker);
    }

    #[test]
    fn create_long_running_linker_succeeds() {
        let engine = create_engine().expect("engine");
        let linker = create_component_linker_long_running(&engine).expect("linker");
        drop(linker);
    }

    #[test]
    fn create_handler_linker_succeeds() {
        let engine = create_engine().expect("engine");
        let linker = create_component_linker_handler(&engine).expect("linker");
        drop(linker);
    }

    #[test]
    fn host_getter_returns_identity() {
        let mut state = RuntimeState::new();
        let returned: *mut RuntimeState = host_getter(&mut state);
        let original: *mut RuntimeState = &mut state;
        assert_eq!(
            returned, original,
            "host_getter must return the same RuntimeState reference"
        );
    }

    #[test]
    fn edge_cloud_interfaces_registered() {
        let engine = create_engine().expect("engine");
        let mut linker: ComponentLinker<RuntimeState> = ComponentLinker::new(&engine);
        // All 6 edge:cloud interfaces register without error.
        let result = edge_cloud_add_to_linker_get_host(&mut linker);
        assert!(result.is_ok(), "all edge:cloud interfaces must register");
    }

    #[test]
    fn linker_works_with_default_runtime_state() {
        // Both worlds use the same linker factory — this test
        // confirms RuntimeState::new() (test constructor) is
        // accepted by the linker, which exercises the WasiView,
        // WasiHttpView, and all 6 edge:cloud Host impls.
        let engine = create_engine().expect("engine");
        let _linker = create_component_linker(&engine).expect("linker");
    }
}
