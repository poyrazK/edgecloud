//! edge-runtime: WASI Preview 2 host interfaces for edge computing.

// wasmtime 45's bindgen! macro no longer accepts the top-level
// `tracing` / `verbose_tracing` keys — they're replaced by per-function
// `imports:` / `exports:` blocks. The old `false, false` defaults still
// apply (no per-function tracing block here), so the silence is intentional.

pub mod egress;
pub(crate) mod egress_transport;
pub mod engine;
pub mod limits;
pub mod linker;
pub mod memory;
pub mod metering;
pub mod runtime;
pub mod socket_egress;
pub mod store;

pub mod interfaces;

// Generated WIT bindings — two worlds, each in its own submodule so the
// `edge::cloud::*` paths do not collide between worlds.
//
//   * `edge_runtime_long`    — long-running world (edge:cloud/edge-runtime@0.2.0)
//   * `edge_runtime_handler` — FaaS world (edge:cloud/edge-runtime-handler@0.2.0)
//
// Both worlds live in a SINGLE file (`edge-cloud.wit`) because
// wit-parser 0.217 errors on duplicate interface definitions when a
// package is split across multiple .wit files. Each world includes
// `wasi:cli/command@0.2.1` to pull in the canonical wasi:* surface
// (io, clocks, filesystem, random, sockets, cli/*) without re-listing
// each interface. We pass `path: "src/wit"` (a directory) so bindgen
// calls `wit_parser::Resolve::push_dir`, which also scans
// `src/wit/deps/` for vendored WASI Preview 2 packages (see
// `src/wit/deps/README.md`).
//
// ## Linker wiring strategy (Phase C)
//
// We do NOT use per-interface `with:` mappings for the wasi:* imports.
// The bindgen-generated `LongRunningWorld::add_to_linker` requires the
// supplied `T` to implement each `Host` trait bound to a `with:`-mapped
// interface — `wit-bindgen 0.51` (bundled with wasmtime 25) does NOT
// auto-wrap in `WasiImpl`. Mapping wasi:* to `wasmtime_wasi::bindings`
// would demand `RuntimeState: wasmtime_wasi::bindings::io::error::Host`
// — reimplementing 100+ wasi: host methods for no gain.
//
// Instead, `linker.rs` does the canonical split:
//
//   * `wasmtime_wasi::add_to_linker_async(&mut linker)` — registers every
//     `wasi:cli/command` import (`wasi:io/*`, `wasi:clocks/*`,
//     `wasi:filesystem/*`, `wasi:random/*`, `wasi:sockets/*`,
//     `wasi:cli/*`) using the canonical `WasiImpl<T>` wrapper internally.
//   * `wasmtime_wasi_http::add_only_http_to_linker_async(&mut linker)` —
//     adds `wasi:http/{outgoing-handler,types}` using `WasiHttpImpl<T>`.
//   * `edge_runtime_long::edge::cloud::*::add_to_linker_get_host(...)` —
//     one call per edge:cloud/* interface, with `|state| state` since
//     RuntimeState implements those Host traits in `runtime.rs`.
//
// We DO `with:` map `wasi:http/incoming-handler` (handler world only)
// to `wasmtime_wasi_http::p2::bindings::exports::wasi::http::incoming_handler`
// — required so the handler world's EXPORT trait is the SAME
// `Guest` trait wasmtime_wasi_http::ProxyPre calls into. Without this,
// type-matched dispatch through `ProxyPre::call_handle` would fail
// (the host would call a local Guest that doesn't exist).
pub mod edge_runtime_long {
    wasmtime::component::bindgen!({
        world: "edge-runtime",
        path: "src/wit",
    });
}

pub mod edge_runtime_handler {
    wasmtime::component::bindgen!({
        world: "edge-runtime-handler",
        path: "src/wit",
        // See the module-level comment in `edge_runtime_long` for the
        // rationale on `with:`. The only `with:` mapping we use is for
        // the `wasi:http/incoming-handler` export, so the bindgen-
        // generated `Guest` trait in our handler bindings IS the same
        // `Guest` that `wasmtime_wasi_http::ProxyPre::call_handle` is
        // statically dispatched against.
        with: {
            "wasi:http/incoming-handler": wasmtime_wasi_http::p2::bindings::exports::wasi::http::incoming_handler,
        },
    });
}

pub use egress::EgressPolicy;
pub use engine::create_engine;
pub use linker::{create_component_linker_handler, create_component_linker_long_running};
pub use metering::RequestMeter;
pub use runtime::{is_safe_tenant_id, RuntimeState};
pub use store::create_store;
