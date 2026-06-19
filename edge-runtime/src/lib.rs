//! edge-runtime: WASI Preview 2 host interfaces for edge computing.

pub mod engine;
pub mod limits;
pub mod linker;
pub mod memory;
pub mod metering;
pub mod runtime;
pub mod store;

#[cfg(any(feature = "http-client", feature = "http-server"))]
pub mod streams;

pub mod interfaces;

// Generated WIT bindings — creates edge_runtime module at crate root
wasmtime::component::bindgen!({
    path: "src/wit/edge.wit",
});

pub use engine::create_engine;
pub use interfaces::is_safe_tenant_id;
pub use metering::RequestMeter;
pub use runtime::RuntimeState;
pub use store::create_store;
