//! edge-js-runtime-long: rlib helper for JS long-running shims.
//!
//! Issue #426: prior to this change, `edge-js-runtime` only exported
//! the `edge-runtime-handler` (FaaS) world. JS apps that needed
//! `wasi:sockets/tcp`, WebSocket listeners, scheduled jobs, or any
//! other long-lived behavior had to compile a separate Rust shim
//! (as `samples/hello-js-ws/` did). This crate supplies the
//! per-namespace `register_*` helpers + the bindgen-internal
//! `edge::cloud::*` re-export, so a thin shim can produce a long-
//! running cdylib without redefining the seven `EdgeCloud.*`
//! registrars.
//!
//! **This crate is rlib-only.** It produces NO cdylib. The cdylib is
//! produced by the shim (`samples/hello-js-ws/` today, or any future
//! `edge init --world=edge-runtime` scaffold). See
//! [`register::register_all`] for the shim-facing entry point.
//!
//! **Why a separate crate, not a `long-running` Cargo feature on
//! `edge-js-runtime`?** Each `wit_bindgen::generate!` invocation
//! produces a `wasi:cli/run` export (the `wasi:cli/command@0.2.1`
//! include requires it). Two such invocations in the same cdylib
//! collide on the `wasi:cli/run` symbol at link time. The sibling
//! crate is the cleanest fix — each crate generates its own
//! bindgen output in its own crate-type. See
//! <https://github.com/poyrazK/edgecloud/issues/426> for the full
//! design notes.
//!
//! **Why rlib-only, not cdylib?** The canonical `edge-runtime` world
//! declares `export start: func();` as its entry point. The shim is
//! the one that calls `wit_bindgen::generate!{ world = "edge-runtime"
//! } + export!(Shim)`, so the shim owns the `start` symbol. If this
//! crate is also a cdylib, two `start` symbols land in the final
//! `wasm32-wasip1` link and rustc errors with `Linking globals named
//! 'start': symbol multiply defined`. rlib keeps the bindgen-
//! generated symbols and the `register_all` helpers accessible to
//! the shim without polluting the final link.

#[cfg(target_arch = "wasm32")]
pub mod wasm_only {
    wit_bindgen::generate!({
        world: "edge-runtime",
        path: "../wit",
        generate_all,
    });

    // Re-export the bindgen-generated `edge:cloud/*` modules so the
    // sibling `register` module can `use super::wasm_only::{kv_store,
    // cache, ...}` without naming the bindgen-internal
    // `edge::cloud::` path. **Only** the `pub use` — a non-`pub`
    // `use self::edge::cloud::*` would shadow this re-export with a
    // private alias and break the sibling import (rustc E0603
    // "module import is private"). The FaaS-side mirror at
    // `edge-js-runtime/src/lib.rs::wasm_only` follows the same
    // single-line pattern.
    pub use self::edge::cloud::{cache, kv_store, observe, process, scheduling, time, websocket};
}

/// Long-running `edge-runtime` world's `register_all` + per-namespace
/// registrars. The shim calls [`register::register_all`] exactly once
/// before evaluating the user bundle.
#[cfg(target_arch = "wasm32")]
pub mod register;
