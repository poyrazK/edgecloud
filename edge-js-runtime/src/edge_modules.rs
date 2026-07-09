//! Host-safe `globalThis.EdgeCloud` stub for the `warm_vs_cold` bench.
//!
//! The wasm-target `register_all` + per-namespace registrars (which
//! bind to the `wit_bindgen`-generated `edge:cloud/*` modules) live
//! in `lib.rs::wasm_only`. They can't be compiled on the host target
//! because the bindgen output only exists when targeting
//! `wasm32-wasip1`. This file mirrors the same `globalThis.EdgeCloud`
//! shape (one `Object` per namespace, each with `Function::new`
//! closures) with no-op bodies, so the bench's per-iter `register`
//! cost approximates the real one.
//!
//! Always compiled (no `cfg` gate) because `cargo bench --features
//! bench` activates the feature for the bench target only — the lib
//! is built separately as a normal dependency. The stub is ~21
//! `Function::new` closures and a handful of `Object` allocations; it
//! does not bloat the wasm artifact meaningfully (a few KB at most).

use rquickjs::{Ctx, Function, Object, Value};

pub fn register_all_stub<'js>(ctx: &Ctx<'js>) -> rquickjs::Result<()> {
    let edge_cloud = Object::new(ctx.clone())?;

    for ns_name in [
        "kv",
        "cache",
        "observe",
        "time",
        "scheduling",
        "process",
        "websocket",
    ] {
        let ns = Object::new(ctx.clone())?;
        // Three closures per namespace — close enough to the real
        // average closure count per `register_*` helper in lib.rs.
        for method in ["get", "set", "delete"] {
            let method_name = method.to_string();
            let f = Function::new(
                ctx.clone(),
                move |_ctx: Ctx<'js>, _args: Value<'js>| -> rquickjs::Result<Value<'js>> {
                    Ok(Value::new_null(_ctx.clone()))
                },
            )?;
            ns.set(method_name.as_str(), f)?;
        }
        edge_cloud.set(ns_name, ns)?;
    }

    ctx.globals().set("EdgeCloud", edge_cloud)?;
    Ok(())
}
