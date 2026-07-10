//! Cold-start microbenchmark for edge-js-runtime.
//!
//! Compares three init paths for the same ~100 KB user bundle:
//!
//! 1. `cold_source` — `Runtime::new + Context::full + register_all_stub +
//!    ctx.eval(wrapped_bundle)`. **Pre-#425 shape.** The dominant cost
//!    here is QuickJS lex+parse of the full bundle on every iter.
//!
//! 2. `warm_bytecode_load` — `Runtime::new + Context::full + register_all_stub +
//!    Module::load(&cached_bytecode) + module.eval()`. **Post-#425 shape,
//!    no handler call yet.** `cached_bytecode` is pre-built once outside
//!    the timed region via `edge_js_runtime::compile_user_bundle`.
//!
//! 3. `warm_bytecode_call` — same as (2) plus `ctx.eval("globalThis.handleRequest(__req)")`.
//!    **Post-#425 end-to-end.**
//!
//! The ratio of `cold_source` to `warm_bytecode_call` is the user-visible
//! win from issue #425.
//!
//! Run:
//! ```
//! cargo bench --manifest-path edge-js-runtime/Cargo.toml --bench warm_vs_cold --features bench
//! ```

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rquickjs::{Context, Runtime};

const SYNTHETIC_BUNDLE: &str = include_str!("fixtures/bundle_100kb.js");

/// Stub for `register_all` — same `globalThis.EdgeCloud` shape (7
/// sub-namespaces + closures), no-op bodies so we don't need a WASI
/// host at bench time. Always exported by `edge_modules.rs`.
use edge_js_runtime::edge_modules::register_all_stub;

/// Real host-safe helpers from `lib.rs` — wrap the user bundle as a
/// module and AOT-compile it to bytecode outside the timed region.
use edge_js_runtime::{compile_user_bundle, wrap_as_module};

fn bench_init_paths(c: &mut Criterion) {
    let mut group = c.benchmark_group("init_path");
    // Report throughput in bytes of bundle — meaningful because the
    // bench's purpose is to compare init cost for the same bundle.
    group.throughput(Throughput::Bytes(SYNTHETIC_BUNDLE.len() as u64));

    // Build the cached bytecode once outside the timed region. The
    // bench loop body mirrors what `wasm::Guest::handle` does after
    // the bytecode cache is warm. (`wrap_as_module` is invoked
    // internally by `compile_user_bundle`.)
    let pre_rt = Runtime::new().expect("pre rt");
    let cached_bc = compile_user_bundle(&pre_rt).expect("compile");

    group.bench_function(BenchmarkId::new("cold_source", "100kb"), |b| {
        // `cold_source` mirrors the **pre-#425** script-form path: eval
        // the raw bundle (esbuild's IIFE + globalThis.handleRequest
        // assignment) directly. We do NOT use the wrapped module-form
        // source because `Context::eval` is script-mode — `export const`
        // is a syntax error there. The lex+parse cost we measure here
        // is the same one the pre-#425 lib.rs::handle paid per request.
        b.iter(|| {
            let rt = black_box(Runtime::new().unwrap());
            let ctx = black_box(Context::full(&rt).unwrap());
            ctx.with(|ctx| {
                register_all_stub(&ctx).unwrap();
                black_box(ctx.eval::<(), _>(black_box(SYNTHETIC_BUNDLE)).unwrap());
            });
        });
    });

    group.bench_function(BenchmarkId::new("warm_bytecode_load", "100kb"), |b| {
        b.iter(|| {
            let rt = black_box(Runtime::new().unwrap());
            let ctx = black_box(Context::full(&rt).unwrap());
            ctx.with(|ctx| {
                register_all_stub(&ctx).unwrap();
                // SAFETY: `cached_bc` was produced by `Module::write_le` on
                // the same QuickJS engine version, so the bytes are
                // well-formed for `Module::load`.
                let module =
                    unsafe { rquickjs::module::Module::load(ctx.clone(), black_box(&cached_bc)) }
                        .unwrap();
                let (_m, promise) = module.eval().unwrap();
                // Drop the resolved promise.
                drop(promise);
            });
        });
    });

    group.bench_function(BenchmarkId::new("warm_bytecode_call", "100kb"), |b| {
        b.iter(|| {
            let rt = black_box(Runtime::new().unwrap());
            let ctx = black_box(Context::full(&rt).unwrap());
            ctx.with(|ctx| {
                register_all_stub(&ctx).unwrap();
                let module =
                    unsafe { rquickjs::module::Module::load(ctx.clone(), black_box(&cached_bc)) }
                        .unwrap();
                let (_m, promise) = module.eval().unwrap();
                drop(promise);
                // Build a tiny req object so `globalThis.handleRequest(req)`
                // matches the runtime's contract.
                let js_req = rquickjs::Object::new(ctx.clone()).unwrap();
                js_req.set("method", "GET").unwrap();
                js_req.set("path", "/").unwrap();
                js_req
                    .set("headers", rquickjs::Object::new(ctx.clone()).unwrap())
                    .unwrap();
                js_req.set("body", "").unwrap();
                ctx.globals().set("__req", js_req).unwrap();
                let result: rquickjs::Value = black_box(
                    ctx.eval(black_box("globalThis.handleRequest(__req)"))
                        .unwrap(),
                );
                black_box(result);
            });
        });
    });

    group.finish();
}

criterion_group!(benches, bench_init_paths);
criterion_main!(benches);
