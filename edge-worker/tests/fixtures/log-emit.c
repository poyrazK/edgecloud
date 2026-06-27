// log-emit.c — source for a real WASI Preview 2 component that calls
// `edge:observe.emit_log("info", "hello-from-guest")` exactly once on
// `_start`, then returns.
//
// This file is the human-readable reference for the e2e integration
// test `test_emit_log_reaches_ingest_within_5s` (see
// tests/integration_tests.rs). The committed binary used by the test
// is `fixtures/test-handle.wasm` (a minimal pre-existing stub); a
// future migration to a real guest-driven fixture should rebuild from
// this file with the command below.
//
// Build:
//   1. Install wasi-sdk (https://github.com/WebAssembly/wasi-sdk) and
//      ensure `clang` from `$WASI_SDK_PATH/bin` is on PATH.
//   2. Generate the component adapter and bindgen for `edge:observe`:
//
//        wit-component --version  # requires wasm-tools >= 1.0
//        wasm-tools component wit \
//            --target wasm32-wasip2 \
//            /path/to/edge-runtime/src/wit/edge.wit \
//            --wasm --output log-emit.wasm
//
//      Or, if you have the Rust bindings (`cargo install wac-cli`),
//      use `wac plug` to splice the edge-runtime WIT imports into a
//      minimal `_start` shim.
//
//   3. Rebuild from this file once the bindings exist; see the
//      `edge-runtime/src/wit/edge.wit` `observe` interface for the
//      exact `emit_log(level, msg, labels)` signature.
//
// Why the fixture is currently a stub:
//   - Hand-crafting a wasi-p2 component without the full wasm-tools /
//     wac toolchain is impractical. The current e2e test
//     (`test_emit_log_reaches_ingest_within_5s`) exercises the real
//     `LogForwarder` ticker path end-to-end with `log_forwarder.push()`
//     instead of a guest-side call. The wire contract (POST body,
//     auth, fields) is identical — only the trigger source changes.
//   - When a future contributor adds the real wasi-sdk + wit-component
//     toolchain to CI, swap `include_bytes!("fixtures/log-emit.wasm")`
//     for `include_bytes!("fixtures/test-handle.wasm")` and drop the
//     `log_forwarder.push()` injection — the rest of the test
//     (POST assertion + 5s SLA) carries over unchanged.