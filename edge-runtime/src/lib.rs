//! edge-runtime: WASI Preview 2 host interfaces for edge computing.

pub mod engine;
pub mod limits;
pub mod linker;
pub mod memory;
pub mod metering;
pub mod runtime;
pub mod store;

pub mod interfaces;

// Generated WIT bindings — creates edge_runtime module at crate root
wasmtime::component::bindgen!({
    world: "edge-runtime",
    inline: "
package edge:cloud@0.1.0;

interface http-client {
  record request {
    method: string,
    url: string,
    headers: list<tuple<string, string>>,
    body: option<list<u8>>,
  }
  record response {
    status: u16,
    headers: list<tuple<string, string>>,
    body: list<u8>,
  }
  fetch: func(req: request) -> result<response, string>;
}

interface kv-store {
  get: func(key: string) -> option<list<u8>>;
  set: func(key: string, value: list<u8>, ttl-secs: option<u32>);
  delete: func(key: string);
  list-keys: func(prefix: string) -> list<string>;
}

interface cache {
  get: func(key: string) -> option<list<u8>>;
  set: func(key: string, value: list<u8>, ttl-secs: option<u32>);
  delete: func(key: string);
  clear: func();
  size: func() -> u32;
}

interface observe {
  increment-counter: func(name: string, labels: list<tuple<string, string>>);
  record-gauge: func(name: string, value: f64, labels: list<tuple<string, string>>);
  record-histogram: func(name: string, value: f64, labels: list<tuple<string, string>>);
  emit-log: func(level: string, message: string);
}

interface time {
  now: func() -> u64;
  sleep: func(duration-ms: u64);
  resolution: func() -> u64;
}

interface scheduling {
  schedule-once: func(delay-ms: u64, payload: list<u8>) -> string;
  schedule-repeating: func(interval-ms: u64, payload: list<u8>) -> string;
  cancel-scheduled: func(id: string);
}

interface process {
  get-env: func(key: string) -> option<string>;
  get-all-env: func() -> list<tuple<string, string>>;
  get-args: func() -> list<string>;
  exit: func(code: u32);
}

interface networking {
  resolve: func(hostname: string) -> list<string>;
}

interface http-server {
  record incoming-request {
    id: u64,
    method: string,
    path: string,
    query: option<string>,
    headers: list<tuple<string, string>>,
    body: list<u8>,
  }
  start: func(port: u16, host: option<string>);
  poll: func() -> option<incoming-request>;
  respond: func(req-id: u64, status: u16, headers: list<tuple<string, string>>, body: list<u8>);
}

world edge-runtime {
  import http-client;
  import networking;
  import kv-store;
  import cache;
  import observe;
  import time;
  import scheduling;
  import process;
  import http-server;
}
",
});

pub use engine::create_engine;
pub use metering::RequestMeter;
pub use runtime::RuntimeState;
pub use store::create_store;
