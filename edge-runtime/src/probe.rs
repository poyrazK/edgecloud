#![allow(dead_code)]
//! Probe: access edge_runtime from within crate (runtime.rs context)

pub mod inner {
    use edge_runtime::edge_runtime::http_client;
    use edge_runtime::edge_runtime::time;
    use edge_runtime::edge_runtime::process;
    use edge_runtime::edge_runtime::networking;
    use edge_runtime::edge_runtime::kv_store;
    use edge_runtime::edge_runtime::cache;
    use edge_runtime::edge_runtime::observe;
    use edge_runtime::edge_runtime::scheduling;

    pub fn check() {
        let _r: Option<http_client::Request> = None;
        let _h: Option<http_client::Host> = None;
    }
}
