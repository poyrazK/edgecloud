//! Root runtime state implementing all WIT Host traits.

use crate::edge::cloud::cache::Host as CacheHost;
use crate::edge::cloud::http_client::{Host as HttpClientHost, Request, Response};
use crate::edge::cloud::http_server::{Host as HttpServerHost, IncomingRequest};
use crate::edge::cloud::kv_store::Host as KvStoreHost;
use crate::edge::cloud::networking::Host as NetworkingHost;
use crate::edge::cloud::observe::Host as ObserveHost;
use crate::edge::cloud::process::Host as ProcessHost;
use crate::edge::cloud::scheduling::Host as SchedulingHost;
use crate::edge::cloud::time::Host as TimeHost;
use crate::interfaces::{
    cache, http_client, http_server, kv_store, networking, observe, process, scheduling, time,
};

/// Root state for a wasmtime Store — implements all WIT Host traits.
pub struct RuntimeState {
    pub http_client: http_client::HttpClient,
    pub kv_store: kv_store::KvStore,
    pub cache: cache::Cache,
    pub observe: observe::Observer,
    pub time: time::Clock,
    pub scheduling: scheduling::Scheduler,
    pub process: process::Process,
    pub networking: networking::Network,
    pub http_server: http_server::HttpServer,
}

impl RuntimeState {
    pub fn new() -> Self {
        Self {
            http_client: http_client::HttpClient::new(),
            kv_store: kv_store::KvStore::new(),
            cache: cache::Cache::new(1000),
            observe: observe::Observer::new(),
            time: time::Clock::new(),
            scheduling: scheduling::Scheduler::new(),
            process: process::Process::new(),
            networking: networking::Network::new(),
            http_server: http_server::HttpServer::new(),
        }
    }
}

impl HttpClientHost for RuntimeState {
    fn fetch(&mut self, req: Request) -> Result<Response, String> {
        let method = req.method.as_str();
        let url = req.url.as_str();
        let headers: Vec<(String, String)> = req.headers.iter().cloned().collect();
        let body = req.body.as_ref().map(|b| b.as_slice());

        let resp = self.http_client.fetch(method, url, &headers, body)?;
        Ok(Response {
            status: resp.status,
            headers: resp.headers.into_iter().collect(),
            body: resp.body,
        })
    }
}

impl KvStoreHost for RuntimeState {
    fn get(&mut self, key: String) -> Option<Vec<u8>> {
        self.kv_store.get(&key)
    }

    fn set(&mut self, key: String, value: Vec<u8>, ttl_secs: Option<u32>) {
        self.kv_store.set(key, value, ttl_secs);
    }

    fn delete(&mut self, key: String) {
        self.kv_store.delete(&key);
    }

    fn list_keys(&mut self, prefix: String) -> Vec<String> {
        self.kv_store.list_keys(&prefix)
    }
}

impl CacheHost for RuntimeState {
    fn get(&mut self, key: String) -> Option<Vec<u8>> {
        self.cache.get(&key)
    }

    fn set(&mut self, key: String, value: Vec<u8>, ttl_secs: Option<u32>) {
        self.cache.set(key, value, ttl_secs);
    }

    fn delete(&mut self, key: String) {
        self.cache.delete(&key);
    }

    fn clear(&mut self) {
        self.cache.clear();
    }

    fn size(&mut self) -> u32 {
        self.cache.size() as u32
    }
}

impl ObserveHost for RuntimeState {
    fn increment_counter(&mut self, name: String, labels: Vec<(String, String)>) {
        self.observe.increment_counter(&name, &labels);
    }

    fn record_gauge(&mut self, name: String, value: f64, labels: Vec<(String, String)>) {
        self.observe.record_gauge(&name, value, &labels);
    }

    fn record_histogram(&mut self, name: String, value: f64, labels: Vec<(String, String)>) {
        self.observe.record_histogram(&name, value, &labels);
    }

    fn emit_log(&mut self, level: String, message: String) {
        self.observe.emit_log(&level, &message);
    }
}

impl TimeHost for RuntimeState {
    fn now(&mut self) -> u64 {
        self.time.now()
    }

    fn sleep(&mut self, duration_ms: u64) {
        self.time.sleep(duration_ms);
    }

    fn resolution(&mut self) -> u64 {
        self.time.resolution()
    }
}

impl SchedulingHost for RuntimeState {
    fn schedule_once(&mut self, delay_ms: u64, payload: Vec<u8>) -> String {
        self.scheduling.schedule_once(delay_ms, payload)
    }

    fn schedule_repeating(&mut self, interval_ms: u64, payload: Vec<u8>) -> String {
        self.scheduling.schedule_repeating(interval_ms, payload)
    }

    fn cancel_scheduled(&mut self, id: String) {
        self.scheduling.cancel(&id);
    }
}

impl ProcessHost for RuntimeState {
    fn get_env(&mut self, key: String) -> Option<String> {
        self.process.get_env(&key)
    }

    fn get_all_env(&mut self) -> Vec<(String, String)> {
        self.process.get_all_env()
    }

    fn get_args(&mut self) -> Vec<String> {
        self.process.get_args()
    }

    fn exit(&mut self, code: u32) {
        self.process.exit(code)
    }
}

impl NetworkingHost for RuntimeState {
    fn resolve(&mut self, hostname: String) -> Vec<String> {
        self.networking.resolve(&hostname)
    }
}

impl HttpServerHost for RuntimeState {
    fn start(&mut self, port: u16, host: Option<String>) {
        let _ = self.http_server.start(port, host);
    }

    fn poll(&mut self) -> Option<IncomingRequest> {
        self.http_server.poll().map(|r| IncomingRequest {
            id: r.id,
            method: r.method,
            path: r.path,
            query: r.query,
            headers: r.headers,
            body: r.body,
        })
    }

    fn respond(&mut self, req_id: u64, status: u16, headers: Vec<(String, String)>, body: Vec<u8>) {
        let _ = self.http_server.respond(req_id, status, headers, body);
    }
}
