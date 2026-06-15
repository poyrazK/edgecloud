//! Root runtime state — implements all WIT Host traits.

use crate::edge::cloud::cache::Host as CacheHost;
use crate::edge::cloud::http_client::{Host as HttpClientHost, Request, Response};
use crate::edge::cloud::http_server::Host as HttpServerHost;
use crate::edge::cloud::kv_store::Host as KvStoreHost;
use crate::edge::cloud::networking::Host as NetworkingHost;
use crate::edge::cloud::observe::Host as ObserveHost;
use crate::edge::cloud::process::Host as ProcessHost;
use crate::edge::cloud::scheduling::Host as SchedulingHost;
use crate::edge::cloud::time::Host as TimeHost;
use crate::interfaces::{
    cache, http_client, http_server, kv_store, networking, observe, process, scheduling, time,
};
use crate::metering::RequestMeter;
use std::sync::Arc;

pub struct RuntimeState {
    pub http_client: http_client::HttpClient,
    pub kv_store: Arc<kv_store::KvStore>,
    pub cache: Arc<cache::Cache>,
    pub observe: observe::Observer,
    pub time: time::Clock,
    pub scheduling: scheduling::Scheduler,
    pub process: process::Process,
    pub networking: networking::NetworkingState,
    pub http_server: http_server::HttpServer,
}

impl RuntimeState {
    pub fn new() -> Self {
        Self {
            http_client: http_client::HttpClient::new(),
            kv_store: Arc::new(kv_store::KvStore::new()),
            cache: Arc::new(cache::Cache::new(1000)),
            observe: observe::Observer::new(),
            time: time::Clock::new(),
            scheduling: scheduling::Scheduler::new(),
            process: process::Process::new(),
            networking: networking::NetworkingState::new(),
            http_server: http_server::HttpServer::new(),
        }
    }

    /// Create a RuntimeState with per-app environment variables for tenant isolation.
    pub fn with_env(env: std::collections::HashMap<String, String>) -> Self {
        Self {
            http_client: http_client::HttpClient::new(),
            kv_store: Arc::new(kv_store::KvStore::new()),
            cache: Arc::new(cache::Cache::new(1000)),
            observe: observe::Observer::new(),
            time: time::Clock::new(),
            scheduling: scheduling::Scheduler::new(),
            process: process::Process::with_env(Arc::new(env)),
            networking: networking::NetworkingState::new(),
            http_server: http_server::HttpServer::new(),
        }
    }

    /// Create a RuntimeState with per-app env vars and a request meter.
    pub fn with_env_and_meter(
        env: std::collections::HashMap<String, String>,
        meter: Option<Arc<RequestMeter>>,
    ) -> Self {
        Self {
            http_client: http_client::HttpClient::new(),
            kv_store: Arc::new(kv_store::KvStore::new()),
            cache: Arc::new(cache::Cache::new(1000)),
            observe: observe::Observer::new(),
            time: time::Clock::new(),
            scheduling: scheduling::Scheduler::new(),
            process: process::Process::with_env(Arc::new(env)),
            networking: networking::NetworkingState::new(),
            http_server: http_server::HttpServer::with_meter(meter),
        }
    }
}

impl Default for RuntimeState {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpClientHost for RuntimeState {
    fn fetch(&mut self, req: Request) -> Option<Response> {
        let method = req.method.as_str();
        let url = req.url.as_str();
        let headers: Vec<(String, String)> = req.headers.to_vec();
        let body = req.body.as_deref();
        match self.http_client.fetch(method, url, &headers, body) {
            Ok(resp) => Some(Response {
                status: resp.status,
                headers: resp.headers.into_iter().collect(),
                body: resp.body,
            }),
            Err(_) => None,
        }
    }
}

impl KvStoreHost for RuntimeState {
    fn get(&mut self, key: String) -> Option<Vec<u8>> {
        self.kv_store.get(&key).ok().flatten()
    }
    fn set(&mut self, key: String, value: Vec<u8>, ttl_secs: Option<u32>) {
        let _ = self.kv_store.set(key, value, ttl_secs);
    }
    fn delete(&mut self, key: String) {
        let _ = self.kv_store.delete(&key);
    }
    fn list_keys(&mut self, prefix: String) -> Vec<String> {
        self.kv_store.list_keys(&prefix).ok().unwrap_or_default()
    }
}

impl CacheHost for RuntimeState {
    fn get(&mut self, key: String) -> Option<Vec<u8>> {
        self.cache.get(&key).ok().flatten()
    }
    fn set(&mut self, key: String, value: Vec<u8>, ttl_secs: Option<u32>) {
        let _ = self.cache.set(key, value, ttl_secs);
    }
    fn delete(&mut self, key: String) {
        let _ = self.cache.delete(&key);
    }
    fn clear(&mut self) {
        let _ = self.cache.clear();
    }
    fn size(&mut self) -> u32 {
        self.cache.size().unwrap_or(0)
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
        let _ = self.time.sleep(duration_ms);
    }
    fn resolution(&mut self) -> u64 {
        self.time.resolution()
    }
}

impl SchedulingHost for RuntimeState {
    fn schedule_once(&mut self, delay_ms: u64, payload: Vec<u8>) -> String {
        self.scheduling
            .schedule_once(delay_ms, payload)
            .unwrap_or_default()
    }
    fn schedule_repeating(&mut self, interval_ms: u64, payload: Vec<u8>) -> String {
        self.scheduling
            .schedule_repeating(interval_ms, payload)
            .unwrap_or_default()
    }
    fn cancel_scheduled(&mut self, id: String) {
        let _ = self.scheduling.cancel(&id);
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
        self.networking.resolve(&hostname).unwrap_or_default()
    }
}

impl HttpServerHost for RuntimeState {
    fn start(&mut self, port: u16, host: Option<String>) {
        let rt = tokio::runtime::Handle::current();
        let _ = rt.block_on(self.http_server.start(port, host));
    }
    fn poll(&mut self) -> Option<crate::edge::cloud::http_server::IncomingRequest> {
        let rt = tokio::runtime::Handle::current();
        rt.block_on(self.http_server.poll())
            .ok()
            .flatten()
            .map(|req| crate::edge::cloud::http_server::IncomingRequest {
                id: req.id,
                method: req.method,
                path: req.path,
                query: req.query,
                headers: req.headers,
                body: req.body,
            })
    }
    fn respond(&mut self, req_id: u64, status: u16, headers: Vec<(String, String)>, body: Vec<u8>) {
        let rt = tokio::runtime::Handle::current();
        let _ = rt.block_on(self.http_server.respond(req_id, status, headers, body));
    }
}
