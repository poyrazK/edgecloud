//! Root runtime state — implements all WIT Host traits.

use crate::edge::cloud::cache::Host as CacheHost;
use crate::edge::cloud::http_client::{
    Host as HttpClientHost, Request, RequestBodySource, Response, ResponseBodySource,
};
use crate::edge::cloud::http_server::{
    BodySource as HttpServerBodySource, Host as HttpServerHost, IncomingRequest,
};
use crate::edge::cloud::kv_store::Host as KvStoreHost;
use crate::edge::cloud::networking::Host as NetworkingHost;
use crate::edge::cloud::observe::Host as ObserveHost;
use crate::edge::cloud::process::Host as ProcessHost;
use crate::edge::cloud::scheduling::Host as SchedulingHost;
#[cfg(any(feature = "http-client", feature = "http-server"))]
use crate::edge::cloud::streams::Host as StreamsHost;
use crate::edge::cloud::time::Host as TimeHost;
use crate::egress::EgressPolicy;
#[cfg(feature = "http-server")]
use crate::interfaces::http_server::BodySource as HttpServerInternalBodySource;
use crate::interfaces::{
    cache, http_client, http_server, kv_store, networking, observe, process, scheduling, time,
};
use crate::metering::RequestMeter;
#[cfg(any(feature = "http-client", feature = "http-server"))]
use crate::streams::{IncomingEntry, OutgoingEntry};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
#[cfg(any(feature = "http-client", feature = "http-server"))]
use std::sync::Mutex as StdMutex;

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
    /// Tenant that owns this runtime instance. Used to scope persisted stores
    /// to per-tenant directories so one tenant's data cannot be accessed by another.
    pub tenant_id: String,
    /// Per-deployment egress policy. Checked in HttpClientHost::fetch before
    /// every outbound request. Enforces the tenant's allowlist and always
    /// hard-denies private/loopback/link-local IP ranges.
    pub egress: Arc<EgressPolicy>,
    /// Shared exit-code flag set by Process::exit when the guest calls process.exit.
    /// This allows execute_app to distinguish a clean guest exit from a wasm trap.
    pub exit_code: Arc<AtomicU32>,
    /// State backing the `streams::incoming` resource. Keyed by the rep
    /// of the WIT-generated `Resource<Incoming>`. We maintain our own map
    /// rather than stuffing our types into the typed `ResourceTable`,
    /// because the WIT-generated resource type is an empty marker and
    /// doesn't match our state shape.
    #[cfg(any(feature = "http-client", feature = "http-server"))]
    pub incoming_streams: StdMutex<std::collections::HashMap<u32, IncomingEntry>>,
    /// State backing the `streams::outgoing` resource, keyed by rep.
    #[cfg(any(feature = "http-client", feature = "http-server"))]
    pub outgoing_streams: StdMutex<std::collections::HashMap<u32, OutgoingEntry>>,
    /// Monotonic counter for stream resource reps. A single counter mints
    /// keys for BOTH the `incoming_streams` and `outgoing_streams` maps;
    /// the two maps are disjoint, so collisions are not possible, but the
    /// shared rep space must not be split across two counters. Renamed from
    /// `next_outgoing_rep` (which misleadingly suggested outgoing-only).
    #[cfg(any(feature = "http-client", feature = "http-server"))]
    pub next_stream_rep: AtomicU32,
}

impl RuntimeState {
    /// Test-only constructor. Always uses ephemeral in-memory stores and an
    /// unrestricted egress policy regardless of env vars.
    #[cfg(test)]
    pub fn new() -> Self {
        let exit_code = Arc::new(AtomicU32::new(0));
        let networking = networking::NetworkingState::new();
        Self {
            http_client: http_client::HttpClient::new(),
            kv_store: Arc::new(kv_store::KvStore::new()),
            cache: Arc::new(cache::Cache::new(1000)),
            observe: observe::Observer::new(),
            time: time::Clock::new(),
            scheduling: scheduling::Scheduler::new(),
            process: process::Process::with_env_and_exit_code(
                Arc::new(process::filter_env_vars(std::env::vars()).collect()),
                exit_code.clone(),
            ),
            networking,
            http_server: http_server::HttpServer::new(),
            tenant_id: String::new(),
            egress: Arc::new(EgressPolicy::allow_all()),
            exit_code,
            #[cfg(any(feature = "http-client", feature = "http-server"))]
            incoming_streams: StdMutex::new(std::collections::HashMap::new()),
            #[cfg(any(feature = "http-client", feature = "http-server"))]
            outgoing_streams: StdMutex::new(std::collections::HashMap::new()),
            #[cfg(any(feature = "http-client", feature = "http-server"))]
            next_stream_rep: AtomicU32::new(1),
        }
    }

    /// Create a RuntimeState with per-app environment variables, log sink,
    /// and app context for tenant isolation. `log_sink` receives every record
    /// emitted by `edge:observe.emit_log`; `app_ctx` is stamped onto each
    /// forwarded record so downstream sinks know the tenant/app/deployment.
    /// Tenant isolation derives from `app_ctx.tenant_id`. `egress` is the
    /// per-tenant outbound allowlist enforced by `edge:http-client.fetch`
    /// (see `EgressPolicy`). `metrics_acc` is a shared accumulator the
    /// supervisor holds to snapshot metrics at heartbeat time; pass `None`
    /// for tests or callers that don't export metrics.
    pub fn with_env(
        env: std::collections::HashMap<String, String>,
        log_sink: Arc<dyn observe::LogSink>,
        app_ctx: observe::AppLogContext,
        egress: Arc<EgressPolicy>,
        metrics_acc: Option<Arc<observe::MetricsAccumulator>>,
    ) -> Self {
        let tenant_id = app_ctx.tenant_id.clone();
        let exit_code = Arc::new(AtomicU32::new(0));
        let networking = networking::NetworkingState::new();
        let mut obs_cfg = observe::ObserveConfig::new()
            .with_log_sink(log_sink)
            .with_app_ctx(app_ctx);
        if let Some(acc) = metrics_acc {
            obs_cfg = obs_cfg.with_metrics_accumulator(acc);
        }
        Self {
            http_client: http_client::HttpClient::new(),
            kv_store: Self::make_kv_store_for_tenant(&tenant_id),
            cache: Self::make_cache_for_tenant(&tenant_id),
            observe: observe::Observer::from_config(obs_cfg),
            time: time::Clock::new(),
            scheduling: Self::make_scheduler_for_tenant(&tenant_id),
            process: process::Process::with_env_and_exit_code(Arc::new(env), exit_code.clone()),
            networking,
            http_server: http_server::HttpServer::new(),
            tenant_id,
            egress,
            exit_code,
            #[cfg(any(feature = "http-client", feature = "http-server"))]
            incoming_streams: StdMutex::new(std::collections::HashMap::new()),
            #[cfg(any(feature = "http-client", feature = "http-server"))]
            outgoing_streams: StdMutex::new(std::collections::HashMap::new()),
            #[cfg(any(feature = "http-client", feature = "http-server"))]
            next_stream_rep: AtomicU32::new(1),
        }
    }

    /// Create a RuntimeState with per-app env vars, request meter, log sink,
    /// and app context. The worker's per-app `execute_app` path uses this.
    /// Tenant isolation is derived from `app_ctx.tenant_id` so the call site
    /// doesn't have to thread `tenant_id` as a separate parameter.
    /// `metrics_acc` is a shared accumulator the supervisor holds to snapshot
    /// metrics at heartbeat time; pass `None` for tests.
    pub fn with_env_and_meter(
        env: std::collections::HashMap<String, String>,
        meter: Option<Arc<RequestMeter>>,
        log_sink: Arc<dyn observe::LogSink>,
        app_ctx: observe::AppLogContext,
        egress: Arc<EgressPolicy>,
        metrics_acc: Option<Arc<observe::MetricsAccumulator>>,
    ) -> Self {
        let tenant_id = app_ctx.tenant_id.clone();
        let exit_code = Arc::new(AtomicU32::new(0));
        let networking = networking::NetworkingState::new();
        let mut obs_cfg = observe::ObserveConfig::new()
            .with_log_sink(log_sink)
            .with_app_ctx(app_ctx);
        if let Some(acc) = metrics_acc {
            obs_cfg = obs_cfg.with_metrics_accumulator(acc);
        }
        Self {
            http_client: http_client::HttpClient::new(),
            kv_store: Self::make_kv_store_for_tenant(&tenant_id),
            cache: Self::make_cache_for_tenant(&tenant_id),
            observe: observe::Observer::from_config(obs_cfg),
            time: time::Clock::new(),
            scheduling: Self::make_scheduler_for_tenant(&tenant_id),
            process: process::Process::with_env_and_exit_code(Arc::new(env), exit_code.clone()),
            networking,
            http_server: http_server::HttpServer::with_meter(meter),
            tenant_id,
            egress,
            exit_code,
            #[cfg(any(feature = "http-client", feature = "http-server"))]
            incoming_streams: StdMutex::new(std::collections::HashMap::new()),
            #[cfg(any(feature = "http-client", feature = "http-server"))]
            outgoing_streams: StdMutex::new(std::collections::HashMap::new()),
            #[cfg(any(feature = "http-client", feature = "http-server"))]
            next_stream_rep: AtomicU32::new(1),
        }
    }

    /// Attempt to create a persistent KvStore scoped to `tenant_id`.
    /// Falls back to ephemeral if `EDGE_KV_STORE_PATH` is unset or the path is unusable.
    fn make_kv_store_for_tenant(tenant_id: &str) -> Arc<kv_store::KvStore> {
        match kv_store::KvStore::from_env_for_tenant(tenant_id) {
            Ok(Some(store)) => Arc::new(store),
            Ok(None) => Arc::new(kv_store::KvStore::new()),
            Err(e) => {
                tracing::error!(
                    tenant_id,
                    "KV store persistence unavailable, using ephemeral: {}",
                    e
                );
                Arc::new(kv_store::KvStore::new())
            }
        }
    }

    /// Attempt to create a persistent Scheduler scoped to `tenant_id`.
    /// Falls back to ephemeral if `EDGE_SCHEDULING_PATH` is unset or the path is unusable.
    fn make_scheduler_for_tenant(tenant_id: &str) -> scheduling::Scheduler {
        match scheduling::Scheduler::from_env_for_tenant(tenant_id) {
            Ok(Some(s)) => s,
            Ok(None) => scheduling::Scheduler::new(),
            Err(e) => {
                tracing::error!(
                    tenant_id,
                    "scheduling persistence unavailable, using ephemeral: {}",
                    e
                );
                scheduling::Scheduler::new()
            }
        }
    }

    /// Attempt to create a persistent Cache scoped to `tenant_id`.
    /// Falls back to ephemeral if `EDGE_CACHE_PATH` is unset or the path is unusable.
    fn make_cache_for_tenant(tenant_id: &str) -> Arc<cache::Cache> {
        match cache::Cache::from_env_for_tenant(tenant_id, 1000) {
            Ok(Some(c)) => Arc::new(c),
            Ok(None) => Arc::new(cache::Cache::new(1000)),
            Err(e) => {
                tracing::error!(
                    tenant_id,
                    "cache persistence unavailable, using ephemeral: {}",
                    e
                );
                Arc::new(cache::Cache::new(1000))
            }
        }
    }

    /// Returns `Some(code)` if the guest WASM component called `process.exit(code)`,
    /// `None` if no exit was requested.
    pub fn exit_requested(&self) -> Option<u32> {
        let code = self.exit_code.load(Ordering::SeqCst);
        if code == 0 {
            None
        } else {
            Some(code)
        }
    }
}

#[cfg(test)]
impl Default for RuntimeState {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpClientHost for RuntimeState {
    fn fetch(&mut self, req: Request) -> Option<Response> {
        let url = req.url.as_str();

        // Enforce egress policy before issuing any outbound request.
        // Returns 403 to the guest so it can handle the denial gracefully.
        if let Err(reason) = self.egress.check(url) {
            return Some(Response {
                status: 403,
                headers: Vec::new(),
                body: ResponseBodySource::Buffered(Vec::new()),
                error: Some(reason),
            });
        }

        // DNS rebinding guard: resolve the destination hostname and verify none
        // of the returned IPs fall in a hard-deny range. Catches the common
        // attack where an allowlisted domain is redirected to a metadata/private
        // IP via a zero-TTL DNS record.
        //
        // Known residual race (TOCTOU): reqwest performs its own DNS query when
        // it opens the TCP connection. A TTL-0 record can change between our
        // check and that dial. Eliminating this gap requires a custom reqwest
        // Resolve impl that both checks and reuses the same resolved address.
        //
        // Fail-closed: any DNS error or empty result set is treated as a denial
        // so an attacker cannot force a lookup failure to bypass the guard.
        {
            use url::Url;
            if let Ok(parsed) = Url::parse(url) {
                if let Some(url::Host::Domain(hostname)) = parsed.host() {
                    let resolved = match self.networking.resolve(hostname) {
                        Ok(ips) if !ips.is_empty() => ips,
                        Ok(_) => {
                            return Some(Response {
                                status: 403,
                                headers: Vec::new(),
                                body: ResponseBodySource::Buffered(Vec::new()),
                                error: Some(
                                    "egress denied: DNS resolution returned no addresses"
                                        .to_string(),
                                ),
                            });
                        }
                        Err(_) => {
                            return Some(Response {
                                status: 403,
                                headers: Vec::new(),
                                body: ResponseBodySource::Buffered(Vec::new()),
                                error: Some("egress denied: DNS resolution failed".to_string()),
                            });
                        }
                    };
                    for ip_str in &resolved {
                        match ip_str.parse::<std::net::IpAddr>() {
                            Ok(ip) => {
                                if let Err(reason) = self.egress.check_resolved_ip(ip) {
                                    return Some(Response {
                                        status: 403,
                                        headers: Vec::new(),
                                        body: ResponseBodySource::Buffered(Vec::new()),
                                        error: Some(reason),
                                    });
                                }
                            }
                            Err(_) => {
                                // Unparseable IP string (e.g. scoped IPv6 zone ID).
                                // Fail-closed: deny rather than skip the check.
                                return Some(Response {
                                    status: 403,
                                    headers: Vec::new(),
                                    body: ResponseBodySource::Buffered(Vec::new()),
                                    error: Some(format!(
                                        "egress denied: unparseable IP in DNS response: {ip_str}"
                                    )),
                                });
                            }
                        }
                    }
                }
            }
        }

        let method = req.method.as_str();
        let headers: Vec<(String, String)> = req.headers.to_vec();
        let trace_context = req.trace_context.as_ref().map(|tc| tc.traceparent.as_str());
        let tracestate = req
            .trace_context
            .as_ref()
            .and_then(|tc| tc.tracestate.as_deref());

        // Resolve WIT request body to HttpClient's BodySource enum.
        let body = match req.body {
            RequestBodySource::None => http_client::BodySource::None,
            RequestBodySource::Buffered(bytes) => http_client::BodySource::Buffered(bytes),
            RequestBodySource::Chunked(handle) => {
                #[cfg(feature = "http-client")]
                {
                    let mut outgoing = self
                        .outgoing_streams
                        .lock()
                        .unwrap_or_else(|e| e.into_inner());
                    let adapter = match outgoing.get_mut(&handle.rep()) {
                        Some(entry) => match entry.adapter.take() {
                            Some(a) => a,
                            None => {
                                return Some(self.chunked_body_error_response(
                                    "chunked body: adapter already consumed",
                                ));
                            }
                        },
                        None => {
                            return Some(self.chunked_body_error_response(
                                "chunked body: resource entry missing",
                            ));
                        }
                    };
                    http_client::BodySource::Streamed(adapter)
                }
                #[cfg(not(feature = "http-client"))]
                {
                    let _ = handle;
                    unreachable!("chunked body without http-client feature enabled");
                }
            }
        };

        let rt = tokio::runtime::Handle::current();
        let resp = rt.block_on(self.http_client.fetch(
            method,
            url,
            &headers,
            body,
            req.timeout_ms,
            trace_context,
            tracestate,
        ));

        // Record buffered response bytes. Streaming responses set body_bytes=0
        // here; their bytes are counted per-chunk in streams_impl::read_chunk
        // via IncomingEntry::count_as_outbound. record_outbound_bytes(0) is a
        // no-op so calling it unconditionally is safe and avoids ambiguity.
        if let Some(ref meter) = self.http_server.meter {
            meter.record_outbound_bytes(resp.body_bytes);
        }

        let body = match resp.body {
            http_client::ResponseBody::None => ResponseBodySource::None,
            http_client::ResponseBody::Buffered(bytes) => ResponseBodySource::Buffered(bytes),
            http_client::ResponseBody::Streamed(stream) => {
                #[cfg(feature = "http-client")]
                {
                    let rep = self
                        .next_stream_rep
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    self.incoming_streams
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .insert(
                            rep,
                            IncomingEntry {
                                stream,
                                count_as_outbound: true,
                            },
                        );
                    let handle = wasmtime::component::Resource::<
                        crate::edge::cloud::streams::Incoming,
                    >::new_own(rep);
                    ResponseBodySource::Chunked(handle)
                }
                #[cfg(not(feature = "http-client"))]
                {
                    let _ = stream;
                    unreachable!("streamed response without http-client feature enabled");
                }
            }
        };

        Some(Response {
            status: resp.status,
            headers: resp.headers.into_iter().collect(),
            body,
            error: resp.error,
        })
    }
}

impl RuntimeState {
    #[cfg(feature = "http-client")]
    fn chunked_body_error_response(&self, msg: &str) -> Response {
        Response {
            status: 502,
            headers: Vec::new(),
            body: ResponseBodySource::None,
            error: Some(msg.to_string()),
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
    fn get_many(&mut self, keys: Vec<String>) -> Vec<Option<Vec<u8>>> {
        self.kv_store.get_many(&keys)
    }
    fn set_many(&mut self, items: Vec<(String, Vec<u8>, Option<u32>)>) {
        let _ = self.kv_store.set_many(&items);
    }
    fn delete_many(&mut self, keys: Vec<String>) {
        let _ = self.kv_store.delete_many(&keys);
    }
    fn exists(&mut self, key: String) -> bool {
        self.kv_store.exists(&key)
    }
    fn clear(&mut self) {
        self.kv_store.clear();
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
    fn exists(&mut self, key: String) -> bool {
        self.cache.exists(&key)
    }
    fn list_keys(&mut self, prefix: String) -> Vec<String> {
        self.cache.list_keys(&prefix)
    }
    fn get_many(&mut self, keys: Vec<String>) -> Vec<Option<Vec<u8>>> {
        self.cache.get_many(&keys)
    }
    fn set_many(&mut self, items: Vec<(String, Vec<u8>, Option<u32>)>) {
        let _ = self.cache.set_many(&items);
    }
    fn delete_many(&mut self, keys: Vec<String>) {
        let _ = self.cache.delete_many(&keys);
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
    fn emit_log(&mut self, level: String, message: String, labels: Vec<(String, String)>) {
        self.observe.emit_log(&level, &message, &labels);
    }
    fn emit_log_record(&mut self, r: crate::edge::cloud::observe::LogRecord) {
        let record = observe::LogRecord {
            timestamp_ms: r.timestamp_ms,
            level: match r.level {
                crate::edge::cloud::observe::LogLevel::Error => observe::LogLevel::Error,
                crate::edge::cloud::observe::LogLevel::Warn => observe::LogLevel::Warn,
                crate::edge::cloud::observe::LogLevel::Info => observe::LogLevel::Info,
                crate::edge::cloud::observe::LogLevel::Debug => observe::LogLevel::Debug,
                crate::edge::cloud::observe::LogLevel::Trace => observe::LogLevel::Trace,
            },
            message: r.message,
            labels: r.labels,
        };
        self.observe.emit_log_record(&record);
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
    fn get_cwd(&mut self) -> Result<String, String> {
        self.process.get_cwd()
    }
    fn exit(&mut self, code: u32) {
        self.process.exit(code)
    }
}

impl NetworkingHost for RuntimeState {
    fn resolve(&mut self, hostname: String) -> Vec<String> {
        // Enforce egress policy on DNS lookups: deny resolution of hostnames
        // that wouldn't be permitted for outbound HTTP. This prevents tenants
        // with an empty allowlist from using DNS to enumerate internal services.
        if self
            .egress
            .check(&format!("https://{}/", hostname))
            .is_err()
        {
            return Vec::new();
        }
        self.networking.resolve(&hostname).unwrap_or_default()
    }
}

impl HttpServerHost for RuntimeState {
    fn start(&mut self, port: u16, host: Option<String>) -> Result<(), String> {
        let rt = tokio::runtime::Handle::current();
        rt.block_on(self.http_server.start(port, host))
    }
    fn poll(&mut self) -> Result<Option<IncomingRequest>, String> {
        let rt = tokio::runtime::Handle::current();
        rt.block_on(self.http_server.poll()).map(|opt| {
            opt.map(|req| {
                #[cfg(feature = "http-server")]
                let body = match req.body {
                    HttpServerInternalBodySource::None => HttpServerBodySource::None,
                    HttpServerInternalBodySource::Buffered(bytes) => {
                        HttpServerBodySource::Buffered(bytes)
                    }
                    HttpServerInternalBodySource::Streamed(stream) => {
                        let rep = self
                            .next_stream_rep
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        self.incoming_streams.lock().unwrap().insert(
                            rep,
                            IncomingEntry {
                                stream,
                                count_as_outbound: false,
                            },
                        );
                        let handle = wasmtime::component::Resource::<
                            crate::edge::cloud::streams::Incoming,
                        >::new_own(rep);
                        HttpServerBodySource::Chunked(handle)
                    }
                };
                #[cfg(not(feature = "http-server"))]
                let body = {
                    let _ = req.body;
                    HttpServerBodySource::None
                };
                IncomingRequest {
                    id: req.id,
                    method: req.method,
                    path: req.path,
                    query: req.query,
                    headers: req.headers,
                    body,
                    trace: req
                        .trace
                        .map(|tc| crate::edge::cloud::http_server::TraceContext {
                            traceparent: tc.traceparent,
                            tracestate: tc.tracestate,
                        }),
                }
            })
        })
    }
    fn respond(
        &mut self,
        req_id: u64,
        status: u16,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    ) -> Result<(), String> {
        let rt = tokio::runtime::Handle::current();
        rt.block_on(self.http_server.respond(req_id, status, headers, body))
    }
    fn respond_stream(
        &mut self,
        req_id: u64,
        status: u16,
        headers: Vec<(String, String)>,
        body_stream: wasmtime::component::Resource<crate::edge::cloud::streams::Outgoing>,
    ) -> Result<(), String> {
        #[cfg(feature = "http-server")]
        let adapter = {
            let mut outgoing = self.outgoing_streams.lock().unwrap();
            let entry = outgoing
                .get_mut(&body_stream.rep())
                .ok_or_else(|| "respond_stream: resource entry missing".to_string())?;
            entry
                .adapter
                .take()
                .ok_or_else(|| "respond_stream: adapter already consumed".to_string())?
        };
        #[cfg(not(feature = "http-server"))]
        {
            let _ = body_stream;
            return Err("respond_stream called without http-server feature".to_string());
        }
        #[cfg(feature = "http-server")]
        {
            let rt = tokio::runtime::Handle::current();
            rt.block_on(
                self.http_server
                    .respond_stream(req_id, status, headers, adapter),
            )
        }
    }
    fn shutdown(&mut self) {
        let rt = tokio::runtime::Handle::current();
        rt.block_on(self.http_server.shutdown());
    }
    fn get_assigned_port(&mut self) -> u16 {
        self.http_server.get_assigned_port().unwrap_or(0)
    }
    fn stop(&mut self) {
        let rt = tokio::runtime::Handle::current();
        rt.block_on(self.http_server.shutdown());
    }
}

// ---- Streams host impls ----------------------------------------------------

#[cfg(any(feature = "http-client", feature = "http-server"))]
mod streams_impl {
    use super::RuntimeState;
    use crate::edge::cloud::streams::{
        HostIncoming, HostOutgoing, Incoming, Outgoing, StreamError as WitStreamError,
    };
    use crate::streams::{self, OutgoingEntry, StreamError, DEFAULT_STREAM_CAPACITY};
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use wasmtime::component::Resource;

    fn block_on_timeout<F, T>(fut: F) -> Result<T, StreamError>
    where
        F: std::future::Future<Output = Result<T, StreamError>>,
    {
        let rt = tokio::runtime::Handle::current();
        rt.block_on(async move {
            match tokio::time::timeout(Duration::from_secs(5), fut).await {
                Ok(inner) => inner,
                Err(_) => Err(StreamError::Closed),
            }
        })
    }

    impl HostIncoming for RuntimeState {
        fn read_chunk(
            &mut self,
            self_: Resource<Incoming>,
            _max_bytes: u32,
        ) -> Result<Vec<u8>, WitStreamError> {
            let (mut cloned, count_as_outbound) = {
                let incoming = self
                    .incoming_streams
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                let entry = incoming.get(&self_.rep()).ok_or(WitStreamError::Closed)?;
                (entry.stream.clone(), entry.count_as_outbound)
            };
            let chunk = block_on_timeout(cloned.read_chunk()).map_err(streams::to_wit)?;
            if count_as_outbound {
                if let Some(ref meter) = self.http_server.meter {
                    meter.record_outbound_bytes(chunk.len() as u64);
                }
            }
            Ok(chunk)
        }

        fn cancel(&mut self, self_: Resource<Incoming>) {
            if let Some(entry) = self
                .incoming_streams
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get_mut(&self_.rep())
            {
                entry.stream.cancel();
            }
        }

        fn drop(&mut self, rep: Resource<Incoming>) -> wasmtime::Result<()> {
            self.incoming_streams
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&rep.rep());
            Ok(())
        }
    }

    impl HostOutgoing for RuntimeState {
        fn new(&mut self) -> Resource<Outgoing> {
            let rep = self.next_stream_rep.fetch_add(1, Ordering::Relaxed);
            self.outgoing_streams
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(rep, OutgoingEntry::new(DEFAULT_STREAM_CAPACITY));
            Resource::new_own(rep)
        }

        fn write_chunk(
            &mut self,
            self_: Resource<Outgoing>,
            bytes: Vec<u8>,
        ) -> Result<(), WitStreamError> {
            let mut cloned = {
                let outgoing = self
                    .outgoing_streams
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                let entry = outgoing.get(&self_.rep()).ok_or(WitStreamError::Closed)?;
                entry.stream.clone()
            };
            block_on_timeout(cloned.write_chunk(bytes)).map_err(streams::to_wit)
        }

        fn finish(&mut self, self_: Resource<Outgoing>) -> Result<(), WitStreamError> {
            let mut cloned = {
                let outgoing = self
                    .outgoing_streams
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                let entry = outgoing.get(&self_.rep()).ok_or(WitStreamError::Closed)?;
                entry.stream.clone()
            };
            block_on_timeout(cloned.finish()).map_err(streams::to_wit)
        }

        fn drop(&mut self, rep: Resource<Outgoing>) -> wasmtime::Result<()> {
            self.outgoing_streams
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&rep.rep());
            Ok(())
        }
    }
}

#[cfg(any(feature = "http-client", feature = "http-server"))]
impl StreamsHost for RuntimeState {}

// ── Egress integration tests ────────────────────────────────────────────────

#[cfg(test)]
mod egress_http_tests {
    use crate::edge::cloud::http_client::{Request, RequestBodySource};
    use crate::egress::EgressPolicy;
    use crate::RuntimeState;
    use std::sync::Arc;

    #[tokio::test]
    async fn egress_check_returns_403_in_fetch() {
        use crate::interfaces::observe::{AppLogContext, NoopLogSink};
        let egress = Arc::new(EgressPolicy::new(vec![]));
        let env = std::collections::HashMap::new();
        let log_sink: Arc<dyn crate::interfaces::observe::LogSink> = Arc::new(NoopLogSink);
        let mut runtime_state = RuntimeState::with_env_and_meter(
            env,
            None,
            log_sink,
            AppLogContext::empty(),
            egress,
            None,
        );

        let req = Request {
            method: "GET".into(),
            url: "http://127.0.0.1:9999/should-not-be-reached".into(),
            headers: Vec::new(),
            body: RequestBodySource::None,
            timeout_ms: Some(5000),
            trace_context: None,
        };

        let resp = crate::edge::cloud::http_client::Host::fetch(&mut runtime_state, req);

        assert!(resp.is_some());
        let resp = resp.unwrap();
        assert_eq!(resp.status, 403);
        assert!(resp
            .error
            .as_ref()
            .is_some_and(|e| e.contains("egress denied")));
    }

    #[test]
    fn egress_allowlist_passes_matching_host_via_policy_check() {
        let policy = EgressPolicy::new(vec!["api.stripe.com".to_string()]);
        let result = policy.check("http://api.stripe.com:9999/v1/charges");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn egress_check_denies_non_allowlisted_host() {
        use crate::interfaces::observe::{AppLogContext, NoopLogSink};
        let egress = Arc::new(EgressPolicy::new(vec!["api.stripe.com".to_string()]));
        let env = std::collections::HashMap::new();
        let log_sink: Arc<dyn crate::interfaces::observe::LogSink> = Arc::new(NoopLogSink);
        let mut runtime_state = RuntimeState::with_env_and_meter(
            env,
            None,
            log_sink,
            AppLogContext::empty(),
            egress,
            None,
        );

        let req = Request {
            method: "GET".into(),
            url: "http://evil.com/".into(),
            headers: Vec::new(),
            body: RequestBodySource::None,
            timeout_ms: Some(1000),
            trace_context: None,
        };

        let resp = crate::edge::cloud::http_client::Host::fetch(&mut runtime_state, req);
        assert!(resp.is_some());
        let resp = resp.unwrap();
        assert_eq!(resp.status, 403);
        assert!(resp
            .error
            .as_ref()
            .is_some_and(|e| e.contains("not in the allowlist")));
    }

    #[tokio::test]
    async fn hard_deny_loopback_returns_403_even_with_star_allowlist() {
        use crate::interfaces::observe::{AppLogContext, NoopLogSink};
        let egress = Arc::new(EgressPolicy::new(vec!["*".to_string()]));
        let env = std::collections::HashMap::new();
        let log_sink: Arc<dyn crate::interfaces::observe::LogSink> = Arc::new(NoopLogSink);
        let mut runtime_state = RuntimeState::with_env_and_meter(
            env,
            None,
            log_sink,
            AppLogContext::empty(),
            egress,
            None,
        );

        let req = Request {
            method: "GET".into(),
            url: "http://127.0.0.1/".into(),
            headers: Vec::new(),
            body: RequestBodySource::None,
            timeout_ms: Some(1000),
            trace_context: None,
        };

        let resp = crate::edge::cloud::http_client::Host::fetch(&mut runtime_state, req);
        assert!(resp.is_some());
        assert_eq!(resp.unwrap().status, 403);
    }

    #[test]
    fn egress_allowlist_passes_public_host() {
        let policy = EgressPolicy::new(vec!["example.com".to_string()]);
        let result = policy.check("http://example.com/");
        assert!(result.is_ok());
    }

    // ── NetworkingHost::resolve egress gate ───────────────────────────────

    #[test]
    fn resolve_blocked_when_empty_allowlist() {
        use crate::edge::cloud::networking::Host as NetworkingHost;
        use crate::interfaces::observe::{AppLogContext, NoopLogSink};
        let egress = Arc::new(EgressPolicy::new(vec![]));
        let env = std::collections::HashMap::new();
        let app_ctx = AppLogContext {
            app_name: "test".to_string(),
            tenant_id: "t_test".to_string(),
            deployment_id: "test".to_string(),
        };
        let mut rs = RuntimeState::with_env_and_meter(
            env,
            None,
            Arc::new(NoopLogSink) as Arc<dyn crate::interfaces::observe::LogSink>,
            app_ctx,
            egress,
            None,
        );
        let ips = NetworkingHost::resolve(&mut rs, "example.com".to_string());
        assert!(
            ips.is_empty(),
            "deny-all policy must return empty vec from resolve"
        );
    }

    #[test]
    fn resolve_blocked_for_disallowed_host() {
        use crate::edge::cloud::networking::Host as NetworkingHost;
        use crate::interfaces::observe::{AppLogContext, NoopLogSink};
        let egress = Arc::new(EgressPolicy::new(vec!["api.stripe.com".to_string()]));
        let env = std::collections::HashMap::new();
        let app_ctx = AppLogContext {
            app_name: "test".to_string(),
            tenant_id: "t_test".to_string(),
            deployment_id: "test".to_string(),
        };
        let mut rs = RuntimeState::with_env_and_meter(
            env,
            None,
            Arc::new(NoopLogSink) as Arc<dyn crate::interfaces::observe::LogSink>,
            app_ctx,
            egress,
            None,
        );
        let ips = NetworkingHost::resolve(&mut rs, "evil.com".to_string());
        assert!(
            ips.is_empty(),
            "host not in allowlist must return empty vec from resolve"
        );
    }
}
