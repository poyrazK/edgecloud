//! Caddy admin-API client and Caddyfile-JSON renderer.
//!
//! Caddy exposes a JSON admin API on a configurable port (default `:2019`).
//! We render the full Caddyfile-JSON in Rust and `POST /load` it on every
//! routing change. The config is small (one route per app) and the round
//! trip is fast (~50ms for thousands of routes), so a full reload is fine
//! for v1.
//!
//! TODO(incremental-caddy): when route count exceeds ~10k, switch to
//! `PUT /id/<id>/apps/http/servers/edge_https/routes/<n>` patches and
//! track per-route handles in the `RoutingTable` snapshot.

use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde_json::{json, Value};

use crate::config::{ingress_host, Config};
use crate::routing::RouteEntry;

const SERVER_NAME_HTTPS: &str = "edge_https";
const SERVER_NAME_HTTP: &str = "edge_http";

#[derive(Clone)]
pub struct CaddyClient {
    http: Client,
    admin_url: String,
    token: Option<String>,
}

impl CaddyClient {
    pub fn new(admin_url: &str, token: Option<String>) -> Result<Self> {
        let http = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .context("building reqwest client")?;
        Ok(Self {
            http,
            admin_url: admin_url.trim_end_matches('/').to_string(),
            token,
        })
    }

    /// POST the rendered config to Caddy's `/load` endpoint. Replaces the
    /// entire config. Bearer-token header is added when configured.
    pub async fn load_config(&self, config: &Value) -> Result<()> {
        let url = format!("{}/load", self.admin_url);
        let mut req = self.http.post(&url).json(config);
        if let Some(t) = &self.token {
            req = req.bearer_auth(t);
        }
        let resp = req.send().await.context("calling Caddy /load")?;
        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("Caddy /load returned {status}: {body}"));
        }
        Ok(())
    }
}

/// Render the full Caddyfile-JSON for a set of routes. Pure function — no
/// I/O, easy to unit-test.
///
/// Hosts are formatted as `<tenant_id>-<app_name>.edgecloud.dev` so two
/// tenants creating an app named `api` don't collide on the shared wildcard.
///
/// When multiple entries exist for the same `(tenant_id, app_name)` (canary /
/// blue-green), they are rendered as weighted upstreams in a single route:
/// ```json
/// "upstreams": [
///   {"dial": "1.2.3.4:8081", "weight": 95},
///   {"dial": "1.2.3.5:8082", "weight": 5}
/// ]
/// ```
///
/// A single entry omits the `weight` key, which Caddy defaults to `1` for
/// that sole upstream — identical routing behaviour to the legacy path.
pub fn render_routes(entries: &[RouteEntry], cfg: &Config) -> Value {
    // Group entries by (tenant_id, app_name). Each entry in a group represents
    // a different deployment_id for the same app (canary/blue-green).
    let mut groups: std::collections::HashMap<
        (&str, &str),
        Vec<&RouteEntry>,
    > = std::collections::HashMap::new();
    for e in entries {
        groups
            .entry((e.tenant_id.as_str(), e.app_name.as_str()))
            .or_default()
            .push(e);
    }

    // Sort groups by (tenant_id, app_name) for deterministic output.
    let mut group_keys: Vec<_> = groups.keys().collect();
    group_keys.sort_by(|a, b| a.0.cmp(b.0).then_with(|| a.1.cmp(b.1)));

    let routes: Vec<Value> = group_keys
        .iter()
        .map(|(tenant_id, app_name)| {
            let host = ingress_host(tenant_id, app_name);
            let group = &groups[&(*tenant_id, *app_name)];
            // Sort group by deployment_id so output is deterministic.
            let mut group_sorted = (*group).clone();
            group_sorted.sort_by(|a, b| {
                a.deployment_id
                    .cmp(&b.deployment_id)
                    .then_with(|| a.worker_addr.cmp(&b.worker_addr))
            });

            let upstreams: Value = if group_sorted.len() == 1 {
                // Single deployment — no weight key needed.
                let e = group_sorted[0];
                serde_json::json!([{"dial": format!("{}:{}", e.worker_addr, e.port)}])
            } else {
                // Multiple deployments — weighted upstreams.
                serde_json::json!(group_sorted.iter().map(|e| {
                    let upstream = serde_json::json!({
                        "dial": format!("{}:{}", e.worker_addr, e.port),
                        "weight": e.weight,
                    });
                    upstream
                }).collect::<Vec<_>>())
            };

            json!({
                "match": [{"host": [host]}],
                "handle": [{
                    "handler": "subroute",
                    "routes": [{
                        "handle": [{
                            "handler": "reverse_proxy",
                            "upstreams": upstreams,
                            "health_checks": {
                                "active": {"uri": "/", "expect_status": 2}
                            }
                        }]
                    }]
                }],
                "terminal": true
            })
        })
        .collect();

    let mut servers = serde_json::Map::new();
    servers.insert(
        SERVER_NAME_HTTPS.to_string(),
        json!({
            "listen": [cfg.listen_https],
            "routes": routes,
        }),
    );
    if cfg.http_to_https {
        servers.insert(
            SERVER_NAME_HTTP.to_string(),
            json!({
                "listen": [cfg.listen_http],
                "routes": [{
                    "handle": [{
                        "handler": "static_response",
                        "headers": {"Location": ["{http.request.uri}"]},
                        "status_code": 308
                    }]
                }]
            }),
        );
    }

    json!({
        "apps": {
            "http": {
                "servers": servers,
                "automatic_https": {"disable": true}
            },
            "tls": {
                "certificates": {
                    "load_files": [
                        {"certificate": cfg.cert_file, "key": cfg.key_file}
                    ]
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::RouteEntry;
    use std::time::Instant;

    fn entry(tenant: &str, app: &str, addr: &str, port: u16) -> RouteEntry {
        RouteEntry {
            tenant_id: tenant.to_string(),
            app_name: app.to_string(),
            deployment_id: None,
            weight: 100,
            worker_addr: addr.to_string(),
            port,
            last_seen: Instant::now(),
        }
    }

    fn canary_entry(
        tenant: &str,
        app: &str,
        deployment_id: &str,
        addr: &str,
        port: u16,
        weight: u8,
    ) -> RouteEntry {
        RouteEntry {
            tenant_id: tenant.to_string(),
            app_name: app.to_string(),
            deployment_id: Some(deployment_id.to_string()),
            weight,
            worker_addr: addr.to_string(),
            port,
            last_seen: Instant::now(),
        }
    }

    fn test_cfg() -> Config {
        Config {
            nats_url: "nats://localhost:4222".into(),
            caddy_admin_url: "http://127.0.0.1:2019".into(),
            region: "test".into(),
            cert_file: "/etc/caddy/tls/cert.pem".into(),
            key_file: "/etc/caddy/tls/key.pem".into(),
            listen_http: ":80".into(),
            listen_https: ":443".into(),
            refresh_debounce_ms: 1000,
            http_to_https: true,
            admin_token: None,
        }
    }

    #[test]
    fn render_empty_table_still_emits_servers_and_tls() {
        let cfg = test_cfg();
        let cfg_json = render_routes(&[], &cfg);
        let servers = cfg_json["apps"]["http"]["servers"].as_object().unwrap();
        assert!(servers.contains_key(SERVER_NAME_HTTPS));
        assert!(servers.contains_key(SERVER_NAME_HTTP));
        assert_eq!(
            servers[SERVER_NAME_HTTPS]["routes"]
                .as_array()
                .unwrap()
                .len(),
            0
        );
        let load_files = &cfg_json["apps"]["tls"]["certificates"]["load_files"]
            .as_array()
            .unwrap();
        assert_eq!(load_files.len(), 1);
        assert_eq!(load_files[0]["certificate"], "/etc/caddy/tls/cert.pem");
        assert_eq!(load_files[0]["key"], "/etc/caddy/tls/key.pem");
    }

    #[test]
    fn render_three_entries_produces_three_routes_with_correct_hosts() {
        let cfg = test_cfg();
        let entries = vec![
            entry("t_acme", "api", "1.2.3.4", 8081),
            entry("t_acme", "web", "1.2.3.4", 8082),
            entry("t_globex", "api", "5.6.7.8", 9000),
        ];
        let cfg_json = render_routes(&entries, &cfg);
        let routes = cfg_json["apps"]["http"]["servers"][SERVER_NAME_HTTPS]["routes"]
            .as_array()
            .unwrap();
        assert_eq!(routes.len(), 3);

        // Sorted by (tenant, app) — t_acme/api, t_acme/web, t_globex/api.
        let hosts: Vec<String> = routes
            .iter()
            .map(|r| r["match"][0]["host"][0].as_str().unwrap().to_string())
            .collect();
        assert_eq!(
            hosts,
            vec![
                "t_acme-api.edgecloud.dev".to_string(),
                "t_acme-web.edgecloud.dev".to_string(),
                "t_globex-api.edgecloud.dev".to_string(),
            ]
        );

        // Dials must reflect the right upstream per route.
        let dials: Vec<String> = routes
            .iter()
            .map(|r| {
                r["handle"][0]["routes"][0]["handle"][0]["upstreams"][0]["dial"]
                    .as_str()
                    .unwrap()
                    .to_string()
            })
            .collect();
        assert_eq!(dials, vec!["1.2.3.4:8081", "1.2.3.4:8082", "5.6.7.8:9000"]);
    }

    /// Two entries for the same (tenant, app) with different deployment_ids
    /// must be rendered as a single route with weighted upstreams.
    #[test]
    fn canary_two_deployments_rendered_as_weighted_upstreams() {
        let cfg = test_cfg();
        let entries = vec![
            canary_entry("t_acme", "api", "d_v1", "1.2.3.4", 8081, 95),
            canary_entry("t_acme", "api", "d_v2", "1.2.3.5", 8082, 5),
        ];
        let cfg_json = render_routes(&entries, &cfg);
        let routes = cfg_json["apps"]["http"]["servers"][SERVER_NAME_HTTPS]["routes"]
            .as_array()
            .unwrap();
        assert_eq!(routes.len(), 1, "only one route for t_acme/api");

        // Host must be the same for both deployments.
        let hosts: Vec<String> = routes[0]["match"][0]["host"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect();
        assert_eq!(hosts, vec!["t_acme-api.edgecloud.dev"]);

        // Upstreams must include both dial addrs with correct weights.
        let upstreams = &routes[0]["handle"][0]["routes"][0]["handle"][0]["upstreams"];
        let upstreams_arr = upstreams.as_array().unwrap();
        assert_eq!(upstreams_arr.len(), 2);

        // Sort by dial for deterministic assertion.
        let mut sorted = upstreams_arr.clone();
        sorted.sort_by(|a, b| {
            a["dial"]
                .as_str()
                .unwrap()
                .cmp(&b["dial"].as_str().unwrap())
        });
        assert_eq!(sorted[0]["dial"], "1.2.3.4:8081");
        assert_eq!(sorted[0]["weight"], 95);
        assert_eq!(sorted[1]["dial"], "1.2.3.5:8082");
        assert_eq!(sorted[1]["weight"], 5);
    }

    /// Single deployment omits the `weight` key so Caddy uses its default.
    #[test]
    fn single_deployment_omits_weight_key() {
        let cfg = test_cfg();
        let entries = vec![canary_entry("t_acme", "api", "d_v1", "1.2.3.4", 8081, 100)];
        let cfg_json = render_routes(&entries, &cfg);
        let upstreams = &cfg_json["apps"]["http"]["servers"][SERVER_NAME_HTTPS]["routes"]
            [0]["handle"][0]["routes"][0]["handle"][0]["upstreams"];
        assert_eq!(upstreams.as_array().unwrap().len(), 1);
        let upstream = &upstreams[0];
        assert_eq!(upstream["dial"], "1.2.3.4:8081");
        // weight key must be absent (Caddy defaults to unweighted for single upstream)
        assert!(
            !upstream.as_object().unwrap().contains_key("weight"),
            "single upstream should not have a weight key"
        );
    }

    #[test]
    fn http_to_https_disabled_omits_port_80_server() {
        let mut cfg = test_cfg();
        cfg.http_to_https = false;
        let cfg_json = render_routes(&[], &cfg);
        let servers = cfg_json["apps"]["http"]["servers"].as_object().unwrap();
        assert!(servers.contains_key(SERVER_NAME_HTTPS));
        assert!(!servers.contains_key(SERVER_NAME_HTTP));
    }

    #[test]
    fn automatic_https_is_disabled_so_wildcard_cert_wins() {
        let cfg_json = render_routes(&[], &test_cfg());
        assert_eq!(
            cfg_json["apps"]["http"]["automatic_https"]["disable"],
            serde_json::Value::Bool(true)
        );
    }
}
