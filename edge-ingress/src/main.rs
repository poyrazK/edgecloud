//! edge-ingress — public ingress / edge proxy for edgeCloud.
//!
//! Wraps a Caddy process via its JSON admin API. Subscribes to NATS
//! heartbeats to learn which worker hosts which app, and renders a
//! Caddyfile-JSON that maps `<tenant_id>-<app_name>.edgecloud.dev` to
//! `http://<worker>:<port>`. See `edge-ingress/README.md` for the operator
//! runbook (env vars, cert provisioning, Caddy invocation).

mod caddy;
mod config;
mod domains;
pub mod heartbeats;
mod messages;
mod routing;
pub mod traffic;

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tokio::sync::Notify;
use tokio::time::sleep;
use tracing_subscriber::EnvFilter;

use crate::caddy::CaddyClient;
use crate::config::Config;
use crate::routing::RoutingTable;

#[derive(Parser, Debug)]
#[command(name = "edge-ingress", about = "Public ingress for edgeCloud")]
struct Args {
    /// Optional path to a TOML config file. Env vars always win; this is
    /// just a convenience for operators who prefer files.
    #[arg(long)]
    config: Option<std::path::PathBuf>,
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let _args = Args::parse();

    let cfg = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(err = %e, "config error");
            return ExitCode::from(2);
        }
    };
    tracing::info!(
        region = %cfg.region,
        caddy = %cfg.caddy_admin_url,
        cert = %cfg.cert_file,
        "edge-ingress starting"
    );

    let table = Arc::new(RoutingTable::new());
    let caddy = match CaddyClient::new(&cfg.caddy_admin_url, cfg.admin_token.clone()) {
        Ok(c) => Arc::new(c),
        Err(e) => {
            tracing::error!(err = %e, "failed to build Caddy client");
            return ExitCode::from(1);
        }
    };

    // Custom-domain poller (issue #83). Spawned as a fire-and-forget
    // task; if the control plane rejects our token repeatedly
    // (rotated JWT secret, revoked ingest token) the poller returns
    // Err and we exit non-zero so the orchestrator restarts us with a
    // fresh token. Heartbeats keep running in parallel.
    if !cfg.control_plane_url.is_empty() {
        let dom_cfg = cfg.clone();
        let dom_table = table.clone();
        let dom_notify = Arc::new(Notify::new());
        tokio::spawn(async move {
            if let Err(e) = domains::run(dom_cfg, dom_table, dom_notify).await {
                tracing::error!(err = %e, "domain poller exited; restarting process");
                std::process::exit(1);
            }
        });
    } else {
        tracing::info!("CONTROL_PLANE_URL unset; running in default-only mode (no custom domains)");
    }

    // The heartbeat subscription can drop on NATS reconnect; mirror the
    // worker's pattern of re-subscribing with backoff.
    loop {
        match heartbeats::run(cfg.clone(), table.clone(), caddy.clone()).await {
            Ok(()) => {
                tracing::warn!("heartbeats::run returned cleanly; re-running in 1s");
            }
            Err(e) => {
                tracing::error!(err = %e, "heartbeats::run failed; re-running in 5s");
                sleep(Duration::from_secs(5)).await;
            }
        }
        sleep(Duration::from_secs(1)).await;
    }
}
