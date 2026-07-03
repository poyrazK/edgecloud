//! `edge quota` — show tenant quota and usage.

use anyhow::Result;
use std::path::Path;

use crate::api::ApiClient;
use crate::config::EdgeToml;

/// Show tenant quota and usage from `GET /api/v1/quotas`.
#[cfg(feature = "network")]
pub fn run(path: &Path) -> Result<()> {
    let edge_toml = EdgeToml::from_path(path)?;
    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    let q = client.get_quota()?;

    println!("Tenant ID:        {}", q.tenant_id);
    println!("Period start:     {}", q.quota_period_start);
    println!();
    println!("{:<24} {:>12} {:>12}", "RESOURCE", "LIMIT", "USED");
    println!("{}", "-".repeat(50));
    print_row("Deployments", q.max_deployments, 0);
    print_row("Apps", q.max_apps, 0);
    print_row("Workers", q.max_workers, 0);
    print_row("Memory (MB)", q.max_memory_mb, 0);
    print_row(
        "Outbound (MB)",
        q.max_outbound_mb,
        q.used_outbound_bytes / (1024 * 1024),
    );
    println!();
    println!(
        "Requests:  {:>12} / {}{}",
        q.used_request_count,
        limit_or_unlimited(q.max_requests_per_month),
        q.usage_pct
            .map(|p| format!(" ({:.1}%)", p))
            .unwrap_or_default()
    );
    Ok(())
}

fn limit_or_unlimited(n: i32) -> String {
    if n < 0 {
        "unlimited".into()
    } else {
        n.to_string()
    }
}

fn print_row(resource: &str, limit: i32, used: i64) {
    let limit_str = limit_or_unlimited(limit);
    let used_str = if limit < 0 {
        "-".into()
    } else {
        used.to_string()
    };
    println!("{:<24} {:>12} {:>12}", resource, limit_str, used_str);
}

#[cfg(not(feature = "network"))]
pub fn run(_path: &Path) -> Result<()> {
    anyhow::bail!("edge quota requires network support; rebuild with --features network")
}
