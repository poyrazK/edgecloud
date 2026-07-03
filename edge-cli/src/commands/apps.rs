//! `edge apps` — list all apps, create a new one, or show details.
//!
//! * `edge apps` (no subcommand) → list all apps for the tenant.
//! * `edge apps create <name>` → create a new app.
//! * `edge apps get <name>` → show details for a specific app.

use anyhow::{Context, Result};
use std::path::Path;

use crate::api::ApiClient;
use crate::config::EdgeToml;
use crate::output;

/// List all apps for the authenticated tenant.
#[cfg(feature = "network")]
pub fn list(path: &Path) -> Result<()> {
    let edge_toml = EdgeToml::from_path(path)?;
    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    let apps = client.list_apps()?;

    if apps.is_empty() {
        println!("No apps found.");
    } else {
        println!("{:<24} {:<24} CREATED", "ID", "NAME");
        println!("{}", "-".repeat(64));
        for a in apps {
            println!("{:<24} {:<24} {}", a.id, a.name, a.created_at);
        }
    }
    Ok(())
}

#[cfg(not(feature = "network"))]
pub fn list(_path: &Path) -> Result<()> {
    anyhow::bail!("edge apps requires network support; rebuild with --features network")
}

/// Create a new app.
#[cfg(feature = "network")]
pub fn create(path: &Path, name: &str, description: Option<&str>) -> Result<()> {
    let edge_toml = EdgeToml::from_path(path)?;
    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    let app = client.create_app(name, description)?;

    output::success(&format!("Created app '{}'", app.name));
    println!("  ID:          {}", app.id);
    if let Some(ref d) = app.description {
        println!("  Description: {d}");
    }
    println!("  Created:     {}", app.created_at);
    Ok(())
}

#[cfg(not(feature = "network"))]
pub fn create(_path: &Path, _name: &str, _description: Option<&str>) -> Result<()> {
    anyhow::bail!("edge apps requires network support; rebuild with --features network")
}

/// Show details for a specific app.
#[cfg(feature = "network")]
pub fn get(path: &Path, name: &str) -> Result<()> {
    let edge_toml = EdgeToml::from_path(path)?;
    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    let app = client
        .get_app(name)
        .with_context(|| format!("fetching app '{name}'"))?;

    println!("ID:          {}", app.id);
    println!("Name:        {}", app.name);
    println!("Tenant ID:   {}", app.tenant_id);
    match app.description {
        Some(ref d) => println!("Description: {d}"),
        None => println!("Description: (none)"),
    }
    println!("Created:     {}", app.created_at);
    Ok(())
}

#[cfg(not(feature = "network"))]
pub fn get(_path: &Path, _name: &str) -> Result<()> {
    anyhow::bail!("edge apps requires network support; rebuild with --features network")
}
