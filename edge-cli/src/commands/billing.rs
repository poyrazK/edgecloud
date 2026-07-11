//! `edge billing` — checkout, portal, and subscription helpers.

use anyhow::{Context, Result};
use clap::{Subcommand, ValueEnum};
use std::path::Path;

use crate::api::{ApiClient, BillingSubscriptionResponse};
use crate::config::EdgeToml;
use crate::output;

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum BillingPlan {
    #[value(name = "pro")]
    Pro,
    #[value(name = "business")]
    Business,
    #[value(name = "enterprise")]
    Enterprise,
}

impl BillingPlan {
    fn as_str(self) -> &'static str {
        match self {
            BillingPlan::Pro => "pro",
            BillingPlan::Business => "business",
            BillingPlan::Enterprise => "enterprise",
        }
    }
}

#[derive(Subcommand)]
pub enum BillingAction {
    /// Open a hosted checkout page for a paid plan.
    Checkout {
        /// Paid plan to subscribe to.
        #[arg(value_enum)]
        plan: BillingPlan,
    },
    /// Open the provider-hosted self-service billing portal.
    Portal {
        /// URL Stripe should return to when the user leaves the portal.
        #[arg(long)]
        return_url: Option<String>,
    },
    /// Print the current subscription mirror stored by the control plane.
    Subscription,
}

#[cfg(feature = "network")]
pub fn run(path: &Path, action: BillingAction) -> Result<()> {
    match action {
        BillingAction::Checkout { plan } => checkout(path, plan),
        BillingAction::Portal { return_url } => portal(path, return_url.as_deref()),
        BillingAction::Subscription => subscription(path),
    }
}

#[cfg(feature = "network")]
fn checkout(path: &Path, plan: BillingPlan) -> Result<()> {
    let client = client_from_project(path)?;
    let session = client
        .create_billing_checkout(plan.as_str())
        .with_context(|| format!("failed to create checkout for {} plan", plan.as_str()))?;

    output::success(&format!(
        "Checkout session {} created for {}",
        session.session_id,
        plan.as_str()
    ));
    if let Some(expires) = session.expires_at.as_deref() {
        println!("  Expires:  {expires}");
    }
    open_url(&session.checkout_url, "checkout")?;
    Ok(())
}

#[cfg(feature = "network")]
fn portal(path: &Path, return_url: Option<&str>) -> Result<()> {
    let edge_toml = EdgeToml::from_path(path)?;
    let return_url = return_url
        .map(str::to_string)
        .or_else(|| edge_toml.deployment.api.clone())
        .unwrap_or_else(|| "https://edgecloud.dev/account".to_string());
    let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
    let session = client
        .create_billing_portal(&return_url)
        .with_context(|| "failed to create billing portal session")?;

    output::success("Billing portal session created");
    open_url(&session.portal_url, "billing portal")?;
    Ok(())
}

#[cfg(feature = "network")]
fn subscription(path: &Path) -> Result<()> {
    let client = client_from_project(path)?;
    let sub = client
        .get_billing_subscription()
        .with_context(|| "failed to fetch billing subscription")?;
    print_subscription(&sub);
    Ok(())
}

#[cfg(feature = "network")]
fn client_from_project(path: &Path) -> Result<ApiClient> {
    let edge_toml = EdgeToml::from_path(path)?;
    ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))
}

#[cfg(feature = "network")]
fn open_url(url: &str, label: &str) -> Result<()> {
    open::that(url).with_context(|| format!("failed to open {label} URL in a browser"))?;
    println!("Opening {label}: {url}");
    Ok(())
}

fn print_subscription(sub: &BillingSubscriptionResponse) {
    println!("Tenant ID:          {}", sub.tenant_id);
    println!("Provider:           {}", sub.provider);
    println!("Plan:               {}", sub.plan);
    println!("Status:             {}", sub.status);
    println!("Cancel at period:   {}", sub.cancel_at_period_end);
    if let Some(v) = sub.current_period_end.as_deref() {
        println!("Current period end: {v}");
    }
    if let Some(v) = sub.provider_customer_id.as_deref() {
        println!("Customer ID:        {v}");
    }
    if let Some(v) = sub.provider_subscription_id.as_deref() {
        println!("Subscription ID:    {v}");
    }
    println!("Created:            {}", sub.created_at);
    println!("Updated:            {}", sub.updated_at);
}

#[cfg(not(feature = "network"))]
pub fn run(_path: &Path, _action: BillingAction) -> Result<()> {
    anyhow::bail!("edge billing requires network support; rebuild with --features network")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_as_str_matches_clap_values() {
        assert_eq!(BillingPlan::Pro.as_str(), "pro");
        assert_eq!(BillingPlan::Business.as_str(), "business");
        assert_eq!(BillingPlan::Enterprise.as_str(), "enterprise");
    }
}
