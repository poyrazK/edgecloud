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
        .unwrap_or_else(|| edge_toml.web_url("https://edgecloud.dev/account"));
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
    // Compute the widest label so the column stays aligned even when
    // a future field is added. The label set includes both the
    // unconditional labels and the longest possible conditional one
    // — conditional labels are rendered via the same output::kv
    // helper with the same width, so a missed include only changes
    // the column width by a few chars (cosmetic, not correctness).
    let labels = [
        "Tenant ID:",
        "Provider:",
        "Plan:",
        "Status:",
        "Cancel at period:",
        "Current period end:",
        "Customer ID:",
        "Subscription ID:",
        "Created:",
        "Updated:",
    ];
    let width = labels.iter().map(|l| l.len()).max().unwrap_or(0);
    output::kv("Tenant ID:", &sub.tenant_id, width);
    output::kv("Provider:", &sub.provider, width);
    output::kv("Plan:", &sub.plan, width);
    output::kv("Status:", &sub.status, width);
    output::kv("Cancel at period:", sub.cancel_at_period_end, width);
    if let Some(v) = sub.current_period_end.as_deref() {
        output::kv("Current period end:", v, width);
    }
    if let Some(v) = sub.provider_customer_id.as_deref() {
        output::kv("Customer ID:", v, width);
    }
    if let Some(v) = sub.provider_subscription_id.as_deref() {
        output::kv("Subscription ID:", v, width);
    }
    output::kv("Created:", &sub.created_at, width);
    output::kv("Updated:", &sub.updated_at, width);
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
