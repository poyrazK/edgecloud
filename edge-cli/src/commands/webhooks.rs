//! `edge webhooks` — manage tenant webhook subscriptions (issue #565).
//!
//! Subcommands:
//! - `add <url> [--events e1,e2] [--description <s>] [--secret <s> | --no-echo]`
//! - `list`
//! - `update <id> [--url <u>] [--events e1,e2] [--description <s>] [--enabled|--disable] [--secret <s>]`
//! - `remove <id>`
//!
//! The CLI does NOT persist webhook state to `.edge/state.json` —
//! every invocation is a fresh query against the control plane.
//! This matches the `domains` precedent (`commands/domains.rs:11`):
//! webhooks are tenant-scoped, not deployment-scoped, and a stale
//! local copy would mislead tenants when the server side has
//! diverged.
//!
//! **Secret UX (issue #565 amendment).** The Go `Webhook.Secret`
//! field is `json:"-"` (`edge-control-plane/internal/domain/webhook.go:14`)
//! — the server deliberately does NOT echo the secret back on
//! Create, unlike API keys' one-time-token model. The CLI mirrors
//! the `edge auth login` three-way pattern (`commands/auth.rs:215`):
//! `--secret <s>` for CI scripts, `--no-echo` for TTY paste via
//! `rpassword`, stdin default for non-interactive pipes. The CLI
//! does NOT echo the secret on success — the tenant must store it
//! before the prompt window exits; a `output::warn` reminder
//! follows the create success line.
//!
//! Retry-aware paths (`list`, `remove`) route through
//! `commands::retry::call_with_retry_no_interrupt`. `add` and
//! `update` are Phase-2 deferred — see the docstring on `run`.

use anyhow::{Context, Result};
use std::io::Read;
use std::path::Path;

use super::retry::{
    call_with_retry_no_interrupt, DEFAULT_MAX_RETRIES, DEFAULT_RETRY_BASE_MS, DEFAULT_RETRY_CAP_MS,
};
use crate::api::ApiClient;
use crate::config::EdgeToml;
use crate::output;

/// The canonical event-type set. Source of truth:
/// `edge-control-plane/internal/domain/webhook.go` `ValidWebhookEvents`
/// const list. If the server adds a new event type, both this
/// constant AND the corresponding `validate_events` test must be
/// updated — see `event_type_constants_match_server` in the unit
/// tests below.
///
/// **Do NOT** use this constant for client-side guard rails — every
/// value the user types must still flow through `validate_events`
/// so the CLI's error messages list the valid set explicitly.
pub const VALID_EVENTS: &[&str] = &["deploy", "activate", "rollback", "auto_rollback"];

/// The four subcommands. Mirrors the route table in
/// `edge-control-plane/internal/handler/webhook.go`.
#[derive(Debug)]
pub enum WebhooksAction {
    Add {
        url: String,
        events: Vec<String>,
        description: String,
        secret: Option<String>,
        no_echo: bool,
    },
    List,
    Update {
        id: String,
        url: Option<String>,
        events: Option<Vec<String>>,
        description: Option<String>,
        enabled: Option<bool>,
        secret: Option<String>,
    },
    Remove {
        id: String,
    },
}

/// Split a comma-separated `--events` value into a `Vec<String>`,
/// validating each token against `VALID_EVENTS`. Empty input or
/// any unknown token returns an `Err` whose message names the
/// offending value AND lists the canonical set.
///
/// The server-side `validateWebhookRequest`
/// (`edge-control-plane/internal/handler/webhook.go:147`) does the
/// same validation, but doing it client-side gives a 1-message
/// answer ("invalid event: delete (valid: deploy, activate, ...)")
/// instead of a 400 round-trip.
///
/// Public because `main.rs` re-validates at the clap boundary so an
/// unknown event token surfaces as a `clap::Error::exit()` (clean
/// exit code 2) rather than reaching the runtime anyhow chain. The
/// function is the single source of truth for the message text in
/// both call sites.
pub fn validate_events(input: &str) -> Result<Vec<String>> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        anyhow::bail!(
            "at least one event type is required (valid: {})",
            VALID_EVENTS.join(", ")
        );
    }
    let mut out = Vec::new();
    for tok in trimmed.split(',') {
        let t = tok.trim();
        if t.is_empty() {
            anyhow::bail!("empty event token (use comma-separated values, no spaces)");
        }
        if !VALID_EVENTS.contains(&t) {
            anyhow::bail!("invalid event: {t} (valid: {})", VALID_EVENTS.join(", "));
        }
        if !out.iter().any(|existing| existing == t) {
            out.push(t.to_string());
        }
    }
    Ok(out)
}

/// Acquire the webhook signing secret. Three-way pattern:
/// - `--secret <s>` wins (consistent with `auth login` precedence
///   at `commands/auth.rs:216`).
/// - `--no-echo` reads from /dev/tty via `rpassword`.
/// - No flag reads from stdin (non-interactive default).
///
/// Returns the trimmed secret; bails if the secret is empty after
/// trimming OR shorter than 16 characters (mirrors the server-side
/// `validateWebhookRequest` length check at
/// `edge-control-plane/internal/handler/webhook.go:158`).
fn acquire_secret(secret: Option<&str>, no_echo: bool) -> Result<String> {
    let value = match (secret, no_echo) {
        (Some(s), _) => s.trim().to_string(),
        (None, true) => rpassword::prompt_password("Paste webhook secret: ")
            .context("failed to read webhook secret from /dev/tty (is one attached?)")?
            .trim()
            .to_string(),
        (None, false) => {
            eprintln!("Paste the webhook signing secret (stdin, Ctrl-D to cancel):");
            let mut buf = String::new();
            std::io::stdin()
                .lock()
                .read_to_string(&mut buf)
                .context("failed to read webhook secret from stdin")?;
            buf.trim().to_string()
        }
    };
    if value.is_empty() {
        anyhow::bail!("webhook secret is empty");
    }
    if value.len() < 16 {
        anyhow::bail!("webhook secret must be at least 16 characters");
    }
    Ok(value)
}

impl WebhooksAction {
    /// Run the action. `path` is the project root, used to load
    /// `edge.toml` (for the control plane URL).
    ///
    /// We intentionally do NOT require `.edge/state.json` here.
    /// Webhooks are tenant-scoped, not app-scoped, so the state
    /// file's `app_name` is irrelevant. Forcing its presence would
    /// break `edge webhooks add ...` for tenants who manage
    /// webhooks before their first deploy.
    ///
    /// **Phase-2 deferred (issue #571 follow-up).** `Add` is a POST
    /// insert; a retried POST would silently insert a duplicate
    /// row since the schema has no unique `(tenant_id, url)`
    /// constraint today (`edge-control-plane/migrations/` has no
    /// unique index on the `webhooks` table). The right fix is
    /// CP-side `Idempotency-Key` schema extension — until that
    /// lands, `Add` does NOT route through `call_with_retry`.
    /// Same reasoning applies to `Update` (PUT-with-side-effects;
    /// replaying a successful update with a stale body is worse
    /// than failing once). `Remove` IS retryable — DELETE is
    /// naturally idempotent (second call returns 404 with no side
    /// effect), and `List` is a read.
    #[cfg(feature = "network")]
    pub fn run(self, path: &Path) -> Result<()> {
        let edge_toml = EdgeToml::from_path(path)?;
        let client = ApiClient::new(edge_toml.api_url("https://api.edgecloud.dev"))?;
        let webhooks = client.webhooks();

        match self {
            WebhooksAction::Add {
                url,
                events,
                description,
                secret,
                no_echo,
            } => {
                let secret = acquire_secret(secret.as_deref(), no_echo)?;
                let wh = webhooks
                    .add(&url, &events, &description, &secret)
                    .with_context(|| format!("adding webhook for {url}"))?;
                println!("Created webhook {}", wh.id);
                println!("  URL:         {}", wh.url);
                println!("  Events:      {}", wh.events.join(", "));
                if !wh.description.is_empty() {
                    println!("  Description: {}", wh.description);
                }
                println!(
                    "  Status:      {}",
                    if wh.enabled { "ENABLED" } else { "DISABLED" }
                );
                output::warn(
                    "the secret you entered is NOT shown again — store it now (the server \
                     deliberately does not echo webhook secrets)",
                );
                Ok(())
            }
            WebhooksAction::List => {
                let rows = call_with_retry_no_interrupt(
                    "webhooks list",
                    || webhooks.list(),
                    DEFAULT_MAX_RETRIES,
                    DEFAULT_RETRY_BASE_MS,
                    DEFAULT_RETRY_CAP_MS,
                )
                .context("listing webhooks")?;
                if rows.is_empty() {
                    println!("No webhook subscriptions.");
                } else {
                    println!(
                        "{:<14} {:<48} {:<28} {:<10} CREATED",
                        "ID", "URL", "EVENTS", "STATUS"
                    );
                    println!("{}", "-".repeat(112));
                    for w in rows {
                        let events = w.events.join(",");
                        let status = if w.enabled { "ENABLED" } else { "DISABLED" };
                        // Truncate URL to keep the table under 130 cols on
                        // 80-col terminals. Event list and status fields
                        // get their own truncation if they exceed the
                        // declared column width — `format` left-pads with
                        // spaces which would visually break the alignment
                        // for short rows; clip + ellipsize to keep the
                        // header/divider widths honest.
                        let url = clip(&w.url, 48);
                        let events = clip(&events, 28);
                        println!(
                            "{:<14} {:<48} {:<28} {:<10} {}",
                            w.id, url, events, status, w.created_at
                        );
                    }
                }
                Ok(())
            }
            WebhooksAction::Update {
                id,
                url,
                events,
                description,
                enabled,
                secret,
            } => {
                let updated = webhooks
                    .update(
                        &id,
                        url.as_deref(),
                        events.as_deref(),
                        description.as_deref(),
                        enabled,
                        secret.as_deref(),
                    )
                    .with_context(|| format!("updating webhook {id}"))?;
                println!("Updated webhook {}", updated.id);
                println!("  URL:         {}", updated.url);
                println!("  Events:      {}", updated.events.join(", "));
                if !updated.description.is_empty() {
                    println!("  Description: {}", updated.description);
                }
                println!(
                    "  Status:      {}",
                    if updated.enabled {
                        "ENABLED"
                    } else {
                        "DISABLED"
                    }
                );
                Ok(())
            }
            WebhooksAction::Remove { id } => {
                call_with_retry_no_interrupt(
                    "webhooks remove",
                    || webhooks.remove(&id),
                    DEFAULT_MAX_RETRIES,
                    DEFAULT_RETRY_BASE_MS,
                    DEFAULT_RETRY_CAP_MS,
                )
                .with_context(|| format!("removing webhook {id}"))?;
                println!("Removed webhook {id}.");
                Ok(())
            }
        }
    }

    #[cfg(not(feature = "network"))]
    pub fn run(self, _path: &Path) -> Result<()> {
        anyhow::bail!("edge webhooks requires network support; rebuild with --features network")
    }
}

/// Clip a string to `max` chars; append an ellipsis when truncated
/// so the table column widths declared in the header line stay
/// honest. Single-pass: `chars().nth(max)` short-circuits on
/// short input (returns `None` → no truncation needed) without
/// counting the whole string first. Byte-based clip avoids
/// panicking on multi-byte UTF-8 (rare in URLs / event lists, but
/// a default-build panic is cheap to avoid).
fn clip(s: &str, max: usize) -> String {
    // `nth(max)` returns `None` when the string has at most `max`
    // chars — short-circuit before allocating. O(max) instead of
    // O(2n).
    if s.chars().nth(max).is_none() {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(max - 1).collect();
        out.push('…');
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validate_events_accepts_all_four_canonical_types() {
        let v = validate_events("deploy,activate,rollback,auto_rollback").unwrap();
        assert_eq!(
            v,
            vec![
                "deploy".to_string(),
                "activate".to_string(),
                "rollback".to_string(),
                "auto_rollback".to_string(),
            ]
        );
    }

    #[test]
    fn validate_events_accepts_whitespace_around_tokens() {
        let v = validate_events(" deploy , activate ").unwrap();
        assert_eq!(v, vec!["deploy".to_string(), "activate".to_string()]);
    }

    #[test]
    fn validate_events_deduplicates_repeated_tokens() {
        let v = validate_events("deploy,deploy,activate").unwrap();
        assert_eq!(v, vec!["deploy".to_string(), "activate".to_string()]);
    }

    #[test]
    fn validate_events_rejects_unknown_token() {
        let err = validate_events("deploy,delete").unwrap_err();
        let msg = format!("{err}");
        // Message names the offending token AND lists the valid set so
        // the user can self-correct without reading docs.
        assert!(msg.contains("invalid event: delete"), "msg = {msg}");
        assert!(msg.contains("deploy"), "msg = {msg}");
        assert!(msg.contains("activate"), "msg = {msg}");
    }

    #[test]
    fn validate_events_rejects_empty_string() {
        let err = validate_events("").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("at least one event"), "msg = {msg}");
    }

    #[test]
    fn validate_events_rejects_whitespace_only() {
        let err = validate_events("   ").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("at least one event"), "msg = {msg}");
    }

    #[test]
    fn validate_events_rejects_trailing_comma() {
        // Trailing comma produces an empty token after split. We
        // surface that explicitly rather than silently dropping it.
        let err = validate_events("deploy,").unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("empty event token"), "msg = {msg}");
    }

    #[test]
    fn event_type_constants_match_server() {
        // Pinned against the Go ValidWebhookEvents const at
        // edge-control-plane/internal/domain/webhook.go. If the
        // server adds a new event type, BOTH sides must change.
        assert_eq!(
            VALID_EVENTS,
            &["deploy", "activate", "rollback", "auto_rollback"]
        );
    }

    #[test]
    fn clip_short_string_is_returned_unchanged() {
        assert_eq!(clip("hello", 10), "hello");
    }

    #[test]
    fn clip_at_exact_boundary_is_returned_unchanged() {
        assert_eq!(clip("hello", 5), "hello");
    }

    #[test]
    fn clip_long_string_is_truncated_with_ellipsis() {
        let s = "a".repeat(60);
        let clipped = clip(&s, 48);
        assert_eq!(clipped.chars().count(), 48);
        assert!(clipped.ends_with('…'));
    }

    #[test]
    fn clip_handles_multibyte_without_panic() {
        // 30 emoji × 4 bytes each = 120 bytes, but 30 chars.
        let s = "🚀".repeat(30);
        let clipped = clip(&s, 10);
        assert_eq!(clipped.chars().count(), 10);
        assert!(clipped.ends_with('…'));
    }

    // acquire_secret boundary tests. The function has two bail!
    // arms (empty secret, <16 chars) that cannot be exercised by a
    // wiremock test — the CLI exits before the wire round-trip.
    // Pinning them offline catches drift in the server-side length
    // check at `internal/handler/webhook.go:158` (currently `len
    // < 16`), which is the contract this helper mirrors.

    #[test]
    fn acquire_secret_rejects_empty_string() {
        let err = acquire_secret(Some(""), false).unwrap_err();
        assert!(
            format!("{err}").contains("empty"),
            "expected empty-secret error, got: {err}"
        );
    }

    #[test]
    fn acquire_secret_rejects_whitespace_only_string() {
        // `trim()` runs first, so `"   "` becomes `""` and falls
        // into the empty-secret arm. Pin the behavior — a future
        // refactor that drops the trim() would silently accept
        // whitespace-only secrets.
        let err = acquire_secret(Some("   "), false).unwrap_err();
        assert!(
            format!("{err}").contains("empty"),
            "expected empty-secret error after trim, got: {err}"
        );
    }

    #[test]
    fn acquire_secret_rejects_secret_below_16_chars() {
        let err = acquire_secret(Some("short"), false).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("at least 16"),
            "expected length-floor error, got: {msg}"
        );
    }

    #[test]
    fn acquire_secret_rejects_secret_at_15_chars() {
        // 15 is below the floor; pin the off-by-one boundary so a
        // future `len < 15` / `len <= 15` drift on either side
        // (CLI or server) fails CI here.
        let err = acquire_secret(Some("abcdefghijklmno"), false).unwrap_err();
        assert!(
            format!("{err}").contains("at least 16"),
            "expected 15-char rejection, got: {err}"
        );
    }

    #[test]
    fn acquire_secret_accepts_secret_at_16_chars() {
        // Exactly 16 chars passes — the floor is inclusive
        // (`len < 16` bails, `len == 16` succeeds). Pin the
        // boundary on the success side too.
        let value = acquire_secret(Some("abcdefghijklmnop"), false).unwrap();
        assert_eq!(value, "abcdefghijklmnop");
    }

    #[test]
    fn acquire_secret_trims_surrounding_whitespace() {
        // Surrounding whitespace is trimmed before the length
        // check, so a 14-char secret with a leading + trailing
        // space (16 chars raw, 14 after trim) is rejected. Pin
        // this so a future refactor that bypasses trim() and
        // counts raw bytes is caught.
        let err = acquire_secret(Some("  twelve-chars  "), false).unwrap_err();
        assert!(
            format!("{err}").contains("at least 16"),
            "expected length-floor error after trim, got: {err}"
        );
    }
}
