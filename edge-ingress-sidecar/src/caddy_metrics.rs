//! Caddy admin `/metrics` scraper (issue #665, PR B).
//!
//! Reads the `caddy_http_requests_total` Prometheus counter from
//! Caddy's admin API on every tick, diffs against the previous tick's
//! value, and returns the per-second RPS delta. The publisher
//! ([`crate::nats_pub`]) takes that delta and emits a [`DeltaMsg`] to
//! JetStream.
//!
//! ## Why a separate module
//!
//! Caddy's `/metrics` is the only ingress-side data source for the
//! sidecar; isolating the parse + diff lets us unit-test the
//! "missed second decays gracefully" invariant without standing up a
//! real Caddy process.
//!
//! ## Why Prometheus text format and not JSON
//!
//! Caddy exposes BOTH a JSON admin API and a Prometheus text endpoint.
//! The JSON API is keyed by listener / route, which would force us
//! to enumerate every route and sum. The Prometheus endpoint exposes
//! `caddy_http_requests_total{...}` as a flat counter — a single
//! GET + line scan returns the platform total. PR E's verification
//! test asserts the latter sums the same way the consumer expects.
//!
//! ## Wire shape
//!
//! [`DeltaMsg`] is the per-tick payload:
//! ```json
//! {"replica_id": "<pod>", "ts_unix_ms": 1700000000000, "rps": 1234}
//! ```
//!
//! `ts_unix_ms` is stamped at scrape time (NOT at publish time) so the
//! consumer's windowing uses the moment the measurement was taken, not
//! the moment the message landed in JetStream. Skew between scrape and
//! publish is bounded by the sidecar's tick cadence (1s) and is
//! negligible relative to the 1s window.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

/// Per-tick payload published by `nats_pub::spawn_publisher` and
/// consumed by `aggregate::Aggregator`.
///
/// `rps` is the diff between this scrape's counter and the previous
/// scrape's counter, normalized to "per second". Saturating math
/// protects against the brief window where Caddy restarts and the
/// counter resets to 0; the saturation would otherwise underflow to
/// u32::MAX and poison the platform total.
///
/// `scraped_at_unix_ms` is stamped at scrape time so the consumer can
/// reason about freshness independent of arrival time. Without this
/// stamp, a `LastPerSubject` replay that arrives 60s late would be
/// treated as a fresh 1-second measurement and poison the sliding
/// window (the platform total would suddenly include a stale value).
/// See `nats_sub::spawn_consumer` for the staleness-rejection
/// invariant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaMsg {
    pub replica_id: String,
    pub rps: u32,
    pub scraped_at_unix_ms: u64,
}

impl DeltaMsg {
    /// Render the wire payload. Kept as a `serde_json::Value` rather
    /// than a typed struct so the consumer's `DeltaMsgWire` (which
    /// must tolerate missing fields for forward-compat) can match
    /// without coupling to the publisher's full schema.
    pub fn to_wire(&self) -> serde_json::Value {
        serde_json::json!({
            "replica_id": self.replica_id,
            "scraped_at_unix_ms": self.scraped_at_unix_ms,
            "rps": self.rps,
        })
    }
}

/// Unix milliseconds since epoch. Wrapped so tests can mock it
/// (production callers use the `SystemTime` form below).
fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Diff between two scrapes of `caddy_http_requests_total`. The
/// scrape function below returns this struct; the publisher converts
/// it to a [`DeltaMsg`].
///
/// `current` and `previous` are the cumulative counter values; the
/// `rps` field is the per-tick delta. `current < previous` indicates
/// Caddy restarted (counter reset to 0) — we treat that as 0 RPS for
/// this tick rather than as an underflow, since the next tick will
/// re-establish a baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScrapeDiff {
    pub current: u64,
    pub previous: u64,
    pub rps: u32,
}

/// Parse the `caddy_http_requests_total` counter from a Prometheus
/// text-format body. Returns the sum of every series (Caddy labels
/// by `code` / `handler` / `listener` etc.; we don't care about the
/// labels — only the total).
///
/// The format is:
/// ```text
/// # HELP caddy_http_requests_total ...
/// # TYPE caddy_http_requests_total counter
/// caddy_http_requests_total{code="200",...} 1234
/// ```
///
/// **Prefix-match gotcha:** the metric name MUST be followed by `{`
/// (labeled form), whitespace (unlabeled form), or end-of-line. A
/// bare `strip_prefix("caddy_http_requests_total")` would also match
/// `caddy_http_requests_total_created` (the timestamp helper metric
/// Prom auto-emits for every counter), which would inflate the total
/// by one entry per scrape. We require a delimiter after the name.
///
/// Returns `None` if the counter is absent (very early scrape, before
/// Caddy has served any requests — shouldn't happen in steady state
/// but the caller's `Option` handles it gracefully).
pub fn parse_caddy_counter(body: &str) -> Option<u64> {
    const NAME: &str = "caddy_http_requests_total";
    let mut total: u64 = 0;
    let mut found = false;
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // Match the metric name + a delimiter that distinguishes
        // the real counter from derivatives like `*_created` /
        // `*_sum` / `*_count`.
        let Some(rest) = line.strip_prefix(NAME) else {
            continue;
        };
        let next = rest.chars().next();
        if !matches!(next, Some('{') | Some(' ') | Some('\t') | None) {
            // E.g. `caddy_http_requests_total_created` — skip.
            continue;
        }
        // Skip the label block (anything up to the first whitespace).
        let value_str = rest
            .split_whitespace()
            .nth(if rest.starts_with('{') { 1 } else { 0 })
            .unwrap_or("");
        match value_str.parse::<f64>() {
            Ok(v) if v.is_finite() && v >= 0.0 => {
                total = total.saturating_add(v as u64);
                found = true;
            }
            Ok(_) => {
                // NaN, +Inf, -Inf, or negative — skip. Prom counters
                // shouldn't ever produce these in steady state; we
                // filter defensively so a buggy exporter doesn't
                // poison the platform total.
                warn!(
                    value = value_str,
                    "parse_caddy_counter: non-finite or negative value, skipping"
                );
            }
            Err(_) => {
                // Malformed numeric — skip without poisoning other
                // lines (the rest of the series may still be valid).
            }
        }
    }
    if found {
        Some(total)
    } else {
        None
    }
}

/// Compute the RPS delta between two scrapes.
///
/// `current < previous` ⇒ Caddy restarted; return 0 RPS rather than
/// underflowing (the next tick will pick up a fresh baseline).
pub fn diff_scrapes(current: Option<u64>, previous: Option<u64>) -> ScrapeDiff {
    match (current, previous) {
        (Some(c), Some(p)) if c >= p => ScrapeDiff {
            current: c,
            previous: p,
            rps: (c - p) as u32,
        },
        _ => ScrapeDiff {
            current: current.unwrap_or(0),
            previous: previous.unwrap_or(0),
            rps: 0,
        },
    }
}

/// Spawn the scraper task. It ticks at 1 Hz, fetches Caddy's
/// `/metrics`, computes the diff, and pushes a [`DeltaMsg`] into the
/// channel the publisher consumes.
///
/// The `client` is shared with the publisher so a single connection
/// pool serves both the scrape and any future HTTP traffic; tests
/// inject a custom client (typically a wiremock-backed one).
pub fn spawn_scraper(
    client: reqwest::Client,
    caddy_admin_url: String,
    replica_id: String,
    tx: tokio::sync::mpsc::Sender<DeltaMsg>,
    shutdown: tokio_util::sync::CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut previous: Option<u64> = None;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    tracing::info!("scraper: shutdown received");
                    return;
                }
                _ = ticker.tick() => {
                    let url = format!("{}/metrics", caddy_admin_url.trim_end_matches('/'));
                    let body = match client.get(&url).send().await {
                        Ok(r) => match r.text().await {
                            Ok(b) => b,
                            Err(e) => {
                                warn!(err = %e, "scraper: caddy /metrics body read failed");
                                continue;
                            }
                        },
                        Err(e) => {
                            warn!(err = %e, "scraper: caddy /metrics fetch failed");
                            continue;
                        }
                    };
                    let current = parse_caddy_counter(&body);
                    let diff = diff_scrapes(current, previous);
                    previous = current;
                    debug!(
                        current = diff.current,
                        previous = diff.previous,
                        rps = diff.rps,
                        "scraper: tick"
                    );
                    let msg = DeltaMsg {
                        replica_id: replica_id.clone(),
                        rps: diff.rps,
                        scraped_at_unix_ms: unix_ms_now(),
                    };
                    if tx.send(msg).await.is_err() {
                        warn!("scraper: aggregator dropped the channel; bailing");
                        return;
                    }
                }
            }
        }
    })
}

/// Convenience wrapper for unit-test paths that want to fetch once
/// outside the spawn loop. Returns the parsed body as a String so
/// callers can run assertions against the raw text.
#[allow(dead_code)] // reserved for integration tests + future ops tooling
pub async fn fetch_caddy_metrics(
    client: &reqwest::Client,
    caddy_admin_url: &str,
) -> anyhow::Result<String> {
    let url = format!("{}/metrics", caddy_admin_url.trim_end_matches('/'));
    let resp = client
        .get(&url)
        .send()
        .await
        .context("caddy /metrics fetch")?;
    resp.text().await.context("caddy /metrics body")
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── parse_caddy_counter tests ──────────────────────────────────

    #[test]
    fn parse_empty_body_returns_none() {
        assert_eq!(parse_caddy_counter(""), None);
    }

    #[test]
    fn parse_body_without_target_returns_none() {
        let body = "# HELP something_else_total ...\n# TYPE something_else_total counter\nsomething_else_total 100\n";
        assert_eq!(parse_caddy_counter(body), None);
    }

    #[test]
    fn parse_single_series() {
        let body = "# HELP caddy_http_requests_total ...\n# TYPE caddy_http_requests_total counter\ncaddy_http_requests_total{code=\"200\"} 1234\n";
        assert_eq!(parse_caddy_counter(body), Some(1234));
    }

    #[test]
    fn parse_sums_multiple_labeled_series() {
        // Caddy labels by `code`, `handler`, etc. — sum them all.
        let body = "\
# HELP caddy_http_requests_total ...
# TYPE caddy_http_requests_total counter
caddy_http_requests_total{code=\"200\",handler=\"static\"} 100
caddy_http_requests_total{code=\"404\",handler=\"static\"} 5
caddy_http_requests_total{code=\"500\",handler=\"reverse_proxy\"} 2
";
        assert_eq!(parse_caddy_counter(body), Some(107));
    }

    #[test]
    fn parse_handles_scientific_notation() {
        let body = "caddy_http_requests_total 1.5e3\n";
        assert_eq!(parse_caddy_counter(body), Some(1500));
    }

    #[test]
    fn parse_ignores_unparseable_values() {
        // Malformed line should not poison the parse — skip it and
        // continue with subsequent valid lines.
        let body = "\
caddy_http_requests_total 100
caddy_http_requests_total not_a_number
caddy_http_requests_total 50
";
        assert_eq!(parse_caddy_counter(body), Some(150));
    }

    #[test]
    fn parse_rejects_caddy_http_requests_total_created() {
        // The `_created` metric is the timestamp Prom auto-emits for
        // every counter; without the delimiter guard it would match
        // `strip_prefix("caddy_http_requests_total")` and inflate the
        // total by one entry per scrape. Pin the rejection here.
        let body = "# HELP caddy_http_requests_total_created ...\n# TYPE caddy_http_requests_total_created counter\ncaddy_http_requests_total_created 1.7e9\ncaddy_http_requests_total{code=\"200\"} 100\n";
        assert_eq!(parse_caddy_counter(body), Some(100));
    }

    #[test]
    fn parse_rejects_nan_and_infinity() {
        // Defensive: a buggy exporter emitting NaN / ±Inf / negatives
        // would otherwise poison the total (NaN→0u64, +Inf→u64::MAX,
        // -Inf→saturating_neg panics).
        let body = "\
caddy_http_requests_total NaN
caddy_http_requests_total +Inf
caddy_http_requests_total -Inf
caddy_http_requests_total -5
caddy_http_requests_total 7
";
        assert_eq!(parse_caddy_counter(body), Some(7));
    }

    #[test]
    fn parse_handles_unlabeled_form_with_whitespace() {
        // Unlabeled Prometheus counters have just `name value`.
        // Pin that the delimiter guard accepts whitespace (not just
        // `{`).
        let body = "caddy_http_requests_total 42\n";
        assert_eq!(parse_caddy_counter(body), Some(42));
    }

    // ── diff_scrapes tests ─────────────────────────────────────────

    #[test]
    fn diff_growing_counter() {
        let d = diff_scrapes(Some(100), Some(80));
        assert_eq!(d.rps, 20);
    }

    #[test]
    fn diff_zero_on_first_scrape() {
        let d = diff_scrapes(Some(100), None);
        assert_eq!(d.rps, 0, "first scrape has no baseline");
    }

    #[test]
    fn diff_zero_on_counter_reset() {
        // Caddy restarted — counter wrapped to 0. Return 0 RPS
        // rather than underflowing to u32::MAX.
        let d = diff_scrapes(Some(0), Some(999));
        assert_eq!(d.rps, 0);
    }

    #[test]
    fn diff_zero_on_fetch_failure() {
        // Both scrapes failed — no measurement. Return 0 rather than
        // fabricating a delta from None/None.
        let d = diff_scrapes(None, None);
        assert_eq!(d.rps, 0);
    }

    // ── DeltaMsg::to_wire tests ────────────────────────────────────

    #[test]
    fn delta_msg_wire_shape() {
        let m = DeltaMsg {
            replica_id: "pod-1".into(),
            rps: 5000,
            scraped_at_unix_ms: 1_700_000_000_000,
        };
        let v = m.to_wire();
        assert_eq!(v["replica_id"], "pod-1");
        assert_eq!(v["rps"], 5000);
        // scraped_at_unix_ms is the wire field that drives the
        // consumer's staleness rejection (issue #665 PR B review
        // follow-up). Pin its presence + value here so the consumer
        // doesn't silently lose the field.
        assert_eq!(v["scraped_at_unix_ms"], 1_700_000_000_000_u64);
    }
}
