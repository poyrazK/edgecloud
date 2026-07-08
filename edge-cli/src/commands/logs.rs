//! `edge logs <app>` — read recent log entries for one of your apps.
//!
//! Issue #77. The control-plane ingest side (worker → Postgres) shipped
//! in PR #98; this command is the read side (Postgres → tenant).
//!
//! Behavior:
//!
//! * Single-shot: one request, print results, exit.
//! * `--follow`:  poll every 2s; advance `since` to the latest entry's
//!   ts on each tick; client-side dedup by id (so the boundary row
//!   from the previous tick is not reprinted). SIGINT (Ctrl-C) exits
//!   cleanly. Bounded at 30 minutes; the user is unlikely to want a
//!   longer follow than that interactively.
//! * TTY mode: pretty line per entry, ANSI-colored level.
//! * Pipe mode: one JSON object per line (jq-friendly).
//! * One-shot `crashed` hint: if a worker has reported the app as
//!   `crashed` (issue #77 §5) within the last 5 minutes, we print a
//!   `output::hint` line pointing at `edge rollback <app>`. The
//!   hint is shown once at startup, not on every `--follow` tick —
//!   the status endpoint is not polled during follow, so a
//!   deployment that crashes mid-follow will not trigger the hint
//!   until the user restarts the command.

use anyhow::{Context, Result};
use std::io::IsTerminal;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::state_io::load_state_optional;
use crate::api::{ApiClient, LogEntry};
use crate::config::EdgeToml;
use crate::output;
use crate::state::State;

/// How long `--follow` polls before exiting on its own. SIGINT still
/// wins; this is the upper bound so a forgotten `edge logs -f` does
/// not pin a control plane worker forever.
///
/// Tests override this via `EDGE_LOGS_FOLLOW_TIMEOUT_SECS` to keep
/// the wiremock run time bounded (the default is 30 minutes).
const FOLLOW_TIMEOUT: Duration = Duration::from_secs(30 * 60);

/// Read the follow-timeout override from the env, or fall back to
/// the 30-minute default. Returns the same value on every call
/// within a single `run_follow` invocation (we snapshot once at the
/// top) so a parallel `env` change mid-loop doesn't break the
/// invariant.
fn follow_timeout() -> Duration {
    match std::env::var("EDGE_LOGS_FOLLOW_TIMEOUT_SECS") {
        Ok(s) => s
            .parse::<u64>()
            .ok()
            .filter(|&secs| secs > 0)
            .map(Duration::from_secs)
            .unwrap_or(FOLLOW_TIMEOUT),
        Err(_) => FOLLOW_TIMEOUT,
    }
}

/// Sleep between follow-ticks. 2s is a reasonable default for a
/// log-tailing UX: matches `docker logs -f` and the human response
/// time of "watch a screen for new lines". Shorter values just
/// amplify DB load; longer values feel laggy.
const FOLLOW_INTERVAL: Duration = Duration::from_secs(2);

/// Granularity at which the follow loop checks the SIGINT flag
/// during its idle sleep. 100ms is the right tradeoff between
/// poll overhead (negligible — it's a SeqCst load on a shared
/// cache line) and Ctrl-C latency (worst case 100ms instead of
/// `FOLLOW_INTERVAL`).
const FOLLOW_POLL_GRANULARITY: Duration = Duration::from_millis(100);

/// Sleep for `total`, polling the SIGINT flag every
/// `FOLLOW_POLL_GRANULARITY`. Returns early when `stop` is set so
/// Ctrl-C exits the follow loop within 100ms instead of up to
/// `FOLLOW_INTERVAL` (2s).
fn interruptible_sleep(total: Duration, stop: &AtomicBool) {
    let start = Instant::now();
    while start.elapsed() < total {
        if stop.load(Ordering::SeqCst) {
            return;
        }
        let remaining = total.saturating_sub(start.elapsed());
        std::thread::sleep(remaining.min(FOLLOW_POLL_GRANULARITY));
    }
}

/// `edge logs <app>`.
///
/// `app` may be empty; if so, we fall back to `.edge/state.json` and
/// otherwise error — same precedence as `edge rollback`.
///
/// `since` is a *relative* duration (e.g. 5m, 30s, 1h) that the CLI
/// converts into an absolute RFC3339 cutoff before calling the
/// control plane. The conversion happens here so the wire format
/// stays an absolute timestamp (which is what `--follow` advances
/// incrementally).
///
/// `follow` enables the polling loop. The initial request is made
/// with the requested `since`; on every subsequent tick we advance
/// the cutoff to the timestamp of the newest entry we've printed,
/// and dedupe by id.
#[cfg(feature = "network")]
pub fn run(
    path: &Path,
    app: &str,
    since: Duration,
    level: Option<&str>,
    follow: bool,
    limit: u32,
    offset: Option<u32>,
) -> Result<()> {
    let state = load_state_optional(path)?;
    let app_name = resolve_app_name(app, state.as_ref())?;

    let edge_toml = EdgeToml::from_path(path)
        .with_context(|| "edge logs requires edge.toml with [deployment] api = \"<url>\"")?;
    let base_url = edge_toml.api_url("https://api.edgecloud.dev");

    let client = ApiClient::new(base_url)?;

    let is_tty = std::io::stdout().is_terminal();
    let since_rfc = rfc3339_in_past(since);

    // Issue #77 §5: if a worker has reported the app as `crashed`
    // within the last 5 minutes, prepend a hint pointing at
    // `edge rollback <app>`. Done BEFORE the first `logs().list()`
    // call so the hint appears above the user's first batch. In
    // `--follow` mode the status endpoint is not polled again —
    // this is a one-shot startup hint, by design.
    maybe_print_crashed_hint(&client, &app_name);

    if follow {
        run_follow(&client, &app_name, &since_rfc, level, limit, is_tty)
    } else {
        let resp = client
            .logs()
            .list(&app_name, Some(&since_rfc), level, Some(limit), offset)?;
        for entry in &resp.items {
            print_entry(entry, is_tty);
        }
        maybe_print_next_page_hint(resp.next_offset);
        Ok(())
    }
}

#[cfg(not(feature = "network"))]
pub fn run(
    _path: &Path,
    _app: &str,
    _since: Duration,
    _level: Option<&str>,
    _follow: bool,
    _limit: u32,
    _offset: Option<u32>,
) -> Result<()> {
    anyhow::bail!("logs requires network support; rebuild with --features network")
}

/// The follow loop. Pulls an initial batch, prints it, then polls
/// every [`FOLLOW_INTERVAL`] advancing the cutoff to the latest
/// entry's ts. Stops on SIGINT, after [`FOLLOW_TIMEOUT`], or when
/// the user hits the upper bound.
#[cfg(feature = "network")]
fn run_follow(
    client: &ApiClient,
    app_name: &str,
    since_rfc: &str,
    level: Option<&str>,
    limit: u32,
    is_tty: bool,
) -> Result<()> {
    // ctrlc handler: set a shared flag the polling loop polls.
    // Using an AtomicBool + busy-check keeps the handler trivial and
    // doesn't require an async runtime inside the handler closure.
    let stop = Arc::new(AtomicBool::new(false));
    let stop_for_handler = stop.clone();
    ctrlc::set_handler(move || {
        stop_for_handler.store(true, Ordering::SeqCst);
    })
    .context("installing SIGINT handler for --follow")?;

    let deadline = Instant::now() + follow_timeout();
    // ids we've already printed — used to dedupe the boundary row
    // that the server returns on every poll. The server's filter is
    // `ts >= cutoff` and our cutoff is set to the last entry's `ts`
    // verbatim (we do NOT add +1ms), so the same boundary row comes
    // back on every tick. Without dedup, the boundary row would
    // print repeatedly. Dedupe by id is correct because (a) id is
    // DB-assigned and unique per row, (b) we want to print rows
    // that share a `ts` but differ in id (a worker can emit two
    // rows in the same microsecond).
    let mut printed_ids: std::collections::HashSet<i64> = std::collections::HashSet::new();

    // Initial tick uses the user-supplied since; later ticks use
    // the newest entry's ts. The server returns newest-first, so
    // `resp.items.first().ts` is the largest ts in the batch (and
    // therefore the right new cutoff).
    let mut since = since_rfc.to_string();

    loop {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        if Instant::now() >= deadline {
            if is_tty {
                output::hint("follow timeout (30m) reached; exiting");
            }
            break;
        }

        let resp = client
            .logs()
            .list(app_name, Some(&since), level, Some(limit), None)?;
        if resp.items.is_empty() {
            // No new rows. Sleep interruptibly so SIGINT exits
            // promptly (up to FOLLOW_POLL_GRANULARITY instead of
            // FOLLOW_INTERVAL).
            interruptible_sleep(FOLLOW_INTERVAL, &stop);
            continue;
        }
        for entry in &resp.items {
            if !printed_ids.contains(&entry.id) {
                print_entry(entry, is_tty);
                printed_ids.insert(entry.id);
            }
        }
        if let Some(first) = resp.items.first() {
            since = first.ts.clone();
        }
        interruptible_sleep(FOLLOW_INTERVAL, &stop);
    }
    Ok(())
}

/// Print one entry in either TTY (pretty) or pipe (JSON) mode.
fn print_entry(entry: &LogEntry, is_tty: bool) {
    if is_tty {
        println!("{}", format_entry_tty(entry));
    } else {
        // One JSON object per line. Serialization failure is
        // catastrophic (we lose an entry) but the entry is already
        // in the server's history, so the user can re-query.
        match serde_json::to_string(entry) {
            Ok(s) => println!("{s}"),
            Err(e) => eprintln!("edge logs: failed to serialize entry: {e}"),
        }
    }
}

fn format_entry_tty(entry: &LogEntry) -> String {
    // Layout: [ts] LEVEL region deployment_id: message
    // We intentionally truncate neither field; tenants have a
    // terminal and the message is the whole point. Wrapping is the
    // terminal's job.
    format!(
        "[{ts}] {level:>5} {region} {deployment_id}: {message}",
        ts = entry.ts,
        level = colorize_level(&entry.level),
        region = entry.region,
        deployment_id = entry.deployment_id,
        message = entry.message,
    )
}

fn colorize_level(level: &str) -> String {
    use console::style;
    match level {
        "trace" => style(level).dim().to_string(),
        "debug" => style(level).blue().to_string(),
        "info" => style(level).green().to_string(),
        "warn" => style(level).yellow().to_string(),
        "error" => style(level).red().bold().to_string(),
        // Unknown level: pass through uncolored rather than panic.
        // The server rejects unknown levels at the boundary; this
        // branch only triggers for hand-crafted rows in tests.
        _ => level.to_string(),
    }
}

/// Build an RFC3339 timestamp `now - d` for the `since` parameter.
/// We use the local clock here because the *client* is the authority
/// on what "since 5 minutes ago" means in their head. The server
/// still applies its own `NOW() - make_interval(secs)` arithmetic
/// against the DB clock, so the two clocks are not coupled
/// (clock-skew defense: see LogEntryRepository.DeleteOlderThanBatched).
fn rfc3339_in_past(d: Duration) -> String {
    // Use a simple SystemTime arithmetic to avoid pulling chrono.
    // RFC3339 formatting without subsecond precision is fine — the
    // server's seconds-precision interval math will absorb the
    // subsecond remainder either way.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let past_secs = now.saturating_sub(d).as_secs();
    // Seconds since epoch → UTC date/time. The math below is the
    // civil-from-days algorithm (Howard Hinnant) so we don't have to
    // pull chrono or time as a dependency for this one conversion.
    format_utc_rfc3339(past_secs)
}

fn format_utc_rfc3339(secs: u64) -> String {
    // Days since 1970-01-01 (Unix epoch).
    let days = (secs / 86_400) as i64;
    let secs_of_day = (secs % 86_400) as u32;
    let hh = secs_of_day / 3600;
    let mm = (secs_of_day % 3600) / 60;
    let ss = secs_of_day % 60;

    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    let year = if m <= 2 { y + 1 } else { y };

    format!(
        "{year:04}-{month:02}-{day:02}T{h:02}:{mm:02}:{ss:02}Z",
        year = year,
        month = m,
        day = d,
        h = hh,
        mm = mm,
        ss = ss,
    )
}

/// Resolve the app name to use for logs.
///
/// Delegates to `state_io::resolve_app_name` so the precedence rule
/// stays in one place.
fn resolve_app_name(app: &str, state: Option<&State>) -> Result<String> {
    super::state_io::resolve_app_name("edge logs", app, state)
}

/// One-shot `crashed` hint for issue #77 §5. Called at the top of
/// `run` for both the single-shot and `--follow` paths. Silently
/// swallows every error: a missing or broken `/status` endpoint
/// must never fail the `edge logs` command — the log query is the
/// primary purpose, the hint is a courtesy.
fn maybe_print_crashed_hint(client: &ApiClient, app_name: &str) {
    let Ok(status) = client.get_app_status(app_name) else {
        return;
    };
    if status.status == "crashed" && is_fresh(status.last_heartbeat.as_deref()) {
        output::hint(&format!(
            "deployment is crashed — run `edge rollback {app_name}` to roll back"
        ));
    }
}

/// Threshold for "the worker is still alive and just reported this".
/// 5 minutes = heartbeat interval (30s, per whitepaper §5.6) × 10 —
/// generous enough to tolerate a few missed heartbeats, tight enough
/// that a dead worker stops emitting the hint within a few minutes.
/// A stale crash is a deployment in an unknown state, not an
/// actively-crashed one; we suppress the hint to avoid misleading
/// the user.
const STALE_HEARTBEAT: Duration = Duration::from_secs(5 * 60);

/// Parse an RFC3339 timestamp and return true iff it is within
/// `STALE_HEARTBEAT` of now. Returns false on any parse failure
/// (missing field, malformed timestamp, clock skew) so a broken
/// timestamp never produces a misleading hint.
fn is_fresh(rfc3339: Option<&str>) -> bool {
    let Some(s) = rfc3339 else {
        return false;
    };
    let Ok(parsed) = chrono_parse_rfc3339(s) else {
        return false;
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let age = now - parsed;
    age >= 0 && age <= STALE_HEARTBEAT.as_secs() as i64
}

/// Minimal RFC3339 parser for the subset the server emits:
/// `YYYY-MM-DDTHH:MM:SSZ` (UTC, second precision, trailing `Z`).
/// We parse it without pulling chrono because the CLI already
/// formats RFC3339 timestamps with `format_utc_rfc3339` (seconds
/// precision, trailing `Z`) — so any timestamp the server sends
/// in response to `GET /status` is guaranteed to match this
/// format. A more permissive parser (offsets, fractional seconds)
/// would mask upstream changes; a stricter one would surface them
/// as parse failures that the helper correctly swallows.
fn chrono_parse_rfc3339(s: &str) -> Result<i64, ()> {
    // Expected layout: "YYYY-MM-DDTHH:MM:SSZ" (17 bytes after
    // subtracting the date and `Z`; total 20 bytes).
    if s.len() != 20 {
        return Err(());
    }
    let bytes = s.as_bytes();
    if bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes[10] != b'T'
        || bytes[13] != b':'
        || bytes[16] != b':'
        || bytes[19] != b'Z'
    {
        return Err(());
    }
    let parse_u32 = |a: usize, c: usize| -> Result<u32, ()> {
        let chunk = &s[a..c];
        if !chunk.bytes().all(|b| b.is_ascii_digit()) {
            return Err(());
        }
        // We've already validated length and digits; unwrap is safe.
        chunk.parse::<u32>().map_err(|_| ())
    };
    let year = parse_u32(0, 4)?;
    let month = parse_u32(5, 7)?;
    let day = parse_u32(8, 10)?;
    let hour = parse_u32(11, 13)?;
    let minute = parse_u32(14, 16)?;
    let second = parse_u32(17, 19)?;

    // Days-from-civil (Howard Hinnant). Adapted for the
    // March-as-3 pivot to share math with `format_utc_rfc3339`.
    let (m, y_adj) = if month <= 2 {
        (month + 9, year - 1)
    } else {
        (month - 3, year)
    };
    let era = (y_adj as i64).div_euclid(400);
    let yoe = (y_adj as i64 - era * 400) as u64; // [0, 399]
    let doy = (153 * (m as u64) + 2) / 5 + (day as u64) - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    let days = era * 146_097 + (doe as i64) - 719_468;

    let secs_of_day = (hour as i64) * 3600 + (minute as i64) * 60 + (second as i64);
    Ok(days * 86_400 + secs_of_day)
}

/// Print a hint pointing at `--offset` when the response includes a
/// `next_offset` field, indicating more results exist beyond this page.
fn maybe_print_next_page_hint(next_offset: Option<u32>) {
    if let Some(no) = next_offset {
        output::hint(&format!(
            "More entries available — run with `--offset {no}` for the next page"
        ));
    }
}

// ---------------------------------------------------------------------------
// Tests for the pure helpers. Integration tests for the wire path live in
// tests/logs.rs.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::LogEntry;

    fn make_entry(id: i64, level: &str, msg: &str) -> LogEntry {
        LogEntry {
            id,
            tenant_id: "t_test".into(),
            deployment_id: "d_xyz".into(),
            app_name: "myapp".into(),
            worker_id: "w_us-east-1_h01".into(),
            region: "us-east-1".into(),
            level: level.into(),
            message: msg.into(),
            labels: serde_json::json!({}),
            ts: "2026-06-24T12:00:00Z".into(),
        }
    }

    // resolve_app_name is exercised in commands/state_io.rs::tests;
    // this placeholder keeps `cargo test` happy with at least one
    // test per commands/* module file (clippy's -D warnings turns
    // empty `#[cfg(test)] mod tests` into a build failure).
    #[test]
    fn placeholder_for_centralized_resolve_tests() {
        // Intentionally empty: real coverage lives in state_io.
    }

    #[test]
    fn format_utc_rfc3339_known_value() {
        // 1782450896 seconds since epoch = 2026-06-26T05:14:56Z.
        // Verified externally via `date -u -r 1782450896`.
        // Pinning a real conversion (not the epoch) catches
        // year/month/day arithmetic regressions in
        // civil_from_days that the epoch test would miss.
        assert_eq!(format_utc_rfc3339(1_782_450_896), "2026-06-26T05:14:56Z");
    }

    #[test]
    fn format_utc_rfc3339_unix_epoch() {
        assert_eq!(format_utc_rfc3339(0), "1970-01-01T00:00:00Z");
    }

    #[test]
    fn format_entry_tty_includes_all_fields() {
        let entry = make_entry(1, "info", "hello world");
        let line = format_entry_tty(&entry);
        assert!(line.contains("2026-06-24T12:00:00Z"), "missing ts: {line}");
        assert!(line.contains("info"), "missing level: {line}");
        assert!(line.contains("us-east-1"), "missing region: {line}");
        assert!(line.contains("d_xyz"), "missing deployment id: {line}");
        assert!(line.contains("hello world"), "missing message: {line}");
    }

    #[test]
    fn format_entry_tty_does_not_panic_on_unknown_level() {
        // The server rejects unknown levels, but the formatter must
        // not panic if a hand-crafted row (e.g. in a test) leaks
        // through. We just assert it produces *something*.
        let entry = make_entry(1, "critical", "x");
        let line = format_entry_tty(&entry);
        assert!(line.contains("critical"));
    }

    #[test]
    fn colorize_level_passes_unknown_through() {
        // No panic, no transformation. The user will see "critical"
        // uncolored rather than a panic in their terminal.
        let s = colorize_level("critical");
        assert_eq!(s, "critical");
    }

    #[test]
    fn json_serialization_round_trip() {
        // The pipe-mode serialization must not lose fields; otherwise
        // a tenant piping to `jq` would silently drop columns.
        let entry = make_entry(42, "warn", "rate limit approaching");
        let s = serde_json::to_string(&entry).unwrap();
        for key in [
            "id",
            "tenant_id",
            "deployment_id",
            "app_name",
            "worker_id",
            "region",
            "level",
            "message",
            "labels",
            "ts",
        ] {
            assert!(s.contains(key), "json missing key {key}: {s}");
        }
    }
}
