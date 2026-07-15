//! Proactive JWT refresh loop (issue #504).
//!
//! Runs as a `tokio::spawn`'d task in `crate::main`. Each tick:
//!
//! 1. Read `signer.snapshot()` under the read lock.
//! 2. If `now > expires_at - REFRESH_LEAD`, run a refresh.
//! 3. The refresh delegates to a `RefreshSource` — `Static` for the
//!    legacy `WORKER_JWT_SECRET` mode and `Enrolled` for the
//!    post-#430 bootstrap enrollment path. Both return a
//!    `RefreshOutcome { kid, secret }` that we feed back into
//!    `signer.set_secret`.
//! 4. On failure: `compute_backoff_ms(attempt, base, cap)` with
//!    ±25% jitter (mirror of `edge-worker/src/backoff.rs:84-90`)
//!    delays the next attempt; bump
//!    `edge_worker_jwt_refresh_total{outcome="err"}`; **keep the
//!    previous snapshot serving requests** so a transient refresh
//!    failure doesn't blackhole outbound calls.
//!
//! Shutdown: the task awaits `CancellationToken::cancelled()` and
//! exits cleanly. `main` then awaits the task with a 2-second grace
//! (mirrors the `metrics_server` shutdown pattern).
//!
//! The single-flight gate for the refresh itself is a
//! `tokio::sync::Mutex<()>` inside `RefreshSource::refresh`. If
//! another task (or the reactive 401 helper, Commit 5) is already
//! inside a refresh, this tick skips; the next tick will pick it up.
//! This avoids a thundering herd if both paths observe staleness
//! within microseconds of each other.

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

use crate::auth::WorkerJwtSigner;
use crate::backoff::compute_backoff_ms;
use crate::metrics::WorkerMetrics;

/// Source of refresh truth. `Static` (the legacy `WORKER_JWT_SECRET`
/// path) re-signs with the same secret on every tick — there's
/// nothing to refresh. `Enrolled` re-runs the bootstrap handshake to
/// re-derive the per-worker HS256 secret.
///
/// Both branches live behind the same trait object so the loop is
/// agnostic to which mode the worker is in.
#[derive(Clone)]
pub enum RefreshSource {
    /// Legacy mode: a static `WORKER_JWT_SECRET`. The "refresh" is
    /// a no-op; the loop observes whether the cached token is past
    /// `expires_at - REFRESH_LEAD` and re-signs (no network). Kept
    /// on the enum so the loop branches once and stops special-casing
    /// at call sites.
    Static,
    /// Bootstrap enrollment mode (issue #430, primary path post-#504).
    /// `refresh` runs the full handshake and returns the new
    /// `(kid, secret)` to install via `signer.set_secret`.
    Enrolled(Arc<dyn EnrollmentRefresher>),
}

impl RefreshSource {
    /// Run a refresh. Returns `RefreshOutcome` on success or an
    /// `anyhow::Error` describing the failure. Implementations are
    /// expected to internally serialize concurrent callers (single-
    /// flight gate) so two ticks in rapid succession collapse to
    /// one handshake.
    async fn refresh(&self) -> anyhow::Result<RefreshOutcome> {
        match self {
            RefreshSource::Static => Ok(RefreshOutcome {
                kid: None, // kid stays at whatever the signer already has
                secret: None,
            }),
            RefreshSource::Enrolled(r) => {
                let derived = r.refresh_once().await?;
                Ok(RefreshOutcome {
                    kid: Some(derived.kid),
                    secret: Some(derived.secret),
                })
            }
        }
    }
}

/// Trait for the per-worker bootstrap re-enrollment. WireMock-backed
/// in tests; the production implementation wraps `BootstrapClient`.
#[async_trait::async_trait]
pub trait EnrollmentRefresher: Send + Sync {
    async fn refresh_once(&self) -> anyhow::Result<crate::bootstrap::DerivedSecret>;
}

/// Result of a successful refresh. `kid` and `secret` are `None`
/// for the static secret mode (the signer's existing fields stay).
pub struct RefreshOutcome {
    pub kid: Option<String>,
    pub secret: Option<Vec<u8>>,
}

/// Spawn the refresh loop. Returns a `JoinHandle` so `main` can
/// await it during graceful shutdown (with a 2-second grace).
///
/// Cancellation: `shutdown_token.cancelled()` exits the loop on
/// the next tick boundary. The ticker uses
/// `tokio::time::interval` with `MissedTickBehavior::Skip` so a
/// slow refresh doesn't queue subsequent ticks.
///
/// Metrics: each call to `refresh()` bumps either
/// `edge_worker_jwt_refresh_total{outcome="ok"}` or
/// `…{outcome="err"}`. The gauge `edge_worker_jwt_expires_at_seconds`
/// is updated on every successful refresh (and once at startup).
pub fn spawn_jwt_refresh_loop(
    signer: Arc<WorkerJwtSigner>,
    source: RefreshSource,
    tick: Duration,
    refresh_lead: Duration,
    shutdown_token: CancellationToken,
    metrics: Arc<WorkerMetrics>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(tick);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Skip the first immediate tick; let the worker finish
        // initial enrollment before we start checking.
        ticker.tick().await;

        let mut attempt: u32 = 0;
        loop {
            tokio::select! {
                _ = shutdown_token.cancelled() => {
                    tracing::info!("jwt refresh loop cancelled by shutdown");
                    return;
                }
                _ = ticker.tick() => {
                    let now = Instant::now();
                    let snap = signer.snapshot();
                    let deadline = snap.expires_at.checked_sub(refresh_lead).unwrap_or(snap.expires_at);
                    if now < deadline {
                        // Not stale yet; tick again.
                        attempt = 0;
                        continue;
                    }

                    tracing::info!(
                        expires_at = ?snap.expires_at,
                        "jwt token approaching expiry; running proactive refresh"
                    );
                    match source.refresh().await {
                        Ok(outcome) => {
                            attempt = 0;
                            // Install the new secret + kid under one
                            // write lock; generation bumps
                            // automatically (WorkerJwtSigner invariant).
                            if outcome.kid.is_some() || outcome.secret.is_some() {
                                signer.set_secret(
                                    outcome.secret.unwrap_or_default(),
                                    outcome.kid,
                                );
                            }
                            metrics.refresh_outcome_inc("ok");
                            metrics.set_jwt_expires_at(signer.snapshot().expires_at);
                            tracing::info!(
                                kid = ?signer.snapshot().kid,
                                "jwt refresh succeeded"
                            );
                        }
                        Err(err) => {
                            attempt = attempt.saturating_add(1);
                            let delay = compute_backoff_ms(attempt, 5_000, 300_000);
                            metrics.refresh_outcome_inc("err");
                            tracing::warn!(
                                attempt,
                                backoff_ms = delay,
                                error = %err,
                                "jwt refresh failed; previous token remains valid"
                            );
                            // Sleep the backoff BEFORE the next
                            // tick — keeps the loop from spinning
                            // on a persistent CP outage.
                            tokio::time::sleep(Duration::from_millis(delay)).await;
                        }
                    }
                }
            }
        }
    })
}
