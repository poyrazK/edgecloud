//! Integration test for the sidecar's full pipeline (issue #665, PR E).
//!
//! Spins up a real JetStream-enabled NATS container via testcontainers,
//! stands up three `NatsPublisher`s (one per simulated replica),
//! publishes a per-replica `DeltaMsg` to its own subject leaf, and
//! drives the publisher → push consumer → aggregator → `Snapshot`
//! path end-to-end. The test self-skips when Docker is unavailable or
//! `RUN_INTEGRATION_TESTS` is unset (see
//! `edge_test_helpers::should_skip_integration_tests`).
//!
//! **Verification target:** 3 replicas × 10k RPS at cap=10k ⇒
//! `per_replica_cap == Some(1)`. This is the load-bearing arithmetic
//! from the issue #665 plan that no unit test exercises end-to-end.
//!
//! Run with:
//!   RUN_INTEGRATION_TESTS=1 cargo nextest run -p edge-ingress-sidecar --test integration_test

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use edge_ingress_sidecar::aggregate::Aggregator;
use edge_ingress_sidecar::caddy_metrics::DeltaMsg;
use edge_ingress_sidecar::nats_pub::NatsPublisher;
use edge_ingress_sidecar::nats_sub::spawn_consumer;

use edge_test_helpers::{should_skip_integration_tests, start_nats};

/// Reconciliation grace window — how long the test waits for the
/// aggregator to observe all replicas. Production has the same 1s
/// window; tests give it 10× headroom because CI Docker cold-start
/// inflates the wire latency past the production norm (the spawn
/// task must finish get_stream + get_or_create_consumer +
/// consumer.messages() RPCs before subscribed; that chain can take
/// >1s under load).
const RECONCILE_GRACE: Duration = Duration::from_secs(10);

/// Build a `DeltaMsg` stamped at the current wall-clock time. The
/// consumer's freshness gate (`MAX_MESSAGE_AGE = 2s`) drops messages
/// older than this threshold.
fn fresh_delta(replica_id: &str, rps: u32) -> DeltaMsg {
    let scraped_at_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);
    DeltaMsg {
        replica_id: replica_id.to_string(),
        rps,
        scraped_at_unix_ms,
    }
}

#[tokio::test]
async fn three_replica_load_balances_to_per_replica_cap_one() {
    if should_skip_integration_tests() {
        eprintln!(
            "skipping: integration tests disabled (set RUN_INTEGRATION_TESTS=1 + reachable Docker)"
        );
        return;
    }

    let (_nats, nats_url) = start_nats().await;

    // Three publishers — one per simulated replica. Each one ensures
    // the stream idempotently (the first wins, the rest are no-ops;
    // mirrors the production boot where multiple sidecars race to
    // declare the same stream).
    let replicas = [
        "ingress-pod-fra-1",
        "ingress-pod-fra-2",
        "ingress-pod-fra-3",
    ];
    let mut publishers = Vec::new();
    for _rid in &replicas {
        let p = NatsPublisher::connect(&nats_url, 1)
            .await
            .expect("nats connect");
        p.ensure_stream().await.expect("ensure_stream");
        publishers.push(p);
    }

    // The consumer+aggregator pair drives the third replica's view of
    // the platform. This mirrors production: each sidecar is its own
    // aggregator, each one consumes the platform-wide stream and
    // computes the per-replica cap from the *other* replicas' deltas
    // minus its own.
    let observer = replicas[2];
    let consumer_publisher = NatsPublisher::connect(&nats_url, 1)
        .await
        .expect("nats connect for consumer");
    consumer_publisher
        .ensure_stream()
        .await
        .expect("ensure_stream");

    let (agg_tx, mut agg_rx) = mpsc::channel::<DeltaMsg>(256);
    let shutdown = CancellationToken::new();
    let consumer_handle = spawn_consumer(
        consumer_publisher.client(),
        format!("{}-rl-consumer", observer),
        agg_tx,
        shutdown.clone(),
    );

    // Note: we deliberately do NOT pre-sleep before publishing. The
    // 3s "wait for the consumer to attach" idiom didn't bound the
    // race on slow CI (see issue #665 PR E review thread). The
    // re-publish loop below is what gives the spawned consumer task
    // ample attach-time headroom — by the time we observe 3
    // distinct replicas in the aggregator's window, the consumer's
    // push subscription has been live for at least one round.

    let aggregator = Aggregator::new(observer.to_string(), 10_000);

    // Drive the publisher → consumer → aggregator pipeline until the
    // aggregator observes all 3 replicas at the steady-state RPS.
    // Each round re-publishes a fresh `DeltaMsg` per replica (same
    // shape, fresh `scraped_at_unix_ms`) for two reasons:
    //
    //   - the spawned consumer task has to complete get_stream +
    //     get_or_create_consumer + consumer.messages() RPCs before
    //     its push subscription is attached, and on slow CI runners
    //     that chain can take several seconds (LastPerSubject means
    //     messages published before attach are lost);
    //   - re-publishing keeps the stream `Interest` and the consumer's
    //     freshness gate (`MAX_MESSAGE_AGE = 2s`) satisfied on every
    //     round.
    //
    // Re-publishing every ~250ms is more resilient than a fixed
    // `tokio::time::sleep`; the timeout budget is RECONCILE_GRACE.
    let rps_per_replica = 10_000u32;
    let t0 = Instant::now();
    let snap = loop {
        for (i, rid) in replicas.iter().enumerate() {
            let msg = fresh_delta(rid, rps_per_replica);
            publishers[i]
                .publish_delta(rid, &msg)
                .await
                .expect("publish");
            publishers[i].client().flush().await.expect("flush");
        }

        let snap = aggregator.tick(&mut agg_rx, Instant::now()).await;
        if snap.replicas_seen == 3 && snap.platform_total == 30_000 {
            break snap;
        }
        if t0.elapsed() > RECONCILE_GRACE {
            panic!(
                "aggregator never observed 3 replicas within {RECONCILE_GRACE:?}; \
                 last snap = {snap:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };

    // Field-by-field assertion on the steady-state snapshot.
    assert_eq!(snap.configured_cap, 10_000);
    assert_eq!(snap.platform_total, 30_000);
    assert_eq!(snap.this_replica_rps, 10_000, "observer is replica 2");
    assert_eq!(snap.replicas_seen, 3);

    // The verification target: 3 replicas × 10k RPS at cap=10k ⇒
    // per_replica_cap == Some(1). Documented at issue #665 plan
    // §verification-target.
    assert_eq!(
        snap.per_replica_cap(),
        Some(1),
        "3 replicas × 10k RPS at cap=10k must yield per_replica_cap == 1 \
         (issue #665 verification target); got {:?}",
        snap.per_replica_cap()
    );

    shutdown.cancel();
    let _ = consumer_handle.await;
}

/// Wire a stale `scraped_at_unix_ms` (60s old) and assert the
/// consumer's freshness gate drops it before the aggregator sees it.
/// Mirrors the unit-test surface at `nats_sub::MAX_MESSAGE_AGE = 2s`.
#[tokio::test]
async fn stale_scraped_at_is_dropped_by_consumer() {
    if should_skip_integration_tests() {
        return;
    }

    let (_nats, nats_url) = start_nats().await;
    let shutdown = CancellationToken::new();

    let publisher = NatsPublisher::connect(&nats_url, 1)
        .await
        .expect("nats connect");
    publisher.ensure_stream().await.expect("ensure_stream");

    let consumer_publisher = NatsPublisher::connect(&nats_url, 1)
        .await
        .expect("nats connect");
    consumer_publisher
        .ensure_stream()
        .await
        .expect("ensure_stream");

    // Build a stale delta: scraped 60s ago, well beyond MAX_MESSAGE_AGE=2s.
    let stale_msg = {
        let scraped_at_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis().saturating_sub(60_000) as u64)
            .unwrap_or(0);
        DeltaMsg {
            replica_id: "stale-replica".to_string(),
            rps: 9_999,
            scraped_at_unix_ms,
        }
    };
    publisher
        .publish_delta("stale-replica", &stale_msg)
        .await
        .expect("publish stale");

    // Stand up a consumer + aggregator and verify the stale delta
    // never lands in the window. 2s wait > MAX_MESSAGE_AGE=2s, so any
    // delivered message at this point would itself be stale and the
    // freshness gate would have rejected it earlier.
    let (agg_tx, mut agg_rx) = mpsc::channel::<DeltaMsg>(256);
    let consumer_handle = spawn_consumer(
        consumer_publisher.client(),
        "stale-test-consumer".to_string(),
        agg_tx,
        shutdown.clone(),
    );
    let agg = Aggregator::new("stale-test".to_string(), 10_000);

    tokio::time::sleep(Duration::from_secs(2)).await;
    let snap = agg.tick(&mut agg_rx, Instant::now()).await;
    assert_eq!(snap.replicas_seen, 0, "stale delta must be dropped");
    assert_eq!(snap.platform_total, 0);
    assert_eq!(snap.per_replica_cap(), None, "fail-closed on empty window");

    shutdown.cancel();
    let _ = consumer_handle.await;
}
