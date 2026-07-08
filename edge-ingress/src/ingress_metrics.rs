//! Prometheus metric descriptions for edge-ingress.
//!
//! Every metric used in the ingress codebase is registered here via
//! `describe_*!` macros so they appear in the `/metrics` output even
//! before any events are recorded (i.e. all counters show as `0` from
//! startup). Call `describe_metrics()` once after installing the
//! Prometheus recorder, before any instrumentation code runs.

/// Register all metric metadata with the global recorder.
pub fn describe_metrics() {
    // ── Heartbeats ────────────────────────────────────────────────────
    metrics::describe_counter!(
        "ingress.heartbeats.received",
        "Total heartbeats received from NATS, tagged by region"
    );
    metrics::describe_counter!(
        "ingress.heartbeats.parse_failed",
        "Heartbeats that failed JSON deserialization"
    );
    metrics::describe_counter!(
        "ingress.heartbeats.no_addr",
        "Heartbeats received without a valid worker_addr"
    );
    metrics::describe_counter!(
        "ingress.heartbeats.apps_total",
        "Cumulative number of app entries across all heartbeats"
    );

    // ── Routing table state ───────────────────────────────────────────
    metrics::describe_gauge!(
        "ingress.routes.active",
        "Current number of active routes in the routing table"
    );
    metrics::describe_gauge!(
        "ingress.fqdns.active",
        "Current number of active FQDN bindings"
    );
    metrics::describe_counter!(
        "ingress.routes.changed",
        "Number of heartbeat mutations that triggered a Caddy reload notification"
    );

    // ── Caddy operations ──────────────────────────────────────────────
    metrics::describe_counter!(
        "ingress.caddy.reload_total",
        "Caddy config reload attempts, tagged by status (success|failure)"
    );
    metrics::describe_histogram!(
        "ingress.caddy.reload_duration_seconds",
        "Duration of Caddy POST /load requests"
    );
    metrics::describe_histogram!(
        "ingress.caddy.render_duration_seconds",
        "Duration of Caddyfile-JSON rendering"
    );

    // ── Pruner ────────────────────────────────────────────────────────
    metrics::describe_counter!(
        "ingress.pruner.removed_total",
        "Total number of stale routes pruned"
    );

    // ── Domain poller ─────────────────────────────────────────────────
    metrics::describe_counter!(
        "ingress.domain_poll.total",
        "Domain poller fetch attempts, tagged by status (success|failure|auth_error)"
    );

    // ── Traffic split fetcher ─────────────────────────────────────────
    metrics::describe_counter!(
        "ingress.traffic_fetch.total",
        "Traffic split fetch attempts, tagged by status (success|failure|unauthorized)"
    );

    // ── Rate limit fetcher ────────────────────────────────────────────
    metrics::describe_counter!(
        "ingress.rate_limit_fetch.total",
        "Rate limit fetch attempts, tagged by status (success|failure|not_found)"
    );

    // ── NATS connectivity ─────────────────────────────────────────────
    metrics::describe_counter!(
        "ingress.nats.reconnects_total",
        "Total number of NATS reconnect attempts in the main loop"
    );
}

#[cfg(test)]
mod tests {
    use metrics_exporter_prometheus::PrometheusBuilder;

    #[tokio::test]
    async fn metrics_exposition_format() {
        // Build the recorder only (no HTTP listener needed for unit test).
        let recorder = PrometheusBuilder::new().build_recorder();
        let handle = recorder.handle();

        // Register descriptions and record values via the global recorder.
        let _ = metrics::set_global_recorder(recorder);
        super::describe_metrics();

        metrics::counter!("ingress.heartbeats.received", "region" => "test").increment(1);
        metrics::counter!("ingress.caddy.reload_total", "status" => "success").increment(1);
        metrics::counter!("ingress.caddy.reload_total", "status" => "failure").increment(2);
        metrics::gauge!("ingress.routes.active").set(42.0);
        metrics::gauge!("ingress.fqdns.active").set(7.0);

        let output = handle.render();

        // ── Assert all metric names appear in the output ───────────────
        assert!(
            output.contains("ingress_heartbeats_received"),
            "expected ingress_heartbeats_received in output"
        );
        assert!(
            output.contains("ingress_routes_active"),
            "expected ingress_routes_active in output"
        );
        assert!(
            output.contains("ingress_fqdns_active"),
            "expected ingress_fqdns_active in output"
        );
        assert!(
            output.contains("ingress_caddy_reload_total"),
            "expected ingress_caddy_reload_total in output"
        );

        // ── Assert Prometheus exposition format ───────────────────────
        assert!(
            output.contains("# HELP"),
            "output should contain HELP lines"
        );
        assert!(
            output.contains("# TYPE"),
            "output should contain TYPE lines"
        );
        assert!(
            output.contains("# TYPE ingress_heartbeats_received counter"),
            "heartbeats counter type should be present"
        );
        assert!(
            output.contains("# TYPE ingress_routes_active gauge"),
            "routes gauge type should be present"
        );

        // ── Assert counter values (tag sets) ──────────────────────────
        assert!(
            output.contains("ingress_heartbeats_received{region=\"test\"} 1"),
            "expected region=test counter with value 1"
        );
        assert!(
            output.contains("ingress_caddy_reload_total{status=\"success\"} 1"),
            "expected success counter"
        );
        assert!(
            output.contains("ingress_caddy_reload_total{status=\"failure\"} 2"),
            "expected failure counter with value 2"
        );

        // ── Assert gauge values ───────────────────────────────────────
        assert!(
            output.contains("ingress_routes_active 42"),
            "expected routes gauge = 42"
        );
        assert!(
            output.contains("ingress_fqdns_active 7"),
            "expected fqdns gauge = 7"
        );
    }
}
