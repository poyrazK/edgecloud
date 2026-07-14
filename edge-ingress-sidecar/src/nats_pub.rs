//! JetStream publisher. Filled in by PR B (issue #665).
//!
//! Mirrors the durable-publish half of `edge-worker/src/nats.rs:97-106`
//! (stream config) and `edge-worker/src/nats.rs:252-261` (per-tick
//! fire-and-forget publish). The publisher reads deltas from
//! `caddy_metrics`, frames them as the wire payload
//! `{"replica_id":..., "ts_unix_ms":..., "rps":<u32>}`, and publishes to
//! `edgecloud.rate-limit.global.delta.<replica-id>` on the
//! `edgecloud-rl-global` JetStream stream (declared by PR C).

#![allow(dead_code)]
