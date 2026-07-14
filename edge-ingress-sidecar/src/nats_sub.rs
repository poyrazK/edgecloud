//! JetStream push consumer. Filled in by PR B (issue #665).
//!
//! Mirrors `edge-worker/src/nats.rs:69-92` `build_consumer_config` with:
//!   - `filter_subject = "edgecloud.rate-limit.global.>"`
//!   - `deliver_policy: DeliverPolicy::LastPerSubject` (replay only the
//!     most recent delta per replica on reconnect)
//!   - `ack_policy: Explicit`, `max_deliver: 5`
//!
//! Pushes raw `DeltaMsg`s into a tokio mpsc channel that `aggregate.rs`
//! consumes.

#![allow(dead_code)]
