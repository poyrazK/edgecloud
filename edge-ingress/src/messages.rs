//! Re-exported wire types from `edge-worker`. The ingress must use the
//! same canonical types the worker emits so a heartbeat cannot drift
//! between producer and consumer (the previous hand-cloned copies in
//! this file had already started to drift — `port: u16` vs `Option<u16>`).

// `AppStatus` is unused inside this crate (the heartbeat loop only names
// `HeartbeatMessage` and walks the map), but it is part of the public
// surface for downstream tests / tooling.
#[allow(unused_imports)]
pub use edge_worker::messages::{AppStatus, HeartbeatMessage};
