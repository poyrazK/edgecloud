//! Re-exported wire types from `edge-worker`. The ingress must use the
//! same canonical type the worker emits so a heartbeat cannot drift
//! between producer and consumer (the previous hand-cloned copy in
//! this file had already started to drift — `port: u16` vs `Option<u16>`).

pub use edge_worker::messages::HeartbeatMessage;
