//! UDS datagram writer. Filled in by PR B (issue #665).
//!
//! Writes a single datagram per tick to
//! `/var/run/edge-ingress/global-rps.sock` carrying
//! `{"ts":..., "platform_total":N, "configured":M, "this_replica":K, "local_cap":L}`.
//! The ingress binary (PR D) reads this socket at 1 Hz and consults it
//! in `caddy.rs:719-746` to render the per-replica `rates.rps`.

#![allow(dead_code)]
