//! # parachuter
//!
//! Reliable, rate-limited file downlink over lossy unidirectional UDP links.
//!
//! `parachuter` is a generic Rust crate inspired by, and first used on, the
//! SuperBIT balloon telescope. It provides a versioned packet protocol, a
//! chunker / reassembler that survives partial deliveries, an embedded
//! SQLite ledger of sent files, and a length-prefixed JSON control plane
//! that can be driven by the `parachuter ctl` subcommand to retune chunk
//! size, link IPs, downlink speed and pause/resume the sender at runtime.
//!
//! The crate is organised into five subsystems:
//!
//! * [`proto`] — on-the-wire packet layout, framing, parsing, CRCs.
//! * [`chunker`] — split a file into a stream of [`proto::Packet`]s.
//! * [`reassembler`] — durably reassemble packets into a file using a packed
//!   bitmap manifest.
//! * [`ledger`] — SQLite-backed table of files seen, sent, and acknowledged.
//! * [`control`] — Unix-domain-socket control plane shared by every mode of
//!   the `parachuter` binary.
//!
//! See `docs/DESIGN.md` for the architecture overview.

#![deny(rust_2018_idioms)]
#![warn(missing_docs)]

pub mod chunker;
pub mod config;
pub mod control;
pub mod error;
pub mod ledger;
pub mod proto;
pub mod rate_limiter;
pub mod reassembler;
pub mod transport;

pub use error::{Error, Result};
