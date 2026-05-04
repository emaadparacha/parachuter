//! Length-prefixed JSON control plane.
//!
//! Each `parachuter` daemon mode (sender, receiver, cleaner) listens on a
//! Unix domain socket, accepts one request at a time, parses it as JSON and
//! writes one JSON response back. The framing is a 4-byte big-endian length
//! followed by the JSON bytes.
//!
//! ## Why Unix sockets?
//!
//! * Permissions are filesystem ACLs; no extra auth layer to ship.
//! * `parachuter ctl` and the cleaner mode can both poke the sender from the
//!   same host without opening a TCP port.
//! * Easy to mock / test (use `/tmp/x.sock` in tests).
//!
//! ## Why JSON?
//!
//! * Trivial to script from a shell (`echo '{"op":"status"}' | nc -U …`).
//! * Easy to extend without bumping a wire version.
//! * Inspectable in `tcpdump`/`socat` traces.

mod client;
mod messages;
mod server;

pub use client::ControlClient;
pub use messages::{
    AssemblyState, CleanerStatus, LinkOverride, ReceiverStatus, Request, RequestedRange,
    Response, SenderQueueSnapshot, SenderStatus, SubmitAck,
};
pub use server::{ControlHandler, ControlServer};
