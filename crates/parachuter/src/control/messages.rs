//! Wire types for the control plane.
//!
//! Every daemon mode recognises every variant; unsupported operations return
//! `Response::Unsupported` rather than tearing down the connection.

use serde::{Deserialize, Serialize};

use crate::config::SenderState;

/// Request from `parachuter ctl` (or the cleaner mode) to a daemon.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    /// "Hello, are you alive?" – returns the daemon's identity.
    Ping,

    // --- Sender ops ---
    /// Read the sender's queue snapshot.
    SenderStatus,
    /// Override active link, chunk size or downlink kbps.
    SenderConfigure(LinkOverride),
    /// Force the sender into a new state.
    SenderSetState(SenderState),
    /// Enqueue a manual request: send `count` chunks starting at `start` of
    /// the file with this `file_id`. `start = -1` means "the whole file".
    SenderEnqueue {
        /// Logical file id, looked up against the ledger.
        file_id: i64,
        /// First chunk to send (or `-1` for the whole file).
        start: i32,
        /// How many chunks to send (`0` with `start == -1` means all).
        count: u32,
        /// `true` to put this request at the head of the queue.
        interrupt: bool,
    },
    /// Resend just the manifest (filename) packet for a file.
    SenderResendName {
        /// Logical file id.
        file_id: i64,
    },
    /// Drop everything from the queue without changing state.
    SenderFlush,
    /// Submit an arbitrary file by absolute path. The sender registers it in
    /// the ledger (or reuses the existing record if already known) and queues
    /// a fresh full-file send. Returns [`Response::Submitted`] with the
    /// assigned `file_id`, total chunk count and file size.
    ///
    /// This is the agnostic ingest API: any external program (Python script,
    /// systemd timer, shell, another Rust process, …) can hand the sender a
    /// path without writing into a watched directory first.
    SenderSubmit {
        /// Absolute path of the file to enqueue.
        path: String,
        /// `true` to push to the head of the queue.
        interrupt: bool,
    },

    // --- Receiver ops ---
    /// Read in-flight assemblies and missing chunk counts.
    ReceiverStatus,

    // --- Cleaner ops ---
    /// Read the cleaner's TTL dedup table and recent activity.
    CleanerStatus,
    /// Tell the cleaner to immediately rescan the holding directory.
    CleanerRunNow,
}

/// One configurable knob; any field set to `Some` is applied, others left
/// alone. Keeps the API additive.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LinkOverride {
    /// New active link key.
    pub active_link: Option<String>,
    /// New chunk size (bytes including header).
    pub chunk_size: Option<usize>,
    /// New max kbps for the active link.
    pub max_kbps: Option<u64>,
    /// New destination IP (overrides the active link's `ip`).
    pub link_ip: Option<String>,
    /// New destination port (overrides the active link's `port`).
    pub link_port: Option<u16>,
}

/// Response envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Response {
    /// Generic "OK, no payload".
    Ok,
    /// Daemon identity reply.
    Pong {
        /// Which daemon this is – `"sender"`, `"receiver"`, `"cleaner"`.
        daemon: String,
        /// Build version.
        version: String,
    },
    /// Sender status snapshot.
    SenderStatus(SenderStatus),
    /// Receiver status snapshot.
    ReceiverStatus(ReceiverStatus),
    /// Cleaner status snapshot.
    CleanerStatus(CleanerStatus),
    /// Successful reply to [`Request::SenderSubmit`].
    Submitted(SubmitAck),
    /// The op name was understood but the field values were rejected.
    BadRequest {
        /// Human-readable reason.
        reason: String,
    },
    /// The op is not implemented by this daemon.
    Unsupported,
    /// The op failed at runtime.
    Error {
        /// Stringified error.
        message: String,
    },
}

/// What the sender returns on `SenderStatus`. The cleaner uses this to
/// decide whether a chunk-range request is already in flight.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SenderStatus {
    /// Current sender state.
    pub state: SenderState,
    /// Active link key.
    pub active_link: String,
    /// Effective destination IP.
    pub link_ip: String,
    /// Effective destination port.
    pub link_port: u16,
    /// Currently configured chunk size.
    pub chunk_size: usize,
    /// Currently configured max throughput.
    pub max_kbps: u64,
    /// File id currently being transmitted (or `0`).
    pub current_file_id: i64,
    /// Total number of chunks remaining in the queue.
    pub queue_depth: u32,
    /// Per-file pending chunk ranges (newest at the back).
    pub pending: Vec<SenderQueueSnapshot>,
    /// Sequence number that increments on every ledger or queue mutation;
    /// callers can use it to detect changes since the last poll.
    pub revision: u64,
}

/// Per-file portion of [`SenderStatus`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SenderQueueSnapshot {
    /// File id.
    pub file_id: i64,
    /// Chunk ranges still queued for this file (start, count).
    pub ranges: Vec<RequestedRange>,
    /// `true` if the manifest packet is still pending.
    pub manifest_pending: bool,
}

/// One contiguous range of chunks.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub struct RequestedRange {
    /// First chunk index in the range.
    pub start: u32,
    /// Number of chunks (always >= 1).
    pub count: u32,
}

impl RequestedRange {
    /// Half-open end (exclusive).
    pub fn end(&self) -> u32 {
        self.start + self.count
    }
    /// Returns true if `chunk` falls inside this range.
    pub fn contains(&self, chunk: u32) -> bool {
        chunk >= self.start && chunk < self.end()
    }
}

/// What the receiver returns on `ReceiverStatus`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReceiverStatus {
    /// Bind address as seen by the OS.
    pub bind: String,
    /// In-flight assemblies (file_id, chunks_total, chunks_received).
    pub assemblies: Vec<AssemblyState>,
}

/// One in-flight reassembly's status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssemblyState {
    /// Logical file id.
    pub file_id: i64,
    /// Total chunks expected.
    pub chunks_total: u32,
    /// Chunks received so far.
    pub chunks_received: u32,
    /// `true` if the manifest packet has arrived.
    pub has_name: bool,
    /// Age of the assembly (seconds since last write).
    pub age_secs: u64,
}

/// Reply payload for [`Request::SenderSubmit`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitAck {
    /// Logical file id assigned by the ledger.
    pub file_id: i64,
    /// Total chunks the file is divided into at the current chunk size.
    pub num_chunks: u32,
    /// File size in bytes at submission time.
    pub file_size: u64,
}

/// What the cleaner returns on `CleanerStatus`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CleanerStatus {
    /// Current dedup-table entries (per file_id, ranges).
    pub in_flight: Vec<SenderQueueSnapshot>,
    /// Number of requests sent on the active link in the last minute.
    pub recent_requests: u32,
    /// Active link.
    pub active_link: String,
}
