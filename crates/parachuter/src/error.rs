//! Error type used across the crate.

use std::path::PathBuf;
use thiserror::Error;

/// Crate-wide result alias.
pub type Result<T> = std::result::Result<T, Error>;

/// All recoverable failures returned by parachuter APIs.
///
/// Every fallible operation returns one of these variants so the caller can
/// decide how to handle each failure mode rather than panicking.
#[derive(Debug, Error)]
pub enum Error {
    /// Underlying I/O failure (open, read, write, socket, etc.).
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// SQLite ledger error.
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    /// JSON decode failure on the control socket.
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    /// TOML decode failure on the config file.
    #[error("toml error: {0}")]
    Toml(#[from] toml::de::Error),

    /// A file path that should exist could not be found.
    #[error("path not found: {0}")]
    NotFound(PathBuf),

    /// Packet failed CRC, magic, or version validation.
    #[error("malformed packet: {0}")]
    BadPacket(&'static str),

    /// Manifest sidecar file is corrupt or version-mismatched.
    #[error("manifest corrupt: {0}")]
    BadManifest(String),

    /// The configured chunk size is invalid (too small or too large for UDP).
    #[error("invalid chunk size: {0}")]
    BadChunkSize(usize),

    /// Generic configuration validation error.
    #[error("invalid config: {0}")]
    BadConfig(String),

    /// Caller asked for a chunk index past the end of the file.
    #[error("chunk index {index} out of range (have {total})")]
    OutOfRange {
        /// Requested index.
        index: u32,
        /// Total chunks the file has.
        total: u32,
    },
}
