//! Versioned, CRC-protected packet protocol for parachuter.
//!
//! ## Design
//!
//! Every parachuter datagram is a fixed 32-byte header followed by a
//! variable-length payload. The header carries:
//!
//! * A magic prefix so stray UDP datagrams cannot be parsed as packets.
//! * A version byte so the layout can evolve safely.
//! * A CRC32 over the full datagram so silent corruption (cosmic rays,
//!   lossy radios) is caught at parse time.
//! * An explicit `ptype` enum rather than overloaded sentinel values, so
//!   adding new packet types does not collide with existing fields.
//!
//! The parachuter wire format:
//!
//! ```text
//! offset  size  field
//! 0       4     magic = b"PARC"
//! 4       1     version (currently 1)
//! 5       1     ptype  (see [`PacketType`])
//! 6       2     flags  (u16 LE bitfield, see [`PacketFlags`])
//! 8       8     file_id (i64 LE) – widened from i32
//! 16      4     chunk_id (u32 LE)
//! 20      4     num_chunks (u32 LE) – total chunks in this file
//! 24      4     payload_len (u32 LE) – bytes following the header
//! 28      4     crc32 (u32 LE) – CRC32 of [bytes 0..28 with crc=0] || payload
//! 32      ...   payload (payload_len bytes)
//! ```
//!
//! Every parser checks magic, version and CRC before trusting any other field.

mod packets;

pub use packets::{packet_flags, Packet, PacketFlags, PacketType, HEADER_LEN, MAGIC, PROTOCOL_VERSION};

/// Sensible default chunk size for the SuperBIT-style radio link.
///
/// 16 192 bytes is what SuperBIT's flight-to-ground link supports
/// end-to-end; deployments on other links should override via the
/// `chunk_size` config field.
pub const DEFAULT_CHUNK_SIZE: usize = 16_192;

/// Hard upper bound on chunk size; conservative for typical MTU paths.
///
/// UDP datagram size is theoretically up to 65 507 bytes but most paths
/// fragment past ~1 472 (Ethernet) or ~9 000 (jumbo). We allow up to 65 000
/// and let the operator pick.
pub const MAX_CHUNK_SIZE: usize = 65_000;

/// Lower bound; anything smaller than the header plus a few payload bytes is
/// pointless.
pub const MIN_CHUNK_SIZE: usize = HEADER_LEN + 32;
