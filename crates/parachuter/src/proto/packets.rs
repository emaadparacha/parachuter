//! Packet structs, encoding, and parsing.

use crate::error::{Error, Result};

/// Magic prefix identifying a parachuter datagram.
pub const MAGIC: [u8; 4] = *b"PARC";

/// Current protocol version. Bump when the on-the-wire layout changes.
pub const PROTOCOL_VERSION: u8 = 1;

/// Length of the fixed-width packet header in bytes.
pub const HEADER_LEN: usize = 32;

/// Discriminator for what kind of packet is being sent.
///
/// Encoded as a `u8`; receivers MUST tolerate unknown values by dropping the
/// packet without crashing.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
#[repr(u8)]
pub enum PacketType {
    /// First-pass file data. `chunk_id` is the index into the file.
    Data = 1,
    /// File-name / metadata packet. `payload` is the UTF-8 path string.
    Manifest = 2,
    /// A specific chunk being retransmitted in response to a request.
    Retransmit = 3,
    /// Same payload as [`Manifest`] but explicitly resent on demand.
    NameOnly = 4,
    /// Sender heartbeat / status beacon, payload is JSON status. Receivers
    /// may ignore.
    Heartbeat = 5,
}

impl PacketType {
    fn from_u8(b: u8) -> Result<Self> {
        Ok(match b {
            1 => Self::Data,
            2 => Self::Manifest,
            3 => Self::Retransmit,
            4 => Self::NameOnly,
            5 => Self::Heartbeat,
            _ => return Err(Error::BadPacket("unknown packet type")),
        })
    }
}

/// Bitfield flags carried in the packet header.
pub mod packet_flags {
    /// This is the last data chunk of the file. Receivers can use the payload
    /// length to truncate the destination file to its true size.
    pub const LAST_CHUNK: u16 = 1 << 0;
    /// Payload is bzip2-compressed (informational; reassembler does not
    /// decompress – downstream tooling should).
    pub const COMPRESSED: u16 = 1 << 1;
    /// This packet was emitted by an automated retransmit, not the main loop.
    pub const RETRANSMIT_HINT: u16 = 1 << 2;
}

/// Packet flags wrapper that derefs to the raw bitfield.
#[derive(Debug, Default, Copy, Clone, Eq, PartialEq)]
pub struct PacketFlags(pub u16);

impl PacketFlags {
    /// Construct an empty (zeroed) flag set.
    pub fn empty() -> Self {
        Self(0)
    }
    /// Mark `LAST_CHUNK`.
    pub fn last(mut self) -> Self {
        self.0 |= packet_flags::LAST_CHUNK;
        self
    }
    /// Test whether `LAST_CHUNK` is set.
    pub fn is_last(self) -> bool {
        self.0 & packet_flags::LAST_CHUNK != 0
    }
}

/// A fully-parsed parachuter packet, header + payload.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Packet {
    /// What this packet means.
    pub ptype: PacketType,
    /// Bitfield of optional hints.
    pub flags: PacketFlags,
    /// Logical file identifier (negative values reserved – use `>=1` in
    /// production).
    pub file_id: i64,
    /// 0-based chunk index. Meaningful for [`PacketType::Data`] and
    /// [`PacketType::Retransmit`]; ignored for the rest.
    pub chunk_id: u32,
    /// Total chunks the file is divided into.
    pub num_chunks: u32,
    /// Application payload (data bytes, filename, status JSON, …).
    pub payload: Vec<u8>,
}

impl Packet {
    /// Encode the packet into a byte buffer suitable for `send_to`.
    ///
    /// Computes the CRC32 last so the receiver can validate the header in one
    /// pass.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = vec![0u8; HEADER_LEN + self.payload.len()];
        buf[0..4].copy_from_slice(&MAGIC);
        buf[4] = PROTOCOL_VERSION;
        buf[5] = self.ptype as u8;
        buf[6..8].copy_from_slice(&self.flags.0.to_le_bytes());
        buf[8..16].copy_from_slice(&self.file_id.to_le_bytes());
        buf[16..20].copy_from_slice(&self.chunk_id.to_le_bytes());
        buf[20..24].copy_from_slice(&self.num_chunks.to_le_bytes());
        buf[24..28].copy_from_slice(&(self.payload.len() as u32).to_le_bytes());
        // CRC field at 28..32 starts zero.
        buf[HEADER_LEN..].copy_from_slice(&self.payload);
        let crc = crc32fast::hash(&buf[..]);
        buf[28..32].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Parse a datagram into a [`Packet`], validating magic, version, length
    /// and CRC32.
    pub fn decode(buf: &[u8]) -> Result<Self> {
        if buf.len() < HEADER_LEN {
            return Err(Error::BadPacket("short header"));
        }
        if buf[0..4] != MAGIC {
            return Err(Error::BadPacket("bad magic"));
        }
        if buf[4] != PROTOCOL_VERSION {
            return Err(Error::BadPacket("unsupported version"));
        }
        let ptype = PacketType::from_u8(buf[5])?;
        let flags = PacketFlags(u16::from_le_bytes([buf[6], buf[7]]));
        let file_id = i64::from_le_bytes(buf[8..16].try_into().unwrap());
        let chunk_id = u32::from_le_bytes(buf[16..20].try_into().unwrap());
        let num_chunks = u32::from_le_bytes(buf[20..24].try_into().unwrap());
        let payload_len =
            u32::from_le_bytes(buf[24..28].try_into().unwrap()) as usize;
        let expected_crc = u32::from_le_bytes(buf[28..32].try_into().unwrap());

        if buf.len() != HEADER_LEN + payload_len {
            return Err(Error::BadPacket("payload length mismatch"));
        }

        // Recompute CRC over the buffer with the CRC field zeroed.
        let mut tmp = buf.to_vec();
        tmp[28..32].fill(0);
        let actual_crc = crc32fast::hash(&tmp);
        if actual_crc != expected_crc {
            return Err(Error::BadPacket("crc mismatch"));
        }

        Ok(Self {
            ptype,
            flags,
            file_id,
            chunk_id,
            num_chunks,
            payload: buf[HEADER_LEN..].to_vec(),
        })
    }

    /// Maximum payload bytes that fit alongside a 32-byte header in `chunk_size`.
    pub const fn max_payload_for(chunk_size: usize) -> usize {
        chunk_size - HEADER_LEN
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_data_packet() {
        let p = Packet {
            ptype: PacketType::Data,
            flags: PacketFlags::empty().last(),
            file_id: 42,
            chunk_id: 7,
            num_chunks: 12,
            payload: b"hello, parachuter".to_vec(),
        };
        let bytes = p.encode();
        let parsed = Packet::decode(&bytes).unwrap();
        assert_eq!(p, parsed);
        assert!(parsed.flags.is_last());
    }

    #[test]
    fn detects_corruption() {
        let p = Packet {
            ptype: PacketType::Data,
            flags: PacketFlags::empty(),
            file_id: 1,
            chunk_id: 0,
            num_chunks: 1,
            payload: vec![1, 2, 3, 4],
        };
        let mut bytes = p.encode();
        // flip a bit in the payload
        bytes[HEADER_LEN] ^= 0x01;
        assert!(matches!(
            Packet::decode(&bytes),
            Err(Error::BadPacket("crc mismatch"))
        ));
    }

    #[test]
    fn rejects_bad_magic() {
        let mut bytes = Packet {
            ptype: PacketType::Data,
            flags: PacketFlags::empty(),
            file_id: 1,
            chunk_id: 0,
            num_chunks: 1,
            payload: vec![],
        }
        .encode();
        bytes[0] = b'X';
        assert!(matches!(
            Packet::decode(&bytes),
            Err(Error::BadPacket("bad magic"))
        ));
    }
}
