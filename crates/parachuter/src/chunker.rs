//! Split a file into a sequence of [`crate::proto::Packet`]s.
//!
//! Unlike the original `chunk_file`, this is:
//!
//! * **Streaming** – it iterates with `read_at`, never loading the full file
//!   into memory. The original code loaded the whole image into a `Vec<u8>`
//!   even for multi-GB GoPro media; that was the single biggest memory bug.
//! * **Range-aware** – the same `Chunker` produces full-file streams or
//!   partial retransmits without duplicating the splitting code.
//! * **Configurable chunk size** – passed explicitly rather than baked in via
//!   a `const`.
//! * **Correct on the last chunk** – the original code mishandled the boundary
//!   (it pushed an empty last chunk when the file size was an exact multiple
//!   of the chunk size, then patched around it with a `+1` in the receiver).

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::error::{Error, Result};
use crate::proto::{Packet, PacketFlags, PacketType, HEADER_LEN, MAX_CHUNK_SIZE, MIN_CHUNK_SIZE};

/// Streaming chunker for a file at a fixed `chunk_size`.
#[derive(Debug)]
pub struct Chunker {
    file: File,
    file_size: u64,
    file_id: i64,
    chunk_size: usize,
    payload_size: usize,
    num_chunks: u32,
}

impl Chunker {
    /// Open a file for chunking. `chunk_size` is the *total datagram size*
    /// including the 32-byte header.
    pub fn open(
        path: impl AsRef<Path>,
        file_id: i64,
        chunk_size: usize,
    ) -> Result<Self> {
        if !(MIN_CHUNK_SIZE..=MAX_CHUNK_SIZE).contains(&chunk_size) {
            return Err(Error::BadChunkSize(chunk_size));
        }
        let path = path.as_ref();
        let file = File::open(path).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => Error::NotFound(path.to_path_buf()),
            _ => Error::Io(e),
        })?;
        let file_size = file.metadata()?.len();
        let payload_size = chunk_size - HEADER_LEN;
        let num_chunks = chunks_for_size(file_size, payload_size);
        Ok(Self {
            file,
            file_size,
            file_id,
            chunk_size,
            payload_size,
            num_chunks,
        })
    }

    /// Total number of data chunks for this file (excluding the manifest packet).
    pub fn num_chunks(&self) -> u32 {
        self.num_chunks
    }

    /// File size in bytes.
    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    /// Chunk size (datagram size) in bytes.
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    /// Read a single data chunk by index.
    pub fn data_packet(&mut self, chunk_id: u32) -> Result<Packet> {
        if chunk_id >= self.num_chunks {
            return Err(Error::OutOfRange {
                index: chunk_id,
                total: self.num_chunks,
            });
        }
        let offset = chunk_id as u64 * self.payload_size as u64;
        let remaining = self.file_size - offset;
        let to_read = remaining.min(self.payload_size as u64) as usize;

        self.file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; to_read];
        self.file.read_exact(&mut buf)?;

        let mut flags = PacketFlags::empty();
        if chunk_id == self.num_chunks - 1 {
            flags = flags.last();
        }
        Ok(Packet {
            ptype: PacketType::Data,
            flags,
            file_id: self.file_id,
            chunk_id,
            num_chunks: self.num_chunks,
            payload: buf,
        })
    }

    /// Read a single retransmit packet by index. Identical to a data packet
    /// except the `ptype` is [`PacketType::Retransmit`] – the receiver uses it
    /// to skip the "is this packet 1 the first one for this file" cold-start
    /// path and instead patches an existing reassembly in place.
    pub fn retransmit_packet(&mut self, chunk_id: u32) -> Result<Packet> {
        let mut p = self.data_packet(chunk_id)?;
        p.ptype = PacketType::Retransmit;
        // Retransmits are explicitly marked so the cleaner can dedupe.
        p.flags.0 |= crate::proto::packet_flags::RETRANSMIT_HINT;
        Ok(p)
    }

    /// Build the manifest packet containing the file path. The original code
    /// sent the bare path as bytes; we do the same so existing integrations
    /// just need to swap the header layout, not the filename semantics.
    pub fn manifest_packet(&self, path: impl AsRef<Path>) -> Packet {
        let path_str = path.as_ref().to_string_lossy().into_owned();
        Packet {
            ptype: PacketType::Manifest,
            flags: PacketFlags::empty(),
            file_id: self.file_id,
            chunk_id: 0,
            num_chunks: self.num_chunks,
            payload: path_str.into_bytes(),
        }
    }

    /// Iterate every data packet in order. Cheap because each iteration
    /// `seek`s and reads only its own slice.
    pub fn iter_data(&mut self) -> impl Iterator<Item = Result<Packet>> + '_ {
        let total = self.num_chunks;
        (0..total).map(|i| self.data_packet(i))
    }
}

/// Compute the number of chunks needed to hold `file_size` bytes when each
/// chunk holds at most `payload_size` payload bytes.
///
/// Empty files still get a single zero-length chunk so receivers see a
/// `LAST_CHUNK` packet.
pub fn chunks_for_size(file_size: u64, payload_size: usize) -> u32 {
    if file_size == 0 {
        return 1;
    }
    let p = payload_size as u64;
    ((file_size + p - 1) / p) as u32
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn make_file(bytes: &[u8]) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(bytes).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn computes_chunk_count() {
        // A file size that's an exact multiple of payload_size used to
        // produce an off-by-one in the original code – verify we don't.
        assert_eq!(chunks_for_size(0, 1024), 1);
        assert_eq!(chunks_for_size(1, 1024), 1);
        assert_eq!(chunks_for_size(1024, 1024), 1);
        assert_eq!(chunks_for_size(1025, 1024), 2);
        assert_eq!(chunks_for_size(2048, 1024), 2);
    }

    #[test]
    fn chunks_round_trip() {
        let data: Vec<u8> = (0..5000u32).map(|i| (i & 0xff) as u8).collect();
        let f = make_file(&data);
        let mut c = Chunker::open(f.path(), 99, 1024).unwrap();
        let mut reassembled = Vec::new();
        for pkt in c.iter_data() {
            let pkt = pkt.unwrap();
            reassembled.extend_from_slice(&pkt.payload);
        }
        assert_eq!(reassembled, data);
    }

    #[test]
    fn last_chunk_is_marked() {
        let f = make_file(b"abcdef");
        let mut c = Chunker::open(f.path(), 1, 64).unwrap();
        let p = c.data_packet(0).unwrap();
        assert!(p.flags.is_last());
    }
}
