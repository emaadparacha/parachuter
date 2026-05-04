//! Durable, bitmap-based packet reassembly.
//!
//! ## Improvements over the old `dotfile` design
//!
//! The original code used a text "dotfile" that stored one ASCII character
//! (`'x'` or `'.'`) per chunk. That had several problems:
//!
//! 1. **8× space waste** vs a packed bitmap.
//! 2. **No magic / version** – any leftover dotfile from an unrelated run
//!    could be picked up, and there was no way to evolve the format.
//! 3. **Filename appended after `||`** – mixing two encodings inside the same
//!    file made the parser brittle (witness `dotfile_is_complete` re-parsing
//!    the same string in three places with subtle differences).
//! 4. **No fsync ordering** – on power loss the dotfile and the data file
//!    could disagree, after which type-1 packets would silently *delete* the
//!    surviving sibling because `(dotfile_exists ^ file_exists)` was treated
//!    as fatal corruption.
//! 5. **No chunk-size record** – the receiver had to assume the same constant
//!    as the sender. Changing chunk size on the fly was impossible.
//! 6. **Race conditions** – multiple packets could be processed concurrently
//!    if the receive loop was ever made multi-threaded.
//!
//! The parachuter manifest is a single sidecar file `<file_id>.parmanifest`
//! whose layout is:
//!
//! ```text
//! offset  size  field
//! 0       4     magic = b"PMAN"
//! 4       1     version (currently 1)
//! 5       1     reserved
//! 6       2     reserved
//! 8       4     chunk_payload_size (u32 LE) – payload bytes per chunk
//! 12      4     num_chunks (u32 LE)
//! 16      8     file_size_hint (u64 LE) – set when LAST_CHUNK arrives
//! 24      4     name_len (u32 LE)
//! 28      4     header_crc32 (u32 LE) – CRC of bytes 0..28 with crc=0
//! 32      ..    name bytes (utf-8, name_len long)
//! 32+nl   ..    bitmap, ceil(num_chunks / 8) bytes
//! ```
//!
//! Each `1` bit in the bitmap means "this chunk has been received and
//! fsync'd into the data file". The bitmap byte indexed by `chunk_id / 8`
//! has bit `chunk_id % 8` set.
//!
//! Operations on the manifest are guarded by a process-wide lock so the
//! reassembler is safe to call from multiple threads.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::error::{Error, Result};
use crate::proto::{Packet, PacketType};

const MANIFEST_MAGIC: [u8; 4] = *b"PMAN";
const MANIFEST_VERSION: u8 = 1;
const MANIFEST_HEADER_LEN: usize = 32;

/// Stats returned by [`Reassembler::stats`].
#[derive(Debug, Clone, Copy)]
pub struct AssemblyStats {
    /// Total chunks the file is divided into.
    pub chunks_total: u32,
    /// Chunks already received and durably written.
    pub chunks_received: u32,
    /// Whether the manifest packet has been seen.
    pub has_name: bool,
}

/// Outcome of feeding a packet to [`Reassembler::ingest`].
#[derive(Debug, Eq, PartialEq)]
pub enum IngestOutcome {
    /// Packet was new and applied.
    Accepted,
    /// Packet was a duplicate of one already received.
    Duplicate,
    /// File now has every chunk – call [`Reassembler::finalize`] to move it
    /// into the final directory and drop the manifest.
    Complete,
    /// Packet was ignored because it didn't apply to this assembly (wrong
    /// chunk_size, wrong num_chunks, etc.).
    Mismatched,
    /// Packet was a [`PacketType::Heartbeat`] or other meta packet that the
    /// reassembler doesn't act on.
    Ignored,
}

/// Reassembler that owns one *holding directory* and one *final directory*.
///
/// Holding holds in-flight `<id>.incoming` data files plus
/// `<id>.parmanifest` sidecars. On completion files are renamed under
/// `final/` using the path that arrived inside the manifest packet.
pub struct Reassembler {
    holding: PathBuf,
    finals: PathBuf,
    /// Manifest writes are not concurrent-safe in general because we mutate
    /// a tiny in-place region; serialise per-process to make multi-threaded
    /// receivers safe by construction.
    write_lock: Mutex<()>,
}

impl Reassembler {
    /// Construct a reassembler. The holding directory is created if missing.
    pub fn new(
        holding: impl Into<PathBuf>,
        finals: impl Into<PathBuf>,
    ) -> Result<Self> {
        let holding = holding.into();
        let finals = finals.into();
        std::fs::create_dir_all(&holding)?;
        std::fs::create_dir_all(&finals)?;
        Ok(Self {
            holding,
            finals,
            write_lock: Mutex::new(()),
        })
    }

    /// Path of the data file for an in-flight assembly.
    pub fn incoming_path(&self, file_id: i64) -> PathBuf {
        self.holding.join(format!("{file_id}.incoming"))
    }

    /// Path of the manifest sidecar for an in-flight assembly.
    pub fn manifest_path(&self, file_id: i64) -> PathBuf {
        self.holding.join(format!("{file_id}.parmanifest"))
    }

    /// Apply one packet to disk. Returns what happened.
    pub fn ingest(&self, pkt: &Packet) -> Result<IngestOutcome> {
        match pkt.ptype {
            PacketType::Data | PacketType::Retransmit => self.ingest_chunk(pkt),
            PacketType::Manifest | PacketType::NameOnly => self.ingest_manifest(pkt),
            PacketType::Heartbeat => Ok(IngestOutcome::Ignored),
        }
    }

    fn ingest_chunk(&self, pkt: &Packet) -> Result<IngestOutcome> {
        let _g = self.write_lock.lock().unwrap();
        let payload_size = pkt.payload.len();
        // First chunk for a never-seen file: cold-start the manifest using
        // the chunk's own payload size (which sets payload_size for the file
        // unless overridden by the LAST_CHUNK packet).
        let mut manifest = match Manifest::open(self.manifest_path(pkt.file_id).as_path()) {
            Ok(m) => m,
            Err(Error::NotFound(_)) => {
                // For non-last chunks the payload size IS the per-chunk size.
                // For the last chunk specifically we have to rely on chunk_id
                // == num_chunks - 1 to tell us this is the tail – treat the
                // payload size as a *minimum* in that case and don't lock the
                // manifest to it.
                let assumed_payload = if pkt.flags.is_last() && pkt.num_chunks > 1 {
                    // We cannot infer per-chunk size from a partial last
                    // chunk; defer creation until a non-last chunk arrives.
                    return Ok(IngestOutcome::Mismatched);
                } else {
                    payload_size as u32
                };
                Manifest::create(
                    self.manifest_path(pkt.file_id).as_path(),
                    assumed_payload,
                    pkt.num_chunks,
                    "",
                )?
            }
            Err(e) => return Err(e),
        };

        // Must agree on the file's chunk count; otherwise the sender changed
        // chunk size mid-flight and we should discard.
        if manifest.num_chunks != pkt.num_chunks {
            return Ok(IngestOutcome::Mismatched);
        }

        // If the manifest was created by an ingest_manifest call (name packet
        // arrived before any data), chunk_payload_size is 0 because it could
        // not be inferred from the name packet alone.  Patch it in now from
        // this data chunk.  Without this fix every chunk would be written at
        // file offset 0 (chunk_id × 0 = 0), producing a corrupt assembly.
        if manifest.chunk_payload_size == 0 {
            if pkt.flags.is_last() && pkt.num_chunks > 1 {
                // Still can't infer the per-chunk size from the last chunk of
                // a multi-chunk file; drop and wait for an earlier chunk.
                return Ok(IngestOutcome::Mismatched);
            }
            manifest.chunk_payload_size = payload_size as u32;
            // Save immediately so the next ingest_chunk finds the patched value.
            manifest.save()?;
        }

        // Make sure data file exists and is sized correctly.
        let data_path = self.incoming_path(pkt.file_id);
        let mut data = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&data_path)?;

        let payload = manifest.chunk_payload_size as u64;
        let needed_min = (pkt.chunk_id as u64) * payload + payload_size as u64;
        if data.metadata()?.len() < needed_min {
            data.set_len(needed_min)?;
        }

        // Write the chunk.
        data.seek(SeekFrom::Start(pkt.chunk_id as u64 * payload))?;
        data.write_all(&pkt.payload)?;
        data.sync_data()?;

        // If this is the last chunk, lock in the file size.
        if pkt.flags.is_last() {
            let real_size = (pkt.chunk_id as u64) * payload + payload_size as u64;
            data.set_len(real_size)?;
            data.sync_all()?;
            manifest.file_size_hint = real_size;
        }

        // Mark the bit. If it was already set this is a duplicate.
        let already_set = manifest.is_set(pkt.chunk_id);
        if already_set {
            return Ok(IngestOutcome::Duplicate);
        }
        manifest.set(pkt.chunk_id);
        manifest.save()?;

        if manifest.is_complete() && !manifest.name.is_empty() {
            Ok(IngestOutcome::Complete)
        } else {
            Ok(IngestOutcome::Accepted)
        }
    }

    fn ingest_manifest(&self, pkt: &Packet) -> Result<IngestOutcome> {
        let _g = self.write_lock.lock().unwrap();
        let path_str = String::from_utf8_lossy(&pkt.payload).into_owned();

        let manifest_path = self.manifest_path(pkt.file_id);
        let mut manifest = match Manifest::open(&manifest_path) {
            Ok(m) => m,
            Err(Error::NotFound(_)) => {
                // We received the name before any data; allocate a manifest
                // with payload_size=0 (will be patched by the first data
                // packet) and zero chunks (placeholder).
                Manifest::create(&manifest_path, 0, pkt.num_chunks, &path_str)?
            }
            Err(e) => return Err(e),
        };

        if manifest.num_chunks != pkt.num_chunks {
            return Ok(IngestOutcome::Mismatched);
        }
        if manifest.name.is_empty() {
            manifest.name = path_str;
            manifest.save()?;
        }
        if manifest.is_complete() && !manifest.name.is_empty() {
            Ok(IngestOutcome::Complete)
        } else {
            Ok(IngestOutcome::Accepted)
        }
    }

    /// Read the missing chunk ranges for an in-flight assembly. Returns
    /// `(start_chunk, count)` pairs sorted by start.
    pub fn missing_ranges(&self, file_id: i64) -> Result<Vec<(u32, u32)>> {
        let m = Manifest::open(&self.manifest_path(file_id))?;
        Ok(m.missing_ranges())
    }

    /// Returns `true` if the assembly for `file_id` has its filename packet
    /// (i.e. only chunks remain).
    pub fn has_name(&self, file_id: i64) -> Result<bool> {
        let m = Manifest::open(&self.manifest_path(file_id))?;
        Ok(!m.name.is_empty())
    }

    /// All file_ids currently being reassembled.
    pub fn in_flight(&self) -> Result<Vec<i64>> {
        let mut ids = Vec::new();
        for entry in std::fs::read_dir(&self.holding)? {
            let entry = entry?;
            let name = entry.file_name();
            let s = name.to_string_lossy();
            if let Some(stem) = s.strip_suffix(".parmanifest") {
                if let Ok(id) = stem.parse::<i64>() {
                    ids.push(id);
                }
            }
        }
        ids.sort_unstable();
        Ok(ids)
    }

    /// Move a complete assembly into its final destination and drop the
    /// manifest. Returns the final path written.
    ///
    /// This is the equivalent of `rename_and_move_file` from the original
    /// code, but it `rename`s atomically (same filesystem) or `copy + remove`
    /// across filesystems, and never leaves orphaned files behind on error.
    pub fn finalize(&self, file_id: i64) -> Result<PathBuf> {
        let _g = self.write_lock.lock().unwrap();
        let manifest_path = self.manifest_path(file_id);
        let manifest = Manifest::open(&manifest_path)?;
        if !manifest.is_complete() {
            return Err(Error::BadManifest("finalize before complete".into()));
        }
        if manifest.name.is_empty() {
            return Err(Error::BadManifest("missing filename".into()));
        }

        // Strip a leading slash off the embedded name so it sits inside `finals/`.
        let cleaned_name = manifest.name.trim_start_matches('/');
        let dest = self.finals.join(cleaned_name);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let incoming = self.incoming_path(file_id);
        // Try fast rename, fall back to copy + remove for cross-fs case.
        if std::fs::rename(&incoming, &dest).is_err() {
            std::fs::copy(&incoming, &dest)?;
            std::fs::remove_file(&incoming)?;
        }
        std::fs::remove_file(&manifest_path)?;
        Ok(dest)
    }

    /// Quick stats about an in-flight assembly.
    pub fn stats(&self, file_id: i64) -> Result<AssemblyStats> {
        let m = Manifest::open(&self.manifest_path(file_id))?;
        let total = m.num_chunks;
        let missing: u32 = m.missing_ranges().iter().map(|(_, c)| *c).sum();
        Ok(AssemblyStats {
            chunks_total: total,
            chunks_received: total.saturating_sub(missing),
            has_name: !m.name.is_empty(),
        })
    }

    /// How old is this assembly's last write, in seconds?
    pub fn age_secs(&self, file_id: i64) -> Result<u64> {
        let mp = self.manifest_path(file_id);
        let meta = std::fs::metadata(&mp)?;
        let modified = meta.modified()?;
        Ok(modified
            .elapsed()
            .map(|d| d.as_secs())
            .unwrap_or(0))
    }
}

// ---------------------------------------------------------------------------
// Manifest sidecar
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct Manifest {
    path: PathBuf,
    chunk_payload_size: u32,
    num_chunks: u32,
    file_size_hint: u64,
    name: String,
    bitmap: Vec<u8>,
}

impl Manifest {
    fn bitmap_len(num_chunks: u32) -> usize {
        ((num_chunks + 7) / 8) as usize
    }

    fn create(path: &Path, chunk_payload_size: u32, num_chunks: u32, name: &str) -> Result<Self> {
        let m = Self {
            path: path.to_path_buf(),
            chunk_payload_size,
            num_chunks,
            file_size_hint: 0,
            name: name.to_owned(),
            bitmap: vec![0; Self::bitmap_len(num_chunks)],
        };
        m.save()?;
        Ok(m)
    }

    fn open(path: &Path) -> Result<Self> {
        let mut f = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(Error::NotFound(path.to_path_buf()));
            }
            Err(e) => return Err(e.into()),
        };
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        if buf.len() < MANIFEST_HEADER_LEN {
            return Err(Error::BadManifest("short header".into()));
        }
        if buf[0..4] != MANIFEST_MAGIC {
            return Err(Error::BadManifest("bad magic".into()));
        }
        if buf[4] != MANIFEST_VERSION {
            return Err(Error::BadManifest("bad version".into()));
        }
        let chunk_payload_size = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let num_chunks = u32::from_le_bytes(buf[12..16].try_into().unwrap());
        let file_size_hint = u64::from_le_bytes(buf[16..24].try_into().unwrap());
        let name_len = u32::from_le_bytes(buf[24..28].try_into().unwrap()) as usize;
        let expected_crc = u32::from_le_bytes(buf[28..32].try_into().unwrap());

        let mut tmp = buf.clone();
        tmp[28..32].fill(0);
        let actual_crc = crc32fast::hash(&tmp[..MANIFEST_HEADER_LEN]);
        if actual_crc != expected_crc {
            return Err(Error::BadManifest("crc mismatch".into()));
        }

        let name_end = MANIFEST_HEADER_LEN + name_len;
        let bitmap_len = Self::bitmap_len(num_chunks);
        if buf.len() < name_end + bitmap_len {
            return Err(Error::BadManifest("truncated".into()));
        }
        let name = String::from_utf8_lossy(&buf[MANIFEST_HEADER_LEN..name_end]).into_owned();
        let bitmap = buf[name_end..name_end + bitmap_len].to_vec();
        Ok(Self {
            path: path.to_path_buf(),
            chunk_payload_size,
            num_chunks,
            file_size_hint,
            name,
            bitmap,
        })
    }

    fn save(&self) -> Result<()> {
        let bitmap_len = Self::bitmap_len(self.num_chunks);
        debug_assert_eq!(self.bitmap.len(), bitmap_len);
        let name_bytes = self.name.as_bytes();
        let mut buf = Vec::with_capacity(MANIFEST_HEADER_LEN + name_bytes.len() + bitmap_len);
        buf.extend_from_slice(&MANIFEST_MAGIC);
        buf.push(MANIFEST_VERSION);
        buf.push(0); // reserved
        buf.extend_from_slice(&[0u8, 0u8]); // reserved
        buf.extend_from_slice(&self.chunk_payload_size.to_le_bytes());
        buf.extend_from_slice(&self.num_chunks.to_le_bytes());
        buf.extend_from_slice(&self.file_size_hint.to_le_bytes());
        buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&[0u8; 4]); // crc placeholder
        let crc = crc32fast::hash(&buf[..MANIFEST_HEADER_LEN]);
        buf[28..32].copy_from_slice(&crc.to_le_bytes());
        buf.extend_from_slice(name_bytes);
        buf.extend_from_slice(&self.bitmap);
        // Atomic-rename for crash safety. The original code wrote in place,
        // which is exactly how it could end up with a manifest out of sync
        // with its data file after a power loss.
        let tmp = self.path.with_extension("parmanifest.tmp");
        {
            let mut f = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp)?;
            f.write_all(&buf)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }

    fn is_set(&self, chunk_id: u32) -> bool {
        let i = (chunk_id / 8) as usize;
        let b = chunk_id % 8;
        i < self.bitmap.len() && self.bitmap[i] & (1 << b) != 0
    }

    fn set(&mut self, chunk_id: u32) {
        let i = (chunk_id / 8) as usize;
        let b = chunk_id % 8;
        if i < self.bitmap.len() {
            self.bitmap[i] |= 1 << b;
        }
    }

    fn is_complete(&self) -> bool {
        // Every chunk bit must be set; check whole bytes then the trailing
        // partial byte.
        let total_bytes = self.bitmap.len();
        let full_bytes = (self.num_chunks / 8) as usize;
        if self.bitmap[..full_bytes].iter().any(|&b| b != 0xff) {
            return false;
        }
        let leftover = self.num_chunks % 8;
        if leftover != 0 && full_bytes < total_bytes {
            let mask = (1u8 << leftover) - 1;
            if self.bitmap[full_bytes] & mask != mask {
                return false;
            }
        }
        true
    }

    /// Compact list of (start_chunk, count) ranges that are still missing.
    fn missing_ranges(&self) -> Vec<(u32, u32)> {
        let mut out = Vec::new();
        let mut i = 0u32;
        let total = self.num_chunks;
        while i < total {
            if !self.is_set(i) {
                let start = i;
                while i < total && !self.is_set(i) {
                    i += 1;
                }
                out.push((start, i - start));
            } else {
                i += 1;
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{Packet, PacketFlags, PacketType};
    use tempfile::tempdir;

    fn pkt(file_id: i64, chunk_id: u32, num_chunks: u32, last: bool, data: &[u8]) -> Packet {
        let flags = if last {
            PacketFlags::empty().last()
        } else {
            PacketFlags::empty()
        };
        Packet {
            ptype: PacketType::Data,
            flags,
            file_id,
            chunk_id,
            num_chunks,
            payload: data.to_vec(),
        }
    }

    #[test]
    fn three_chunks_complete_with_name() {
        let dir = tempdir().unwrap();
        let r = Reassembler::new(dir.path().join("hold"), dir.path().join("final")).unwrap();
        let chunk_size = 4;
        let p1 = pkt(1, 0, 3, false, b"abcd");
        let p2 = pkt(1, 1, 3, false, b"efgh");
        let p3 = pkt(1, 2, 3, true, b"ij");
        assert_eq!(r.ingest(&p1).unwrap(), IngestOutcome::Accepted);
        assert_eq!(r.ingest(&p2).unwrap(), IngestOutcome::Accepted);
        assert_eq!(r.ingest(&p3).unwrap(), IngestOutcome::Accepted);

        // Name not yet known - assembly is data-complete but not finalisable.
        assert!(r.has_name(1).unwrap() == false);

        let manifest_pkt = Packet {
            ptype: PacketType::Manifest,
            flags: PacketFlags::empty(),
            file_id: 1,
            chunk_id: 0,
            num_chunks: 3,
            payload: b"my/dir/file.bin".to_vec(),
        };
        assert_eq!(r.ingest(&manifest_pkt).unwrap(), IngestOutcome::Complete);

        let final_path = r.finalize(1).unwrap();
        let bytes = std::fs::read(&final_path).unwrap();
        assert_eq!(bytes, b"abcdefghij");
        assert!(final_path.ends_with("my/dir/file.bin"));
        assert_eq!(chunk_size, 4); // sanity
    }

    #[test]
    fn duplicate_chunk_is_reported() {
        let dir = tempdir().unwrap();
        let r = Reassembler::new(dir.path().join("hold"), dir.path().join("final")).unwrap();
        let p = pkt(2, 0, 2, false, b"xx");
        assert_eq!(r.ingest(&p).unwrap(), IngestOutcome::Accepted);
        assert_eq!(r.ingest(&p).unwrap(), IngestOutcome::Duplicate);
    }

    #[test]
    fn missing_ranges_are_compact() {
        let dir = tempdir().unwrap();
        let r = Reassembler::new(dir.path().join("hold"), dir.path().join("final")).unwrap();
        // 10 chunks; receive 0,1,4,5,9.
        for i in [0u32, 1, 4, 5, 9] {
            r.ingest(&pkt(7, i, 10, i == 9, b"aa")).unwrap();
        }
        let m = r.missing_ranges(7).unwrap();
        assert_eq!(m, vec![(2, 2), (6, 3)]);
    }
}
