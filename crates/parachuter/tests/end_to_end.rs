//! End-to-end integration tests for `parachuter`.
//!
//! These tests exercise the full stack from [`Chunker`] through the
//! on-the-wire [`Packet`] encoding, through [`Reassembler`], all without
//! standing up real UDP sockets. They verify:
//!
//! * Correct packet framing (CRC, magic, version).
//! * Correct chunking for exact-multiple and non-multiple file sizes.
//! * Out-of-order chunk delivery; manifest-first and data-first both work.
//! * Last-chunk-first deferral for multi-chunk files.
//! * Duplicate detection.
//! * Missing-range reporting.
//! * Empty files produce exactly one zero-length chunk.
//! * Large files spanning many bitmap bytes.
//! * Ledger idempotency and CSV export.
//! * Config validation rejects bad inputs.

use parachuter::chunker::{chunks_for_size, Chunker};
use parachuter::ledger::Ledger;
use parachuter::proto::{Packet, PacketFlags, PacketType, HEADER_LEN};
use parachuter::reassembler::{IngestOutcome, Reassembler};
use tempfile::tempdir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a raw Data packet without going through Chunker.
fn data_pkt(file_id: i64, chunk_id: u32, num_chunks: u32, last: bool, payload: &[u8]) -> Packet {
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
        payload: payload.to_vec(),
    }
}

/// Build a Manifest packet.
fn manifest_pkt(file_id: i64, num_chunks: u32, name: &str) -> Packet {
    Packet {
        ptype: PacketType::Manifest,
        flags: PacketFlags::empty(),
        file_id,
        chunk_id: 0,
        num_chunks,
        payload: name.as_bytes().to_vec(),
    }
}

/// Round-trip a packet through encode → decode.
fn wire(pkt: &Packet) -> Packet {
    Packet::decode(&pkt.encode()).expect("round-trip decode must not fail")
}

// ---------------------------------------------------------------------------
// Test 1 – basic out-of-order delivery (even chunks, manifest, odd chunks)
// ---------------------------------------------------------------------------

#[test]
fn chunker_roundtrips_through_reassembler_out_of_order() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("source.bin");
    let payload: Vec<u8> = (0..13_000u32).map(|i| (i % 251) as u8).collect();
    std::fs::write(&src, &payload).unwrap();

    let mut chunker = Chunker::open(&src, 100, 4096).unwrap();
    let manifest = chunker.manifest_packet("/data/example/source.bin");
    let data_packets: Vec<Packet> = chunker.iter_data().collect::<Result<_, _>>().unwrap();

    // Re-encode and re-decode to prove the wire format is symmetric.
    let encoded: Vec<Vec<u8>> = std::iter::once(manifest.encode())
        .chain(data_packets.iter().map(|p| p.encode()))
        .collect();
    let parsed: Vec<Packet> = encoded.iter().map(|b| Packet::decode(b).unwrap()).collect();

    let r = Reassembler::new(dir.path().join("hold"), dir.path().join("final")).unwrap();

    // Even-indexed data first, then manifest, then odd data.
    let mut evens = Vec::new();
    let mut odds = Vec::new();
    for (i, p) in parsed[1..].iter().enumerate() {
        if i % 2 == 0 {
            evens.push(p);
        } else {
            odds.push(p);
        }
    }
    for p in &evens {
        let _ = r.ingest(p).unwrap();
    }
    let _ = r.ingest(&parsed[0]).unwrap(); // manifest
    let mut last = IngestOutcome::Accepted;
    for p in &odds {
        last = r.ingest(p).unwrap();
    }
    assert_eq!(last, IngestOutcome::Complete);

    let final_path = r.finalize(100).unwrap();
    let on_disk = std::fs::read(&final_path).unwrap();
    assert_eq!(on_disk, payload);
}

// ---------------------------------------------------------------------------
// Test 2 – manifest arrives BEFORE any data chunks (the manifest-first path)
//          Previously produced corrupt output (all chunks wrote at offset 0)
//          because ingest_manifest created the sidecar with chunk_payload_size=0.
// ---------------------------------------------------------------------------

#[test]
fn manifest_first_then_data_produces_correct_output() {
    let dir = tempdir().unwrap();
    let r = Reassembler::new(dir.path().join("hold"), dir.path().join("final")).unwrap();

    // 3 chunks of 4 bytes each (except the last = 2 bytes).
    let name = "subdir/manifest_first.bin";
    let mp = manifest_pkt(55, 3, name);
    assert_eq!(r.ingest(&wire(&mp)).unwrap(), IngestOutcome::Accepted);

    let p0 = data_pkt(55, 0, 3, false, b"abcd");
    let p1 = data_pkt(55, 1, 3, false, b"efgh");
    let p2 = data_pkt(55, 2, 3, true, b"ij");
    assert_eq!(r.ingest(&wire(&p0)).unwrap(), IngestOutcome::Accepted);
    assert_eq!(r.ingest(&wire(&p1)).unwrap(), IngestOutcome::Accepted);
    assert_eq!(r.ingest(&wire(&p2)).unwrap(), IngestOutcome::Complete);

    let final_path = r.finalize(55).unwrap();
    let bytes = std::fs::read(&final_path).unwrap();
    assert_eq!(bytes, b"abcdefghij");
    assert!(final_path.ends_with(name));
}

// ---------------------------------------------------------------------------
// Test 3 – data arrives first, then manifest (the happy path)
// ---------------------------------------------------------------------------

#[test]
fn data_first_then_manifest_produces_correct_output() {
    let dir = tempdir().unwrap();
    let r = Reassembler::new(dir.path().join("hold"), dir.path().join("final")).unwrap();

    let p0 = data_pkt(10, 0, 2, false, b"hello, ");
    let p1 = data_pkt(10, 1, 2, true, b"world");
    assert_eq!(r.ingest(&wire(&p0)).unwrap(), IngestOutcome::Accepted);
    assert_eq!(r.ingest(&wire(&p1)).unwrap(), IngestOutcome::Accepted);

    assert!(!r.has_name(10).unwrap(), "name not yet known");

    let mp = manifest_pkt(10, 2, "hello/world.txt");
    assert_eq!(r.ingest(&wire(&mp)).unwrap(), IngestOutcome::Complete);

    let final_path = r.finalize(10).unwrap();
    let bytes = std::fs::read(&final_path).unwrap();
    assert_eq!(bytes, b"hello, world");
}

// ---------------------------------------------------------------------------
// Test 4 – single-chunk file (common for small telemetry packets)
// ---------------------------------------------------------------------------

#[test]
fn single_chunk_file_roundtrips() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("tiny.bin");
    std::fs::write(&src, b"tiny payload").unwrap();

    let mut chunker = Chunker::open(&src, 7, 1024).unwrap();
    assert_eq!(chunker.num_chunks(), 1);
    assert_eq!(chunker.file_size(), 12);

    let r = Reassembler::new(dir.path().join("hold"), dir.path().join("final")).unwrap();
    let dp = chunker.data_packet(0).unwrap();
    assert!(dp.flags.is_last(), "single chunk must have LAST_CHUNK set");

    assert_eq!(r.ingest(&wire(&dp)).unwrap(), IngestOutcome::Accepted);
    let mp = chunker.manifest_packet("tiny.bin");
    assert_eq!(r.ingest(&wire(&mp)).unwrap(), IngestOutcome::Complete);

    let fp = r.finalize(7).unwrap();
    assert_eq!(std::fs::read(&fp).unwrap(), b"tiny payload");
}

// ---------------------------------------------------------------------------
// Test 5 – empty file (zero bytes). Chunker must produce exactly one chunk.
// ---------------------------------------------------------------------------

#[test]
fn empty_file_produces_one_zero_length_chunk() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("empty.bin");
    std::fs::write(&src, b"").unwrap();

    let mut chunker = Chunker::open(&src, 99, 512).unwrap();
    assert_eq!(chunker.num_chunks(), 1);
    assert_eq!(chunker.file_size(), 0);

    let dp = chunker.data_packet(0).unwrap();
    assert!(dp.flags.is_last());
    assert_eq!(dp.payload.len(), 0);

    let r = Reassembler::new(dir.path().join("hold"), dir.path().join("final")).unwrap();
    r.ingest(&wire(&dp)).unwrap();
    let mp = chunker.manifest_packet("empty.bin");
    assert_eq!(r.ingest(&wire(&mp)).unwrap(), IngestOutcome::Complete);

    let fp = r.finalize(99).unwrap();
    assert_eq!(std::fs::read(&fp).unwrap(), b"");
}

// ---------------------------------------------------------------------------
// Test 6 – file size is an EXACT multiple of payload size.
//          The original code produced a spurious empty last chunk here.
// ---------------------------------------------------------------------------

#[test]
fn exact_payload_multiple_no_spurious_chunk() {
    let chunk_size = 512usize;
    let payload_size = chunk_size - HEADER_LEN; // 480

    let data: Vec<u8> = (0..payload_size * 3).map(|i| (i & 0xff) as u8).collect();
    assert_eq!(
        chunks_for_size(data.len() as u64, payload_size),
        3,
        "exact multiple must not produce a 4th empty chunk"
    );

    let dir = tempdir().unwrap();
    let src = dir.path().join("exact.bin");
    std::fs::write(&src, &data).unwrap();

    let mut chunker = Chunker::open(&src, 42, chunk_size).unwrap();
    assert_eq!(chunker.num_chunks(), 3);

    let r = Reassembler::new(dir.path().join("hold"), dir.path().join("final")).unwrap();
    for pkt in chunker.iter_data() {
        r.ingest(&wire(&pkt.unwrap())).unwrap();
    }
    let mp = chunker.manifest_packet("exact.bin");
    assert_eq!(r.ingest(&wire(&mp)).unwrap(), IngestOutcome::Complete);

    let fp = r.finalize(42).unwrap();
    assert_eq!(std::fs::read(&fp).unwrap(), data);
}

// ---------------------------------------------------------------------------
// Test 7 – large file spanning many bitmap bytes (> 64 chunks = > 8 bitmap bytes)
//          to exercise byte boundaries in is_complete().
// ---------------------------------------------------------------------------

#[test]
fn large_file_many_chunks_bitmap_boundary() {
    // 67 chunks → bitmap needs 9 bytes (covers the 64-chunk byte boundary).
    let payload_size = 100usize;
    let chunk_size = payload_size + HEADER_LEN;
    let data: Vec<u8> = (0..payload_size * 67).map(|i| (i & 0xff) as u8).collect();

    let dir = tempdir().unwrap();
    let src = dir.path().join("big.bin");
    std::fs::write(&src, &data).unwrap();

    let mut chunker = Chunker::open(&src, 200, chunk_size).unwrap();
    assert_eq!(chunker.num_chunks(), 67);

    let r = Reassembler::new(dir.path().join("hold"), dir.path().join("final")).unwrap();

    // Deliver in reverse order to stress-test the bitmap.
    let pkts: Vec<Packet> = chunker.iter_data().collect::<Result<_, _>>().unwrap();
    for p in pkts.iter().rev() {
        r.ingest(&wire(p)).unwrap();
    }
    // The last chunk arrives first under reverse delivery; per the design
    // tested in `last_chunk_first_multi_chunk_returns_mismatched`, the
    // reassembler drops it because it can't infer chunk_payload_size from a
    // possibly-partial tail. The sender retransmits it once earlier chunks
    // have populated the manifest.
    r.ingest(&wire(pkts.last().unwrap())).unwrap();
    let mp = chunker.manifest_packet("big.bin");
    assert_eq!(r.ingest(&wire(&mp)).unwrap(), IngestOutcome::Complete);

    let fp = r.finalize(200).unwrap();
    assert_eq!(std::fs::read(&fp).unwrap(), data);
}

// ---------------------------------------------------------------------------
// Test 8 – duplicate chunk is reported and not applied a second time.
// ---------------------------------------------------------------------------

#[test]
fn duplicate_chunk_is_reported_and_idempotent() {
    let dir = tempdir().unwrap();
    let r = Reassembler::new(dir.path().join("hold"), dir.path().join("final")).unwrap();

    let p = data_pkt(2, 0, 2, false, b"xxxx");
    assert_eq!(r.ingest(&wire(&p)).unwrap(), IngestOutcome::Accepted);
    assert_eq!(r.ingest(&wire(&p)).unwrap(), IngestOutcome::Duplicate);
    assert_eq!(r.ingest(&wire(&p)).unwrap(), IngestOutcome::Duplicate); // third time too
}

// ---------------------------------------------------------------------------
// Test 9 – missing_ranges() returns compact (start, count) pairs.
// ---------------------------------------------------------------------------

#[test]
fn missing_ranges_compact() {
    let dir = tempdir().unwrap();
    let r = Reassembler::new(dir.path().join("hold"), dir.path().join("final")).unwrap();

    // 10 chunks; receive 0, 1, 4, 5, 9.
    for i in [0u32, 1, 4, 5, 9] {
        r.ingest(&wire(&data_pkt(7, i, 10, i == 9, b"aa"))).unwrap();
    }
    let m = r.missing_ranges(7).unwrap();
    assert_eq!(m, vec![(2, 2), (6, 3)]);
}

// ---------------------------------------------------------------------------
// Test 10 – stats() returns sensible numbers mid-assembly.
// ---------------------------------------------------------------------------

#[test]
fn assembly_stats_are_accurate() {
    let dir = tempdir().unwrap();
    let r = Reassembler::new(dir.path().join("hold"), dir.path().join("final")).unwrap();

    for i in 0u32..5 {
        r.ingest(&wire(&data_pkt(9, i, 10, i == 9, b"bb"))).unwrap();
    }
    let stats = r.stats(9).unwrap();
    assert_eq!(stats.chunks_total, 10);
    assert_eq!(stats.chunks_received, 5);
    assert!(!stats.has_name);
}

// ---------------------------------------------------------------------------
// Test 11 – packet corruption detected via CRC.
// ---------------------------------------------------------------------------

#[test]
fn detects_chunk_corruption() {
    use parachuter::Error;
    let pkt = Packet {
        ptype: PacketType::Data,
        flags: PacketFlags::empty().last(),
        file_id: 1,
        chunk_id: 0,
        num_chunks: 1,
        payload: b"hello world".to_vec(),
    };
    // Corrupt a payload byte.
    let mut bytes = pkt.encode();
    bytes[HEADER_LEN] ^= 0xff;
    assert!(matches!(
        Packet::decode(&bytes),
        Err(parachuter::Error::BadPacket("crc mismatch"))
    ));
    // Corrupt the magic bytes.
    let mut bytes2 = pkt.encode();
    bytes2[0] = b'X';
    assert!(matches!(
        Packet::decode(&bytes2),
        Err(Error::BadPacket("bad magic"))
    ));
}

// ---------------------------------------------------------------------------
// Test 12 – Ledger.upsert_file() is idempotent by name.
// ---------------------------------------------------------------------------

#[test]
fn ledger_upsert_is_idempotent() {
    let dir = tempdir().unwrap();
    let mut l = Ledger::open(dir.path().join("ledger.sqlite")).unwrap();
    let now = chrono::Utc::now();
    let id1 = l.upsert_file("/data/foo.fits", 100, now, 0, 0, 0, 16192).unwrap();
    let id2 = l.upsert_file("/data/foo.fits", 100, now, 0, 0, 0, 16192).unwrap();
    assert_eq!(id1, id2, "same name must yield same file_id");
}

// ---------------------------------------------------------------------------
// Test 13 – Ledger.mark_gone() is persisted.
// ---------------------------------------------------------------------------

#[test]
fn ledger_mark_gone_persists() {
    let dir = tempdir().unwrap();
    let mut l = Ledger::open(dir.path().join("ledger.sqlite")).unwrap();
    l.upsert_file("/data/x.bin", 1, chrono::Utc::now(), 0, 0, 0, 16192).unwrap();
    l.mark_gone("/data/x.bin").unwrap();
    let r = l.get_by_name("/data/x.bin").unwrap().unwrap();
    assert!(!r.still_exists);
}

// ---------------------------------------------------------------------------
// Test 14 – Ledger.dump_csv() produces a parseable CSV with the right rows.
// ---------------------------------------------------------------------------

#[test]
fn ledger_csv_dump_is_parseable() {
    let dir = tempdir().unwrap();
    let mut l = Ledger::open(dir.path().join("ledger.sqlite")).unwrap();
    let now = chrono::Utc::now();
    l.upsert_file("/science/img_001.fits", 123456, now, 5, 3, 2, 16192).unwrap();
    l.upsert_file("/science/img_002.fits", 7890, now, 5, 3, 2, 16192).unwrap();

    let csv_path = dir.path().join("out.csv");
    l.dump_csv(&csv_path).unwrap();

    let content = std::fs::read_to_string(&csv_path).unwrap();
    let mut lines = content.lines();
    let header = lines.next().unwrap();
    assert!(header.starts_with("file_id,file_name,file_size"), "bad CSV header: {header}");
    let rows: Vec<&str> = lines.collect();
    assert_eq!(rows.len(), 2);
    assert!(rows[0].contains("/science/img_001.fits"));
    assert!(rows[1].contains("/science/img_002.fits"));
}

// ---------------------------------------------------------------------------
// Test 15 – chunks_for_size is correct at boundary values.
// ---------------------------------------------------------------------------

#[test]
fn chunks_for_size_boundary_values() {
    assert_eq!(chunks_for_size(0, 1024), 1, "empty file → 1 chunk");
    assert_eq!(chunks_for_size(1, 1024), 1, "1 byte → 1 chunk");
    assert_eq!(chunks_for_size(1024, 1024), 1, "exact multiple → 1 chunk (not 2)");
    assert_eq!(chunks_for_size(1025, 1024), 2, "1 over → 2 chunks");
    assert_eq!(chunks_for_size(2048, 1024), 2, "exact double → 2 chunks (not 3)");
    assert_eq!(chunks_for_size(2049, 1024), 3, "1 over double → 3 chunks");
}

// ---------------------------------------------------------------------------
// Test 16 – all packet types survive wire encoding.
// ---------------------------------------------------------------------------

#[test]
fn all_packet_types_roundtrip() {
    for ptype in [
        PacketType::Data,
        PacketType::Manifest,
        PacketType::Retransmit,
        PacketType::NameOnly,
        PacketType::Heartbeat,
    ] {
        let p = Packet {
            ptype,
            flags: PacketFlags::empty(),
            file_id: 1,
            chunk_id: 0,
            num_chunks: 1,
            payload: b"test".to_vec(),
        };
        let decoded = wire(&p);
        assert_eq!(decoded.ptype, ptype, "ptype must survive wire for {ptype:?}");
    }
}

// ---------------------------------------------------------------------------
// Test 17 – in_flight() lists the right file IDs.
// ---------------------------------------------------------------------------

#[test]
fn in_flight_lists_all_active_assemblies() {
    let dir = tempdir().unwrap();
    let r = Reassembler::new(dir.path().join("hold"), dir.path().join("final")).unwrap();

    for id in [11i64, 22, 33] {
        r.ingest(&wire(&data_pkt(id, 0, 2, false, b"xx"))).unwrap();
    }
    let mut ids = r.in_flight().unwrap();
    ids.sort_unstable();
    assert_eq!(ids, vec![11, 22, 33]);
}

// ---------------------------------------------------------------------------
// Test 18 – PacketType::Retransmit is treated identically to Data by the
//           reassembler (only the sender distinguishes them).
// ---------------------------------------------------------------------------

#[test]
fn retransmit_packet_accepted_by_reassembler() {
    let dir = tempdir().unwrap();
    let r = Reassembler::new(dir.path().join("hold"), dir.path().join("final")).unwrap();

    let p0 = data_pkt(50, 0, 2, false, b"AAAA");
    r.ingest(&wire(&p0)).unwrap();

    let p1 = Packet {
        ptype: PacketType::Retransmit,
        flags: PacketFlags::empty().last(),
        file_id: 50,
        chunk_id: 1,
        num_chunks: 2,
        payload: b"BBBB".to_vec(),
    };
    assert_eq!(r.ingest(&wire(&p1)).unwrap(), IngestOutcome::Accepted);

    let mp = manifest_pkt(50, 2, "retx/test.bin");
    assert_eq!(r.ingest(&wire(&mp)).unwrap(), IngestOutcome::Complete);

    let fp = r.finalize(50).unwrap();
    assert_eq!(std::fs::read(&fp).unwrap(), b"AAAABBBB");
}

// ---------------------------------------------------------------------------
// Test 19 – last-chunk-first with num_chunks > 1 is deferred (Mismatched).
//           Without the fix, chunk_payload_size would remain 0 and subsequent
//           chunks would write at offset 0, corrupting the file.
// ---------------------------------------------------------------------------

#[test]
fn last_chunk_first_multi_chunk_returns_mismatched() {
    let dir = tempdir().unwrap();
    let r = Reassembler::new(dir.path().join("hold"), dir.path().join("final")).unwrap();

    // Deliver only the last chunk of a 3-chunk file with no prior context.
    let p_last = data_pkt(77, 2, 3, true, b"tail");
    assert_eq!(
        r.ingest(&wire(&p_last)).unwrap(),
        IngestOutcome::Mismatched,
        "last chunk first with no prior context must be deferred"
    );

    // Now the sender delivers the earlier chunks; assembly can proceed.
    let p0 = data_pkt(77, 0, 3, false, b"AAAA");
    assert_eq!(r.ingest(&wire(&p0)).unwrap(), IngestOutcome::Accepted);
    let p1 = data_pkt(77, 1, 3, false, b"BBBB");
    assert_eq!(r.ingest(&wire(&p1)).unwrap(), IngestOutcome::Accepted);

    // The last chunk must be retransmitted (it was dropped earlier).
    assert_eq!(r.ingest(&wire(&p_last)).unwrap(), IngestOutcome::Accepted);

    let mp = manifest_pkt(77, 3, "deferred/last_first.bin");
    assert_eq!(r.ingest(&wire(&mp)).unwrap(), IngestOutcome::Complete);
}

// ---------------------------------------------------------------------------
// Test 20 – config default validates successfully.
// ---------------------------------------------------------------------------

#[test]
fn config_default_validates() {
    parachuter::config::Config::default().validate().unwrap();
}

// ---------------------------------------------------------------------------
// Test 21 – config rejects an unknown active_link.
// ---------------------------------------------------------------------------

#[test]
fn config_rejects_unknown_active_link() {
    let mut c = parachuter::config::Config::default();
    c.sender.active_link = "nonexistent_link".into();
    assert!(matches!(c.validate(), Err(parachuter::Error::BadConfig(_))));
}

// ---------------------------------------------------------------------------
// Test 22 – config rejects a chunk size smaller than header + minimum payload.
// ---------------------------------------------------------------------------

#[test]
fn config_rejects_silly_chunk_size() {
    let mut c = parachuter::config::Config::default();
    c.sender.chunk_size = 4;
    assert!(matches!(c.validate(), Err(parachuter::Error::BadChunkSize(4))));
}

// ---------------------------------------------------------------------------
// Test 23 – Chunker returns NotFound for a missing file.
// ---------------------------------------------------------------------------

#[test]
fn chunker_missing_file_returns_not_found() {
    let result = Chunker::open("/this/path/does/not/exist.bin", 1, 1024);
    assert!(
        matches!(result, Err(parachuter::Error::NotFound(_))),
        "expected NotFound, got {result:?}"
    );
}

// ---------------------------------------------------------------------------
// Test 24 – Full Chunker → wire → Reassembler round-trip with an awkward size.
// ---------------------------------------------------------------------------

#[test]
fn full_chunker_reassembler_roundtrip_awkward_size() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("roundtrip.bin");

    let chunk_size = 256usize;
    let payload_size = chunk_size - HEADER_LEN;
    // 1023 bytes: not a multiple of payload_size (224).
    let data: Vec<u8> = (0..1023u32).map(|i| (i.wrapping_mul(7) & 0xff) as u8).collect();
    std::fs::write(&src, &data).unwrap();

    let mut chunker = Chunker::open(&src, 300, chunk_size).unwrap();
    assert_eq!(chunker.num_chunks(), chunks_for_size(1023, payload_size));

    let r = Reassembler::new(dir.path().join("hold"), dir.path().join("final")).unwrap();

    // Manifest first (exercises the manifest-first fix).
    let manifest = chunker.manifest_packet("roundtrip/out.bin");
    r.ingest(&wire(&manifest)).unwrap();

    for pkt in chunker.iter_data() {
        r.ingest(&wire(&pkt.unwrap())).unwrap();
    }

    let stats = r.stats(300).unwrap();
    assert_eq!(stats.chunks_received, stats.chunks_total);
    assert!(stats.has_name);

    let fp = r.finalize(300).unwrap();
    assert_eq!(std::fs::read(&fp).unwrap(), data);
}
