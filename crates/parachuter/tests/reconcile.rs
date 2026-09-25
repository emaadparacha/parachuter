//! End-to-end: a network outage swallows a whole file, the payload thinks it
//! was sent, and reconciliation finds it and brings it back.
//!
//! Uses the real ledger, chunker, reassembler and reconcile logic; only the
//! UDP link is simulated (we choose which packets "arrive").

use std::collections::{HashMap, HashSet};
use std::path::Path;

use chrono::{Duration, Utc};
use parachuter::chunker::Chunker;
use parachuter::ledger::Ledger;
use parachuter::reassembler::{IngestOutcome, Reassembler};
use parachuter::reconcile::{self, GroundState, ReconcileSummary, Verdict};
use tempfile::tempdir;

const CHUNK: usize = 1024;
const GRACE: u64 = 3 * 3600;

/// Downlink a file through the reassembler. `keep` decides which data chunks
/// survive the link; the manifest (filename) packet always survives here.
fn downlink(path: &Path, file_id: i64, r: &Reassembler, keep: impl Fn(u32) -> bool) -> bool {
    let mut c = Chunker::open(path, file_id, CHUNK).unwrap();
    let name = path.to_string_lossy().into_owned();
    let mut complete = r.ingest(&c.manifest_packet(&name)).unwrap() == IngestOutcome::Complete;
    for id in 0..c.num_chunks() {
        if keep(id) && r.ingest(&c.data_packet(id).unwrap()).unwrap() == IngestOutcome::Complete {
            complete = true;
        }
    }
    if complete {
        r.finalize(file_id).unwrap();
    }
    complete
}

fn write_file(path: &Path, len: usize, seed: u8) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(
        path,
        (0..len)
            .map(|i| (i as u8).wrapping_mul(31).wrapping_add(seed))
            .collect::<Vec<_>>(),
    )
    .unwrap();
}

#[test]
fn outage_swallowing_a_whole_file_is_found_and_recovered() {
    let payload = tempdir().unwrap();
    let ground = tempdir().unwrap();
    let final_dir = ground.path().join("downloaded_files");
    let r = Reassembler::new(ground.path().join("holding"), &final_dir).unwrap();

    // --- payload: three science files, all "sent" as far as the ledger knows
    let p = |rel: &str| payload.path().join(rel);
    let (a, b, c) = (
        p("data/qsc/science/m31_001.fits.bz2"),
        p("data/qsc/science/m31_002.fits.bz2"),
        p("data/qsc/science/m31_003.fits.bz2"),
    );
    write_file(&a, 5000, 1);
    write_file(&b, 7000, 2);
    write_file(&c, 3000, 3);
    let mut ledger = Ledger::open(payload.path().join("ledger.sqlite")).unwrap();
    let four_hours_ago = Utc::now() - Duration::hours(4);
    let mut ids = Vec::new();
    for f in [&a, &b, &c] {
        let id = ledger
            .upsert_file(
                &f.to_string_lossy(),
                std::fs::metadata(f).unwrap().len(),
                Utc::now(),
                0,
                0,
                0,
                CHUNK as u32,
            )
            .unwrap();
        ledger.mark_queued(id, four_hours_ago).unwrap();
        ids.push(id);
    }

    // --- the link: file A arrives, file B is cut mid-way, file C is lost whole
    assert!(downlink(&a, ids[0], &r, |_| true));
    assert!(!downlink(&b, ids[1], &r, |id| id < 3));
    // (file C: not a single packet reaches the ground)

    // --- the sender's ledger snapshot is downlinked like any other file
    let dumps = p("data/sql_status/status_dumps");
    std::fs::create_dir_all(&dumps).unwrap();
    let snap = dumps.join(format!("files_sent_list_{}.csv", Utc::now().timestamp()));
    ledger.dump_csv(&snap).unwrap();
    assert!(downlink(&snap, 99, &r, |_| true));

    // --- the ground finds it under final_dir at the mirrored payload path
    let (found, _) = reconcile::find_latest_snapshot(&final_dir).expect("snapshot landed");
    assert_eq!(
        found,
        reconcile::ground_path(&final_dir, &snap.to_string_lossy())
    );
    let rows = reconcile::parse_snapshot(&std::fs::read_to_string(&found).unwrap()).unwrap();

    let state = |r: &Reassembler| GroundState {
        in_flight: r
            .in_flight()
            .unwrap()
            .into_iter()
            .map(|id| (id, 0))
            .collect::<HashMap<_, _>>(),
        sender_pending: Some(HashSet::new()),
    };
    let verdicts = |plan: &[(reconcile::SnapshotRow, Verdict)]| {
        plan.iter()
            .map(|(row, v)| (row.file_id, v.clone()))
            .collect::<HashMap<_, _>>()
    };

    let plan = reconcile::plan(&rows, &final_dir, &state(&r), Utc::now(), GRACE);
    let v = verdicts(&plan);
    assert_eq!(
        v[&ids[0]],
        Verdict::Complete,
        "A landed and its size matches"
    );
    assert_eq!(
        v[&ids[1]],
        Verdict::InFlight { stale: false },
        "B is partial; the cleaner owns it"
    );
    assert_eq!(v[&ids[2]], Verdict::Missing, "C was lost whole");
    let s = ReconcileSummary::from_plan(&plan);
    assert_eq!((s.complete, s.in_flight, s.missing), (1, 1, 1));

    // Within the 3-hour grace period, C would not be called lost yet.
    let early = reconcile::plan(
        &rows,
        &final_dir,
        &state(&r),
        four_hours_ago + Duration::hours(1),
        GRACE,
    );
    assert_eq!(verdicts(&early)[&ids[2]], Verdict::TooRecent);

    // --- the sender resends C in full (what SenderEnqueue{start:-1} does),
    //     and the cleaner finishes B's gaps
    assert!(downlink(&c, ids[2], &r, |_| true));
    assert!(downlink(&b, ids[1], &r, |id| id >= 3));

    let plan = reconcile::plan(&rows, &final_dir, &state(&r), Utc::now(), GRACE);
    assert!(
        plan.iter().all(|(_, v)| *v == Verdict::Complete),
        "{plan:?}"
    );

    // And the files on the ground are byte-for-byte the payload's.
    for f in [&a, &b, &c] {
        let landed = reconcile::ground_path(&final_dir, &f.to_string_lossy());
        assert_eq!(std::fs::read(f).unwrap(), std::fs::read(landed).unwrap());
    }
}

#[test]
fn files_still_queued_on_the_sender_are_not_called_lost() {
    let ground = tempdir().unwrap();
    let rows = reconcile::parse_snapshot(
        "file_id,file_name,file_size,still_exists,queued_at\n5,/data/waiting.fits.bz2,10,t,2026-01-01T00:00:00+00:00\n",
    )
    .unwrap();
    let g = GroundState {
        in_flight: HashMap::new(),
        sender_pending: Some(HashSet::from([5])),
    };
    assert_eq!(
        reconcile::classify(&rows[0], ground.path(), &g, Utc::now(), GRACE),
        Verdict::Queued
    );
}
