//! Ground-side reconciliation: catch files that were lost *whole*.
//!
//! The cleaner repairs gaps in files the ground already knows about. It
//! cannot help with a file whose every packet vanished (a long outage, or a
//! sender restart with a queue in memory): the ground never created a
//! manifest for it, and the sender's ledger says it was sent.
//!
//! The fix needs no acknowledgements and no extra state on the ground:
//!
//! 1. The sender periodically writes its ledger to
//!    `files_sent_list_<unix>.csv` inside its first priority directory, so
//!    the snapshot is downlinked like any other file.
//! 2. Every file lands on the ground at `final_dir` + its full payload path,
//!    and only once it is complete (holding → final is an atomic rename).
//! 3. So for every row in the newest snapshot, "is it in `final_dir` at the
//!    mirrored path with the right size?" is the whole answer. Anything
//!    missing, not still in flight, not still queued, and older than a grace
//!    period is requested again, in full, at the back of the sender's queue.
//!
//! This module holds the pure parts (finding and parsing the snapshot,
//! mapping paths, classifying rows) so they can be tested without sockets.
//! The cleaner daemon does the I/O.

use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// File-name prefix of ledger snapshots written by the sender.
pub const SNAPSHOT_PREFIX: &str = "files_sent_list_";

/// Where a payload path lands on the ground: `final_dir` + the path with its
/// root stripped, so `/data/qsc/science/m31_002.fits.bz2` becomes
/// `<final_dir>/data/qsc/science/m31_002.fits.bz2`.
///
/// `..`, root and drive-prefix components are dropped, so a malformed or
/// hostile name can never escape `final_dir`.
pub fn ground_path(final_dir: &Path, payload_path: &str) -> PathBuf {
    let mut out = final_dir.to_path_buf();
    for c in Path::new(payload_path).components() {
        if let Component::Normal(part) = c {
            out.push(part);
        }
    }
    out
}

/// One row of a ledger snapshot, as far as reconciliation cares.
#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotRow {
    /// Ledger id, the same `file_id` used on the wire.
    pub file_id: i64,
    /// Full path on the payload.
    pub file_name: String,
    /// Size in bytes when registered.
    pub file_size: u64,
    /// File creation time on the payload.
    pub created_at: Option<DateTime<Utc>>,
    /// `false` once the sender noticed the file was deleted.
    pub still_exists: bool,
    /// When the file was last queued for a full send.
    pub queued_at: Option<DateTime<Utc>>,
}

/// Find the newest `files_sent_list_<unix>.csv` anywhere under `final_dir`.
/// Returns its path and the unix timestamp from its name.
pub fn find_latest_snapshot(final_dir: &Path) -> Option<(PathBuf, i64)> {
    walkdir::WalkDir::new(final_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter_map(|e| {
            let name = e.file_name().to_str()?;
            let ts = name.strip_prefix(SNAPSHOT_PREFIX)?.strip_suffix(".csv")?;
            let ts: i64 = ts.parse().ok()?;
            Some((e.into_path(), ts))
        })
        .max_by_key(|(_, ts)| *ts)
}

/// Parse a ledger snapshot. Columns are found by header name, so snapshots
/// from older senders (without `queued_at`) still parse.
pub fn parse_snapshot(text: &str) -> Result<Vec<SnapshotRow>> {
    let mut lines = text.lines().filter(|l| !l.trim().is_empty());
    let header = lines
        .next()
        .ok_or_else(|| Error::BadConfig("empty ledger snapshot".into()))?;
    let cols = split_csv_line(header);
    let idx = |name: &str| cols.iter().position(|c| c == name);
    let (Some(i_id), Some(i_name), Some(i_size)) =
        (idx("file_id"), idx("file_name"), idx("file_size"))
    else {
        return Err(Error::BadConfig(
            "ledger snapshot is missing file_id, file_name or file_size".into(),
        ));
    };
    let (i_created, i_exists, i_queued) =
        (idx("created_at"), idx("still_exists"), idx("queued_at"));

    let time = |f: &[String], i: Option<usize>| {
        i.and_then(|i| f.get(i))
            .filter(|s| !s.is_empty())
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|d| d.with_timezone(&Utc))
    };
    let mut rows = Vec::new();
    for (n, line) in lines.enumerate() {
        let f = split_csv_line(line);
        let bad = || Error::BadConfig(format!("ledger snapshot row {} is malformed", n + 2));
        rows.push(SnapshotRow {
            file_id: f.get(i_id).and_then(|s| s.parse().ok()).ok_or_else(bad)?,
            file_name: f.get(i_name).cloned().ok_or_else(bad)?,
            file_size: f.get(i_size).and_then(|s| s.parse().ok()).ok_or_else(bad)?,
            created_at: time(&f, i_created),
            still_exists: i_exists
                .and_then(|i| f.get(i))
                .map(|s| s != "f" && s != "0" && s != "false")
                .unwrap_or(true),
            queued_at: time(&f, i_queued),
        });
    }
    Ok(rows)
}

/// Split one CSV line, honouring double-quoted fields with `""` escapes
/// (the format `Ledger::dump_csv` writes).
fn split_csv_line(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut quoted = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match (c, quoted) {
            ('"', true) if chars.peek() == Some(&'"') => {
                cur.push('"');
                chars.next();
            }
            ('"', _) => quoted = !quoted,
            (',', false) => out.push(std::mem::take(&mut cur)),
            _ => cur.push(c),
        }
    }
    out.push(cur);
    out
}

/// What the reconciler concluded about one file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Verdict {
    /// On the ground at the mirrored path with the expected size.
    Complete,
    /// On the ground, but the size differs from the ledger. Reported, not
    /// re-requested: the payload copy probably changed after it was sent,
    /// and a resend would not fix the mismatch.
    SizeMismatch {
        /// Size in the ledger.
        expected: u64,
        /// Size found on the ground.
        found: u64,
    },
    /// Partially received; the cleaner is already chasing its gaps.
    InFlight {
        /// `true` if the assembly has not changed for longer than the grace
        /// period, which is worth an operator's attention.
        stale: bool,
    },
    /// Still waiting in the sender's queue.
    Queued,
    /// Queued too recently to call it lost.
    TooRecent,
    /// Deleted on the payload, so it cannot be resent.
    GoneOnPayload,
    /// Lost whole: request it again.
    Missing,
}

/// Everything the reconciler knows about the ground and the sender right now.
#[derive(Debug, Default)]
pub struct GroundState {
    /// file_id → seconds since the assembly last changed, for every
    /// assembly in the holding directory.
    pub in_flight: HashMap<i64, u64>,
    /// file_ids with anything still queued on the sender. `None` if the
    /// sender could not be asked.
    pub sender_pending: Option<HashSet<i64>>,
}

/// Classify one snapshot row.
pub fn classify(
    row: &SnapshotRow,
    final_dir: &Path,
    ground: &GroundState,
    now: DateTime<Utc>,
    grace_secs: u64,
) -> Verdict {
    let landed = ground_path(final_dir, &row.file_name);
    if let Ok(meta) = std::fs::metadata(&landed) {
        if meta.is_file() {
            return if meta.len() == row.file_size {
                Verdict::Complete
            } else {
                Verdict::SizeMismatch {
                    expected: row.file_size,
                    found: meta.len(),
                }
            };
        }
    }
    if let Some(age) = ground.in_flight.get(&row.file_id) {
        return Verdict::InFlight {
            stale: *age > grace_secs,
        };
    }
    if ground
        .sender_pending
        .as_ref()
        .is_some_and(|p| p.contains(&row.file_id))
    {
        return Verdict::Queued;
    }
    if !row.still_exists {
        return Verdict::GoneOnPayload;
    }
    // Rows from ledgers that predate queued_at fall back to creation time.
    if let Some(since) = row.queued_at.or(row.created_at) {
        if (now - since).num_seconds() < grace_secs as i64 {
            return Verdict::TooRecent;
        }
    }
    Verdict::Missing
}

/// Classify every row of a snapshot.
pub fn plan(
    rows: &[SnapshotRow],
    final_dir: &Path,
    ground: &GroundState,
    now: DateTime<Utc>,
    grace_secs: u64,
) -> Vec<(SnapshotRow, Verdict)> {
    rows.iter()
        .map(|r| (r.clone(), classify(r, final_dir, ground, now, grace_secs)))
        .collect()
}

/// Counts from one reconciliation pass, reported in the cleaner's status.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconcileSummary {
    /// When the pass ran (unix seconds, ground clock).
    pub ran_at_unix: i64,
    /// Snapshot file used, if one was found.
    pub snapshot: Option<String>,
    /// Rows in the snapshot.
    pub total: u32,
    /// Landed and verified.
    pub complete: u32,
    /// Partially received and being repaired.
    pub in_flight: u32,
    /// In flight but unchanged for longer than the grace period.
    pub stale_in_flight: u32,
    /// Still in the sender's queue.
    pub queued: u32,
    /// Queued within the grace period.
    pub too_recent: u32,
    /// Deleted on the payload.
    pub gone_on_payload: u32,
    /// Landed with the wrong size.
    pub size_mismatch: u32,
    /// Lost whole.
    pub missing: u32,
    /// Whole-file resends actually requested this pass.
    pub requested: u32,
}

impl ReconcileSummary {
    /// Tally a plan.
    pub fn from_plan(plan: &[(SnapshotRow, Verdict)]) -> Self {
        let mut s = Self {
            total: plan.len() as u32,
            ..Default::default()
        };
        for (_, v) in plan {
            match v {
                Verdict::Complete => s.complete += 1,
                Verdict::SizeMismatch { .. } => s.size_mismatch += 1,
                Verdict::InFlight { stale } => {
                    s.in_flight += 1;
                    if *stale {
                        s.stale_in_flight += 1;
                    }
                }
                Verdict::Queued => s.queued += 1,
                Verdict::TooRecent => s.too_recent += 1,
                Verdict::GoneOnPayload => s.gone_on_payload += 1,
                Verdict::Missing => s.missing += 1,
            }
        }
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn t(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn ground_path_mirrors_payload_path() {
        let p = ground_path(
            Path::new("/downloaded_files"),
            "/data/qsc/science/m31_002.fits.bz2",
        );
        assert_eq!(
            p,
            PathBuf::from("/downloaded_files/data/qsc/science/m31_002.fits.bz2")
        );
    }

    #[test]
    fn ground_path_cannot_escape_final_dir() {
        let p = ground_path(Path::new("/downloaded_files"), "/data/../../etc/passwd");
        assert_eq!(p, PathBuf::from("/downloaded_files/data/etc/passwd"));
        assert!(p.starts_with("/downloaded_files"));
    }

    #[test]
    fn parses_current_and_old_snapshots() {
        let now = "file_id,file_name,file_size,created_at,images_per_dark,images_per_flat,images_per_bias,still_exists,chunk_size,sha256,queued_at\n\
                   7,\"/data/odd, name.fits.bz2\",100,2026-09-25T10:00:00+00:00,0,0,0,t,16192,,2026-09-25T11:00:00+00:00\n\
                   8,/data/gone.fits.bz2,5,2026-09-25T10:00:00+00:00,0,0,0,f,16192,,\n";
        let rows = parse_snapshot(now).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].file_name, "/data/odd, name.fits.bz2");
        assert_eq!(rows[0].queued_at, Some(t("2026-09-25T11:00:00Z")));
        assert!(!rows[1].still_exists);
        assert_eq!(rows[1].queued_at, None);

        let old = "file_id,file_name,file_size,created_at,images_per_dark,images_per_flat,images_per_bias,still_exists,chunk_size,sha256\n\
                   1,/data/a.fits,10,2026-01-01T00:00:00+00:00,0,0,0,t,16192,\n";
        let rows = parse_snapshot(old).unwrap();
        assert_eq!(rows[0].queued_at, None);
        assert_eq!(rows[0].created_at, Some(t("2026-01-01T00:00:00Z")));
    }

    #[test]
    fn finds_newest_snapshot_anywhere_under_final_dir() {
        let d = tempdir().unwrap();
        let dumps = d.path().join("data/sql_status/status_dumps");
        std::fs::create_dir_all(&dumps).unwrap();
        for ts in [100, 300, 200] {
            std::fs::write(dumps.join(format!("files_sent_list_{ts}.csv")), "x").unwrap();
        }
        std::fs::write(dumps.join("files_sent_list_notanumber.csv"), "x").unwrap();
        let (p, ts) = find_latest_snapshot(d.path()).unwrap();
        assert_eq!(ts, 300);
        assert!(p.ends_with("files_sent_list_300.csv"));
    }

    #[test]
    fn classifies_every_case() {
        let d = tempdir().unwrap();
        let fin = d.path();
        let now = t("2026-09-25T18:00:00Z");
        let row = |id: i64, name: &str, size: u64, queued: &str, exists: bool| SnapshotRow {
            file_id: id,
            file_name: name.into(),
            file_size: size,
            created_at: Some(t("2026-09-25T00:00:00Z")),
            still_exists: exists,
            queued_at: Some(t(queued)),
        };
        let landed = ground_path(fin, "/data/ok.fits.bz2");
        std::fs::create_dir_all(landed.parent().unwrap()).unwrap();
        std::fs::write(&landed, vec![0u8; 10]).unwrap();
        std::fs::write(ground_path(fin, "/data/short.fits.bz2"), vec![0u8; 3]).unwrap();

        let ground = GroundState {
            in_flight: HashMap::from([(3, 60), (4, 5 * 3600)]),
            sender_pending: Some(HashSet::from([5])),
        };
        let old = "2026-09-25T12:00:00Z"; // 6 h ago
        let cases = [
            (
                row(1, "/data/ok.fits.bz2", 10, old, true),
                Verdict::Complete,
            ),
            (
                row(2, "/data/short.fits.bz2", 10, old, true),
                Verdict::SizeMismatch {
                    expected: 10,
                    found: 3,
                },
            ),
            (
                row(3, "/data/partial.fits.bz2", 10, old, true),
                Verdict::InFlight { stale: false },
            ),
            (
                row(4, "/data/stuck.fits.bz2", 10, old, true),
                Verdict::InFlight { stale: true },
            ),
            (
                row(5, "/data/waiting.fits.bz2", 10, old, true),
                Verdict::Queued,
            ),
            (
                row(6, "/data/new.fits.bz2", 10, "2026-09-25T17:00:00Z", true),
                Verdict::TooRecent,
            ),
            (
                row(7, "/data/deleted.fits.bz2", 10, old, false),
                Verdict::GoneOnPayload,
            ),
            (
                row(8, "/data/lost.fits.bz2", 10, old, true),
                Verdict::Missing,
            ),
        ];
        for (r, want) in &cases {
            assert_eq!(
                &classify(r, fin, &ground, now, 3 * 3600),
                want,
                "file {}",
                r.file_id
            );
        }
        let s = ReconcileSummary::from_plan(&cases);
        assert_eq!(
            (s.total, s.complete, s.missing, s.stale_in_flight),
            (8, 1, 1, 1)
        );
    }
}
