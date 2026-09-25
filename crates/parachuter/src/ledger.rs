//! Embedded SQLite ledger of files the sender knows about.
//!
//! Replaces the original PostgreSQL `files_sent` table. The schema is
//! intentionally close to what the original code used, so an existing
//! deployment can backfill via a one-shot `INSERT … SELECT` from a Postgres
//! dump, but with a few additions:
//!
//! * `file_id` is `INTEGER PRIMARY KEY AUTOINCREMENT` so `i64`s up to 2^63
//!   work safely – the original `i32` would have wrapped around eventually.
//! * `chunk_size` is recorded per-file so a receiver can verify the sender
//!   didn't change chunk size mid-flight.
//! * `sha256` column is reserved for an optional whole-file checksum the
//!   sender can compute lazily and embed in the manifest packet.
//!
//! * `queued_at` records when the file was last queued for a full send. The
//!   ground-side reconciler uses it to tell "lost" from "still on its way".
//!   Older ledgers gain the column automatically when opened.
//!
//! The connection runs in WAL mode so reads (e.g. a status dashboard) don't
//! block the sender's writes.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};

use crate::error::Result;

const SCHEMA: &str = r"
CREATE TABLE IF NOT EXISTS files (
    file_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    file_name        TEXT    NOT NULL UNIQUE,
    file_size        INTEGER NOT NULL,
    created_at       TEXT    NOT NULL,
    images_per_dark  INTEGER NOT NULL DEFAULT 0,
    images_per_flat  INTEGER NOT NULL DEFAULT 0,
    images_per_bias  INTEGER NOT NULL DEFAULT 0,
    still_exists     INTEGER NOT NULL DEFAULT 1,
    chunk_size       INTEGER NOT NULL DEFAULT 16192,
    sha256           TEXT,
    queued_at        TEXT
);
CREATE INDEX IF NOT EXISTS idx_files_name ON files(file_name);
CREATE INDEX IF NOT EXISTS idx_files_still ON files(still_exists);
";

/// One row in the `files` table.
#[derive(Debug, Clone)]
pub struct FileRecord {
    /// Stable identifier this file gets across all systems.
    pub file_id: i64,
    /// Absolute or repo-relative path on the sender.
    pub file_name: String,
    /// File size in bytes when first registered.
    pub file_size: u64,
    /// File creation timestamp (best-effort; `mtime` is used if `birthtime`
    /// is unavailable).
    pub created_at: DateTime<Utc>,
    /// Calibration counters carried over from the SuperBIT downlinker.
    pub images_per_dark: u32,
    /// As above.
    pub images_per_flat: u32,
    /// As above.
    pub images_per_bias: u32,
    /// `false` if the file disappeared from disk after being registered.
    pub still_exists: bool,
    /// Datagram size used when this file was chunked.
    pub chunk_size: u32,
    /// Optional sha256 hex digest, populated lazily.
    pub sha256: Option<String>,
    /// When the file was last queued for a full send (first pass, or a
    /// whole-file resend). `None` for rows written before this column existed.
    pub queued_at: Option<DateTime<Utc>>,
}

/// Embedded ledger.
pub struct Ledger {
    db: Connection,
    db_path: PathBuf,
}

impl Ledger {
    /// Open or create the ledger at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let db = Connection::open(&path)?;
        db.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA foreign_keys = ON;",
        )?;
        db.execute_batch(SCHEMA)?;
        migrate(&db)?;
        Ok(Self { db, db_path: path })
    }

    /// Path the ledger lives at.
    pub fn path(&self) -> &Path {
        &self.db_path
    }

    /// Insert a new file record. Returns the assigned `file_id`. If the file
    /// already exists by name, returns its existing `file_id` (idempotent).
    pub fn upsert_file(
        &mut self,
        name: &str,
        size: u64,
        created: DateTime<Utc>,
        ipd: u32,
        ipf: u32,
        ipb: u32,
        chunk_size: u32,
    ) -> Result<i64> {
        let tx = self.db.transaction()?;
        let existing: Option<i64> = tx
            .query_row(
                "SELECT file_id FROM files WHERE file_name = ?1",
                params![name],
                |r| r.get(0),
            )
            .optional()?;
        let id = if let Some(id) = existing {
            id
        } else {
            tx.execute(
                "INSERT INTO files
                    (file_name, file_size, created_at, images_per_dark,
                     images_per_flat, images_per_bias, still_exists,
                     chunk_size)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7)",
                params![
                    name,
                    size as i64,
                    created.to_rfc3339(),
                    ipd as i64,
                    ipf as i64,
                    ipb as i64,
                    chunk_size as i64,
                ],
            )?;
            tx.last_insert_rowid()
        };
        tx.commit()?;
        Ok(id)
    }

    /// Look up a record by id.
    pub fn get(&self, file_id: i64) -> Result<Option<FileRecord>> {
        let row = self
            .db
            .query_row(
                "SELECT file_id, file_name, file_size, created_at,
                        images_per_dark, images_per_flat, images_per_bias,
                        still_exists, chunk_size, sha256, queued_at
                   FROM files WHERE file_id = ?1",
                params![file_id],
                row_to_record,
            )
            .optional()?;
        Ok(row)
    }

    /// Look up a record by file name.
    pub fn get_by_name(&self, name: &str) -> Result<Option<FileRecord>> {
        let row = self
            .db
            .query_row(
                "SELECT file_id, file_name, file_size, created_at,
                        images_per_dark, images_per_flat, images_per_bias,
                        still_exists, chunk_size, sha256, queued_at
                   FROM files WHERE file_name = ?1",
                params![name],
                row_to_record,
            )
            .optional()?;
        Ok(row)
    }

    /// Mark a file as no longer present on the sender's filesystem.
    pub fn mark_gone(&mut self, name: &str) -> Result<()> {
        self.db.execute(
            "UPDATE files SET still_exists = 0 WHERE file_name = ?1",
            params![name],
        )?;
        Ok(())
    }

    /// Record that a file was just queued for a full send. Called on the first
    /// pass and on every whole-file resend, so the reconciler's grace period
    /// restarts each time.
    pub fn mark_queued(&mut self, file_id: i64, when: DateTime<Utc>) -> Result<()> {
        self.db.execute(
            "UPDATE files SET queued_at = ?1 WHERE file_id = ?2",
            params![when.to_rfc3339(), file_id],
        )?;
        Ok(())
    }

    /// Iterate every record in the ledger. Returns owned `FileRecord`s for
    /// API simplicity – the table is small in practice (10s of thousands).
    pub fn all(&self) -> Result<Vec<FileRecord>> {
        let mut stmt = self.db.prepare(
            "SELECT file_id, file_name, file_size, created_at,
                    images_per_dark, images_per_flat, images_per_bias,
                    still_exists, chunk_size, sha256, queued_at
               FROM files ORDER BY file_id",
        )?;
        let rows = stmt
            .query_map([], row_to_record)?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// Dump the ledger to a CSV file. Used to ship a synchronisation snapshot
    /// to ground (replacing the PSQL `\copy` dance in the original sender).
    pub fn dump_csv(&self, out: impl AsRef<Path>) -> Result<()> {
        use std::io::Write;
        let mut f = std::fs::File::create(out)?;
        writeln!(
            f,
            "file_id,file_name,file_size,created_at,images_per_dark,images_per_flat,images_per_bias,still_exists,chunk_size,sha256,queued_at"
        )?;
        for r in self.all()? {
            writeln!(
                f,
                "{},{},{},{},{},{},{},{},{},{},{}",
                r.file_id,
                csv_escape(&r.file_name),
                r.file_size,
                r.created_at.to_rfc3339(),
                r.images_per_dark,
                r.images_per_flat,
                r.images_per_bias,
                if r.still_exists { "t" } else { "f" },
                r.chunk_size,
                r.sha256.as_deref().unwrap_or(""),
                r.queued_at.map(|t| t.to_rfc3339()).unwrap_or_default(),
            )?;
        }
        f.sync_all()?;
        Ok(())
    }
}

fn row_to_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<FileRecord> {
    let created_str: String = row.get(3)?;
    let created = DateTime::parse_from_rfc3339(&created_str)
        .map(|d| d.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now());
    Ok(FileRecord {
        file_id: row.get(0)?,
        file_name: row.get(1)?,
        file_size: row.get::<_, i64>(2)? as u64,
        created_at: created,
        images_per_dark: row.get::<_, i64>(4)? as u32,
        images_per_flat: row.get::<_, i64>(5)? as u32,
        images_per_bias: row.get::<_, i64>(6)? as u32,
        still_exists: row.get::<_, i64>(7)? != 0,
        chunk_size: row.get::<_, i64>(8)? as u32,
        sha256: row.get(9)?,
        queued_at: row
            .get::<_, Option<String>>(10)?
            .and_then(|s| DateTime::parse_from_rfc3339(&s).ok())
            .map(|d| d.with_timezone(&Utc)),
    })
}

/// Bring an existing ledger up to the current schema. Only ever adds
/// nullable columns, so it is safe to run on every open.
fn migrate(db: &Connection) -> Result<()> {
    let mut stmt = db.prepare("PRAGMA table_info(files)")?;
    let cols = stmt
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if !cols.iter().any(|c| c == "queued_at") {
        db.execute_batch("ALTER TABLE files ADD COLUMN queued_at TEXT;")?;
    }
    Ok(())
}

fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        let escaped = s.replace('"', "\"\"");
        format!("\"{escaped}\"")
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn upsert_is_idempotent() {
        let dir = tempdir().unwrap();
        let mut l = Ledger::open(dir.path().join("ledger.sqlite")).unwrap();
        let id1 = l
            .upsert_file("/data/foo.fits", 100, Utc::now(), 0, 0, 0, 16192)
            .unwrap();
        let id2 = l
            .upsert_file("/data/foo.fits", 100, Utc::now(), 0, 0, 0, 16192)
            .unwrap();
        assert_eq!(id1, id2);
    }

    #[test]
    fn mark_queued_round_trips_and_lands_in_csv() {
        let dir = tempdir().unwrap();
        let mut l = Ledger::open(dir.path().join("ledger.sqlite")).unwrap();
        let id = l
            .upsert_file("/data/qsc/science/m31_002.fits.bz2", 42, Utc::now(), 0, 0, 0, 16192)
            .unwrap();
        assert!(l.get(id).unwrap().unwrap().queued_at.is_none());
        let when = DateTime::parse_from_rfc3339("2026-09-25T18:00:00Z").unwrap().with_timezone(&Utc);
        l.mark_queued(id, when).unwrap();
        assert_eq!(l.get(id).unwrap().unwrap().queued_at, Some(when));
        let out = dir.path().join("dump.csv");
        l.dump_csv(&out).unwrap();
        let text = std::fs::read_to_string(out).unwrap();
        assert!(text.lines().next().unwrap().ends_with(",queued_at"));
        assert!(text.contains("2026-09-25T18:00:00+00:00"));
    }

    #[test]
    fn old_ledger_gains_queued_at_column() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("old.sqlite");
        {
            // A ledger created before queued_at existed.
            let db = Connection::open(&path).unwrap();
            db.execute_batch(
                "CREATE TABLE files (
                    file_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_name TEXT NOT NULL UNIQUE, file_size INTEGER NOT NULL,
                    created_at TEXT NOT NULL, images_per_dark INTEGER NOT NULL DEFAULT 0,
                    images_per_flat INTEGER NOT NULL DEFAULT 0, images_per_bias INTEGER NOT NULL DEFAULT 0,
                    still_exists INTEGER NOT NULL DEFAULT 1, chunk_size INTEGER NOT NULL DEFAULT 16192,
                    sha256 TEXT);
                 INSERT INTO files (file_name, file_size, created_at) VALUES ('/data/a.fits', 1, '2026-01-01T00:00:00+00:00');",
            )
            .unwrap();
        }
        let mut l = Ledger::open(&path).unwrap();
        let r = l.get_by_name("/data/a.fits").unwrap().unwrap();
        assert!(r.queued_at.is_none());
        l.mark_queued(r.file_id, Utc::now()).unwrap();
        assert!(l.get(r.file_id).unwrap().unwrap().queued_at.is_some());
    }

    #[test]
    fn mark_gone_persists() {
        let dir = tempdir().unwrap();
        let mut l = Ledger::open(dir.path().join("ledger.sqlite")).unwrap();
        l.upsert_file("/data/x.bin", 1, Utc::now(), 0, 0, 0, 16192)
            .unwrap();
        l.mark_gone("/data/x.bin").unwrap();
        let r = l.get_by_name("/data/x.bin").unwrap().unwrap();
        assert!(!r.still_exists);
    }
}
