//! Generic priority-directory scanner.
//!
//! Walks an ordered list of `priority_dirs` and returns the oldest unsent
//! file from the first directory that has one. Whatever ordering you want is
//! expressed entirely by directory order in the TOML config; no recompile.
//!
//! This is one of two ingest paths into the sender. The other is the
//! `submit` control message, which lets any external program (Python, shell,
//! systemd timer, …) push a file by absolute path without writing it to a
//! watched directory first.
//!
//! ## Extension filtering
//!
//! The scanner respects two optional config lists:
//!
//! * `include_extensions`: if non-empty, only files whose tail matches one
//!   of these are eligible.
//! * `skip_extensions`: files whose tail matches one of these are always
//!   rejected.
//!
//! Skip wins on clashes. Filtering applies only to the auto-scanner;
//! explicit `submit` calls always honour the caller's path.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use parachuter::ledger::Ledger;
use parachuter::Result;
use walkdir::WalkDir;

/// Result of a single scan pass.
#[derive(Debug, Clone)]
pub struct Candidate {
    /// Absolute path on disk.
    pub path: PathBuf,
    /// File size in bytes.
    pub size: u64,
    /// Best-effort created-at timestamp.
    pub created_at: DateTime<Utc>,
}

/// Walk `priority_dirs` in order; return the first file that:
///
/// 1. Lives anywhere under a priority dir.
/// 2. Doesn't already exist in the ledger.
/// 3. Doesn't have a sibling whose name is the same minus `.bz2` / `.gz`
///    (i.e. is still being compressed).
/// 4. Passes the include/skip extension filter.
pub fn next_candidate(
    priority_dirs: &[PathBuf],
    include_extensions: &[String],
    skip_extensions: &[String],
    ledger: &Ledger,
) -> Result<Option<Candidate>> {
    for dir in priority_dirs {
        let mut entries: Vec<_> = WalkDir::new(dir)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
            .filter(|e| !is_being_compressed(e.path()))
            .filter(|e| extension_allowed(e.path(), include_extensions, skip_extensions))
            .collect();
        entries.sort_by_key(|e| {
            e.metadata()
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs())
                .unwrap_or(0)
        });
        for entry in entries {
            let p = entry.path();
            let name = p.to_string_lossy();
            if ledger.get_by_name(&name)?.is_some() {
                continue;
            }
            let meta = entry
                .metadata()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            let size = meta.len();
            let created_at: DateTime<Utc> = meta
                .created()
                .or_else(|_| meta.modified())
                .map(Into::into)
                .unwrap_or_else(|_| Utc::now());
            return Ok(Some(Candidate {
                path: p.to_path_buf(),
                size,
                created_at,
            }));
        }
    }
    Ok(None)
}

fn is_being_compressed(p: &Path) -> bool {
    let s = p.to_string_lossy();
    if let Some(stem) = s.strip_suffix(".bz2") {
        return Path::new(stem).exists();
    }
    if let Some(stem) = s.strip_suffix(".gz") {
        return Path::new(stem).exists();
    }
    false
}

/// Returns `true` if `path`'s tail matches `ext` (case-insensitive).
/// `ext` may include or omit the leading dot, and may be compound:
///
///   * `"fits"`     matches `foo.fits` but not `bar.fits.bz2`.
///   * `"fits.bz2"` matches `bar.fits.bz2` but not `foo.fits`.
fn matches_extension(path: &Path, ext: &str) -> bool {
    let needle_body = ext.trim_start_matches('.').to_ascii_lowercase();
    if needle_body.is_empty() {
        return false;
    }
    let needle = format!(".{needle_body}");
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    name.ends_with(&needle)
}

/// Filter precedence:
///
/// 1. If the file matches any entry in `skip`, reject (skip always wins).
/// 2. If `include` is empty, accept (no whitelist restriction).
/// 3. Otherwise, accept iff the file matches at least one entry in `include`.
pub(crate) fn extension_allowed(path: &Path, include: &[String], skip: &[String]) -> bool {
    if skip.iter().any(|e| matches_extension(path, e)) {
        return false;
    }
    if include.is_empty() {
        return true;
    }
    include.iter().any(|e| matches_extension(path, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    fn p(s: &str) -> &Path {
        Path::new(s)
    }

    #[test]
    fn matches_simple_extension() {
        assert!(matches_extension(p("/tmp/foo.fits"), "fits"));
        assert!(matches_extension(p("/tmp/foo.fits"), ".fits"));
        assert!(matches_extension(p("/tmp/foo.FITS"), "fits"));
        assert!(!matches_extension(p("/tmp/foo.fits.bz2"), "fits"));
        assert!(!matches_extension(p("/tmp/foo.txt"), "fits"));
    }

    #[test]
    fn matches_compound_extension() {
        assert!(matches_extension(p("/tmp/foo.fits.bz2"), "fits.bz2"));
        assert!(matches_extension(p("/tmp/foo.fits.bz2"), ".fits.bz2"));
        assert!(!matches_extension(p("/tmp/foo.fits"), "fits.bz2"));
    }

    #[test]
    fn empty_lists_accept_everything() {
        assert!(extension_allowed(p("/x.fits"), &[], &[]));
        assert!(extension_allowed(p("/Makefile"), &[], &[]));
        assert!(extension_allowed(p("/x.log"), &[], &[]));
    }

    #[test]
    fn skip_only_rejects_listed() {
        let skip = vec!["log".into(), "tmp".into()];
        assert!(!extension_allowed(p("/x.log"), &[], &skip));
        assert!(!extension_allowed(p("/y.tmp"), &[], &skip));
        assert!(extension_allowed(p("/z.fits"), &[], &skip));
    }

    #[test]
    fn include_only_accepts_listed() {
        let inc = vec!["fits".into(), "fits.bz2".into()];
        assert!(extension_allowed(p("/x.fits"), &inc, &[]));
        assert!(extension_allowed(p("/x.fits.bz2"), &inc, &[]));
        assert!(!extension_allowed(p("/x.log"), &inc, &[]));
        assert!(!extension_allowed(p("/x"), &inc, &[]));
    }

    #[test]
    fn skip_wins_on_clash() {
        let inc = vec!["fits".into(), "log".into()];
        let skip = vec!["log".into()];
        assert!(extension_allowed(p("/x.fits"), &inc, &skip));
        assert!(!extension_allowed(p("/x.log"), &inc, &skip));
    }

    #[test]
    fn case_insensitive_match() {
        let skip = vec!["LOG".into()];
        assert!(!extension_allowed(p("/y.log"), &[], &skip));
        assert!(!extension_allowed(p("/y.LOG"), &[], &skip));
    }

    #[test]
    fn empty_extension_entry_is_ignored() {
        // A stray empty string in the config should not match every file.
        let skip = vec!["".into()];
        assert!(extension_allowed(p("/x.fits"), &[], &skip));
        assert!(extension_allowed(p("/Makefile"), &[], &skip));
    }
}
