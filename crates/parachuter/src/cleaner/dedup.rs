//! TTL-based dedup table for outstanding cleaner requests.
//!
//! Persists to disk so a cleaner restart does not immediately re-fire
//! requests it sent seconds before the crash.

use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};

use parachuter::control::RequestedRange;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Hash, Clone, Copy)]
pub enum Kind {
    Retransmit,
    ResendName,
}

#[derive(Debug)]
pub struct DedupTable {
    /// (file_id, kind, range) → expiry instant.
    entries: HashMap<(i64, Kind, RequestedRange), Instant>,
    ttl: Duration,
}

#[derive(Serialize, Deserialize)]
struct PersistedEntry {
    file_id: i64,
    kind: Kind,
    range: RequestedRange,
    /// Seconds until expiry from the time the file was written.
    ttl_secs_remaining: u64,
}

#[derive(Serialize, Deserialize, Default)]
struct PersistedTable {
    entries: Vec<PersistedEntry>,
}

impl DedupTable {
    pub fn new(ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            ttl,
        }
    }

    pub fn load(path: &Path, ttl: Duration) -> std::io::Result<Self> {
        if !path.exists() {
            return Ok(Self::new(ttl));
        }
        let data = std::fs::read(path)?;
        let table: PersistedTable = serde_json::from_slice(&data).unwrap_or_default();
        let now = Instant::now();
        let mut entries = HashMap::new();
        for e in table.entries {
            entries.insert(
                (e.file_id, e.kind, e.range),
                now + Duration::from_secs(e.ttl_secs_remaining),
            );
        }
        Ok(Self { entries, ttl })
    }

    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let now = Instant::now();
        let entries: Vec<PersistedEntry> = self
            .entries
            .iter()
            .filter_map(|(&(file_id, kind, range), exp)| {
                let remaining = exp.checked_duration_since(now)?.as_secs();
                Some(PersistedEntry {
                    file_id,
                    kind,
                    range,
                    ttl_secs_remaining: remaining,
                })
            })
            .collect();
        let table = PersistedTable { entries };
        let data = serde_json::to_vec_pretty(&table).unwrap();
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, data)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    /// Returns `true` if the entry was added, `false` if it was already
    /// present and not yet expired.
    pub fn try_insert(
        &mut self,
        file_id: i64,
        range: RequestedRange,
        kind: super::RequestType,
    ) -> bool {
        let key = (file_id, kind_to(kind), range);
        let now = Instant::now();
        if let Some(exp) = self.entries.get(&key) {
            if *exp > now {
                return false;
            }
        }
        self.entries.insert(key, now + self.ttl);
        true
    }

    pub fn gc(&mut self) {
        let now = Instant::now();
        self.entries.retain(|_, exp| *exp > now);
    }

    pub fn recent_count(&self) -> usize {
        self.entries.len()
    }

    pub fn group_by_file(&self) -> Vec<(i64, Vec<RequestedRange>, bool)> {
        use std::collections::BTreeMap;
        let mut by_file: BTreeMap<i64, (Vec<RequestedRange>, bool)> = BTreeMap::new();
        for (file_id, kind, range) in self.entries.keys() {
            let entry = by_file.entry(*file_id).or_default();
            match kind {
                Kind::Retransmit => entry.0.push(*range),
                Kind::ResendName => entry.1 = true,
            }
        }
        by_file.into_iter().map(|(id, (r, m))| (id, r, m)).collect()
    }
}

fn kind_to(k: super::RequestType) -> Kind {
    match k {
        super::RequestType::Retransmit => Kind::Retransmit,
        super::RequestType::ResendName => Kind::ResendName,
    }
}
