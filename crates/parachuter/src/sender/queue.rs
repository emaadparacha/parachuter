//! Per-file packet queue with introspection support.
//!
//! `PacketQueue` keeps work as `(file_id, RequestedRange | Manifest)` items
//! so that:
//!
//! * The cleaner can ask "is chunk 42 of file 7 already pending?" in O(log n).
//! * Front-insertion is O(1) (a `VecDeque`).
//! * Memory cost is `range`, not `range × chunk_size`; we only materialise
//!   the bytes when we actually transmit.

use std::collections::VecDeque;

use parachuter::control::{RequestedRange, SenderQueueSnapshot};

/// One queued unit of work.
#[derive(Debug, Clone)]
pub enum WorkItem {
    /// Send the manifest packet for this file.
    Manifest { file_id: i64, file_name: String },
    /// Send a contiguous range of data chunks for this file.
    Data {
        file_id: i64,
        range: RequestedRange,
    },
    /// Resend a contiguous range as a [`PacketType::Retransmit`].
    Retransmit {
        file_id: i64,
        range: RequestedRange,
    },
}

impl WorkItem {
    /// File this item belongs to.
    pub fn file_id(&self) -> i64 {
        match self {
            WorkItem::Manifest { file_id, .. }
            | WorkItem::Data { file_id, .. }
            | WorkItem::Retransmit { file_id, .. } => *file_id,
        }
    }

    /// Number of packets this item will produce.
    pub fn packet_count(&self) -> u32 {
        match self {
            WorkItem::Manifest { .. } => 1,
            WorkItem::Data { range, .. } | WorkItem::Retransmit { range, .. } => range.count,
        }
    }
}

/// FIFO queue with priority insertion at the front.
#[derive(Debug, Default)]
pub struct PacketQueue {
    items: VecDeque<WorkItem>,
    /// Monotonically increasing version counter. Bumps on every mutation so
    /// the cleaner can tell whether anything has changed since its last poll.
    revision: u64,
}

impl PacketQueue {
    /// Empty queue.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a unit of work to the back.
    pub fn push_back(&mut self, item: WorkItem) {
        self.items.push_back(item);
        self.revision += 1;
    }

    /// Append at the front (used for interrupt requests / retransmits).
    pub fn push_front(&mut self, item: WorkItem) {
        self.items.push_front(item);
        self.revision += 1;
    }

    /// Pop the next item.
    pub fn pop_front(&mut self) -> Option<WorkItem> {
        let v = self.items.pop_front();
        if v.is_some() {
            self.revision += 1;
        }
        v
    }

    /// Peek the next item.
    pub fn front(&self) -> Option<&WorkItem> {
        self.items.front()
    }

    /// Total packets enqueued across all items.
    pub fn depth(&self) -> u32 {
        self.items.iter().map(|i| i.packet_count()).sum()
    }

    /// Drop everything.
    pub fn clear(&mut self) {
        if !self.items.is_empty() {
            self.items.clear();
            self.revision += 1;
        }
    }

    /// Current revision counter.
    pub fn revision(&self) -> u64 {
        self.revision
    }

    /// Group queue contents by file_id for the control-plane snapshot.
    pub fn snapshot(&self) -> Vec<SenderQueueSnapshot> {
        use std::collections::BTreeMap;
        let mut by_file: BTreeMap<i64, SenderQueueSnapshot> = BTreeMap::new();
        for item in &self.items {
            let entry = by_file.entry(item.file_id()).or_insert(SenderQueueSnapshot {
                file_id: item.file_id(),
                ranges: Vec::new(),
                manifest_pending: false,
            });
            match item {
                WorkItem::Manifest { .. } => entry.manifest_pending = true,
                WorkItem::Data { range, .. } | WorkItem::Retransmit { range, .. } => {
                    entry.ranges.push(*range);
                }
            }
        }
        by_file.into_values().collect()
    }

    /// Is `(file_id, chunk)` already enqueued?
    #[allow(dead_code)]
    pub fn contains_chunk(&self, file_id: i64, chunk: u32) -> bool {
        self.items.iter().any(|i| match i {
            WorkItem::Manifest { .. } => false,
            WorkItem::Data { file_id: f, range }
            | WorkItem::Retransmit { file_id: f, range } => *f == file_id && range.contains(chunk),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(start: u32, count: u32) -> RequestedRange {
        RequestedRange { start, count }
    }

    #[test]
    fn snapshot_groups_by_file() {
        let mut q = PacketQueue::new();
        q.push_back(WorkItem::Data {
            file_id: 1,
            range: r(0, 10),
        });
        q.push_back(WorkItem::Manifest {
            file_id: 1,
            file_name: "/tmp/a".into(),
        });
        q.push_back(WorkItem::Retransmit {
            file_id: 2,
            range: r(5, 3),
        });
        let snap = q.snapshot();
        assert_eq!(snap.len(), 2);
        let f1 = snap.iter().find(|s| s.file_id == 1).unwrap();
        assert!(f1.manifest_pending);
        assert_eq!(f1.ranges, vec![r(0, 10)]);
        let f2 = snap.iter().find(|s| s.file_id == 2).unwrap();
        assert!(!f2.manifest_pending);
        assert_eq!(f2.ranges, vec![r(5, 3)]);
    }

    #[test]
    fn contains_chunk() {
        let mut q = PacketQueue::new();
        q.push_back(WorkItem::Data {
            file_id: 7,
            range: r(10, 5),
        });
        assert!(q.contains_chunk(7, 12));
        assert!(!q.contains_chunk(7, 16));
        assert!(!q.contains_chunk(8, 12));
    }
}
