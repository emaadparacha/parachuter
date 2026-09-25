//! The cleaner's reconciliation pass: compare the newest downlinked ledger
//! snapshot with what has landed in `final_dir`, and ask the sender to resend
//! any file that was lost whole. See [`parachuter::reconcile`] for the logic;
//! this file is the I/O around it.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::Utc;
use parachuter::config::LiveConfig;
use parachuter::control::{ControlClient, Request, Response};
use parachuter::reassembler::Reassembler;
use parachuter::reconcile::{self, GroundState, ReconcileSummary, Verdict};
use serde::{Deserialize, Serialize};

/// Persisted between restarts so the 6-hour schedule survives a reboot and a
/// lost file is requested at most once per period.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ReconcileState {
    /// When the last pass ran (unix seconds).
    pub last_run_unix: Option<i64>,
    /// file_id → when a whole-file resend was requested (unix seconds).
    pub requested: HashMap<i64, i64>,
    /// Summary of the last pass, for `ctl status`.
    pub last_summary: Option<ReconcileSummary>,
}

impl ReconcileState {
    /// Where the state lives, next to the cleaner's dedup table.
    pub fn path_for(state_path: &Path) -> PathBuf {
        state_path.with_extension("reconcile.json")
    }

    pub fn load(path: &Path) -> Self {
        std::fs::read(path)
            .ok()
            .and_then(|d| serde_json::from_slice(&d).ok())
            .unwrap_or_default()
    }

    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, serde_json::to_vec_pretty(self).unwrap())?;
        std::fs::rename(tmp, path)
    }

    /// When the next scheduled pass is due. Never run → due now.
    pub fn next_due_unix(&self, period_secs: u64) -> i64 {
        self.last_run_unix
            .map(|t| t + period_secs as i64)
            .unwrap_or(0)
    }
}

/// Run one reconciliation pass and update `st`. Never panics on a bad or
/// missing snapshot: that is logged and the pass ends early.
pub async fn run(
    live: &LiveConfig,
    reassembler: &Reassembler,
    sender: &ControlClient,
    st: &mut ReconcileState,
) -> anyhow::Result<ReconcileSummary> {
    let cfg = live.current();
    let c = &cfg.cleaner;
    let now = Utc::now();
    st.last_run_unix = Some(now.timestamp());
    let mut summary = ReconcileSummary {
        ran_at_unix: now.timestamp(),
        ..Default::default()
    };

    let Some((snap, snap_ts)) = reconcile::find_latest_snapshot(&c.final_dir) else {
        tracing::info!(final_dir = %c.final_dir.display(), "reconcile: no ledger snapshot has landed yet; nothing to compare");
        st.last_summary = Some(summary.clone());
        return Ok(summary);
    };
    let rows = reconcile::parse_snapshot(&std::fs::read_to_string(&snap)?)?;
    summary.snapshot = Some(snap.display().to_string());

    // What the ground and the sender know right now.
    let mut in_flight = HashMap::new();
    for id in reassembler.in_flight()? {
        in_flight.insert(id, reassembler.age_secs(id).unwrap_or(0));
    }
    let sender_pending = match sender.call(Request::SenderStatus).await {
        Ok(Response::SenderStatus(s)) => Some(s.pending.iter().map(|p| p.file_id).collect()),
        other => {
            tracing::warn!(
                ?other,
                "reconcile: sender status unavailable; queued files cannot be ruled out this pass"
            );
            None
        }
    };
    let sender_reachable = sender_pending.is_some();
    let ground = GroundState {
        in_flight,
        sender_pending,
    };

    let plan = reconcile::plan(&rows, &c.final_dir, &ground, now, c.reconcile_grace_secs);
    let tally = ReconcileSummary::from_plan(&plan);
    summary = ReconcileSummary {
        ran_at_unix: summary.ran_at_unix,
        snapshot: summary.snapshot,
        ..tally
    };

    // Forget requests older than one period, so a file lost twice is asked
    // for again on the next cycle.
    let period = c.reconcile_period_secs as i64;
    st.requested.retain(|_, t| now.timestamp() - *t < period);

    let spacing = c
        .links
        .get(&c.active_link)
        .map(|b| Duration::from_millis(b.min_period_ms))
        .unwrap_or_default();

    for (row, verdict) in &plan {
        match verdict {
            Verdict::SizeMismatch { expected, found } => tracing::warn!(
                file_id = row.file_id, path = %row.file_name, expected, found,
                "reconcile: landed with the wrong size (payload copy may have changed after sending); not re-requesting"
            ),
            Verdict::InFlight { stale: true } => tracing::warn!(
                file_id = row.file_id, path = %row.file_name,
                "reconcile: partial file unchanged for longer than the grace period; the cleaner keeps requesting its gaps"
            ),
            Verdict::GoneOnPayload => tracing::warn!(
                file_id = row.file_id, path = %row.file_name,
                "reconcile: never arrived and was deleted on the payload; cannot be recovered"
            ),
            Verdict::Missing => {
                if !sender_reachable {
                    continue;
                }
                if st.requested.contains_key(&row.file_id) {
                    tracing::debug!(
                        file_id = row.file_id,
                        "reconcile: already requested this cycle"
                    );
                    continue;
                }
                let req = Request::SenderEnqueue {
                    file_id: row.file_id,
                    start: -1,
                    count: 0,
                    interrupt: false,
                };
                match sender.call(req).await {
                    Ok(Response::Ok) => {
                        st.requested.insert(row.file_id, now.timestamp());
                        summary.requested += 1;
                        tracing::info!(
                            file_id = row.file_id, path = %row.file_name, bytes = row.file_size,
                            "reconcile: lost whole, requested again (back of the queue)"
                        );
                    }
                    other => tracing::warn!(
                        file_id = row.file_id,
                        ?other,
                        "reconcile: resend request failed; will retry next pass"
                    ),
                }
                tokio::time::sleep(spacing).await;
            }
            _ => {}
        }
    }

    tracing::info!(
        snapshot = %snap.display(),
        snapshot_age_min = (now.timestamp() - snap_ts) / 60,
        total = summary.total,
        complete = summary.complete,
        in_flight = summary.in_flight,
        queued = summary.queued,
        too_recent = summary.too_recent,
        missing = summary.missing,
        requested = summary.requested,
        gone = summary.gone_on_payload,
        size_mismatch = summary.size_mismatch,
        "reconcile: pass complete"
    );
    st.last_summary = Some(summary.clone());
    Ok(summary)
}
