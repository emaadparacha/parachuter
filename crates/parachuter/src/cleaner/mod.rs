//! Cleaner daemon: looks at the receiver's holding directory, finds gaps
//! in in-flight assemblies, and asks the sender to retransmit them via the
//! sender's control socket.
//!
//! Three layers of dedup keep this from drowning the sender:
//!
//! 1. **Live queue inspection.** The cleaner first calls
//!    `Request::SenderStatus` on the sender's control socket. The reply
//!    enumerates every chunk range currently queued. We subtract those from
//!    our own missing-list and never re-request something already pending.
//! 2. **TTL dedup table.** Even if the control socket is unreachable, every
//!    request the cleaner has made in the last `dedup_ttl_secs` seconds is
//!    cached in `state.json`. Duplicates are dropped.
//! 3. **Per-link budget.** The active link has a `max_in_flight` and
//!    `min_period_ms`. We never exceed either.

mod dedup;

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use parachuter::config::LiveConfig;
use parachuter::control::{
    CleanerStatus, ControlClient, ControlHandler, ControlServer, Request, RequestedRange, Response,
    SenderQueueSnapshot, SenderStatus,
};
use parachuter::reassembler::Reassembler;
use tokio::sync::{Mutex, Notify};

use self::dedup::DedupTable;
use crate::DaemonArgs;

pub async fn run(args: DaemonArgs) -> anyhow::Result<()> {
    let live = LiveConfig::load(&args.config)
        .with_context(|| format!("loading {}", args.config.display()))?;
    live.clone().watch()?;

    let cfg_snap = live.current();
    let reassembler = Arc::new(
        Reassembler::new(&cfg_snap.cleaner.holding_dir, &cfg_snap.cleaner.final_dir)
            .with_context(|| "init reassembler")?,
    );
    let dedup = Arc::new(Mutex::new(DedupTable::load(
        &cfg_snap.cleaner.state_path,
        Duration::from_secs(cfg_snap.cleaner.dedup_ttl_secs),
    )?));
    let sender_client = Arc::new(ControlClient::new(
        cfg_snap.cleaner.sender_control_socket.clone(),
    ));

    let run_now = Arc::new(Notify::new());

    let control = CleanerControl {
        dedup: dedup.clone(),
        active_link: cfg_snap.cleaner.active_link.clone(),
        run_now: run_now.clone(),
    };
    let control_path = cfg_snap.cleaner.control_socket.clone();
    let server = ControlServer::bind(&control_path, control).await?;
    tokio::spawn(async move {
        if let Err(e) = server.serve().await {
            tracing::error!(?e, "cleaner control server crashed");
        }
    });

    if args.once {
        return run_pass(&live, &reassembler, &dedup, &sender_client).await;
    }

    spawn_shutdown_watcher();

    let period = Duration::from_secs(cfg_snap.cleaner.run_period_secs);
    loop {
        if let Err(e) = run_pass(&live, &reassembler, &dedup, &sender_client).await {
            tracing::warn!(?e, "cleaner pass failed");
        }
        tokio::select! {
            _ = tokio::time::sleep(period) => {},
            _ = run_now.notified() => {
                tracing::info!("cleaner pass triggered by control request");
            }
        }
    }
}

fn spawn_shutdown_watcher() {
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate()).expect("sigterm handler");
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {},
                _ = sigterm.recv() => {},
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        tracing::info!("cleaner shutting down");
        std::process::exit(0);
    });
}

async fn run_pass(
    live: &LiveConfig,
    reassembler: &Reassembler,
    dedup: &Arc<Mutex<DedupTable>>,
    sender_client: &ControlClient,
) -> anyhow::Result<()> {
    let cfg = live.current();
    let active_link = cfg.cleaner.active_link.clone();
    let budget = cfg
        .cleaner
        .links
        .get(&active_link)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("no link budget for `{active_link}`"))?;

    let sender_status: Option<SenderStatus> =
        match sender_client.call(Request::SenderStatus).await {
            Ok(Response::SenderStatus(s)) => Some(s),
            Ok(other) => {
                tracing::warn!(?other, "unexpected sender status response");
                None
            }
            Err(e) => {
                tracing::warn!(?e, "sender control unreachable; falling back to TTL dedup");
                None
            }
        };

    let in_flight = reassembler.in_flight()?;
    let mut planned: Vec<(i64, RequestedRange, RequestType)> = Vec::new();
    for id in in_flight {
        if !reassembler.has_name(id).unwrap_or(false) {
            planned.push((id, RequestedRange { start: 0, count: 0 }, RequestType::ResendName));
        }
        if let Ok(missing) = reassembler.missing_ranges(id) {
            for (start, count) in missing {
                planned.push((
                    id,
                    RequestedRange { start, count },
                    RequestType::Retransmit,
                ));
            }
        }
    }

    let mut sent = 0u32;
    let mut last_send = Instant::now()
        .checked_sub(Duration::from_secs(60))
        .unwrap_or_else(Instant::now);

    for (file_id, range, kind) in planned {
        if sent >= budget.max_in_flight {
            tracing::debug!(active_link = %active_link, "link budget exhausted this pass");
            break;
        }
        if is_already_pending(&sender_status, file_id, range, kind) {
            tracing::debug!(file_id, ?range, "skipping; sender already has it");
            continue;
        }
        {
            let mut ddt = dedup.lock().await;
            if !ddt.try_insert(file_id, range, kind) {
                tracing::debug!(file_id, ?range, "skipping; in dedup cache");
                continue;
            }
        }

        let since = last_send.elapsed();
        let min = Duration::from_millis(budget.min_period_ms);
        if since < min {
            tokio::time::sleep(min - since).await;
        }
        last_send = Instant::now();

        let req = match kind {
            RequestType::ResendName => Request::SenderResendName { file_id },
            RequestType::Retransmit => Request::SenderEnqueue {
                file_id,
                start: range.start as i32,
                count: range.count,
                interrupt: true,
            },
        };
        match sender_client.call(req).await {
            Ok(Response::Ok) => {
                sent += 1;
                tracing::info!(file_id, ?range, ?kind, "retransmit requested");
            }
            Ok(other) => tracing::warn!(?other, "retransmit request rejected"),
            Err(e) => tracing::warn!(?e, "retransmit request failed"),
        }
    }

    let mut ddt = dedup.lock().await;
    ddt.gc();
    ddt.save(&cfg.cleaner.state_path)?;
    Ok(())
}

#[derive(Debug, Copy, Clone)]
pub enum RequestType {
    Retransmit,
    ResendName,
}

fn is_already_pending(
    status: &Option<SenderStatus>,
    file_id: i64,
    range: RequestedRange,
    kind: RequestType,
) -> bool {
    let Some(s) = status else {
        return false;
    };
    let entry = s.pending.iter().find(|p| p.file_id == file_id);
    let Some(entry) = entry else { return false };
    match kind {
        RequestType::ResendName => entry.manifest_pending,
        RequestType::Retransmit => range_intersects_any(range, &entry.ranges),
    }
}

fn range_intersects_any(target: RequestedRange, existing: &[RequestedRange]) -> bool {
    let t_end = target.end();
    existing
        .iter()
        .any(|e| target.start < e.end() && e.start < t_end)
}

struct CleanerControl {
    dedup: Arc<Mutex<DedupTable>>,
    active_link: String,
    run_now: Arc<Notify>,
}

impl ControlHandler for CleanerControl {
    async fn handle(&self, req: Request) -> Response {
        match req {
            Request::Ping => Response::Pong {
                daemon: "cleaner".into(),
                version: env!("CARGO_PKG_VERSION").into(),
            },
            Request::CleanerStatus => {
                let dt = self.dedup.lock().await;
                let in_flight: Vec<SenderQueueSnapshot> = dt
                    .group_by_file()
                    .into_iter()
                    .map(|(file_id, ranges, manifest_pending)| SenderQueueSnapshot {
                        file_id,
                        ranges,
                        manifest_pending,
                    })
                    .collect();
                Response::CleanerStatus(CleanerStatus {
                    in_flight,
                    recent_requests: dt.recent_count() as u32,
                    active_link: self.active_link.clone(),
                })
            }
            Request::CleanerRunNow => {
                self.run_now.notify_one();
                Response::Ok
            }
            _ => Response::Unsupported,
        }
    }
}
