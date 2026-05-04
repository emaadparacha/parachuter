//! Sender daemon: chunks files and ships them downlink at a configured rate,
//! exposing a control socket for live tuning and external file submission.
//!
//! ## How files enter the sender
//!
//! The sender accepts files from two completely independent ingest paths.
//! Both end up in the same SQLite ledger and the same in-memory queue, so
//! downstream code does not care which one a file came in through.
//!
//! 1. **Priority-directory scan.** When the queue is empty, a periodic scan
//!    walks `[sender] priority_dirs` in order and picks the oldest unsent
//!    file from the first directory that has one. Drop a file into a watched
//!    directory from anywhere — `cp`, `rsync`, a Python script, a C program
//!    that closes a `write()`, anything that produces a regular file —
//!    and the sender picks it up on its next pass. No IPC required.
//!
//! 2. **`submit` control message.** Any process can hand the sender an
//!    absolute path over its Unix domain socket and have it queued
//!    immediately. The path does not need to live under a watched directory.
//!    The control message returns the assigned `file_id`, total chunk count
//!    and file size so the caller can correlate later progress reports.
//!    External programs typically call this through `parachuter ctl submit
//!    --path /some/file`, which is a thin wrapper over the same JSON
//!    request.

mod queue;
mod scanner;
mod state;

use std::sync::Arc;

use anyhow::Context;
use parachuter::config::LiveConfig;
use parachuter::control::{ControlServer, Request, Response};
use parachuter::ledger::Ledger;
use parachuter::transport::UdpSender;
use tokio::sync::Mutex;

use self::state::SenderRuntime;
use crate::DaemonArgs;

pub async fn run(args: DaemonArgs) -> anyhow::Result<()> {
    let live = LiveConfig::load(&args.config)
        .with_context(|| format!("loading {}", args.config.display()))?;
    live.clone().watch()?;
    let cfg = live.current();

    let ledger = Ledger::open(&cfg.sender.ledger_path)
        .with_context(|| format!("opening {}", cfg.sender.ledger_path.display()))?;
    let sender = UdpSender::bind(&cfg.sender.bind_ip, cfg.sender.bind_port).with_context(|| {
        format!(
            "binding sender to {}:{}",
            cfg.sender.bind_ip, cfg.sender.bind_port
        )
    })?;

    let runtime = Arc::new(Mutex::new(SenderRuntime::new(
        live.clone(),
        ledger,
        sender,
    )));

    let control_handler = ControlAdapter {
        rt: runtime.clone(),
    };
    let control_path = cfg.sender.control_socket.clone();
    let server = ControlServer::bind(&control_path, control_handler).await?;
    tokio::spawn(async move {
        if let Err(e) = server.serve().await {
            tracing::error!(?e, "control server crashed");
        }
    });

    if args.once {
        SenderRuntime::tick(&runtime).await?;
        return Ok(());
    }

    spawn_shutdown_watcher();
    SenderRuntime::run(runtime).await
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
        tracing::info!("shutdown requested");
        std::process::exit(0);
    });
}

struct ControlAdapter {
    rt: Arc<Mutex<SenderRuntime>>,
}

impl parachuter::control::ControlHandler for ControlAdapter {
    async fn handle(&self, req: Request) -> Response {
        let mut rt = self.rt.lock().await;
        match req {
            Request::Ping => Response::Pong {
                daemon: "sender".into(),
                version: env!("CARGO_PKG_VERSION").into(),
            },
            Request::SenderStatus => Response::SenderStatus(rt.status()),
            Request::SenderConfigure(o) => match rt.apply_override(o) {
                Ok(()) => Response::Ok,
                Err(e) => Response::BadRequest {
                    reason: e.to_string(),
                },
            },
            Request::SenderSetState(s) => {
                rt.set_state(s);
                Response::Ok
            }
            Request::SenderEnqueue {
                file_id,
                start,
                count,
                interrupt,
            } => match rt.enqueue(file_id, start, count, interrupt) {
                Ok(()) => Response::Ok,
                Err(e) => Response::Error {
                    message: e.to_string(),
                },
            },
            Request::SenderSubmit { path, interrupt } => {
                match rt.submit(std::path::Path::new(&path), interrupt) {
                    Ok(ack) => Response::Submitted(ack),
                    Err(e) => Response::Error {
                        message: e.to_string(),
                    },
                }
            }
            Request::SenderResendName { file_id } => match rt.resend_name(file_id) {
                Ok(()) => Response::Ok,
                Err(e) => Response::Error {
                    message: e.to_string(),
                },
            },
            Request::SenderFlush => {
                rt.flush();
                Response::Ok
            }
            _ => Response::Unsupported,
        }
    }
}
