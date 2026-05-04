//! `parachuter ctl` — talk to a running daemon over its Unix domain socket.
//!
//! ```text
//! parachuter ctl --socket /run/parachuter/sender.sock status
//! parachuter ctl --socket /run/parachuter/sender.sock set-link --link tdrss --kbps 30
//! parachuter ctl --socket /run/parachuter/sender.sock set-state paused
//! parachuter ctl --socket /run/parachuter/sender.sock submit --path /data/foo.fits
//! parachuter ctl --socket /run/parachuter/sender.sock enqueue --file-id 42 --interrupt
//! parachuter ctl --socket /run/parachuter/receiver.sock status
//! parachuter ctl --socket /run/parachuter/cleaner.sock status
//! ```

use std::path::PathBuf;

use anyhow::{anyhow, Context};
use clap::{Args, Subcommand};
use parachuter::config::SenderState;
use parachuter::control::{ControlClient, LinkOverride, Request, Response};

#[derive(Debug, Args)]
pub struct CtlArgs {
    /// Path to the daemon's Unix domain socket.
    #[arg(short, long)]
    socket: PathBuf,

    #[command(subcommand)]
    cmd: CtlCmd,
}

#[derive(Debug, Subcommand)]
enum CtlCmd {
    /// Check whether a daemon answers and report its identity.
    Ping,
    /// Print the daemon's status as JSON.
    Status,
    /// Override link / chunk size / kbps. Any flag not provided is left alone.
    SetLink {
        #[arg(long)]
        link: Option<String>,
        #[arg(long)]
        chunk_size: Option<usize>,
        #[arg(long)]
        kbps: Option<u64>,
        #[arg(long)]
        ip: Option<String>,
        #[arg(long)]
        port: Option<u16>,
    },
    /// Switch the sender's state. One of: auto, manual, paused, debug.
    SetState { state: String },
    /// Submit an arbitrary file to the sender by absolute path. The sender
    /// registers it in the ledger and queues it; the response includes the
    /// assigned file_id, total chunks, and file size as JSON. This is the
    /// agnostic ingest path: any program (Python, shell, etc.) can drive it
    /// by exec'ing `parachuter ctl submit`.
    Submit {
        /// Absolute path of the file to enqueue.
        #[arg(long)]
        path: PathBuf,
        /// Push to the head of the queue (preempt the current send).
        #[arg(long, default_value_t = false)]
        interrupt: bool,
    },
    /// Manually queue a file already in the ledger.
    Enqueue {
        #[arg(long)]
        file_id: i64,
        /// First chunk; -1 means whole file.
        #[arg(long, default_value_t = -1)]
        start: i32,
        /// Number of chunks; ignored when start == -1.
        #[arg(long, default_value_t = 0)]
        count: u32,
        /// Push to the head of the queue.
        #[arg(long, default_value_t = false)]
        interrupt: bool,
    },
    /// Resend just the manifest (filename) packet for a file.
    ResendName {
        #[arg(long)]
        file_id: i64,
    },
    /// Drop everything from the sender's queue.
    Flush,
    /// Tell the cleaner to scan immediately.
    CleanerRun,
}

pub async fn run(args: CtlArgs) -> anyhow::Result<()> {
    let client = ControlClient::new(&args.socket);
    let req = match args.cmd {
        CtlCmd::Ping => Request::Ping,
        CtlCmd::Status => {
            let resp = try_each_status(&client).await?;
            println!("{}", serde_json::to_string_pretty(&resp)?);
            return Ok(());
        }
        CtlCmd::SetLink {
            link,
            chunk_size,
            kbps,
            ip,
            port,
        } => Request::SenderConfigure(LinkOverride {
            active_link: link,
            chunk_size,
            max_kbps: kbps,
            link_ip: ip,
            link_port: port,
        }),
        CtlCmd::SetState { state } => {
            let s = match state.as_str() {
                "auto" => SenderState::Auto,
                "manual" => SenderState::Manual,
                "paused" => SenderState::Paused,
                "debug" => SenderState::Debug,
                other => return Err(anyhow!("unknown state `{other}`")),
            };
            Request::SenderSetState(s)
        }
        CtlCmd::Submit { path, interrupt } => {
            let abs = if path.is_absolute() {
                path
            } else {
                std::env::current_dir()?.join(path)
            };
            Request::SenderSubmit {
                path: abs.to_string_lossy().into_owned(),
                interrupt,
            }
        }
        CtlCmd::Enqueue {
            file_id,
            start,
            count,
            interrupt,
        } => Request::SenderEnqueue {
            file_id,
            start,
            count,
            interrupt,
        },
        CtlCmd::ResendName { file_id } => Request::SenderResendName { file_id },
        CtlCmd::Flush => Request::SenderFlush,
        CtlCmd::CleanerRun => Request::CleanerRunNow,
    };
    let resp = client.call(req).await.context("control call failed")?;
    println!("{}", serde_json::to_string_pretty(&resp)?);
    Ok(())
}

async fn try_each_status(client: &ControlClient) -> anyhow::Result<Response> {
    let pong = client
        .call(Request::Ping)
        .await
        .context("daemon did not answer ping")?;
    let daemon = match &pong {
        Response::Pong { daemon, .. } => daemon.clone(),
        _ => return Ok(pong),
    };
    let status_req = match daemon.as_str() {
        "sender" => Request::SenderStatus,
        "receiver" => Request::ReceiverStatus,
        "cleaner" => Request::CleanerStatus,
        _ => return Ok(pong),
    };
    Ok(client.call(status_req).await?)
}
