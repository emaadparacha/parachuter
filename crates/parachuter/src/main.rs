//! `parachuter` — single multi-mode binary.
//!
//! All five roles (sender daemon, receiver daemon, cleaner daemon, control
//! CLI, status monitor) are subcommands of one binary. Each subcommand owns
//! its own process: a typical ground deployment runs `parachuter receiver`
//! and `parachuter cleaner` as two systemd units pointing at the same binary,
//! while a flight deployment runs `parachuter sender` as its own unit.
//!
//! ```text
//! parachuter sender   --config /etc/parachuter/config.toml
//! parachuter receiver --config /etc/parachuter/config.toml
//! parachuter cleaner  --config /etc/parachuter/config.toml
//! parachuter ctl --socket /run/parachuter/sender.sock status
//! parachuter ctl --socket /run/parachuter/sender.sock submit --path /data/foo.fits
//! parachuter monitor --socket /run/parachuter/receiver.sock
//! ```

mod cleaner;
mod ctl;
mod monitor;
mod receiver;
mod sender;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "parachuter",
    version,
    about = "Reliable, rate-limited file downlink over lossy unidirectional UDP links."
)]
struct Cli {
    #[command(subcommand)]
    cmd: TopCmd,
}

#[derive(Debug, Subcommand)]
enum TopCmd {
    /// Run the sender daemon (chunks files and ships UDP datagrams).
    Sender(DaemonArgs),
    /// Run the receiver daemon (listens for UDP, reassembles files).
    Receiver(ReceiverArgs),
    /// Run the cleaner daemon (requests targeted retransmits for missing chunks).
    Cleaner(DaemonArgs),
    /// Talk to a running daemon over its Unix domain socket.
    Ctl(ctl::CtlArgs),
    /// Live TTY status display polled from the receiver's control socket.
    Monitor(monitor::MonitorArgs),
}

/// Args shared by the sender and cleaner daemons.
#[derive(Debug, clap::Args)]
pub struct DaemonArgs {
    /// Path to the TOML config file.
    #[arg(short, long, default_value = "/etc/parachuter/config.toml")]
    pub config: PathBuf,
    /// Run only one main-loop tick (for tests / cron-style operation).
    #[arg(long, default_value_t = false)]
    pub once: bool,
}

/// Args for the receiver daemon (no `--once`; the receiver runs forever).
#[derive(Debug, clap::Args)]
pub struct ReceiverArgs {
    /// Path to the TOML config file.
    #[arg(short, long, default_value = "/etc/parachuter/config.toml")]
    pub config: PathBuf,
}

fn init_tracing(default: &str) {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default)),
        )
        .with_target(true)
        .init();
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        TopCmd::Sender(args) => {
            init_tracing("info,parachuter=debug");
            tokio_runtime(4).block_on(sender::run(args))
        }
        TopCmd::Receiver(args) => {
            init_tracing("info,parachuter=debug");
            tokio_runtime(4).block_on(receiver::run(args))
        }
        TopCmd::Cleaner(args) => {
            init_tracing("info,parachuter=debug");
            tokio_runtime(2).block_on(cleaner::run(args))
        }
        TopCmd::Ctl(args) => {
            init_tracing("warn");
            tokio_runtime(1).block_on(ctl::run(args))
        }
        TopCmd::Monitor(args) => tokio_runtime(1).block_on(monitor::run(args)),
    }
}

fn tokio_runtime(workers: usize) -> tokio::runtime::Runtime {
    let mut builder = if workers <= 1 {
        tokio::runtime::Builder::new_current_thread()
    } else {
        let mut b = tokio::runtime::Builder::new_multi_thread();
        b.worker_threads(workers);
        b
    };
    builder.enable_all().build().expect("tokio runtime")
}
