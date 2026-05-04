//! `parachuter monitor` — live status display polled from the receiver's
//! control socket.

use std::path::PathBuf;
use std::time::Duration;

use clap::Args;
use parachuter::control::{ControlClient, Request, Response};

#[derive(Debug, Args)]
pub struct MonitorArgs {
    /// Path to the receiver's Unix domain socket.
    #[arg(short, long, default_value = "/run/parachuter/receiver.sock")]
    socket: PathBuf,

    /// Refresh period in milliseconds.
    #[arg(long, default_value_t = 1000)]
    period_ms: u64,
}

pub async fn run(args: MonitorArgs) -> anyhow::Result<()> {
    let client = ControlClient::new(&args.socket);
    loop {
        match client.call(Request::ReceiverStatus).await {
            Ok(Response::ReceiverStatus(s)) => {
                clear_screen();
                println!("parachuter monitor — receiver {}\n", s.bind);
                println!(
                    "{:>10} {:>10} {:>10} {:>10}  has_name",
                    "file_id", "received", "total", "age_s"
                );
                let mut total_received = 0u64;
                for a in &s.assemblies {
                    println!(
                        "{:>10} {:>10} {:>10} {:>10}  {}",
                        a.file_id,
                        a.chunks_received,
                        a.chunks_total,
                        a.age_secs,
                        if a.has_name { "yes" } else { "no" }
                    );
                    total_received += a.chunks_received as u64;
                }
                println!(
                    "\nin-flight: {}  total chunks received: {}",
                    s.assemblies.len(),
                    total_received
                );
            }
            Ok(other) => println!("unexpected response: {other:?}"),
            Err(e) => println!("error: {e}"),
        }
        tokio::time::sleep(Duration::from_millis(args.period_ms)).await;
    }
}

fn clear_screen() {
    print!("\x1B[2J\x1B[1;1H");
}
