//! Receiver daemon: UDP listener that reassembles incoming packets into
//! files and exposes a status socket.

use std::sync::Arc;

use anyhow::Context;
use parachuter::config::LiveConfig;
use parachuter::control::{
    AssemblyState, ControlHandler, ControlServer, ReceiverStatus, Request, Response,
};
use parachuter::proto::{Packet, MAX_CHUNK_SIZE};
use parachuter::reassembler::{IngestOutcome, Reassembler};
use parachuter::transport::UdpReceiver;

use crate::ReceiverArgs;

pub async fn run(args: ReceiverArgs) -> anyhow::Result<()> {
    let live = LiveConfig::load(&args.config)
        .with_context(|| format!("loading {}", args.config.display()))?;
    live.clone().watch()?;
    let cfg = live.current();

    let reassembler = Arc::new(
        Reassembler::new(&cfg.receiver.holding_dir, &cfg.receiver.final_dir)
            .with_context(|| "initialising reassembler")?,
    );

    let control = ReceiverControl {
        reassembler: reassembler.clone(),
        bind: format!("{}:{}", cfg.receiver.bind_ip, cfg.receiver.bind_port),
    };
    let control_path = cfg.receiver.control_socket.clone();
    let server = ControlServer::bind(&control_path, control).await?;
    tokio::spawn(async move {
        if let Err(e) = server.serve().await {
            tracing::error!(?e, "control server crashed");
        }
    });

    let recv_ip = cfg.receiver.bind_ip.clone();
    let recv_port = cfg.receiver.bind_port;
    let r = reassembler.clone();
    tokio::task::spawn_blocking(move || receive_loop(recv_ip, recv_port, r))
        .await
        .context("receiver task panicked")??;
    Ok(())
}

fn receive_loop(ip: String, port: u16, reassembler: Arc<Reassembler>) -> anyhow::Result<()> {
    let socket = UdpReceiver::bind(&ip, port).context("binding receiver socket")?;
    let mut buf = vec![0u8; MAX_CHUNK_SIZE];
    tracing::info!(%ip, port, "receiver listening");
    loop {
        let recv = match socket.recv(&mut buf)? {
            Some(x) => x,
            None => continue,
        };
        let (n, src) = recv;
        let pkt = match Packet::decode(&buf[..n]) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(?e, %src, "drop bad packet");
                continue;
            }
        };
        match reassembler.ingest(&pkt) {
            Ok(IngestOutcome::Complete) => {
                if let Err(e) = reassembler.finalize(pkt.file_id) {
                    tracing::warn!(?e, file_id = pkt.file_id, "finalize failed");
                } else {
                    tracing::info!(file_id = pkt.file_id, "file complete");
                }
            }
            Ok(o) => tracing::debug!(?o, file_id = pkt.file_id, chunk = pkt.chunk_id, "ingest"),
            Err(e) => tracing::warn!(?e, file_id = pkt.file_id, "ingest failed"),
        }
    }
}

struct ReceiverControl {
    reassembler: Arc<Reassembler>,
    bind: String,
}

impl ControlHandler for ReceiverControl {
    async fn handle(&self, req: Request) -> Response {
        match req {
            Request::Ping => Response::Pong {
                daemon: "receiver".into(),
                version: env!("CARGO_PKG_VERSION").into(),
            },
            Request::ReceiverStatus => match self.snapshot() {
                Ok(status) => Response::ReceiverStatus(status),
                Err(e) => Response::Error {
                    message: e.to_string(),
                },
            },
            _ => Response::Unsupported,
        }
    }
}

impl ReceiverControl {
    fn snapshot(&self) -> parachuter::Result<ReceiverStatus> {
        let ids = self.reassembler.in_flight()?;
        let mut assemblies = Vec::with_capacity(ids.len());
        for id in ids {
            let stats = match self.reassembler.stats(id) {
                Ok(s) => s,
                Err(_) => continue,
            };
            assemblies.push(AssemblyState {
                file_id: id,
                chunks_total: stats.chunks_total,
                chunks_received: stats.chunks_received,
                has_name: stats.has_name,
                age_secs: self.reassembler.age_secs(id).unwrap_or(0),
            });
        }
        Ok(ReceiverStatus {
            bind: self.bind.clone(),
            assemblies,
        })
    }
}
