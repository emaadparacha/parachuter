//! `SenderRuntime`: holds queue, ledger, sockets, and the current effective
//! config snapshot. The control adapter pokes it; the main loop drains it.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use parachuter::chunker::{chunks_for_size, Chunker};
use parachuter::config::{LiveConfig, SenderState};
use parachuter::control::{LinkOverride, RequestedRange, SenderStatus, SubmitAck};
use parachuter::ledger::Ledger;
use parachuter::proto::{HEADER_LEN, MAX_CHUNK_SIZE, MIN_CHUNK_SIZE};
use parachuter::rate_limiter::RateLimiter;
use parachuter::transport::UdpSender;
use parachuter::{Error, Result};
use tokio::sync::Mutex;

use super::queue::{PacketQueue, WorkItem};
use super::scanner;

/// Live state for the running sender.
pub struct SenderRuntime {
    cfg: LiveConfig,
    ledger: Ledger,
    sender: UdpSender,
    queue: PacketQueue,
    state: SenderState,

    // Effective overrides from the control plane. `None` means "use the
    // value from the live config".
    override_active_link: Option<String>,
    override_chunk_size: Option<usize>,
    override_max_kbps: Option<u64>,
    override_link_ip: Option<String>,
    override_link_port: Option<u16>,

    rate_limiter: RateLimiter,
    last_recheck: Instant,
    last_status_dump: Instant,
}

impl SenderRuntime {
    /// Build a fresh runtime.
    pub fn new(cfg: LiveConfig, ledger: Ledger, sender: UdpSender) -> Self {
        let snapshot = cfg.current();
        let link = snapshot
            .links
            .get(&snapshot.sender.active_link)
            .expect("validated config");
        Self {
            state: snapshot.sender.initial_state,
            override_active_link: None,
            override_chunk_size: None,
            override_max_kbps: None,
            override_link_ip: None,
            override_link_port: None,
            rate_limiter: RateLimiter::new(link.max_kbps),
            cfg,
            ledger,
            sender,
            queue: PacketQueue::new(),
            last_recheck: Instant::now() - Duration::from_secs(3600),
            last_status_dump: Instant::now(),
        }
    }

    // ------------------------------------------------------------------
    // Effective settings (overrides take precedence over config file).
    // ------------------------------------------------------------------

    fn current_chunk_size(&self) -> usize {
        self.override_chunk_size
            .unwrap_or_else(|| self.cfg.current().sender.chunk_size)
    }

    fn current_link(&self) -> (String, String, u16, u64) {
        let snap = self.cfg.current();
        let key = self
            .override_active_link
            .clone()
            .unwrap_or_else(|| snap.sender.active_link.clone());
        let link = snap
            .links
            .get(&key)
            .cloned()
            .unwrap_or_else(|| snap.links.values().next().cloned().expect("at least one link"));
        let ip = self.override_link_ip.clone().unwrap_or(link.ip);
        let port = self.override_link_port.unwrap_or(link.port);
        let kbps = self.override_max_kbps.unwrap_or(link.max_kbps);
        (key, ip, port, kbps)
    }

    /// Status snapshot for the control plane.
    pub fn status(&self) -> SenderStatus {
        let (key, ip, port, kbps) = self.current_link();
        let pending = self.queue.snapshot();
        let current_file_id = self.queue.front().map(|i| i.file_id()).unwrap_or(0);
        SenderStatus {
            state: self.state,
            active_link: key,
            link_ip: ip,
            link_port: port,
            chunk_size: self.current_chunk_size(),
            max_kbps: kbps,
            current_file_id,
            queue_depth: self.queue.depth(),
            pending,
            revision: self.queue.revision(),
        }
    }

    /// Apply runtime overrides from a control message.
    pub fn apply_override(&mut self, o: LinkOverride) -> Result<()> {
        if let Some(link) = &o.active_link {
            if !self.cfg.current().links.contains_key(link) {
                return Err(Error::BadConfig(format!("unknown link `{link}`")));
            }
            self.override_active_link = Some(link.clone());
        }
        if let Some(c) = o.chunk_size {
            if !(MIN_CHUNK_SIZE..=MAX_CHUNK_SIZE).contains(&c) {
                return Err(Error::BadChunkSize(c));
            }
            self.override_chunk_size = Some(c);
        }
        if let Some(k) = o.max_kbps {
            self.override_max_kbps = Some(k);
            self.rate_limiter.set_kbps(k);
        }
        if let Some(ip) = o.link_ip {
            self.override_link_ip = Some(ip);
        }
        if let Some(p) = o.link_port {
            self.override_link_port = Some(p);
        }
        let (_, _, _, kbps) = self.current_link();
        self.rate_limiter.set_kbps(kbps);
        Ok(())
    }

    /// Switch sender state.
    pub fn set_state(&mut self, new: SenderState) {
        if matches!(new, SenderState::Manual | SenderState::Paused | SenderState::Debug) {
            self.queue.clear();
        }
        self.state = new;
    }

    /// Drop everything from the queue.
    pub fn flush(&mut self) {
        self.queue.clear();
    }

    /// Enqueue a manual request.
    pub fn enqueue(
        &mut self,
        file_id: i64,
        start: i32,
        count: u32,
        interrupt: bool,
    ) -> Result<()> {
        let rec = self
            .ledger
            .get(file_id)?
            .ok_or_else(|| Error::BadConfig(format!("unknown file_id {file_id}")))?;
        let chunk_size = self.current_chunk_size();
        let payload_size = chunk_size - HEADER_LEN;
        let total = chunks_for_size(rec.file_size, payload_size);
        if start < 0 {
            // Whole file.
            self.push_full_file(rec.file_id, rec.file_name, total, interrupt);
        } else {
            let s = start as u32;
            if s >= total {
                return Err(Error::OutOfRange { index: s, total });
            }
            let count = count.max(1).min(total - s);
            let item = WorkItem::Retransmit {
                file_id: rec.file_id,
                range: RequestedRange { start: s, count },
            };
            if interrupt {
                self.queue.push_front(item);
            } else {
                self.queue.push_back(item);
            }
        }
        Ok(())
    }

    /// Submit an arbitrary file by absolute path. This is the agnostic ingest
    /// API: any external program (Python script, systemd timer, shell,
    /// another Rust process, …) can register a file with the sender without
    /// having to write into a watched directory first.
    ///
    /// Behaviour:
    ///
    /// * The path must exist, be a regular file, and be readable.
    /// * If the file is already in the ledger, the existing `file_id` is
    ///   reused and a fresh full-file send is queued.
    /// * Otherwise a new ledger row is created.
    /// * The manifest packet is queued ahead of the data stream so the
    ///   receiver learns the filename immediately.
    pub fn submit(&mut self, path: &Path, interrupt: bool) -> Result<SubmitAck> {
        let meta = std::fs::metadata(path).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => Error::NotFound(path.to_path_buf()),
            _ => Error::Io(e),
        })?;
        if !meta.is_file() {
            return Err(Error::BadConfig(format!(
                "{} is not a regular file",
                path.display()
            )));
        }
        let size = meta.len();
        let created_at = meta
            .created()
            .or_else(|_| meta.modified())
            .map(Into::into)
            .unwrap_or_else(|_| Utc::now());

        let chunk_size = self.current_chunk_size();
        let snap = self.cfg.current();
        let abs: PathBuf = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()
                .map(|d| d.join(path))
                .unwrap_or_else(|_| path.to_path_buf())
        };
        let name = abs.to_string_lossy().into_owned();
        let id = self.ledger.upsert_file(
            &name,
            size,
            created_at,
            snap.sender.images_per_dark,
            snap.sender.images_per_flat,
            snap.sender.images_per_bias,
            chunk_size as u32,
        )?;
        let total = chunks_for_size(size, chunk_size - HEADER_LEN);
        self.push_full_file(id, name, total, interrupt);
        tracing::info!(file_id = id, path = %abs.display(), num_chunks = total, "submit: queued");
        Ok(SubmitAck {
            file_id: id,
            num_chunks: total,
            file_size: size,
        })
    }

    /// Resend just the manifest packet.
    pub fn resend_name(&mut self, file_id: i64) -> Result<()> {
        let rec = self
            .ledger
            .get(file_id)?
            .ok_or_else(|| Error::BadConfig(format!("unknown file_id {file_id}")))?;
        self.queue.push_front(WorkItem::Manifest {
            file_id: rec.file_id,
            file_name: rec.file_name,
        });
        Ok(())
    }

    fn push_full_file(&mut self, file_id: i64, file_name: String, total: u32, interrupt: bool) {
        let item_data = WorkItem::Data {
            file_id,
            range: RequestedRange {
                start: 0,
                count: total,
            },
        };
        let item_manifest = WorkItem::Manifest { file_id, file_name };
        // The manifest packet MUST be transmitted before any data chunk so the
        // receiver learns the filename immediately. For the non-interrupt path
        // we push manifest then data onto the back. For the interrupt path we
        // push in reverse order (data first, manifest on top) so that after
        // both push_fronts the queue head is: [manifest, data, …old queue…].
        if interrupt {
            self.queue.push_front(item_data);
            self.queue.push_front(item_manifest);
        } else {
            self.queue.push_back(item_manifest);
            self.queue.push_back(item_data);
        }
    }

    // ------------------------------------------------------------------
    // Main loop
    // ------------------------------------------------------------------

    /// Run the loop forever.
    pub async fn run(this: Arc<Mutex<Self>>) -> anyhow::Result<()> {
        loop {
            let cont = Self::tick(&this).await?;
            if !cont {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    /// One main-loop iteration. Returns `true` if a packet was sent.
    pub async fn tick(this: &Arc<Mutex<Self>>) -> Result<bool> {
        // 1. Try to send the next packet, if any.
        let sent = {
            let mut rt = this.lock().await;
            rt.send_next()?
        };
        if sent {
            return Ok(true);
        }

        // 2. Otherwise, top up the queue from the priority directories
        //    (auto/debug states only).
        {
            let mut rt = this.lock().await;
            if matches!(rt.state, SenderState::Auto | SenderState::Debug) {
                if rt.last_recheck.elapsed().as_secs() >= rt.cfg.current().sender.recheck_period_secs {
                    rt.last_recheck = Instant::now();
                    rt.scan_for_work()?;
                }
            }
            if rt.last_status_dump.elapsed().as_secs()
                >= rt.cfg.current().sender.status_dump_period_secs
            {
                rt.last_status_dump = Instant::now();
                rt.dump_status_csv()?;
            }
        }
        Ok(false)
    }

    fn send_next(&mut self) -> Result<bool> {
        if matches!(self.state, SenderState::Paused) {
            return Ok(false);
        }
        let item = match self.queue.pop_front() {
            Some(i) => i,
            None => return Ok(false),
        };
        let chunk_size = self.current_chunk_size();
        let (_key, ip, port, kbps) = self.current_link();
        self.rate_limiter.set_kbps(kbps);

        match item {
            WorkItem::Manifest { file_id, file_name } => {
                let chunker = Chunker::open(&file_name, file_id, chunk_size)?;
                let pkt = chunker.manifest_packet(&file_name);
                let bytes = pkt.encode();
                self.rate_limiter.acquire(bytes.len());
                self.sender.send_to(&bytes, &ip, port)?;
                tracing::debug!(file_id, ?file_name, "sent manifest");
            }
            WorkItem::Data { file_id, range } => {
                let rec = self
                    .ledger
                    .get(file_id)?
                    .ok_or_else(|| Error::BadConfig(format!("unknown file_id {file_id}")))?;
                let mut chunker = match Chunker::open(&rec.file_name, file_id, chunk_size) {
                    Ok(c) => c,
                    Err(Error::NotFound(_)) => {
                        tracing::warn!(file_id, name = %rec.file_name, "file gone");
                        self.ledger.mark_gone(&rec.file_name)?;
                        return Ok(false);
                    }
                    Err(e) => return Err(e),
                };
                let first = range.start;
                let pkt = chunker.data_packet(first)?;
                let bytes = pkt.encode();
                self.rate_limiter.acquire(bytes.len());
                self.sender.send_to(&bytes, &ip, port)?;
                tracing::debug!(file_id, chunk = first, "sent data");
                if range.count > 1 {
                    self.queue.push_front(WorkItem::Data {
                        file_id,
                        range: RequestedRange {
                            start: first + 1,
                            count: range.count - 1,
                        },
                    });
                }
            }
            WorkItem::Retransmit { file_id, range } => {
                let rec = self
                    .ledger
                    .get(file_id)?
                    .ok_or_else(|| Error::BadConfig(format!("unknown file_id {file_id}")))?;
                let mut chunker = match Chunker::open(&rec.file_name, file_id, chunk_size) {
                    Ok(c) => c,
                    Err(Error::NotFound(_)) => {
                        tracing::warn!(file_id, name = %rec.file_name, "file gone on retransmit");
                        self.ledger.mark_gone(&rec.file_name)?;
                        return Ok(false);
                    }
                    Err(e) => return Err(e),
                };
                let first = range.start;
                let pkt = chunker.retransmit_packet(first)?;
                let bytes = pkt.encode();
                self.rate_limiter.acquire(bytes.len());
                self.sender.send_to(&bytes, &ip, port)?;
                tracing::debug!(file_id, chunk = first, "sent retransmit");
                if range.count > 1 {
                    self.queue.push_front(WorkItem::Retransmit {
                        file_id,
                        range: RequestedRange {
                            start: first + 1,
                            count: range.count - 1,
                        },
                    });
                }
            }
        }
        Ok(true)
    }

    fn scan_for_work(&mut self) -> Result<()> {
        let snap = self.cfg.current();
        // Debug mode pulls from a separate priority list so flight-day
        // testing does not accidentally drain the live science queue. An
        // empty `priority_dirs_debug` in debug state means "no auto-ingest";
        // explicit `submit` / `enqueue` still work.
        let dirs: &[std::path::PathBuf] = if matches!(self.state, SenderState::Debug) {
            &snap.sender.priority_dirs_debug
        } else {
            &snap.sender.priority_dirs
        };
        let cand = scanner::next_candidate(
            dirs,
            &snap.sender.include_extensions,
            &snap.sender.skip_extensions,
            &self.ledger,
        )?;
        if let Some(c) = cand {
            let chunk_size = self.current_chunk_size();
            let id = self.ledger.upsert_file(
                &c.path.to_string_lossy(),
                c.size,
                c.created_at,
                snap.sender.images_per_dark,
                snap.sender.images_per_flat,
                snap.sender.images_per_bias,
                chunk_size as u32,
            )?;
            let total = chunks_for_size(c.size, chunk_size - HEADER_LEN);
            self.push_full_file(id, c.path.to_string_lossy().into_owned(), total, false);
            tracing::info!(
                file_id = id,
                path = %c.path.display(),
                total,
                state = ?self.state,
                "queued new file",
            );
        }
        Ok(())
    }

    fn dump_status_csv(&self) -> Result<()> {
        let now = Utc::now().timestamp();
        let snap = self.cfg.current();
        let dir = snap
            .sender
            .priority_dirs
            .first()
            .map(|p| p.join("status_dumps"))
            .unwrap_or_else(|| std::path::PathBuf::from("/tmp/parachuter"));
        std::fs::create_dir_all(&dir)?;
        let out = dir.join(format!("files_sent_list_{now}.csv"));
        self.ledger.dump_csv(&out)?;
        tracing::info!(out = %out.display(), "dumped ledger snapshot");
        Ok(())
    }
}
