//! parachuter demo: a full downlink on your own laptop.
//!
//! One process, two threads, one UDP socket pair on 127.0.0.1:
//!
//!   "flight" side (main thread)            "ground" side (receiver thread)
//!   Chunker -> RateLimiter -> lossy link -> UdpReceiver -> Reassembler
//!        ^                                                     |
//!        +------ missing_ranges() (the cleaner's job) ---------+
//!
//! The link deliberately drops a percentage of packets. After each pass the
//! ground side reports exactly which chunk ranges are missing, and the flight
//! side retransmits only those, until the file is complete and byte-identical.
//!
//! Run:  cargo run --release -- [--file PATH] [--loss 20] [--kbps 8000]
//!                               [--chunk-size 4096] [--port 41410]

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use parachuter::chunker::Chunker;
use parachuter::proto::Packet;
use parachuter::rate_limiter::RateLimiter;
use parachuter::reassembler::{IngestOutcome, Reassembler};
use parachuter::transport::{UdpReceiver, UdpSender};

const FILE_ID: i64 = 1;
const MAX_ROUNDS: u32 = 25;

struct Opts {
    file: Option<PathBuf>,
    loss_pct: f64,
    kbps: u64,
    chunk_size: usize,
    port: u16,
    seed: u64,
}

fn parse_args() -> Opts {
    let mut o = Opts {
        file: None,
        loss_pct: 20.0,
        kbps: 8_000,
        chunk_size: 4_096,
        port: 41_410,
        seed: 0x5EED_BA11,
    };
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let val = |i: usize| args.get(i + 1).cloned().unwrap_or_else(|| usage());
        match args[i].as_str() {
            "--file" => o.file = Some(PathBuf::from(val(i))),
            "--loss" => o.loss_pct = val(i).parse().unwrap_or_else(|_| usage()),
            "--kbps" => o.kbps = val(i).parse().unwrap_or_else(|_| usage()),
            "--chunk-size" => o.chunk_size = val(i).parse().unwrap_or_else(|_| usage()),
            "--port" => o.port = val(i).parse().unwrap_or_else(|_| usage()),
            "--seed" => o.seed = val(i).parse().unwrap_or_else(|_| usage()),
            _ => usage(),
        }
        i += 2;
    }
    if !(0.0..100.0).contains(&o.loss_pct) {
        eprintln!("--loss must be in [0, 100)");
        std::process::exit(2);
    }
    o
}

fn usage() -> ! {
    eprintln!(
        "usage: parachuter-demo [--file PATH] [--loss PCT] [--kbps N] \
         [--chunk-size BYTES] [--port N] [--seed N]"
    );
    std::process::exit(2);
}

/// Tiny deterministic PRNG (xorshift64*) so the demo needs no extra crates
/// and the same seed always drops the same packets.
struct Rng(u64);
impl Rng {
    fn next_f64(&mut self) -> f64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        (self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// The simulated radio link: rate-limited, and drops `loss_pct` of packets.
struct LossyLink {
    socket: UdpSender,
    port: u16,
    limiter: RateLimiter,
    kbps: u64,
    rng: Rng,
    loss: f64,
    sent: u64,
    dropped: u64,
}

impl LossyLink {
    /// `RateLimiter` allows a one-second burst after idle time, which on
    /// loopback can overflow the receiver's socket buffer. Empty the bucket
    /// before each pass so the only losses are the simulated ones.
    fn drain_burst(&mut self) {
        self.limiter.acquire((self.kbps * 125) as usize);
    }

    fn transmit(&mut self, pkt: &Packet) -> parachuter::Result<()> {
        let bytes = pkt.encode();
        // Dropped packets still cost airtime, just like a real link.
        self.limiter.acquire(bytes.len());
        self.sent += 1;
        if self.rng.next_f64() < self.loss {
            self.dropped += 1;
            return Ok(());
        }
        self.socket.send_to(&bytes, "127.0.0.1", self.port)
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = parse_args();

    // --- Workspace -------------------------------------------------------
    let out = PathBuf::from("parachuter-demo-out");
    let _ = fs::remove_dir_all(&out);
    let holding = out.join("holding");
    let downloads = out.join("downloads");
    fs::create_dir_all(&out)?;

    let source = match &opts.file {
        Some(p) => p.clone(),
        None => make_sample_file(&out.join("sample-image.fits"), 3 * 1024 * 1024)?,
    };
    let name = source
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "payload.bin".into());

    let mut chunker = Chunker::open(&source, FILE_ID, opts.chunk_size)?;
    let total = chunker.num_chunks();

    banner(&opts, &source, chunker.file_size(), total);

    // --- Ground station (receiver thread) --------------------------------
    let reasm = Arc::new(Reassembler::new(&holding, &downloads)?);
    let rx_socket = UdpReceiver::bind("127.0.0.1", opts.port)?;
    let stop = Arc::new(AtomicBool::new(false));
    let rx_count = Arc::new(AtomicU64::new(0));
    let rx_dupes = Arc::new(AtomicU64::new(0));
    let (done_tx, done_rx) = mpsc::channel::<PathBuf>();

    let ground = {
        let (reasm, stop, rx_count, rx_dupes) =
            (reasm.clone(), stop.clone(), rx_count.clone(), rx_dupes.clone());
        thread::spawn(move || -> parachuter::Result<()> {
            let mut buf = vec![0u8; 65_536];
            while !stop.load(Ordering::Relaxed) {
                let Some((n, _)) = rx_socket.recv(&mut buf)? else { continue };
                let Ok(pkt) = Packet::decode(&buf[..n]) else { continue }; // bad CRC/magic
                rx_count.fetch_add(1, Ordering::Relaxed);
                match reasm.ingest(&pkt)? {
                    IngestOutcome::Complete => {
                        let path = reasm.finalize(pkt.file_id)?;
                        let _ = done_tx.send(path);
                    }
                    IngestOutcome::Duplicate => {
                        rx_dupes.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {}
                }
            }
            Ok(())
        })
    };

    // --- Flight computer (this thread) -----------------------------------
    let mut link = LossyLink {
        socket: UdpSender::bind("127.0.0.1", 0)?,
        port: opts.port,
        limiter: RateLimiter::new(opts.kbps),
        kbps: opts.kbps,
        rng: Rng(opts.seed | 1),
        loss: opts.loss_pct / 100.0,
        sent: 0,
        dropped: 0,
    };

    let started = Instant::now();
    println!("🚀 Pass 1: sending manifest + {total} chunks over the lossy link");
    link.drain_burst();
    link.transmit(&chunker.manifest_packet(&name))?;
    for id in 0..total {
        link.transmit(&chunker.data_packet(id)?)?;
        if id % 16 == 0 || id + 1 == total {
            progress(id + 1, total, link.dropped);
        }
    }
    println!();

    let mut rounds = 1;
    let final_path = loop {
        // Let the ground side drain its socket.
        if let Ok(p) = done_rx.recv_timeout(Duration::from_millis(400)) {
            break p;
        }
        if rounds > MAX_ROUNDS {
            return Err(format!("gave up after {MAX_ROUNDS} rounds").into());
        }
        rounds += 1;
        link.drain_burst();

        // This is the cleaner's job: ask the reassembler what is missing.
        if !reasm.manifest_path(FILE_ID).exists() {
            println!("🧹 Pass {rounds}: nothing arrived yet, resending everything");
            link.transmit(&chunker.manifest_packet(&name))?;
            for id in 0..total {
                link.transmit(&chunker.retransmit_packet(id)?)?;
            }
            continue;
        }
        let ranges = reasm.missing_ranges(FILE_ID)?;
        let missing: u32 = ranges.iter().map(|&(_, c)| c).sum();
        let need_name = !reasm.has_name(FILE_ID)?;

        println!(
            "🧹 Pass {rounds}: ground reports {missing} missing chunk(s) in {} range(s){}",
            ranges.len(),
            if need_name { " + the filename packet" } else { "" }
        );
        println!("   {}", chunk_map(total, &ranges));
        println!("   {}", describe_ranges(&ranges));

        if need_name {
            link.transmit(&chunker.manifest_packet(&name))?;
        }
        for &(start, count) in &ranges {
            for id in start..start + count {
                link.transmit(&chunker.retransmit_packet(id)?)?;
            }
        }
    };
    let elapsed = started.elapsed();

    stop.store(true, Ordering::Relaxed);
    ground.join().expect("receiver thread panicked")?;

    // --- Verify ----------------------------------------------------------
    let identical = fs::read(&source)? == fs::read(&final_path)?;
    let delivered = rx_count.load(Ordering::Relaxed);
    let kernel_lost = (link.sent - link.dropped).saturating_sub(delivered);
    let mb = chunker.file_size() as f64 / 1_048_576.0;

    println!();
    println!("🪂 Landed: {}", final_path.display());
    println!("   rounds (1 pass + retransmits) : {rounds}");
    println!("   packets put on the link       : {}", link.sent);
    println!(
        "   dropped by simulated link     : {} ({:.1}%)",
        link.dropped,
        100.0 * link.dropped as f64 / link.sent as f64
    );
    if kernel_lost > 0 {
        println!("   also lost in the OS UDP buffer: {kernel_lost}");
    }
    println!("   duplicates ignored by ground  : {}", rx_dupes.load(Ordering::Relaxed));
    println!(
        "   overhead vs. a perfect link   : {:.1}%",
        100.0 * (link.sent as f64 / (total as f64 + 1.0) - 1.0)
    );
    println!(
        "   time / goodput                : {:.2}s / {:.2} MB/s",
        elapsed.as_secs_f64(),
        mb / elapsed.as_secs_f64()
    );
    println!(
        "   byte-for-byte identical       : {}",
        if identical { "✅ yes" } else { "❌ NO" }
    );
    if !identical {
        std::process::exit(1);
    }
    Ok(())
}

fn banner(o: &Opts, source: &Path, size: u64, chunks: u32) {
    println!("🪂 parachuter demo");
    println!("   file       : {} ({:.2} MiB)", source.display(), size as f64 / 1_048_576.0);
    println!("   chunk size : {} bytes ({} chunks)", o.chunk_size, chunks);
    println!("   link       : 127.0.0.1:{}, capped at {} kbps", o.port, o.kbps);
    println!("   packet loss: {}% (simulated)", o.loss_pct);
    println!();
}

fn progress(done: u32, total: u32, dropped: u64) {
    const W: usize = 40;
    let filled = (done as usize * W) / total as usize;
    print!(
        "\r   [{}{}] {done}/{total} sent, {dropped} dropped by the link",
        "█".repeat(filled),
        "·".repeat(W - filled)
    );
    use std::io::Write;
    let _ = std::io::stdout().flush();
}

/// One character per bucket of chunks: █ all received, ▒ some missing, ░ all missing.
fn chunk_map(total: u32, ranges: &[(u32, u32)]) -> String {
    const W: u32 = 60;
    let cols = W.min(total).max(1);
    let mut missing = vec![false; total as usize];
    for &(s, c) in ranges {
        for id in s..(s + c).min(total) {
            missing[id as usize] = true;
        }
    }
    let mut out = String::from("[");
    for col in 0..cols {
        let a = (col * total / cols) as usize;
        let b = (((col + 1) * total / cols) as usize).max(a + 1);
        let gone = missing[a..b].iter().filter(|m| **m).count();
        out.push(match gone {
            0 => '█',
            g if g == b - a => '░',
            _ => '▒',
        });
    }
    out.push(']');
    out
}

fn describe_ranges(ranges: &[(u32, u32)]) -> String {
    let parts: Vec<String> = ranges
        .iter()
        .take(8)
        .map(|&(s, c)| if c == 1 { format!("{s}") } else { format!("{s}-{}", s + c - 1) })
        .collect();
    let more = ranges.len().saturating_sub(8);
    let mut s = format!("retransmitting chunks: {}", parts.join(", "));
    if more > 0 {
        s.push_str(&format!(" … and {more} more range(s)"));
    }
    s
}

/// Write a pseudo-random file so there is something to downlink.
fn make_sample_file(path: &Path, len: usize) -> std::io::Result<PathBuf> {
    let mut rng = Rng(0xC0FFEE);
    let data: Vec<u8> = (0..len).map(|_| (rng.next_f64() * 256.0) as u8).collect();
    fs::write(path, data)?;
    Ok(path.to_path_buf())
}
