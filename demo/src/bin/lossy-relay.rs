//! lossy-relay: a fake radio link for the live parachuter demo.
//!
//! Sits between `parachuter sender` and `parachuter receiver` on 127.0.0.1,
//! forwarding UDP datagrams and dropping a percentage of them. Every packet
//! is logged, so the audience can watch losses happen.
//!
//!   sender --> 127.0.0.1:41411 [lossy-relay] --> 127.0.0.1:41410 receiver
//!
//! Type a number and press Enter while it runs to change the loss rate
//! live (e.g. `50`). Type `q` to quit.
//!
//! Run:  lossy-relay [--listen 127.0.0.1:41411] [--forward 127.0.0.1:41410]
//!                   [--loss 20] [--seed N] [--quiet]

use std::io::{BufRead, Write};
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Instant;

use parachuter::proto::{Packet, PacketType};

const GREEN: &str = "\x1b[32m";
const RED: &str = "\x1b[31m";
const YELLOW: &str = "\x1b[33m";
const CYAN: &str = "\x1b[36m";
const DIM: &str = "\x1b[2m";
const BOLD: &str = "\x1b[1m";
const RESET: &str = "\x1b[0m";

fn main() -> std::io::Result<()> {
    let mut listen = "127.0.0.1:41411".to_string();
    let mut forward = "127.0.0.1:41410".to_string();
    let mut loss_pct: f64 = 20.0;
    let mut seed: u64 = 0x5EED_BA11;
    let mut quiet = false;

    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let val = |i: usize| args.get(i + 1).cloned().unwrap_or_else(|| usage());
        match args[i].as_str() {
            "--listen" => { listen = val(i); i += 2; }
            "--forward" => { forward = val(i); i += 2; }
            "--loss" => { loss_pct = val(i).parse().unwrap_or_else(|_| usage()); i += 2; }
            "--seed" => { seed = val(i).parse().unwrap_or_else(|_| usage()); i += 2; }
            "--quiet" => { quiet = true; i += 1; }
            _ => usage(),
        }
    }

    let loss = Arc::new(AtomicU32::new(pct_to_permille(loss_pct)));
    let socket = UdpSocket::bind(&listen)?;

    println!("{BOLD}📡 lossy link{RESET}  {listen}  →  {forward}");
    println!("{DIM}   dropping {loss_pct}% of packets · type a new % and press Enter to change it{RESET}\n");

    // Live loss control from stdin.
    let quit = Arc::new(AtomicBool::new(false));
    {
        let loss = loss.clone();
        let quit = quit.clone();
        std::thread::spawn(move || {
            let stdin = std::io::stdin();
            for line in stdin.lock().lines().map_while(Result::ok) {
                let t = line.trim();
                if t == "q" {
                    quit.store(true, Ordering::Relaxed);
                    std::process::exit(0);
                }
                match t.trim_end_matches('%').parse::<f64>() {
                    Ok(p) if (0.0..=100.0).contains(&p) => {
                        loss.store(pct_to_permille(p), Ordering::Relaxed);
                        println!("{YELLOW}{BOLD}   ⚙  loss rate is now {p}%{RESET}");
                    }
                    _ => println!("{YELLOW}   enter a number from 0 to 100, or q to quit{RESET}"),
                }
            }
        });
    }

    let mut rng = seed | 1;
    let mut buf = vec![0u8; 65_536];
    let (mut fwd, mut dropped) = (0u64, 0u64);
    let started = Instant::now();

    while !quit.load(Ordering::Relaxed) {
        let (n, _) = socket.recv_from(&mut buf)?;
        let p = loss.load(Ordering::Relaxed);
        let drop = next(&mut rng) % 1000 < p as u64;

        let (what, colour) = match Packet::decode(&buf[..n]) {
            Ok(pkt) => describe(&pkt),
            Err(_) => ("unparseable datagram".to_string(), DIM),
        };

        if drop {
            dropped += 1;
        } else {
            socket.send_to(&buf[..n], &forward)?;
            fwd += 1;
        }

        if !quiet {
            let total = fwd + dropped;
            let rate = 100.0 * dropped as f64 / total as f64;
            let tag = if drop {
                format!("{RED}{BOLD}✗ LOST     {RESET}")
            } else {
                format!("{GREEN}→ delivered{RESET}")
            };
            println!(
                "{tag}  {colour}{what:<44}{RESET} {DIM}│ {fwd:>6} ok {dropped:>5} lost ({rate:4.1}%) {:>5.0}s{RESET}",
                started.elapsed().as_secs_f64()
            );
            let _ = std::io::stdout().flush();
        }
    }
    Ok(())
}

/// Plain description plus the colour to paint it in (padding is done on the
/// plain text so the columns stay aligned).
fn describe(p: &Packet) -> (String, &'static str) {
    match p.ptype {
        PacketType::Data => (
            format!("data       file {:<3} chunk {:>5}/{}", p.file_id, p.chunk_id, p.num_chunks),
            "",
        ),
        PacketType::Retransmit => (
            format!("retransmit file {:<3} chunk {:>5}/{}", p.file_id, p.chunk_id, p.num_chunks),
            CYAN,
        ),
        PacketType::Manifest | PacketType::NameOnly => {
            let name = String::from_utf8_lossy(&p.payload);
            let short: String = name.rsplit('/').next().unwrap_or(&name).chars().take(24).collect();
            (format!("filename   file {:<3} \"{}\"", p.file_id, short), YELLOW)
        }
        PacketType::Heartbeat => ("heartbeat".to_string(), DIM),
    }
}

fn pct_to_permille(p: f64) -> u32 {
    (p.clamp(0.0, 100.0) * 10.0).round() as u32
}

/// xorshift64*: no extra crates needed.
fn next(s: &mut u64) -> u64 {
    *s ^= *s >> 12;
    *s ^= *s << 25;
    *s ^= *s >> 27;
    s.wrapping_mul(0x2545_F491_4F6C_DD1D) >> 11
}

fn usage() -> ! {
    eprintln!("usage: lossy-relay [--listen ADDR] [--forward ADDR] [--loss PCT] [--seed N] [--quiet]");
    std::process::exit(2);
}
