# 🪂 parachuter demo

A one-command, laptop-only demo of the `parachuter` crate. It downlinks a file
over UDP on `127.0.0.1` through a deliberately lossy, rate-limited "radio
link", then recovers every missing chunk with targeted retransmits until the
file lands **byte-for-byte identical**.

```
  flight side (main thread)               ground side (receiver thread)
  Chunker -> RateLimiter -> lossy link -> UdpReceiver -> Reassembler
      ^                                                       |
      +-------- missing_ranges()  (the cleaner's job) --------+
```

## Run it

```bash
cd demo
cargo run --release
```

With no arguments it generates a 3 MiB sample file, drops **20%** of packets,
and caps the link at **8000 kbps**. Each retransmit pass prints a chunk map
(█ received, ▒ partly missing, ░ missing) so you can watch the gaps close.

## Knobs for a live audience

| Flag | Default | Try |
|---|---|---|
| `--file PATH` | generated sample | any photo, PDF or FITS file on your laptop |
| `--loss PCT` | `20` | `40` for a rough day, `0` for a perfect link |
| `--kbps N` | `8000` | `2000` to slow it down so people can watch |
| `--chunk-size BYTES` | `4096` | `1400` (Ethernet-safe) for many more chunks |
| `--port N` | `41410` | change if the port is taken |
| `--seed N` | fixed | change for a different loss pattern |

```bash
cargo run --release -- --file ~/Pictures/nebula.jpg --loss 35 --kbps 4000
```

The received file lands in `parachuter-demo-out/downloads/`, so you can open it
afterwards to prove it survived.

## Using the local copy instead of crates.io

`Cargo.toml` depends on the published `parachuter = "0.1"`. To run against the
source in this repo (before publishing, or while changing the crate), swap in:

```toml
parachuter = { path = "../crates/parachuter" }
```

## Live demo: the real daemons, side by side

`live/run.sh` starts the actual `parachuter` sender, receiver, cleaner and
monitor on your laptop in one six-pane tmux window, with a lossy link in the
middle so there's something to recover from:

```
┌──────────────┬──────────────┬──────────────┐
│ SENDER       │ LOSSY LINK   │ RECEIVER     │
├──────────────┼──────────────┼──────────────┤
│ CONTROL      │ CLEANER      │ MONITOR      │
└──────────────┴──────────────┴──────────────┘
```

```bash
brew install tmux          # once
./live/run.sh              # 20% loss; or ./live/run.sh 35
```

In the **CONTROL** pane (you):

| Type | What happens |
|---|---|
| `demo` | sends a sample science frame and a GoPro clip |
| `send ~/Pictures/m31.jpg` | sends your own file (science queue, highest priority) |
| `ctl status` | what the sender is doing right now |
| `ctl set-link --link tdrss` | drop to a satellite-speed link (250 kbps) live |
| `ctl set-state paused` / `auto` | pause and resume |
| `landed`, `verify` | list arrivals, prove they're byte-for-byte identical |
| `stop` | end the demo |

In the **LOSSY LINK** pane, type a number and press Enter to change the loss
rate on the fly (try `50`, then `0`).

Everything lives in `/tmp/parachuter-demo`, including the generated
`config.toml`. Edit it while the demo runs and the daemons pick up the change
within a second. The script uses `parachuter` from your PATH if you've run
`cargo install parachuter`, otherwise it builds it from this repo.
