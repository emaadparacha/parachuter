# 🪂 parachuter

**Reliable, rate-limited file downlink over lossy unidirectional UDP links.**

`parachuter` is a Rust crate and command-line tool inspired by, and first
deployed on, the SuperBIT balloon telescope. It chunks arbitrary files into
CRC-protected UDP datagrams, ships them at a configurable rate over a named
link, reassembles them durably on the ground, and provides a live control
socket to retune every parameter, including chunk size, destination IP, and
throughput cap, without restarting any process.

Everything ships as a single binary, `parachuter`, with mode subcommands.
A typical ground deployment runs `parachuter receiver` and `parachuter
cleaner` as two systemd units pointing at the same binary; a flight
deployment runs `parachuter sender` as its own unit.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Subcommands](#subcommands)
- [Quick Start](#quick-start)
- [How Files Enter the Sender](#how-files-enter-the-sender)
- [Configuration Reference](#configuration-reference)
- [Runtime Control (`parachuter ctl`)](#runtime-control-parachuter-ctl)
- [Named Links](#named-links)
- [Dedup and Re-request Logic](#dedup-and-re-request-logic)
- [Building](#building)
- [Testing](#testing)
- [Deployment (systemd)](#deployment-systemd)
- [Design Notes](#design-notes)

---

## Overview 🪂

```
  ┌──────────────────────┐  UDP datagrams   ┌──────────────────────┐
  │  parachuter sender   │ ─────────────▶   │  parachuter receiver │
  │  (payload / SBC)     │  lossy one-way   │  (ground station)    │
  └────────┬─────────────┘                  └────────┬─────────────┘
           │ Unix socket                             │ Unix socket
  ┌────────▼─────────────┐                  ┌────────▼─────────────┐
  │   parachuter ctl     │                  │  parachuter cleaner  │
  │  (operator CLI)      │                  │  (gap filler / retx) │
  └──────────────────────┘                  └──────────────────────┘
```

The sender chunks files (from a watched priority queue or from explicit
external submissions), ships every chunk over UDP, and waits. The receiver
writes each chunk directly to disk using a CRC-protected bitmap manifest
sidecar. When chunks are missing, the cleaner asks the sender to retransmit
exactly the missing ranges, without re-sending anything that is already in
flight.

---

## Architecture

The whole project is one crate, `parachuter`. It ships both a library
(protocol, chunker, reassembler, ledger, config, control plane) and a
single binary, also called `parachuter`, whose modes are:

| Mode | Role |
|---|---|
| `parachuter sender` | Reads priority dirs, chunks files, ships UDP datagrams, accepts external submissions over its control socket |
| `parachuter receiver` | Listens for UDP, writes chunks to disk, finalises files |
| `parachuter cleaner` | Detects missing chunks, requests targeted retransmits |
| `parachuter ctl` | Pokes a running daemon over its Unix socket (status, set-link, submit, enqueue, flush, …) |
| `parachuter monitor` | Live TTY table of in-progress reassemblies, polled from the receiver |

---

## Subcommands

### `parachuter sender`

Runs on the payload (flight computer / SBC). Watches the priority directories
for new files and accepts file submissions over its Unix socket. Registers
files in a SQLite ledger and ships them chunk by chunk. Config is
hot-reloaded: edit the TOML and changes apply within ~1 second.

```bash
parachuter sender --config /etc/parachuter/config.toml
```

### `parachuter receiver`

Runs on the ground station. Listens on a UDP port, validates every datagram
(magic + CRC), writes payload bytes at the correct file offset, and moves
completed files into the final directory. Also exposes a Unix socket for
status.

```bash
parachuter receiver --config /etc/parachuter/config.toml
```

### `parachuter cleaner`

Runs on the ground station alongside the receiver. Periodically scans the
receiver's holding directory for incomplete assemblies, computes missing
chunk ranges, then asks the sender to retransmit only those ranges. Three
dedup layers prevent flooding the link. Can be triggered on demand with
`parachuter ctl ... cleaner-run`.

```bash
parachuter cleaner --config /etc/parachuter/config.toml
# or one-shot (cron / systemd timer):
parachuter cleaner --config /etc/parachuter/config.toml --once
```

### `parachuter ctl`

Talk to any running daemon from the command line.

```bash
parachuter ctl --socket /run/parachuter/sender.sock   status
parachuter ctl --socket /run/parachuter/receiver.sock status
parachuter ctl --socket /run/parachuter/cleaner.sock  status
```

### `parachuter monitor`

Live terminal table polled from the receiver socket every second.

```bash
parachuter monitor --socket /run/parachuter/receiver.sock --period-ms 500
```

---

## Quick Start 🪂

```bash
# 1. Build the single binary
cd parachuter
cargo build --release
# Binary lives at target/release/parachuter

# 2. Create directories
sudo mkdir -p /etc/parachuter
sudo mkdir -p /var/lib/parachuter/{holding,downloads,csv}
sudo mkdir -p /run/parachuter
sudo mkdir -p /data/parachuter/queue/science

# 3. Install the sample config
sudo cp config/parachuter.toml /etc/parachuter/config.toml
# Edit [links.pilot] ip = "<ground-station-IP>"

# 4. Start the sender (on the payload)
parachuter sender --config /etc/parachuter/config.toml

# 5. Start the receiver (on the ground station)
parachuter receiver --config /etc/parachuter/config.toml

# 6. Start the cleaner (on the ground station)
parachuter cleaner --config /etc/parachuter/config.toml

# 7. Hand the sender a file (either path works):
cp myfile.fits /data/parachuter/queue/science/                    # watched dir
parachuter ctl -s /run/parachuter/sender.sock submit --path /tmp/other.fits

# 8. Watch it arrive 🪂
parachuter monitor --socket /run/parachuter/receiver.sock
```

---

## How Files Enter the Sender

The sender accepts files from two completely independent ingest paths.
Both end up in the same SQLite ledger and the same in-memory queue, so
downstream code does not care which one a file came in through. Use
whichever fits your producer.

### 1. Priority-directory scan (drop a file, walk away)

When the queue is empty, the sender periodically walks the directories
listed in `[sender] priority_dirs` in order, picks the oldest unsent file
from the first directory that has one, registers it in the ledger, and
queues it.

```toml
[sender]
priority_dirs = [
    "/data/parachuter/queue/science",
    "/data/parachuter/queue/dark",
    "/data/parachuter/queue/flat",
]

# Walked instead when the sender is in `debug` state. Empty means no
# auto-ingest in debug.
priority_dirs_debug = [
    "/data/parachuter/queue/debug",
]

# Optional extension filters. See "Configuration Reference" for the full
# precedence rules — short version: skip wins, both empty = allow all.
include_extensions = ["fits", "fits.bz2"]
skip_extensions    = ["log", "tmp"]
```

Anything that produces a regular file under one of those directories
triggers ingest: `cp`, `rsync`, a shell redirect, a Python script that
calls `pathlib.Path.write_bytes`, a C program that closes a `write()`,
a camera driver writing FITS, anything. The sender does not need to know
who put the file there. There is no IPC and no library binding to the
producer — the contract is "a regular file appears under a watched path."

A `.bz2` or `.gz` file is skipped while a sibling without the suffix
exists, so partially-written compressed files are not picked up
mid-compression.

The auto-scanner uses `priority_dirs` in `auto` state and
`priority_dirs_debug` in `debug` state. The two lists are completely
independent, so flight-day testing won't accidentally drain the live
science queue. Switch with `parachuter ctl … set-state debug` /
`set-state auto`.

### 2. `submit` over the control socket (push by absolute path)

Any process can hand the sender a file by absolute path over its Unix
domain socket. The path does not need to live under a watched directory.
The reply is a JSON blob with the assigned `file_id`, total chunk count,
and file size, so the caller can correlate later progress reports.

The easiest way to call it is the bundled CLI wrapper:

```bash
parachuter ctl --socket /run/parachuter/sender.sock submit \
    --path /data/raw/2026-05-04T01-23-45.fits
```

Reply (printed to stdout as JSON):

```json
{
  "kind": "submitted",
  "file_id": 117,
  "num_chunks": 4096,
  "file_size": 66060288
}
```

Pass `--interrupt` to push the file to the head of the queue instead of
the back.

#### Driving it from Python

A producer that writes a file and then submits it:

```python
import json
import subprocess
from pathlib import Path

def submit(path: Path, *, interrupt: bool = False, sock: str = "/run/parachuter/sender.sock"):
    cmd = [
        "parachuter", "ctl",
        "--socket", sock,
        "submit", "--path", str(path),
    ]
    if interrupt:
        cmd.append("--interrupt")
    out = subprocess.run(cmd, check=True, capture_output=True, text=True)
    return json.loads(out.stdout)

result = submit(Path("/data/raw/example.fits"))
print(f"file_id={result['file_id']}  chunks={result['num_chunks']}")
```

#### Driving it from the raw socket

If you do not want a `parachuter` binary on the producer host, the wire
format is plain JSON over a Unix domain socket. Frame: 4-byte big-endian
length, then the JSON bytes; the same framing applies to the response.

```python
import json
import socket
import struct

req = json.dumps({
    "op": "sender_submit",
    "path": "/data/raw/example.fits",
    "interrupt": False,
}).encode()

with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
    s.connect("/run/parachuter/sender.sock")
    s.sendall(struct.pack(">I", len(req)) + req)
    n = struct.unpack(">I", s.recv(4))[0]
    resp = json.loads(s.recv(n))
print(resp)
```

This is the same path `parachuter ctl submit` takes internally, so any
language that can speak Unix sockets and JSON can act as a producer.

### Choosing between them

Watched directories are the path of least resistance for batch producers
that already write files to disk (image pipelines, dump scripts, syncs).
The `submit` socket is right when you want immediate, ordered enqueueing,
need the file_id back synchronously, are submitting from a directory you
do not want the sender to scan, or want `--interrupt` semantics.

Note that `priority_dirs_debug`, `include_extensions`, and
`skip_extensions` only affect the auto-scanner. `submit` always honours
the path the caller hands it, regardless of state and regardless of
extension filters — submission is an explicit instruction.

---

## Configuration Reference

The single TOML file at `/etc/parachuter/config.toml` is shared by every
mode and is hot-reloaded by long-running daemons when it changes on disk.
Any value can also be overridden at runtime via `parachuter ctl` without
touching the file.

### `[links.<name>]`

Define one or more named links. Every mode refers to links by name.

| Key | Type | Description |
|---|---|---|
| `ip` | string | Destination IP address (unicast or multicast). |
| `port` | u16 | Destination UDP port. |
| `max_kbps` | u64 | Sustained throughput cap in kilobits/second. |
| `note` | string (optional) | Human-readable label. |

```toml
[links.pilot]
ip       = "192.168.98.24"
port     = 41410
max_kbps = 1000
note     = "Pilot (tethered) link"

[links.tdrss]
ip       = "239.255.0.2"
port     = 41410
max_kbps = 30
note     = "TDRSS satellite relay (multicast)"
```

### `[sender]`

| Key | Default | Description |
|---|---|---|
| `bind_ip` | `"0.0.0.0"` | Source IP for outbound datagrams. |
| `bind_port` | `34646` | Source port. |
| `chunk_size` | `16192` | Datagram size **including** the 32-byte header. Must be in `[64, 65000]`. |
| `active_link` | `"pilot"` | Which `[links.*]` to send on. Change at runtime with `set-link`. |
| `ledger_path` | `/var/lib/parachuter/ledger.sqlite` | SQLite ledger. |
| `priority_dirs` | (none) | Ordered list of directories the auto-scanner walks in `auto` state; earlier = higher priority. |
| `priority_dirs_debug` | `[]` | Ordered list walked instead of `priority_dirs` when the sender is in `debug` state. Empty means no auto-ingest in debug. |
| `include_extensions` | `[]` | Whitelist of file extensions the auto-scanner is allowed to pick up. See [Extension filtering](#extension-filtering) below. |
| `skip_extensions` | `[]` | Blacklist of file extensions the auto-scanner must skip. Wins over `include_extensions` on clashes. |
| `control_socket` | `/run/parachuter/sender.sock` | Unix socket path. |
| `initial_state` | `"auto"` | One of `auto`, `manual`, `paused`, `debug`. |
| `recheck_period_secs` | `100` | How often to scan priority dirs when queue is empty. |
| `status_dump_period_secs` | `21600` | How often to write a ledger CSV snapshot. |

#### Auto vs. debug priority lists

`auto` state walks `priority_dirs`; `debug` state walks `priority_dirs_debug`.
This lets you carry a separate "things I want to test on the ground today"
queue without disturbing the live science queue. Switch at runtime:

```bash
parachuter ctl -s /run/parachuter/sender.sock set-state debug
parachuter ctl -s /run/parachuter/sender.sock set-state auto
```

If `priority_dirs_debug` is empty in `debug` state, auto-ingest just stops;
explicit `submit` / `enqueue` still work, which is usually what you want
for debug sessions where you're hand-feeding files.

#### Extension filtering

Both `include_extensions` and `skip_extensions` apply *only* to the
auto-scanner. Entries are matched case-insensitively against the file's
tail and may include or omit a leading dot. Compound extensions like
`"fits.bz2"` are supported and only match the full compound (so
`"fits"` does **not** match `foo.fits.bz2` — list both if you want
both).

Precedence:

| `skip_extensions` | `include_extensions` | Result |
|---|---|---|
| empty | empty | Downlink every file the scanner finds. |
| non-empty | empty | Downlink everything except the skip list. |
| empty | non-empty | Downlink only the include list. |
| non-empty | non-empty | Downlink (include − skip): a file must be in `include` *and* not in `skip`. |

Skip always wins on clashes, so listing the same extension in both
effectively just blacklists it. Explicit `parachuter ctl submit --path …`
ignores both lists — if you point at a file by hand, the sender takes
your word for it.

### `[receiver]`

| Key | Default | Description |
|---|---|---|
| `bind_ip` | `"0.0.0.0"` | IP to bind the receive socket. |
| `bind_port` | `41410` | UDP port to listen on. |
| `holding_dir` | `/var/lib/parachuter/holding` | In-flight assemblies live here. |
| `final_dir` | `/var/lib/parachuter/downloads` | Completed files move here. |
| `csv_dir` | `/var/lib/parachuter/csv` | CSV ledger snapshots received from sender. |
| `control_socket` | `/run/parachuter/receiver.sock` | Unix socket path. |

### `[cleaner]`

| Key | Default | Description |
|---|---|---|
| `holding_dir` | same as receiver | Must match receiver's `holding_dir`. |
| `active_link` | `"pilot"` | Which link budget to apply. |
| `run_period_secs` | `60` | How often the cleaner wakes to scan. Can be triggered immediately with `cleaner-run`. |
| `checker_period_secs` | `7200` | How often to reconcile CSV ledgers against `final_dir`. |
| `dedup_ttl_secs` | `300` | How long to suppress a re-request for the same chunk. |
| `state_path` | `/var/lib/parachuter/cleaner-state.json` | Persistent dedup table. |
| `control_socket` | `/run/parachuter/cleaner.sock` | Unix socket path. |

### `[cleaner.links.<name>]`

Per-link budget for the cleaner's retransmit throttle.

| Key | Description |
|---|---|
| `max_in_flight` | Maximum outstanding re-requests per cleaner pass. |
| `min_period_ms` | Minimum milliseconds between consecutive requests. |

---

## Runtime Control (`parachuter ctl`) 🎛️

All flags take effect immediately without restarting any daemon.

```bash
# Check liveness
parachuter ctl -s /run/parachuter/sender.sock ping

# Full status (auto-detects sender / receiver / cleaner)
parachuter ctl -s /run/parachuter/sender.sock status

# Switch to a different named link
parachuter ctl -s /run/parachuter/sender.sock set-link --link tdrss

# Change chunk size on the fly (bytes, including header)
parachuter ctl -s /run/parachuter/sender.sock set-link --chunk-size 8192

# Change throughput cap
parachuter ctl -s /run/parachuter/sender.sock set-link --kbps 250

# Override destination IP and port without changing the named link
parachuter ctl -s /run/parachuter/sender.sock set-link --ip 10.0.0.5 --port 9000

# Pause / resume
parachuter ctl -s /run/parachuter/sender.sock set-state paused
parachuter ctl -s /run/parachuter/sender.sock set-state auto

# Submit any file by absolute path (returns assigned file_id as JSON)
parachuter ctl -s /run/parachuter/sender.sock submit --path /data/raw/foo.fits

# Manually queue a file already in the ledger
parachuter ctl -s /run/parachuter/sender.sock enqueue --file-id 42

# Queue a chunk range only, at the head of the queue
parachuter ctl -s /run/parachuter/sender.sock enqueue --file-id 42 --start 100 --count 50 --interrupt

# Resend only the filename (manifest) packet for a file
parachuter ctl -s /run/parachuter/sender.sock resend-name --file-id 42

# Drop everything from the queue
parachuter ctl -s /run/parachuter/sender.sock flush

# Trigger a cleaner scan immediately (does not wait for the next timer tick)
parachuter ctl -s /run/parachuter/cleaner.sock cleaner-run
```

---

## Named Links 📡

Links are declared in `[links.<name>]` and referenced by name throughout the
config. Changing the active link, even at runtime, switches the destination
IP, port, and throughput cap atomically. The sender keeps a single bound UDP
socket and changes only the `send_to` destination on each packet.

Multicast links (any IP in `224.0.0.0/4`) are automatically joined by the
receiver at startup. No configuration change is needed on the receiver side
when the sender switches from unicast (pilot) to multicast (LOS / TDRSS).

---

## Dedup and Re-request Logic 🧹

The cleaner uses three independent dedup mechanisms to avoid flooding the
link:

1. **Live queue inspection** — On each pass the cleaner calls `SenderStatus`
   and subtracts any chunk ranges already queued from its own request list.

2. **TTL dedup table** — Every request is written to `cleaner-state.json` with
   a TTL (default 5 minutes). If the sender control socket is unreachable, the
   TTL cache still prevents re-requesting something sent seconds ago.

3. **Per-link budget** — Each pass is hard-capped by `max_in_flight`, and
   consecutive requests are spaced at least `min_period_ms` apart.

The cleaner can be triggered on demand via `parachuter ctl ... cleaner-run`,
which wakes the main loop immediately (no waiting for the next timer tick).

---

## Building

```bash
# Debug (fast compile)
cargo build --workspace

# Release (LTO, symbol-stripped, panic=abort)
cargo build --workspace --release

# Run the test suite (requires no external services)
cargo test --workspace
```

Minimum Rust version: **1.74** (declared as `rust-version` in the workspace).

---

## Testing ✅

The test suite is entirely self-contained: no real network sockets, no
running daemons required. Tests use `tempfile` for isolated temporary
directories.

```bash
cargo test --workspace                        # all tests
cargo test -p parachuter                      # core library unit tests only
cargo test -p parachuter --test end_to_end    # integration tests only
cargo test -p parachuter -- --nocapture       # verbose output
```

The integration tests in `crates/parachuter/tests/end_to_end.rs` cover:

- Out-of-order chunk delivery
- Manifest-first and data-first delivery paths
- Single-chunk and empty files
- Exact-multiple payload size
- Large files spanning multiple bitmap bytes (> 64 chunks)
- Duplicate detection
- Missing-range computation
- Packet corruption detection (CRC and magic)
- Ledger idempotency and CSV export
- Config validation

---

## Deployment (systemd)

Two systemd units on the ground both invoke the same binary with different
subcommands; one unit on the payload runs the sender.

```ini
# /etc/systemd/system/parachuter-sender.service  (on payload)
[Unit]
Description=parachuter sender
After=network-online.target

[Service]
ExecStart=/usr/local/bin/parachuter sender --config /etc/parachuter/config.toml
Restart=on-failure
User=parachuter

[Install]
WantedBy=multi-user.target
```

```ini
# /etc/systemd/system/parachuter-receiver.service  (on ground)
[Unit]
Description=parachuter receiver
After=network-online.target

[Service]
ExecStart=/usr/local/bin/parachuter receiver --config /etc/parachuter/config.toml
Restart=on-failure
User=parachuter

[Install]
WantedBy=multi-user.target
```

```ini
# /etc/systemd/system/parachuter-cleaner.service  (on ground)
[Unit]
Description=parachuter cleaner
After=parachuter-receiver.service

[Service]
ExecStart=/usr/local/bin/parachuter cleaner --config /etc/parachuter/config.toml
Restart=on-failure
User=parachuter

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable --now parachuter-sender    # payload
sudo systemctl enable --now parachuter-receiver parachuter-cleaner   # ground
```

All modes handle `SIGTERM` gracefully. The sender drains the current packet
before exiting; the receiver and cleaner flush their state files.

---

## Design Notes

See [`docs/DESIGN.md`](docs/DESIGN.md) for:

- 🪂 Wire protocol layout (full 32-byte header spec)
- 📋 Manifest sidecar format
- 🔧 Design decisions
- ⚠️ Known limitations and future work

---

## License

Copyright (c) 2026 Emaad Paracha

This software is licensed for personal, academic, research, educational, and other non-commercial use only. Commercial use requires prior written permission from the copyright holder. See [LICENSE](LICENSE) for the full terms.
