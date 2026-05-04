# parachuter — Design Document

This document describes the architecture, wire formats, design decisions, and
known limitations of the `parachuter` file-downlink stack.

`parachuter` is inspired by, and was first deployed on, the SuperBIT balloon
telescope. It is shipped as a generic library plus a single multi-mode
binary; nothing in the wire protocol, sidecar format, or control plane is
specific to balloons or telescopes.

---

## Table of Contents

- [Problem Statement](#problem-statement)
- [System Overview](#system-overview)
- [Wire Protocol](#wire-protocol)
- [Manifest Sidecar Format](#manifest-sidecar-format)
- [Chunker](#chunker)
- [Reassembler](#reassembler)
- [SQLite Ledger](#sqlite-ledger)
- [Control Plane](#control-plane)
- [File Ingest](#file-ingest)
- [Rate Limiter](#rate-limiter)
- [File Priority Queue](#file-priority-queue)
- [Cleaner Dedup Logic](#cleaner-dedup-logic)
- [Config Hot-Reload](#config-hot-reload)
- [Single-Binary Layout](#single-binary-layout)
- [Design Decisions](#design-decisions)
- [Known Limitations and Future Work](#known-limitations-and-future-work)

---

## Problem Statement

A balloon-borne or otherwise remote payload needs to deliver large files —
typically multi-gigabyte science images — to a ground station over a mix of
high-bandwidth, lossy, and very narrow link types. SuperBIT's link table is
typical of the regime:

| Link | Bandwidth | Notes |
|---|---|---|
| Pilot (tethered) | ~1 Mbit/s | High-reliability wired link during ascent |
| LOS | ~250 kbit/s | Line-of-sight UHF radio, lossy |
| TDRSS | ~30 kbit/s | NASA TDRS relay, high latency, very expensive |
| Starlink | variable | Ad-hoc, rsync-based (out of scope for the UDP path) |

The link is effectively **unidirectional**: the ground cannot send general
TCP ACKs back to the payload because the uplink budget is consumed by
commands and telemetry. File delivery must be "fire and forget" with a
separate low-rate retransmit-request channel (the cleaner → sender control
socket).

---

## System Overview

```
Payload                                      Ground Station
─────────                                    ──────────────
parachuter sender
  ├── SQLite ledger      ─── UDP datagrams ──▶  parachuter receiver
  ├── priority queue                              ├── holding dir (in-flight)
  ├── token-bucket RL                             ├── final dir (complete files)
  └── Unix ctrl socket                            └── Unix ctrl socket
         ▲                                               ▲
         │ parachuter ctl                                │
         │ (live tuning + submit)              parachuter cleaner
                                                  ├── reads holding dir
                                                  ├── TTL dedup table
                                                  └── Unix ctrl socket ──▶ sender
```

Every box above is the same `parachuter` binary running with a different
subcommand.

---

## Wire Protocol

Every parachuter datagram is a fixed **32-byte header** followed by a
variable payload. All multi-byte integers are **little-endian**.

```
offset  size  field
──────  ────  ─────────────────────────────────────────────────────────────
0       4     magic = b"PARC"
4       1     version (currently 1; bump when layout changes)
5       1     ptype  (see PacketType below)
6       2     flags  (u16 LE bitfield; see Flags below)
8       8     file_id (i64 LE), unique file identifier from the ledger
16      4     chunk_id (u32 LE), 0-based index into the file's chunks
20      4     num_chunks (u32 LE), total chunks this file is divided into
24      4     payload_len (u32 LE), bytes that follow this header
28      4     crc32 (u32 LE), CRC32 of [bytes 0..32 with crc field = 0] ‖ payload
32      …     payload (payload_len bytes)
```

The CRC covers the full datagram (header + payload), with the CRC field
itself zeroed during computation. A receiver must validate magic, version,
and CRC before trusting any other field.

### PacketType values

| Value | Name | Payload |
|---|---|---|
| 1 | `Data` | Raw file bytes for `chunk_id`. |
| 2 | `Manifest` | UTF-8 file path string. |
| 3 | `Retransmit` | Same as `Data` but sender explicitly marks it as a re-send. |
| 4 | `NameOnly` | Same as `Manifest`; used for explicit name-only re-sends. |
| 5 | `Heartbeat` | JSON status blob; receivers may ignore. |

Receivers **must tolerate unknown type values** by dropping the packet
without crashing.

### Flags bitfield (u16)

| Bit | Name | Meaning |
|---|---|---|
| 0 | `LAST_CHUNK` | This is the final chunk of the file. Payload may be shorter than the configured chunk size. |
| 1 | `COMPRESSED` | Payload is bzip2-compressed (informational; parachuter does not decompress). |
| 2 | `RETRANSMIT_HINT` | Packet was emitted by the retransmit path, not the main scan loop. |

### Wire-protocol design choices

The protocol is intentionally minimal: 32 bytes of fixed header, an
explicit magic prefix, an explicit version byte, and a CRC over the full
datagram. Every field has a single meaning. Packet types are an explicit
enum, not sentinel values overloaded into other fields.

The CRC catches single-bit corruption from cosmic rays and from bit-rot in
software-defined radio chains. The magic byte short-circuits any stray UDP
traffic that happens to land on the receive port. The version byte gives
us a clean upgrade path the day we need to change the layout.

---

## Manifest Sidecar Format

Every in-flight assembly has two files in `holding_dir`:

- `<file_id>.incoming`, raw data bytes, written in place at the correct offset.
- `<file_id>.parmanifest`, a binary sidecar tracking which chunks have arrived.

The sidecar layout is:

```
offset  size  field
──────  ────  ─────────────────────────────────────────────────────────────
0       4     magic = b"PMAN"
4       1     version (currently 1)
5       1     reserved
6       2     reserved
8       4     chunk_payload_size (u32 LE), payload bytes per non-last chunk
12      4     num_chunks (u32 LE)
16      8     file_size_hint (u64 LE), set when LAST_CHUNK packet arrives
24      4     name_len (u32 LE)
28      4     header_crc32 (u32 LE), CRC32 of bytes 0..28 with crc = 0
32      …     name bytes (UTF-8, name_len bytes)
32+nl   …     bitmap, ⌈num_chunks / 8⌉ bytes
```

Each bit `i` in the bitmap is set when chunk `i` has been written and
`fsync`'d to `incoming`. Bit `i` is byte `i/8`, bit `i%8`.

The sidecar is written atomically via rename-over-tmp so a power failure
mid-write leaves either the old or new version, never a partial state.

### Sidecar design choices

The sidecar uses a packed bitmap rather than one byte (or character) per
chunk: a 600 MB file at 16 KB chunks needs ~5 KB of bitmap rather than
~37 KB. The format has its own magic prefix (`PMAN`) and version byte, so
a stray file under `holding_dir` cannot be misinterpreted, and the layout
can evolve safely. The filename lives inside the structured header rather
than being appended with a delimiter — there is exactly one parser, in one
place.

The data file's size is written authoritatively only when the
`LAST_CHUNK` packet arrives; before then, partial writes may extend the
file but never report a wrong final size.

---

## Chunker

`Chunker` opens a file once, computes how many chunks it divides into, and
serves individual chunks via random-access `seek` + `read`. It never loads
the whole file into memory, which matters on resource-constrained payload
hardware where science images are several hundred MB each.

Key invariants:

- `chunks_for_size(0, p) == 1`. Empty files always produce one zero-length
  chunk so the receiver sees a `LAST_CHUNK` packet and can finalise
  immediately.
- `chunks_for_size(n*p, p) == n`. Exact multiples do **not** produce a
  spurious extra chunk.
- The last chunk carries `LAST_CHUNK` in flags; its payload may be shorter
  than `chunk_size - HEADER_LEN`.

---

## Reassembler

`Reassembler` owns a `holding_dir` and a `finals_dir`. Its `ingest` method:

1. Validates that the packet's `num_chunks` matches the assembly's
   expectation (guards against mid-flight chunk-size changes).
2. Writes the payload at `chunk_id × chunk_payload_size` using `seek` +
   `write`.
3. `fsync`s the data file, then atomically updates the bitmap in the
   sidecar.
4. Returns `Complete` when all bits are set and the filename is known.

On `Complete`, the caller calls `finalize`, which renames `incoming` to
its final path under `finals_dir`, creating intermediate directories as
needed. If the two directories are on different filesystems, a
`copy + remove` fallback is used.

### Ordering edge cases

| Arrival order | Behaviour |
|---|---|
| Data chunks, then manifest | Assembly accumulates normally; `Complete` returned when the manifest fills in the name. |
| Manifest, then data chunks | Manifest creates sidecar with `chunk_payload_size = 0`; first data chunk patches it in and re-saves the sidecar. |
| Last chunk first (multi-chunk) | Returned as `Mismatched`; chunk dropped. Sender must retransmit once an earlier chunk establishes `chunk_payload_size`. |
| Last chunk first (single-chunk) | Accepted normally (`num_chunks == 1` ⟹ `chunk_payload_size == payload_size`). |
| Duplicate chunk | Returned as `Duplicate`; write skipped. |

---

## SQLite Ledger

The sender maintains a SQLite database (`ledger.sqlite`) that tracks every
file it has registered. Schema excerpt:

```sql
CREATE TABLE files (
    file_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    file_name  TEXT NOT NULL UNIQUE,
    file_size  INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    chunk_size INTEGER NOT NULL DEFAULT 16192,
    sha256     TEXT   -- optional, reserved for future use
    …
);
```

Key design choices:

- `file_id` is `AUTOINCREMENT i64`. An `i32` would have wrapped around at
  2 147 483 647 files; `i64` won't.
- `chunk_size` is stored per file so a receiver can detect when the sender
  changed chunk size between sends of the same file.
- WAL mode is enabled so reads (status dashboard) don't block sender
  writes.
- `PRAGMA synchronous = NORMAL`; safe with WAL — one OS crash at worst
  loses one transaction, not the whole database.

---

## Control Plane

Each daemon listens on a Unix domain socket and speaks a length-prefixed
JSON protocol:

```
[4 bytes big-endian length][JSON payload bytes]
```

Requests and responses are typed via a serde `#[serde(tag = "op")]` /
`tag = "kind"` envelope. Every daemon returns `Response::Unsupported` for
messages it does not handle, so `parachuter ctl` can point at any socket
and get a meaningful answer.

The cleaner uses the sender's control socket to query the sender's live
queue state before deciding what to re-request. If the socket is
unreachable, the cleaner falls back to its TTL dedup table.

### Why Unix sockets

- Permissions are standard filesystem ACLs; no bespoke auth.
- `parachuter ctl` and the cleaner can both reach the sender from the same
  host.
- Easy to test: `socat` or `nc -U`.
- Latency is negligible compared to inter-binary TCP.

---

## File Ingest

The sender takes files from two completely independent ingest paths. Both
paths converge on the same SQLite ledger and the same in-memory queue, so
no downstream code needs to know which one was used.

### 1. Priority-directory scan

When the queue is empty, the sender periodically calls
`scanner::next_candidate(priority_dirs, ledger)`, which walks each
priority directory in order, sorts files by mtime ascending, skips files
that already exist in the ledger, and returns the first match. Compressed
siblings (`foo.fits.bz2` next to `foo.fits`) are skipped — they may still
be being written.

This ingest path requires **no IPC and no library binding** to the
producer. Anything that produces a regular file under a watched path is
ingestable, in any language.

### 2. `submit` over the control socket

A `Request::SenderSubmit { path, interrupt }` JSON message hands the
sender an absolute path and asks it to register and queue the file
synchronously. The reply is `Response::Submitted(SubmitAck)`, which
carries `file_id`, `num_chunks`, and `file_size`. This is the same
queueing path the priority-directory scanner uses internally; it just
short-circuits the directory walk.

The bundled CLI wrapper is `parachuter ctl submit --path …`, which is
useful when the producer is a shell or `subprocess` call. For producers
that prefer to speak the protocol directly, the wire format is plain
length-prefixed JSON over a Unix socket — easily callable from Python,
Go, Node, or any language with a Unix-socket binding.

The submit path returns the assigned `file_id` so the caller can
correlate later progress reports (e.g. from `Request::SenderStatus`)
without polling the ledger.

### Why two paths

The directory-watch path is right for batch producers that already write
files to disk (image pipelines, dump scripts, syncs) and don't care about
exact ordering. The submit path is right when the producer wants
synchronous confirmation of enqueueing, needs to push a file from a
directory the sender does not watch, or wants to preempt the queue with
`--interrupt`. Neither path needs a Rust dependency on `parachuter` —
both are language-agnostic.

---

## Rate Limiter

`RateLimiter` is a token-bucket with a one-second burst. Tokens accrue at
the configured kbps, and the sender takes `chunk_bytes` tokens before each
send. The bucket smooths over brief stalls (disk seeks, context switches)
and has no arbitrary clamp on the slowest representable rate, so very low
kbps configurations (e.g. 30 kbit/s on TDRSS) work without surprises.

---

## File Priority Queue

`scanner::next_candidate` walks `priority_dirs` in order, earliest-modified
file first within each directory, and returns the first file not already
in the ledger. Whatever ordering you want — newest darks, oldest sciences,
science-then-calibration — is expressed entirely by directory order in the
TOML config. No recompile required to reorder.

---

## Cleaner Dedup Logic

On each pass the cleaner:

1. Calls `SenderStatus` to get the sender's live queue.
2. Lists all in-flight assemblies from the holding directory.
3. For each assembly, computes missing chunk ranges via the manifest
   bitmap.
4. For each missing range:
   - **Skip** if the range is already in the sender's queue (live dedup).
   - **Skip** if the range was requested in the last `dedup_ttl_secs`
     seconds (TTL dedup table).
   - **Skip** if the per-link `max_in_flight` budget is exhausted for this
     pass.
   - **Pace** by sleeping `min_period_ms` between consecutive requests.
5. Saves the TTL table to disk (atomic rename).

The `cleaner-run` control command wakes the main loop immediately via a
`tokio::sync::Notify`, bypassing the `run_period_secs` timer.

---

## Config Hot-Reload

`LiveConfig` wraps an `Arc<RwLock<Arc<Config>>>`. A background thread
(spawned by `LiveConfig::watch`) listens for filesystem events via the
`notify` crate and replaces the inner `Arc<Config>` on any change. Callers
use `live.current()`, which is a cheap `Arc` clone with zero allocation if
nothing changed.

The control socket can also apply transient overrides
(`SenderConfigure`) that take effect immediately without touching the TOML
file. The TOML file is the persistent baseline; socket overrides are
cleared on restart.

---

## Single-Binary Layout

Every mode (`sender`, `receiver`, `cleaner`, `ctl`, `monitor`) is a
subcommand of a single binary called `parachuter`. The whole project is
one crate, also called `parachuter`, that ships both the library
(`src/lib.rs`) and the binary (`src/main.rs`); cargo compiles them as
separate crate types within the same package. Each subcommand owns its
own process — a typical ground deployment runs `parachuter receiver` and
`parachuter cleaner` as two systemd units pointing at the same binary,
and a flight deployment runs `parachuter sender` as its own unit.

Process boundaries are preserved between modes, so a panic in the cleaner
does not take out the receiver, and each unit can have its own `User=`,
`ExecStart=`, restart policy, and resource limits.

---

## Design Decisions

These are the engineering choices that the protocol, sidecar, ledger, and
control plane settle on, and the reasoning behind each.

| Area | Choice | Reasoning |
|---|---|---|
| Wire framing | 32-byte fixed header with magic, version byte, and CRC32 over the full datagram | Catches bit-flip corruption (cosmic rays, lossy radio); rejects stray UDP traffic; gives a clean version-bump path. |
| Packet types | Explicit `enum` with reserved unknowns | Receivers must drop unknown types without crashing, which is the conservative behaviour for a forward-compatible protocol. |
| Last chunk | Marked with the `LAST_CHUNK` flag; payload may be shorter than the configured chunk size | Gives the receiver an authoritative end-of-file marker without sending an extra zero-length packet. |
| Sidecar manifest | Binary file with magic, version, length-prefixed name, and packed bitmap | One byte per 8 chunks instead of 1 byte per chunk; structured rather than delimited; safe under power loss via atomic rename. |
| File data write | `seek` + `write` + `fsync` at `chunk_id × chunk_payload_size`, then bitmap update | Tolerates arbitrary out-of-order delivery without buffering; durability point is well-defined. |
| Manifest-first ingest | First data chunk patches `chunk_payload_size` into the sidecar | The receiver can begin tracking by name before knowing chunk geometry. |
| Last-chunk-first delivery | Returns `Mismatched`; sender retransmits later | We cannot infer per-chunk payload size from a possibly-partial tail. |
| Ledger | SQLite (WAL + `synchronous = NORMAL`) | One file, no daemon, atomic transactions, queryable by ad-hoc tools. |
| Ledger key | `i64 AUTOINCREMENT` | Will not wrap in any plausible deployment lifetime. |
| Chunker | Streaming `seek + read_exact` per chunk | Avoids loading multi-GB files into memory; per-chunk I/O cost is negligible. |
| Rate limiter | Token bucket with one-second burst | Catches up after stalls; supports very low kbps without arbitrary clamps. |
| File ingest | Two paths: directory watch and `submit` socket | Producers can choose between zero-IPC drop-in and synchronous JSON submit. Both languages-agnostic. |
| Control plane | Length-prefixed JSON over Unix sockets | Filesystem ACLs for auth; trivially scriptable; inspectable in `tcpdump` / `socat`. |
| Retransmit semantics | Separate `Retransmit` packet type with its own flag | Receiver can distinguish first-pass data from re-sends and avoid resetting cold-start state. |
| Cleaner dedup | Three layers (live queue, TTL cache, per-link budget) | Each layer covers a failure mode the others don't (queue inspection unavailable, recent crash, link saturation). |
| Config | TOML on disk + hot reload + control-socket overrides | Persistent baseline plus instant tuning, without restarts. |
| Binary | Single `parachuter` binary, mode subcommands, separate processes per mode | One artifact to deploy, but blast radius and permissions stay scoped per mode. |

---

## Known Limitations and Future Work

### Last-chunk-first deferral
If the very last chunk of a multi-chunk file arrives before any other
chunk, it is dropped (`Mismatched`). The sender must retransmit it later.
A future improvement could hold the chunk in a small in-memory buffer
until `chunk_payload_size` is established.

### No end-to-end acknowledgement
The UDP channel is purely unidirectional. The receiver has no way to tell
the sender "file complete" without a separate channel. The cleaner's
approach (inspect the holding dir, request only what's missing) is the
intended mechanism, but relies on the cleaner having access to the
holding directory.

### No multipath
Only one link is active at a time. If the pilot link fails, the operator
must `set-link tdrss` manually (or via an automated watchdog). A future
improvement could auto-failover based on delivery rate or last-ack age.

### No encryption or authentication
Packets are unauthenticated. On a trusted ground link this is acceptable;
deployments on untrusted networks should wrap the UDP link in a
WireGuard tunnel.

### Metrics / observability
There is no Prometheus endpoint or structured metrics export beyond the
JSON control-socket status and CSV ledger dumps. Adding a `/metrics` HTTP
endpoint is a natural next step.

### Windows compatibility
The control plane uses Unix domain sockets (`UnixListener` / `UnixStream`),
which require Windows 10 build 17063+ and are not supported by the
standard library on earlier versions. The UDP transport and file I/O are
portable.
