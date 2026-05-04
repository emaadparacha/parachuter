//! TOML-based runtime configuration with hot reload.
//!
//! Why hot reload? The original code baked IPs, chunk size and link selection
//! into Rust constants or one-shot bitcmd packets. Re-tuning meant rebuilding
//! and restarting. parachuter watches its config file with `notify` so changes
//! apply within ~1 second without a restart, and the same values can also be
//! poked over the control socket – the file is the durable record, the socket
//! is the live override.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::proto::DEFAULT_CHUNK_SIZE;

/// One named link, e.g. `pilot`, `los`, `tdrss`, `starlink`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Link {
    /// Destination IP address packets are sent to.
    pub ip: String,
    /// UDP port packets are sent to.
    pub port: u16,
    /// Maximum sustained throughput in kilobits per second.
    pub max_kbps: u64,
    /// Optional comment describing the link.
    #[serde(default)]
    pub note: Option<String>,
}

/// Top-level configuration shared by every parachuter binary.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Config {
    /// Sender-specific configuration.
    pub sender: SenderConfig,
    /// Receiver-specific configuration.
    pub receiver: ReceiverConfig,
    /// Cleaner-specific configuration.
    pub cleaner: CleanerConfig,
    /// Named links indexed by short name.
    pub links: BTreeMap<String, Link>,
}

/// Configuration consumed by `parachuter sender`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SenderConfig {
    /// IP the sender binds for outbound packets.
    pub bind_ip: String,
    /// Source port for outbound datagrams.
    pub bind_port: u16,
    /// Datagram size including the 32-byte header.
    pub chunk_size: usize,
    /// Active link key (must exist in `[links]`).
    pub active_link: String,
    /// Where the SQLite ledger lives.
    pub ledger_path: PathBuf,
    /// File-priority hierarchy used in `Auto` (and as a fallback elsewhere).
    pub priority_dirs: Vec<PathBuf>,
    /// File-priority hierarchy walked when the sender is in `Debug` state.
    /// If empty, no auto-discovery happens in debug mode (manual `submit` /
    /// `enqueue` still work). Defaults to empty.
    #[serde(default)]
    pub priority_dirs_debug: Vec<PathBuf>,
    /// Optional whitelist of file extensions the auto-scanner is allowed to
    /// pick up (matched case-insensitively against the file's tail; entries
    /// may include or omit a leading dot, and may be compound, e.g.
    /// `"fits.bz2"`). Defaults to empty (no restriction).
    #[serde(default)]
    pub include_extensions: Vec<String>,
    /// Optional blacklist of file extensions the auto-scanner must skip.
    /// Same matching rules as [`Self::include_extensions`]. Defaults to
    /// empty. On clashes between this list and `include_extensions`, this
    /// list wins (the file is skipped). The filter applies only to the
    /// auto-scanner; explicit `parachuter ctl submit` always honours the
    /// caller's path.
    #[serde(default)]
    pub skip_extensions: Vec<String>,
    /// Path to the Unix domain socket the sender exposes for control.
    pub control_socket: PathBuf,
    /// Initial state.
    pub initial_state: SenderState,
    /// Calibration counters (mirrored from the SuperBIT downlinker).
    pub images_per_dark: u32,
    /// As above.
    pub images_per_flat: u32,
    /// As above.
    pub images_per_bias: u32,
    /// How often to poll the priority directories when the queue is empty.
    pub recheck_period_secs: u64,
    /// How often to dump the ledger as a CSV to ground.
    pub status_dump_period_secs: u64,
}

/// Sender control states.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SenderState {
    /// Sender pulls work from the priority directories and ships continuously.
    Auto,
    /// Sender only sends what is explicitly requested.
    Manual,
    /// Sender drops new work; useful while the link is down.
    Paused,
    /// Sender services in-flight retransmits but ignores fresh files.
    Debug,
}

/// Configuration consumed by `parachuter receiver`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReceiverConfig {
    /// IP the receiver binds to.
    pub bind_ip: String,
    /// UDP port to bind.
    pub bind_port: u16,
    /// Holding directory for in-flight assemblies.
    pub holding_dir: PathBuf,
    /// Final destination for completed files.
    pub final_dir: PathBuf,
    /// Where to put filenames matching `files_sent_*.csv*`.
    pub csv_dir: PathBuf,
    /// Path to the Unix domain socket the receiver exposes for control.
    pub control_socket: PathBuf,
}

/// Configuration consumed by `parachuter cleaner`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CleanerConfig {
    /// Holding directory for in-flight assemblies (must match the receiver).
    pub holding_dir: PathBuf,
    /// Final directory.
    pub final_dir: PathBuf,
    /// CSV directory.
    pub csv_dir: PathBuf,
    /// Path to the sender's control socket; cleaner queries it for queue
    /// state to dedup requests.
    pub sender_control_socket: PathBuf,
    /// Per-link concurrency limits.
    pub links: BTreeMap<String, LinkBudget>,
    /// Active link key.
    pub active_link: String,
    /// Where to write a JSON state file with everything the cleaner has
    /// requested (TTL dedup backstop).
    pub state_path: PathBuf,
    /// How often the cleaner main loop runs.
    pub run_period_secs: u64,
    /// How often the checker reconciles ledger CSVs against the final dir.
    pub checker_period_secs: u64,
    /// How long an in-flight request stays in the dedup cache, in seconds.
    pub dedup_ttl_secs: u64,
    /// Path to the cleaner's own control socket.
    pub control_socket: PathBuf,
}

/// Bandwidth budget for one uplink.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LinkBudget {
    /// Max unacknowledged requests at any time.
    pub max_in_flight: u32,
    /// Minimum spacing between requests (milliseconds).
    pub min_period_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        let mut links = BTreeMap::new();
        links.insert(
            "pilot".to_string(),
            Link {
                ip: "192.168.98.24".into(),
                port: 41410,
                max_kbps: 1000,
                note: Some("Pilot link".into()),
            },
        );
        links.insert(
            "los".to_string(),
            Link {
                ip: "239.255.0.1".into(),
                port: 41410,
                max_kbps: 250,
                note: Some("LOS multicast".into()),
            },
        );
        links.insert(
            "tdrss".to_string(),
            Link {
                ip: "239.255.0.2".into(),
                port: 41410,
                max_kbps: 30,
                note: Some("TDRSS multicast".into()),
            },
        );

        let mut cleaner_links = BTreeMap::new();
        for k in ["pilot", "los", "tdrss"] {
            cleaner_links.insert(
                k.into(),
                LinkBudget {
                    max_in_flight: 8,
                    min_period_ms: 500,
                },
            );
        }

        Self {
            sender: SenderConfig {
                bind_ip: "0.0.0.0".into(),
                bind_port: 34646,
                chunk_size: DEFAULT_CHUNK_SIZE,
                active_link: "pilot".into(),
                ledger_path: PathBuf::from("/var/lib/parachuter/ledger.sqlite"),
                priority_dirs: vec![PathBuf::from("/data/parachuter/queue")],
                priority_dirs_debug: Vec::new(),
                include_extensions: Vec::new(),
                skip_extensions: Vec::new(),
                control_socket: PathBuf::from("/run/parachuter/sender.sock"),
                initial_state: SenderState::Auto,
                images_per_dark: 0,
                images_per_flat: 0,
                images_per_bias: 0,
                recheck_period_secs: 100,
                status_dump_period_secs: 6 * 60 * 60,
            },
            receiver: ReceiverConfig {
                bind_ip: "0.0.0.0".into(),
                bind_port: 41410,
                holding_dir: PathBuf::from("/var/lib/parachuter/holding"),
                final_dir: PathBuf::from("/var/lib/parachuter/downloads"),
                csv_dir: PathBuf::from("/var/lib/parachuter/csv"),
                control_socket: PathBuf::from("/run/parachuter/receiver.sock"),
            },
            cleaner: CleanerConfig {
                holding_dir: PathBuf::from("/var/lib/parachuter/holding"),
                final_dir: PathBuf::from("/var/lib/parachuter/downloads"),
                csv_dir: PathBuf::from("/var/lib/parachuter/csv"),
                sender_control_socket: PathBuf::from("/run/parachuter/sender.sock"),
                links: cleaner_links,
                active_link: "pilot".into(),
                state_path: PathBuf::from("/var/lib/parachuter/cleaner-state.json"),
                run_period_secs: 60,
                checker_period_secs: 2 * 60 * 60,
                dedup_ttl_secs: 300,
                control_socket: PathBuf::from("/run/parachuter/cleaner.sock"),
            },
            links,
        }
    }
}

impl Config {
    /// Load a config from disk, returning a [`Config::default`] augmented
    /// with whatever the user has set. Missing fields use the defaults.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let s = std::fs::read_to_string(path).map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => Error::NotFound(path.to_path_buf()),
            _ => Error::Io(e),
        })?;
        let cfg: Self = toml::from_str(&s)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Serialise the config back out as TOML.
    pub fn to_toml(&self) -> String {
        toml::to_string_pretty(self).expect("config serialises")
    }

    /// Validate cross-field invariants the type system can't express.
    pub fn validate(&self) -> Result<()> {
        if !self.links.contains_key(&self.sender.active_link) {
            return Err(Error::BadConfig(format!(
                "sender.active_link `{}` not declared in [links]",
                self.sender.active_link
            )));
        }
        if !self.cleaner.links.contains_key(&self.cleaner.active_link) {
            return Err(Error::BadConfig(format!(
                "cleaner.active_link `{}` has no budget in [cleaner.links]",
                self.cleaner.active_link
            )));
        }
        if !(crate::proto::MIN_CHUNK_SIZE..=crate::proto::MAX_CHUNK_SIZE)
            .contains(&self.sender.chunk_size)
        {
            return Err(Error::BadChunkSize(self.sender.chunk_size));
        }
        Ok(())
    }
}

/// Thread-safe handle that holds the *currently active* config, atomically
/// swapped whenever the file changes on disk.
#[derive(Clone)]
pub struct LiveConfig {
    inner: Arc<RwLock<Arc<Config>>>,
    path: PathBuf,
}

impl LiveConfig {
    /// Load the file once and return a handle whose [`Self::current`] returns
    /// the latest version. Spawn [`Self::watch`] in a background thread (or
    /// tokio task) to enable hot reload.
    pub fn load(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let cfg = Config::load(&path)?;
        Ok(Self {
            inner: Arc::new(RwLock::new(Arc::new(cfg))),
            path,
        })
    }

    /// Cheap snapshot of the current configuration. The returned `Arc` is
    /// stable even if the file is reloaded mid-call.
    pub fn current(&self) -> Arc<Config> {
        self.inner.read().unwrap().clone()
    }

    /// Replace the live config in memory (used by the control socket to apply
    /// transient overrides without rewriting the TOML file).
    pub fn override_with(&self, new: Config) -> Result<()> {
        new.validate()?;
        *self.inner.write().unwrap() = Arc::new(new);
        Ok(())
    }

    /// Path of the file backing this config.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Spawn a background thread that polls the file for changes and
    /// hot-reloads it. Errors are logged via `tracing` and don't tear down
    /// the process.
    pub fn watch(self) -> Result<()> {
        use notify::{RecommendedWatcher, RecursiveMode, Watcher};
        let (tx, rx) = std::sync::mpsc::channel();
        let mut watcher: RecommendedWatcher = notify::recommended_watcher(tx)
            .map_err(|e| Error::BadConfig(format!("watcher: {e}")))?;
        watcher
            .watch(&self.path, RecursiveMode::NonRecursive)
            .map_err(|e| Error::BadConfig(format!("watch: {e}")))?;
        std::thread::Builder::new()
            .name("parachuter-config-watch".into())
            .spawn(move || {
                let _watcher = watcher;
                while let Ok(_event) = rx.recv_timeout(Duration::from_secs(5)) {
                    match Config::load(&self.path) {
                        Ok(cfg) => {
                            tracing::info!(
                                path = %self.path.display(),
                                "reloaded config"
                            );
                            *self.inner.write().unwrap() = Arc::new(cfg);
                        }
                        Err(e) => tracing::warn!(?e, "failed to reload config"),
                    }
                }
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_valid() {
        Config::default().validate().unwrap();
    }

    #[test]
    fn rejects_unknown_active_link() {
        let mut c = Config::default();
        c.sender.active_link = "missing".into();
        assert!(matches!(c.validate(), Err(Error::BadConfig(_))));
    }

    #[test]
    fn rejects_silly_chunk_size() {
        let mut c = Config::default();
        c.sender.chunk_size = 4;
        assert!(matches!(c.validate(), Err(Error::BadChunkSize(_))));
    }
}
