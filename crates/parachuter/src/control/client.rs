//! Control-plane client used by `parachuter ctl` and the cleaner mode.

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use crate::error::Result;

use super::messages::{Request, Response};

/// Thin handle over a Unix socket. Each method opens a fresh connection
/// because parachuter daemons handle one request at a time and connection
/// setup is essentially free locally.
pub struct ControlClient {
    path: PathBuf,
}

impl ControlClient {
    /// Construct a client targeting `path`. Doesn't connect until [`Self::call`]
    /// is invoked.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// Send a single request and read the response, with a 5-second timeout.
    pub async fn call(&self, req: Request) -> Result<Response> {
        let fut = self.call_inner(req);
        match tokio::time::timeout(Duration::from_secs(5), fut).await {
            Ok(r) => r,
            Err(_) => Err(crate::Error::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "control call timed out",
            ))),
        }
    }

    async fn call_inner(&self, req: Request) -> Result<Response> {
        let mut stream = UnixStream::connect(&self.path).await?;
        let bytes = serde_json::to_vec(&req)?;
        stream.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
        stream.write_all(&bytes).await?;
        stream.flush().await?;
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).await?;
        let resp: Response = serde_json::from_slice(&payload)?;
        Ok(resp)
    }

    /// Convenience: send `Ping` and return whether a daemon answered at all.
    pub async fn alive(&self) -> bool {
        matches!(self.call(Request::Ping).await, Ok(Response::Pong { .. }))
    }

    /// Path the client connects to.
    pub fn path(&self) -> &Path {
        &self.path
    }
}
