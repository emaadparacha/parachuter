//! Tokio-based control server.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;

use crate::error::Result;

use super::messages::{Request, Response};

/// Trait that each daemon implements to plug its semantics into the
/// generic socket server. `handle` is `async` and may take its own time;
/// requests on a single connection are serialised.
pub trait ControlHandler: Send + Sync + 'static {
    /// Handle one parsed request and produce a response.
    fn handle(&self, req: Request) -> impl Future<Output = Response> + Send;
}

/// Spawnable Unix-socket server.
pub struct ControlServer<H> {
    path: PathBuf,
    handler: Arc<Mutex<H>>,
}

impl<H: ControlHandler> ControlServer<H> {
    /// Bind a fresh listener at `path`. Removes any stale socket file first.
    pub async fn bind(path: impl AsRef<Path>, handler: H) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        // Remove stale socket file if present.
        if tokio::fs::metadata(&path).await.is_ok() {
            tokio::fs::remove_file(&path).await?;
        }
        Ok(Self {
            path,
            handler: Arc::new(Mutex::new(handler)),
        })
    }

    /// Block forever, serving connections until cancelled.
    pub async fn serve(self) -> Result<()> {
        let listener = UnixListener::bind(&self.path)?;
        tracing::info!(path = %self.path.display(), "control socket listening");
        loop {
            let (stream, _) = listener.accept().await?;
            let handler = self.handler.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_conn(stream, handler).await {
                    tracing::warn!(?e, "control connection failed");
                }
            });
        }
    }
}

async fn handle_conn<H: ControlHandler>(
    mut stream: UnixStream,
    handler: Arc<Mutex<H>>,
) -> Result<()> {
    loop {
        let mut len_buf = [0u8; 4];
        if stream.read_exact(&mut len_buf).await.is_err() {
            // Peer disconnected; that's normal.
            return Ok(());
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        if len > 1_000_000 {
            // Sanity bound; the protocol is small.
            return Ok(());
        }
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).await?;
        let req: Request = match serde_json::from_slice(&payload) {
            Ok(r) => r,
            Err(e) => {
                let resp = Response::BadRequest {
                    reason: format!("malformed json: {e}"),
                };
                write_response(&mut stream, &resp).await?;
                continue;
            }
        };
        let resp = handler.lock().await.handle(req).await;
        write_response(&mut stream, &resp).await?;
    }
}

async fn write_response(stream: &mut UnixStream, resp: &Response) -> Result<()> {
    let bytes = serde_json::to_vec(resp)?;
    stream.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
    stream.write_all(&bytes).await?;
    stream.flush().await?;
    Ok(())
}
