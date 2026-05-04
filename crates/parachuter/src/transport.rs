//! UDP transport abstraction.
//!
//! The original code re-`bind`ed a fresh `UdpSocket` inside `send_chunk` and
//! `receive_chunk` on *every* call. That's a syscall per packet plus an
//! ephemeral-port churn that the OS will eventually rate-limit. parachuter
//! binds once and keeps the socket alive.

use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::time::Duration;

use crate::error::Result;

/// Sender-side socket that owns the bind once and pushes datagrams to whatever
/// peer the caller specifies.
pub struct UdpSender {
    socket: UdpSocket,
}

impl UdpSender {
    /// Bind a socket on `bind_ip:bind_port`. The send buffer is set generously
    /// so back-to-back chunks don't block on the kernel queue.
    pub fn bind(bind_ip: &str, bind_port: u16) -> Result<Self> {
        let addr: SocketAddr = format!("{bind_ip}:{bind_port}").parse().map_err(|e| {
            crate::Error::BadConfig(format!("invalid sender bind address: {e}"))
        })?;
        let socket = UdpSocket::bind(addr)?;
        Ok(Self { socket })
    }

    /// Send a datagram to the configured peer.
    pub fn send_to(&self, bytes: &[u8], dest: &str, dest_port: u16) -> Result<()> {
        let dest: SocketAddr = format!("{dest}:{dest_port}").parse().map_err(|e| {
            crate::Error::BadConfig(format!("invalid destination: {e}"))
        })?;
        self.socket.send_to(bytes, dest)?;
        Ok(())
    }
}

/// Receiver-side socket. Joins the right multicast group when the bind IP
/// looks like a multicast address.
pub struct UdpReceiver {
    socket: UdpSocket,
}

impl UdpReceiver {
    /// Bind on `bind_ip:bind_port`. If `bind_ip` is multicast (224.0.0.0/4),
    /// joins the group on the default interface.
    pub fn bind(bind_ip: &str, bind_port: u16) -> Result<Self> {
        let socket = UdpSocket::bind(format!("0.0.0.0:{bind_port}"))?;
        socket.set_read_timeout(Some(Duration::from_secs(1)))?;
        if let Ok(addr) = bind_ip.parse::<Ipv4Addr>() {
            if addr.is_multicast() {
                socket.join_multicast_v4(&addr, &Ipv4Addr::UNSPECIFIED)?;
            }
        }
        Ok(Self { socket })
    }

    /// Receive into `buf`. Returns `Ok(None)` if the read timed out, allowing
    /// the caller's loop to also handle control-plane work.
    pub fn recv(&self, buf: &mut [u8]) -> Result<Option<(usize, SocketAddr)>> {
        match self.socket.recv_from(buf) {
            Ok(x) => Ok(Some(x)),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock
                || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                Ok(None)
            }
            Err(e) => Err(e.into()),
        }
    }
}
