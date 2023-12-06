use anyhow::Result;
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};

pub type Port = u16;

/// Get a port number that (should) be valid
///   could be sniped by somebody else,
///   but successive calls to this will not cause that
pub fn free_local_port() -> Result<Port> {
    let socket = SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0);
    match TcpListener::bind(socket)
        .and_then(|listener| listener.local_addr())
        .map(|addr| addr.port())
    {
        Ok(p) => Ok(p),
        Err(e) => anyhow::bail!("Unable to secure a local port becaus '{}'", e),
    }
}
