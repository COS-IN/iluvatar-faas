pub mod handlers;
pub mod http_server;
use anyhow::Result;

use std::net::SocketAddr;

// instantiate the HTTP server with rpc host and port
pub async fn create_http_server(
    addr: &str,
    port: u16,
    rpc_host: &str,
    rpc_port: u16,
) -> Result<http_server::HttpServer, String> {
    let socket_addr: SocketAddr = format!("{}:{}", addr, port)
        .parse()
        .map_err(|e| format!("Failed to parse SocketAddr: {}", e))?;

    Ok(http_server::HttpServer::new(
        socket_addr,
        rpc_host.to_string(),
        rpc_port,
    ))
}
