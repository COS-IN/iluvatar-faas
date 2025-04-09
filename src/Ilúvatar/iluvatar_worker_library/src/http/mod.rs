pub mod handlers;
pub mod http_server;
use crate::http::http_server::HttpServer;
use crate::worker_api::iluvatar_worker::IluvatarWorkerImpl;
use std::net::SocketAddr;
use std::sync::Arc;

pub async fn create_http_server(
    address: &str,
    port: u16,
    worker: Arc<IluvatarWorkerImpl>,
) -> Result<HttpServer, String> {
    let socket_addr: SocketAddr = format!("{}:{}", address, port)
        .parse()
        .map_err(|e| format!("Failed to parse SocketAddr: {}", e))?;

    Ok(http_server::HttpServer::new(socket_addr, worker))
}
