use crate::services::containers::structs::ParsedResult;
use crate::services::registration::RegisteredFunction;
use async_trait::async_trait;
use iluvatar_library::transaction::TransactionId;
use iluvatar_library::types::ContainerServer;
use iluvatar_library::utils::port::Port;
use std::sync::Arc;
use std::time::Duration;

mod http_client;
mod socket_client;

/// Trait abstracting connection to server inside a container.
#[async_trait]
pub trait ContainerClient: Send + Sync {
    async fn invoke(
        &self,
        json_args: &str,
        tid: &TransactionId,
        container_id: &str,
    ) -> anyhow::Result<(ParsedResult, Duration)>;
    async fn move_to_device(&self, tid: &TransactionId, container_id: &str) -> anyhow::Result<()>;
    async fn move_from_device(&self, tid: &TransactionId, container_id: &str) -> anyhow::Result<()>;
}
pub async fn create_container_client(
    function: &Arc<RegisteredFunction>,
    container_id: &str,
    port: Port,
    address: &str,
    invoke_timeout: u64,
    tid: &TransactionId,
) -> anyhow::Result<Box<dyn ContainerClient>> {
    match function.container_server {
        ContainerServer::HTTP => {
            match http_client::HttpContainerClient::new(container_id, port, address, invoke_timeout, tid) {
                Ok(c) => Ok(Box::new(c)),
                Err(e) => Err(e),
            }
        },
        ContainerServer::UnixSocket => {
            match socket_client::SocketContainerClient::new(container_id, invoke_timeout, tid).await {
                Ok(c) => Ok(Box::new(c)),
                Err(e) => Err(e),
            }
        },
    }
}
