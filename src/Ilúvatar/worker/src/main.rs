extern crate iluvatar_worker;

use std::sync::Arc;
use std::time::Duration;
use iluvatar_worker::config::Configuration;
use iluvatar_worker::containers::containermanager::ContainerManager;
use iluvatar_worker::iluvatar_worker::IluvatarWorkerImpl;
use iluvatar_lib::rpc::iluvatar_worker_server::IluvatarWorkerServer;
use iluvatar_worker::network::namespace_manager::NamespaceManager;
use log::*;
use tonic::transport::Server;
use iluvatar_worker::invocation::invoker::InvokerService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  iluvatar_lib::utils::ensure_temp_dir()?;

  let server_config = Configuration::boxed(None).unwrap();
  let _logger = iluvatar_worker::logging::make_logger(&server_config);
  debug!("loaded configuration = {:?}", server_config);

  let mut netm = NamespaceManager::new(server_config.clone());
  netm.ensure_bridge()?;
  let netm = Arc::new(netm);

  let container_man = Arc::new(ContainerManager::new(server_config.clone(), netm).await?);
  let invoker = Arc::new(InvokerService::new(container_man.clone()));

  let worker = IluvatarWorkerImpl::new(server_config.clone(), container_man, invoker);

  let addr = format!("{}:{}", server_config.address, server_config.port);
  Server::builder()
      .timeout(Duration::from_secs(server_config.timeout_sec))
      .add_service(IluvatarWorkerServer::new(worker))
      .serve(addr.parse()?)
      .await?;
  Ok(())
}
