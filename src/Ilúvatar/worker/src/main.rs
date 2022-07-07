extern crate iluvatar_worker;

use std::time::Duration;
use iluvatar_lib::transaction::{TransactionId, STARTUP_TID};
use iluvatar_lib::worker_api::config::Configuration;
use iluvatar_lib::services::containers::containermanager::ContainerManager;
use iluvatar_worker::iluvatar_worker::IluvatarWorkerImpl;
use iluvatar_lib::rpc::iluvatar_worker_server::IluvatarWorkerServer;
// use iluvatar_lib::services::network::namespace_manager::NamespaceManager;
use log::*;
use tonic::transport::Server;
use iluvatar_worker::invocation::invoker::InvokerService;
use iluvatar_lib::services::LifecycleFactory;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  iluvatar_lib::utils::file_utils::ensure_temp_dir()?;
  let tid: &TransactionId = &STARTUP_TID;

  let server_config = Configuration::boxed(None).unwrap();
  let _logger = iluvatar_worker::logging::make_logger(&server_config, tid);
  debug!("[{}] loaded configuration = {:?}", tid, server_config);

  let factory = LifecycleFactory::new(server_config.clone());
  let lifecycle = factory.get_lifecycle_service(tid).await?;

  // let netm = NamespaceManager::boxed(server_config.clone(), tid);
  // netm.ensure_bridge(tid)?;

  let container_man = ContainerManager::boxed(server_config.clone(), lifecycle).await?;
  let invoker = InvokerService::boxed(container_man.clone(), tid);

  let worker = IluvatarWorkerImpl::new(server_config.clone(), container_man, invoker);

  let addr = format!("{}:{}", server_config.address, server_config.port);
  Server::builder()
      .timeout(Duration::from_secs(server_config.timeout_sec))
      .add_service(IluvatarWorkerServer::new(worker))
      .serve(addr.parse()?)
      .await?;
  Ok(())
}
