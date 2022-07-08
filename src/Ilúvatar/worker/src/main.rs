extern crate iluvatar_worker;

use std::sync::Arc;
use std::time::Duration;
use iluvatar_lib::transaction::{TransactionId, STARTUP_TID};
use iluvatar_lib::worker_api::config::Configuration;
use iluvatar_lib::services::containers::containermanager::ContainerManager;
use iluvatar_worker::args::parse;
use iluvatar_worker::iluvatar_worker::IluvatarWorkerImpl;
use iluvatar_lib::rpc::iluvatar_worker_server::IluvatarWorkerServer;
use iluvatar_lib::utils::config_utils::get_val;
use log::*;
use anyhow::Result;
use tonic::transport::Server;
use iluvatar_worker::invocation::invoker::InvokerService;
use iluvatar_lib::services::{LifecycleFactory};
use flexi_logger::WriteMode;

async fn run(server_config: Arc<Configuration>, tid: &TransactionId) -> Result<()> {
  let _logger = iluvatar_worker::logging::make_logger(&server_config, tid, WriteMode::Async);
  debug!("[{}] loaded configuration = {:?}", tid, server_config);

  let factory = LifecycleFactory::new(server_config.clone());
  let lifecycle = factory.get_lifecycle_service(tid, true).await?;

  let container_man = ContainerManager::boxed(server_config.clone(), lifecycle.clone()).await?;
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

async fn clean(server_config: Arc<Configuration>, tid: &TransactionId) -> Result<()> {
  let _logger = iluvatar_worker::logging::make_logger(&server_config, tid, WriteMode::Direct);
  debug!("[{}] loaded configuration = {:?}", tid, server_config);

  let factory = LifecycleFactory::new(server_config.clone());
  let lifecycle = factory.get_lifecycle_service(tid, false).await?;

  let container_man = ContainerManager::boxed(server_config.clone(), lifecycle.clone()).await?;
  let _invoker = InvokerService::boxed(container_man.clone(), tid);
  lifecycle.clean_containers("default", tid).await?;
  Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  iluvatar_lib::utils::file_utils::ensure_temp_dir()?;
  let tid: &TransactionId = &STARTUP_TID;

  let args = parse();
  let config_pth = get_val("config", &args)?;

  match args.subcommand() {
    ("clean", Some(_)) => {
      let server_config = Configuration::boxed(true, &config_pth).unwrap();
      clean(server_config, tid).await?;      
    },
    (_,_) => { 
      let server_config = Configuration::boxed(false, &config_pth).unwrap();
      run(server_config, tid).await?;
     },
  };
  Ok(())
}
