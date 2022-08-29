use std::sync::Arc;
use std::time::Duration;
use iluvatar_library::logging::start_tracing;
use iluvatar_worker_library::services::{invocation::invoker::InvokerService, containers::LifecycleFactory};
use iluvatar_library::transaction::{TransactionId, STARTUP_TID};
use iluvatar_worker_library::worker_api::config::Configuration;
use iluvatar_worker_library::services::containers::containermanager::ContainerManager;
use iluvatar_worker_library::worker_api::create_worker;
use crate::utils::{parse, register_rpc_to_controller};
use iluvatar_worker_library::rpc::iluvatar_worker_server::IluvatarWorkerServer;
use iluvatar_library::utils::config::get_val;
use anyhow::Result;
use tonic::transport::Server;
use tracing::{debug};

pub mod utils;

async fn run(server_config: Arc<Configuration>, tid: &TransactionId) -> Result<()> {
  debug!(tid=tid.as_str(), config=?server_config, "loaded configuration");

  let worker = create_worker(server_config.clone(), tid).await?;
  let addr = format!("{}:{}", server_config.address, server_config.port);

  register_rpc_to_controller(server_config.clone(), tid.clone());
  Server::builder()
      .timeout(Duration::from_secs(server_config.timeout_sec))
      .add_service(IluvatarWorkerServer::new(worker))
      .serve(addr.parse()?)
      .await?;
  Ok(())
}

async fn clean(server_config: Arc<Configuration>, tid: &TransactionId) -> Result<()> {
  debug!(tid=?tid, config=?server_config, "loaded configuration");

  let factory = LifecycleFactory::new(server_config.container_resources.clone(), server_config.networking.clone());
  let lifecycle = factory.get_lifecycle_service(tid, false).await?;

  let container_man = ContainerManager::boxed(server_config.limits.clone(), server_config.container_resources.clone(), lifecycle.clone(), tid).await?;
  let _invoker = InvokerService::boxed(container_man.clone(), tid, server_config.limits.clone(), server_config.invocation.clone());
  lifecycle.clean_containers("default", tid).await?;
  Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  iluvatar_library::utils::file::ensure_temp_dir()?;
  let tid: &TransactionId = &STARTUP_TID;

  let args = parse();
  let config_pth = get_val("config", &args)?;

  match args.subcommand() {
    Some(("clean", _)) => {
      let server_config = Configuration::boxed(true, &config_pth).unwrap();
      let _guard = start_tracing(server_config.logging.clone(), server_config.graphite.clone(), &server_config.name)?;
      clean(server_config, tid).await?;
    },
    None => { 
      let server_config = Configuration::boxed(false, &config_pth).unwrap();
      let _guard = start_tracing(server_config.logging.clone(), server_config.graphite.clone(), &server_config.name)?;
      run(server_config, tid).await?;
    },
    Some((cmd, _)) => panic!("Unknown command {}, try --help", cmd),
  };
  Ok(())
}
