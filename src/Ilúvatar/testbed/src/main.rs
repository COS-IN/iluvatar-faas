use std::sync::Arc;
use iluvatar_lib::rpc::{InvokeRequest, RegisterRequest};
use iluvatar_lib::services::invocation::invoker::InvokerService;
use iluvatar_lib::transaction::{TransactionId, STARTUP_TID};
use iluvatar_lib::worker_api::config::Configuration;
use iluvatar_lib::services::containers::containermanager::ContainerManager;
use log::*;
use anyhow::Result;
use iluvatar_lib::services::{LifecycleFactory};
use flexi_logger::WriteMode;

async fn run(server_config: Arc<Configuration>, tid: &TransactionId, mode: WriteMode) -> Result<()> {
  let _logger = testbed::logging::make_logger(&server_config, tid, mode);
  debug!("[{}] loaded configuration = {:?}", tid, server_config);

  let factory = LifecycleFactory::new(server_config.clone());
  let lifecycle = factory.get_lifecycle_service(tid, true).await?;

  let container_man = ContainerManager::boxed(server_config.limits.clone(), server_config.container_resources.clone(), lifecycle.clone()).await?;
  let invoker = InvokerService::boxed(container_man.clone(), tid, server_config.limits.clone());

  for i in 0..100 {
    let req = RegisterRequest {
      function_name: "memory_test".to_string(),
      function_version: format!("0.0.{}", i),
      memory: 128,
      cpus: 1,
      image_name: "docker.io/alfuerst/image_processing-iluvatar-action:latest".to_string(),
      transaction_id: tid.to_string(),
      parallel_invokes: 1
    };
    debug!("[{}] registering test iteration {}", tid, i);
    container_man.register(&req).await?;
    // let req = PrewarmRequest {
    //   function_name: "memory_test".to_string(),
    //   function_version: format!("0.0.{}", i),
    //   memory: 128,
    //   cpu: 1,
    //   image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
    //   transaction_id: tid.to_string()
    // };
    // container_man.prewarm(&req).await?;
    let invk = InvokeRequest {
      function_name: "memory_test".to_string(),
      function_version: format!("0.0.{}", i),
      memory: 128,
      json_args: "{}".to_string(),
      transaction_id: tid.to_string()
    };
    debug!("[{}] invoking test iteration {}", tid, i);
    invoker.invoke(invk).await?;
    info!("[{}] Container {} created and invoked", tid, i);
  }

  Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
  iluvatar_lib::utils::file::ensure_temp_dir()?;
  let tid: &TransactionId = &STARTUP_TID;
  let server_config = Configuration::boxed(false, &"".to_string()).unwrap();
  run(server_config, tid, WriteMode::Direct).await?;
  Ok(())
}
