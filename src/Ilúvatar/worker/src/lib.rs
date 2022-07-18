use std::sync::Arc;
use iluvatar_lib::{worker_api::worker_config::Configuration, transaction::TransactionId, load_balancer_api::register_worker};
use log::{debug, info, error};

#[path ="./ilúvatar_worker.rs"]
pub mod ilúvatar_worker;
pub mod logging;
pub mod args;


pub fn register_rpc_to_controller(server_config: Arc<Configuration>, tid: TransactionId) {
  let _ = tokio::spawn(async move {
      debug!("[{}] Controller registration thread started", tid);

      // allow RPC server time to start up
      tokio::time::sleep(std::time::Duration::from_secs(5)).await;
      let comm_method = "RPC".to_string();

      let result = register_worker(&server_config.name, &comm_method, &server_config.container_resources.backend,
      &server_config.address, server_config.port, server_config.container_resources.memory_mb, server_config.container_resources.cores,
        &server_config.load_balancer_url, &tid).await;

      match result {
        Ok(_) => info!("[{}] worker successfully registered with controller", tid),
        Err(e) => error!("[{}] worker registration failed because '{}'", tid, e),
      }
    }
  );
}