use std::sync::Arc;
use iluvatar_lib::{worker_api::worker_config::Configuration, transaction::TransactionId, load_balancer_api::register_worker};
use tracing::{debug, info, error};

pub mod args;

pub fn register_rpc_to_controller(server_config: Arc<Configuration>, tid: TransactionId) {
  let _ = tokio::spawn(async move {
      debug!(tid=%tid, "Controller registration thread started");

      // allow RPC server time to start up
      tokio::time::sleep(std::time::Duration::from_secs(5)).await;
      let comm_method = "RPC".to_string();

      let result = register_worker(&server_config.name, &comm_method, &server_config.container_resources.backend,
      &server_config.address, server_config.port, server_config.container_resources.memory_mb, server_config.container_resources.cores,
        &server_config.load_balancer_url, &tid).await;

      match result {
        Ok(_) => info!(tid=%tid, "Worker successfully registered with controller"),
        Err(e) => error!(tid=%tid, error=%e, "Worker registration failed"),
      }
    }
  );
}
