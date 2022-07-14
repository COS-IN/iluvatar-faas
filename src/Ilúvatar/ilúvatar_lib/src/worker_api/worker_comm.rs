use std::sync::Arc;
use crate::{ilúvatar_api::WorkerAPI, rpc::RCPWorkerAPI, load_balancer_api::structs::internal::RegisteredWorker, bail_error, transaction::TransactionId};
use anyhow::Result;

pub struct WorkerAPIFactory {

}

impl WorkerAPIFactory {
  /// Get the worker API that matches it's implemented communication method
  pub async fn get_worker_api(&self, worker: &Arc<RegisteredWorker>, tid: &TransactionId) -> Result<Box<dyn WorkerAPI + Send>> {
    // TODO: cache APIs to not pay their connection cost repeatedly
    // will that cause issues with unexpected closing?
    // the inner channel can be easily clone()'d though
    if worker.communication_method == "RPC" {
      let api = match RCPWorkerAPI::new(&worker.host, worker.port).await {
        Ok(api) => api,
        Err(e) => bail_error!("[{}] unable to create API for worker '{}' because '{}'", tid, worker.name, e),
      };
      Ok(Box::new(api))
    } else {
      bail_error!("[{}] unknown worker communication method {}", tid, worker.communication_method);
    }
  }
}