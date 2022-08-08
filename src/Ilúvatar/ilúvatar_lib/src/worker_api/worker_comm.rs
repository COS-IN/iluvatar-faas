use std::sync::Arc;
use crate::{ilúvatar_api::WorkerAPI, rpc::RCPWorkerAPI, load_balancer_api::structs::internal::RegisteredWorker, bail_error, transaction::TransactionId};
use anyhow::Result;
use dashmap::DashMap;

pub struct WorkerAPIFactory {
  /// cache of RPC connections to workers
  /// We can clone them for faster connection
  /// better than opening a new one
  rpc_apis: Arc<DashMap<String, RCPWorkerAPI>>,
}

impl WorkerAPIFactory {
  pub fn boxed() -> Arc<WorkerAPIFactory> {
    Arc::new(WorkerAPIFactory {
      rpc_apis: Arc::new(DashMap::new())
    })
  }
}

impl WorkerAPIFactory {
  fn try_get_rpcapi(&self, worker: &Arc<RegisteredWorker>) -> Option<RCPWorkerAPI> {
    match self.rpc_apis.get(&worker.name) {
      Some(r) => Some(r.clone()),
      None => None,
    }
  }

  /// Get the worker API that matches it's implemented communication method
  pub async fn get_worker_api(&self, worker: &Arc<RegisteredWorker>, tid: &TransactionId) -> Result<Box<dyn WorkerAPI + Send>> {
    if worker.communication_method == "RPC" {
      match self.try_get_rpcapi(worker) {
        Some(r) => Ok(Box::new(r)),
        None => {
          let api = match RCPWorkerAPI::new(&worker.host, worker.port).await {
            Ok(api) => api,
            Err(e) => bail_error!(tid=%tid, worker=%worker.name, error=%e, "Unable to create API for worker"),
          };
          self.rpc_apis.insert(worker.name.clone(), api.clone());
          Ok(Box::new(api))
        },
      }
    } else {
      bail_error!(tid=%tid, mathod=%worker.communication_method, "Unknown worker communication method");
    }
  }
}
