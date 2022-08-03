use std::sync::Arc;
use crate::{ilúvatar_api::WorkerAPI, rpc::RCPWorkerAPI, load_balancer_api::structs::internal::RegisteredWorker, bail_error, transaction::TransactionId};
use anyhow::Result;
use dashmap::DashMap;

pub struct WorkerAPIFactory {
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
            Err(e) => bail_error!("[{}] unable to create API for worker '{}' because '{}'", tid, worker.name, e),
          };
          self.rpc_apis.insert(worker.name.clone(), api.clone());
          Ok(Box::new(api))
        },
      }
    } else {
      bail_error!("[{}] unknown worker communication method {}", tid, worker.communication_method);
    }
  }
}