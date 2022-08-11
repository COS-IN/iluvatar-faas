use std::sync::Arc;
use iluvatar_library::{bail_error, transaction::TransactionId};
use anyhow::Result;
use dashmap::DashMap;
use iluvatar_worker_library::{rpc::RPCWorkerAPI, worker_api::{ilúvatar_worker::IluvatarWorkerImpl, WorkerAPI, sim_worker::SimWorkerAPI, create_worker}};

use crate::load_balancer_api::structs::internal::RegisteredWorker;

pub struct WorkerAPIFactory {
  /// cache of RPC connections to workers
  /// We can clone them for faster connection
  /// better than opening a new one
  rpc_apis: Arc<DashMap<String, RPCWorkerAPI>>,
  pub sim_apis: Arc<DashMap<String, Arc<IluvatarWorkerImpl>>>,
}

impl WorkerAPIFactory {
  pub fn boxed() -> Arc<WorkerAPIFactory> {
    Arc::new(WorkerAPIFactory {
      rpc_apis: Arc::new(DashMap::new()),
      sim_apis: Arc::new(DashMap::new()),
    })
  }
}

impl WorkerAPIFactory {
  fn try_get_rpcapi(&self, worker: &Arc<RegisteredWorker>) -> Option<RPCWorkerAPI> {
    match self.rpc_apis.get(&worker.name) {
      Some(r) => Some(r.clone()),
      None => None,
    }
  }

  fn try_get_simapi(&self, worker: &Arc<RegisteredWorker>) -> Option<Arc<IluvatarWorkerImpl>> {
    match self.sim_apis.get(&worker.name) {
      Some(r) => Some(r.clone()),
      None => None,
    }
  }

  /// the list of all workers the factory has cached
  pub fn get_cached_workers(&self) -> Vec<(String, Box<dyn WorkerAPI + Send>)> {
    let mut ret: Vec<(String, Box<dyn WorkerAPI + Send>)> = vec![];
    if self.rpc_apis.len() > 0 {
      self.rpc_apis.iter().for_each(|x| ret.push( (x.key().clone(), Box::new(x.value().clone())) ));
    } 
    if self.sim_apis.len() > 0 {
      self.sim_apis.iter().for_each(|x| ret.push( (x.key().clone(), Box::new(SimWorkerAPI::new(x.value().clone()))) ));
    }
    ret
  }

  /// Get the worker API that matches it's implemented communication method
  pub async fn get_worker_api(&self, worker: &Arc<RegisteredWorker>, tid: &TransactionId) -> Result<Box<dyn WorkerAPI + Send>> {
    if worker.communication_method == "RPC" {
      match self.try_get_rpcapi(worker) {
        Some(r) => Ok(Box::new(r)),
        None => {
          let api = match RPCWorkerAPI::new(&worker.host, worker.port).await {
            Ok(api) => api,
            Err(e) => bail_error!(tid=%tid, worker=%worker.name, error=%e, "Unable to create API for worker"),
          };
          self.rpc_apis.insert(worker.name.clone(), api.clone());
          Ok(Box::new(api))
        },
      }
    } else if worker.communication_method == "simulation" {
      let api = match self.try_get_simapi(worker) {
        Some(api) => api,
        None => {
          let worker_config = iluvatar_worker_library::worker_api::worker_config::Configuration::boxed(false, &worker.host).unwrap();
          let api = Arc::new(create_worker(worker_config, tid).await?);
          self.sim_apis.insert(worker.name.clone(), api.clone());
          api
        },
      };
 
      let w = SimWorkerAPI::new(api);
      return Ok(Box::new(w));
    } else {
      bail_error!(tid=%tid, mathod=%worker.communication_method, "Unknown worker communication method");
    }
  }
}
