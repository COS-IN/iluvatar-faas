use iluvatar_lib::{ilúvatar_api::WorkerAPI, rpc::RCPWorkerAPI, load_balancer_api::structs::RegisterWorker, bail_error, transaction::TransactionId};
use anyhow::Result;

pub struct WorkerAPIFactory {

}

impl WorkerAPIFactory {
  pub async fn get_worker_api(&self, worker: &RegisterWorker, tid: &TransactionId) -> Result<Box<dyn WorkerAPI>> {
    if worker.backend == "containerd" {
      let api = match RCPWorkerAPI::new(&worker.host, worker.port).await {
        Ok(api) => api,
        Err(e) => bail_error!("[{}] unable to create API for worker {} because '{}'", tid, worker.name, e),
      };
      Ok(Box::new(api))
    } else {
      todo!()
    }
  }
}