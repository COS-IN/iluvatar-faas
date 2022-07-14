use std::sync::Arc;
use crate::{services::load_balance::LoadBalancerTrait, transaction::TransactionId};
use crate::load_balancer_api::structs::internal::{RegisteredFunction, RegisteredWorker, WorkerStatus};
use anyhow::Result;

pub struct LeastLoadedBalancer {

}

#[tonic::async_trait]
impl LoadBalancerTrait for LeastLoadedBalancer {
  fn add_worker(&self, _worker: Arc<RegisteredWorker>, _tid: &TransactionId) {
    todo!()
  }

  async fn send_invocation(&self, _func: Arc<RegisteredFunction>, _json_args: String, _tid: &TransactionId) -> Result<String> {
    todo!()
  }

  fn update_worker_status(&self, _worker: &RegisteredWorker, _status: WorkerStatus, _tid: &TransactionId) {
    todo!()
  }

  async fn prewarm(&self, _func: Arc<RegisteredFunction>, _tid: &TransactionId) -> Result<()> {
    todo!()
  }

  async fn send_async_invocation(&self, _func: Arc<RegisteredFunction>, _json_args: String, _tid: &TransactionId) -> Result<(String, Arc<RegisteredWorker>)> {
    todo!()
  }
}
