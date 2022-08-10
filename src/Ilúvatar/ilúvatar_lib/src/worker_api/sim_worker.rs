use std::sync::Arc;
use crate::ilúvatar_api::WorkerAPI;
use crate::rpc::{InvokeResponse, StatusResponse, RPCError};
use crate::{transaction::TransactionId, ilúvatar_api::HealthStatus, types::MemSizeMb};
use anyhow::Result;
use super::ilúvatar_worker::IluvatarWorkerImpl;
use crate::rpc::{iluvatar_worker_server::IluvatarWorker, StatusRequest, HealthRequest};

/// A simulation version of the WOrkerAPI
///   must match [crate::rpc::RPCWorkerAPI] in handling, etc.
pub struct SimWorkerAPI {
  _worker: Arc<IluvatarWorkerImpl>
}
impl SimWorkerAPI {
  pub fn new(worker: Arc<IluvatarWorkerImpl>) -> Self {
    SimWorkerAPI {
      _worker: worker
    }
  }
}

#[tonic::async_trait]
#[allow(unused)]
impl WorkerAPI for SimWorkerAPI {
  async fn ping(&mut self, tid: TransactionId) -> Result<String> {
    todo!()
  }

  async fn invoke(&mut self, function_name: String, version: String, args: String, memory: Option<MemSizeMb>, tid: TransactionId) -> Result<InvokeResponse> {
    todo!()
  }

  async fn invoke_async(&mut self, function_name: String, version: String, args: String, memory: Option<MemSizeMb>, tid: TransactionId) -> Result<String> {
    todo!()
  }

  async fn invoke_async_check(&mut self, cookie: &String, tid: TransactionId) -> Result<InvokeResponse> {
    todo!()
  }

  async fn prewarm(&mut self, function_name: String, version: String, memory: Option<MemSizeMb>, cpu: Option<u32>, image: Option<String>, tid: TransactionId) -> Result<String> {
    todo!()
  }

  async fn register(&mut self, function_name: String, version: String, image_name: String, memory: MemSizeMb, cpus: u32, parallels: u32, tid: TransactionId) -> Result<String> {
    todo!()
  }
  
  async fn status(&mut self, tid: TransactionId) -> Result<StatusResponse> {
    Ok(self._worker.status(tonic::Request::new(StatusRequest{transaction_id:tid})).await?.into_inner())
  }

  async fn health(&mut self, tid: TransactionId) -> Result<HealthStatus> {
    match self._worker.health(tonic::Request::new(HealthRequest{transaction_id:tid})).await?.into_inner().status {
        // HealthStatus::Healthy
        0 => Ok(HealthStatus::HEALTHY),
        // HealthStatus::Unhealthy
        1 => Ok(HealthStatus::UNHEALTHY),
        i => anyhow::bail!(RPCError::new(format!("Got unexpected status of {}", i),  "[RCPWorkerAPI:health]".to_string())),
    }
  }
}
