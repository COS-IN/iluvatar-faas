use std::vec;
use iluvatar_library::energy::energy_logging::{EnergyLogger, EnergyInjectableT};
use crate::services::worker_health::WorkerHealthService;
use crate::services::{invocation::invoker::InvokerService, containers::LifecycleFactory};
use crate::services::status::status_service::StatusService;
use crate::services::containers::containermanager::ContainerManager;
use crate::worker_api::ilúvatar_worker::IluvatarWorkerImpl;
use anyhow::Result;
use iluvatar_library::{transaction::TransactionId, types::MemSizeMb};
use crate::rpc::{StatusResponse, InvokeResponse};
use self::worker_config::WorkerConfig;

pub mod worker_config;
pub use worker_config as config;
#[path ="./ilúvatar_worker.rs"]
pub mod ilúvatar_worker;
pub mod sim_worker;

pub async fn create_worker(worker_config: WorkerConfig, tid: &TransactionId) -> Result<IluvatarWorkerImpl> {
  let factory = LifecycleFactory::new(worker_config.container_resources.clone(), worker_config.networking.clone());
  let lifecycle = factory.get_lifecycle_service(tid, true).await?;

  let container_man = ContainerManager::boxed(worker_config.limits.clone(), worker_config.container_resources.clone(), lifecycle.clone(), tid).await?;
  let invoker = InvokerService::boxed(container_man.clone(), tid, worker_config.limits.clone());
  let status = StatusService::boxed(container_man.clone(), invoker.clone(), worker_config.graphite.clone(), worker_config.name.clone()).await;
  let health = WorkerHealthService::boxed(invoker.clone(), container_man.clone(), tid).await?;

  let inv_cln = invoker.clone();
  let i: EnergyInjectableT = std::sync::Arc::new(move || {
    InvokerService::get_running_string(&inv_cln) 
  });
  let inject = vec![i];
  let energy = EnergyLogger::boxed(worker_config.energy.clone(), tid, Some(vec!["functions".to_string()]), Some(inject))?;
  
  Ok(IluvatarWorkerImpl::new(worker_config.clone(), container_man, invoker, status, health, energy))
}

#[derive(Debug, PartialEq, Eq)]
pub enum HealthStatus {
  HEALTHY,
  UNHEALTHY,
}

#[tonic::async_trait]
pub trait WorkerAPI {
  async fn ping(&mut self, tid: TransactionId) -> Result<String>;
  async fn invoke(&mut self, function_name: String, version: String, args: String, memory: Option<MemSizeMb>, tid: TransactionId) -> Result<InvokeResponse>;
  async fn invoke_async(&mut self, function_name: String, version: String, args: String, memory: Option<MemSizeMb>, tid: TransactionId) -> Result<String>;
  async fn invoke_async_check(&mut self, cookie: &String, tid: TransactionId) -> Result<InvokeResponse>;
  async fn prewarm(&mut self, function_name: String, version: String, memory: Option<MemSizeMb>, cpu: Option<u32>, image: Option<String>, tid: TransactionId) -> Result<String>;
  async fn register(&mut self, function_name: String, version: String, image_name: String, memory: MemSizeMb, cpus: u32, parallels: u32, tid: TransactionId) -> Result<String>;
  async fn status(&mut self, tid: TransactionId) -> Result<StatusResponse>;
  async fn health(&mut self, tid: TransactionId) -> Result<HealthStatus>;
}
