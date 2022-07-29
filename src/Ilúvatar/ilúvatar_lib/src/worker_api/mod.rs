use std::sync::Arc;
use crate::services::invocation::invoker::InvokerService;
use crate::services::status::status_service::StatusService;
use crate::transaction::TransactionId;
use crate::worker_api::config::Configuration;
use crate::services::containers::containermanager::ContainerManager;
use crate::worker_api::ilúvatar_worker::IluvatarWorkerImpl;
use anyhow::Result;
use crate::services::{LifecycleFactory, WorkerHealthService};

pub mod worker_config;
pub use worker_config as config;
pub mod worker_comm;
#[path ="./ilúvatar_worker.rs"]
pub mod ilúvatar_worker;

pub async fn create_worker(server_config: Arc<Configuration>, tid: &TransactionId) -> Result<IluvatarWorkerImpl> {
  let factory = LifecycleFactory::new(server_config.container_resources.clone(), server_config.networking.clone());
  let lifecycle = factory.get_lifecycle_service(tid, true).await?;

  let container_man = ContainerManager::boxed(server_config.limits.clone(), server_config.container_resources.clone(), lifecycle.clone(), tid).await?;
  let invoker = InvokerService::boxed(container_man.clone(), tid, server_config.limits.clone());
  let status = StatusService::boxed(container_man.clone(), invoker.clone(), server_config.graphite.clone(), server_config.name.clone()).await;
  let health = WorkerHealthService::boxed(invoker.clone(), container_man.clone(), tid).await?;

  Ok(IluvatarWorkerImpl::new(server_config.clone(), container_man, invoker, status, health))
}
