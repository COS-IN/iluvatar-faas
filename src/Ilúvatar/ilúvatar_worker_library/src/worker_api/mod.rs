use std::sync::Arc;
use iluvatar_library::influx::InfluxClient;
use iluvatar_library::types::{Isolation, Compute, ResourceTimings};
use iluvatar_library::{bail_error, characteristics_map::CharacteristicsMap};
use iluvatar_library::{energy::energy_logging::EnergyLogger, characteristics_map::AgExponential};
use iluvatar_library::{transaction::TransactionId, types::MemSizeMb};
use crate::services::influx_updater::InfluxUpdater;
use crate::services::invocation::{InvokerFactory, energy_limiter::EnergyLimiter};
use crate::services::registration::RegistrationService;
use crate::services::resources::{gpu::GpuResourceTracker, cpu::CpuResourceTracker};
use crate::services::worker_health::WorkerHealthService;
use crate::services::status::status_service::StatusService;
use crate::services::containers::{containermanager::ContainerManager, IsolationFactory};
use crate::worker_api::ilúvatar_worker::IluvatarWorkerImpl;
use anyhow::Result;
use crate::rpc::{StatusResponse, InvokeResponse, CleanResponse};
use self::worker_config::WorkerConfig;

pub mod worker_config;
pub use worker_config as config;
#[path ="./ilúvatar_worker.rs"]
pub mod ilúvatar_worker;
pub mod sim_worker;
pub mod worker_comm;

pub async fn create_worker(worker_config: WorkerConfig, tid: &TransactionId) -> Result<IluvatarWorkerImpl> {
  let cmap = Arc::new(CharacteristicsMap::new(AgExponential::new(0.6)));

  let factory = IsolationFactory::new(worker_config.clone());
  let cpu = CpuResourceTracker::new(worker_config.container_resources.clone(), tid)?;
  let gpu_resource = GpuResourceTracker::boxed(worker_config.container_resources.clone(), tid)?;
  
  let isos = match factory.get_isolation_services(tid, true).await  {
    Ok(l) => l,
    Err(e) => bail_error!(tid=%tid, error=%e, "Failed to make lifecycle(s)"),
  };

  let container_man = match ContainerManager::boxed(worker_config.container_resources.clone(), isos.clone(), gpu_resource.clone(), tid).await {
    Ok(s) => s,
    Err(e) => bail_error!(tid=%tid, error=%e, "Failed to make container manger"),
  };

  let reg = RegistrationService::new(container_man.clone(), isos.clone(), worker_config.limits.clone(), cmap.clone(), worker_config.container_resources.clone());

  let energy = match EnergyLogger::boxed(worker_config.energy.as_ref(), tid).await {
    Ok(e) => e,
    Err(e) => bail_error!(tid=%tid, error=%e, "Failed to make energy logger"),
  };

  let energy_limit = EnergyLimiter::boxed(&worker_config.invocation, energy.clone());
  let invoker_fact = InvokerFactory::new(container_man.clone(), worker_config.limits.clone(),
                                          worker_config.invocation.clone(), cmap.clone(), cpu,
                                         gpu_resource.clone(), energy_limit.clone());
  let invoker = invoker_fact.get_invoker_service(tid)?;
  let health = match WorkerHealthService::boxed(invoker.clone(), reg.clone(), tid).await {
    Ok(h) => h,
    Err(e) => bail_error!(tid=%tid, error=%e, "Failed to make worker health service"),
  };
  let status = match StatusService::boxed(container_man.clone(), worker_config.name.clone(), tid, worker_config.status.clone(), invoker.clone(), gpu_resource.clone()) {
    Ok(s) => s,
    Err(e) => bail_error!(tid=%tid, error=%e, "Failed to make status service"),
  };

  let influx_updater = match &worker_config.influx {
    Some(i_config) => match InfluxClient::new(i_config.clone(), tid).await {
      Ok(i) => match InfluxUpdater::boxed(i, i_config.clone(), status.clone(), worker_config.name.clone(), tid) {
        Ok(u) => u,
        Err(e) => bail_error!(tid=%tid, error=%e, "Failed to make influx updater"),
      },
      Err(e) => bail_error!(tid=%tid, error=%e, "Failed to make influx client"),
    },
    None => None
  };

  Ok(IluvatarWorkerImpl::new(worker_config.clone(), container_man, invoker, status, health, energy, cmap, reg, influx_updater))
}

#[derive(Debug, PartialEq, Eq)]
pub enum HealthStatus {
  HEALTHY,
  UNHEALTHY,
}

#[tonic::async_trait]
pub trait WorkerAPI {
  async fn ping(&mut self, tid: TransactionId) -> Result<String>;
  async fn invoke(&mut self, function_name: String, version: String, args: String, tid: TransactionId) -> Result<InvokeResponse>;
  async fn invoke_async(&mut self, function_name: String, version: String, args: String, tid: TransactionId) -> Result<String>;
  async fn invoke_async_check(&mut self, cookie: &String, tid: TransactionId) -> Result<InvokeResponse>;
  async fn prewarm(&mut self, function_name: String, version: String, tid: TransactionId, compute: Compute) -> Result<String>;
  async fn register(&mut self, function_name: String, version: String, image_name: String, memory: MemSizeMb, cpus: u32, parallels: u32, tid: TransactionId, 
      isolate: Isolation, compute: Compute, timings: Option<&ResourceTimings>) -> Result<String>;
  async fn status(&mut self, tid: TransactionId) -> Result<StatusResponse>;
  async fn health(&mut self, tid: TransactionId) -> Result<HealthStatus>;
  async fn clean(&mut self, tid: TransactionId) -> Result<CleanResponse>;
}
