use self::worker_config::WorkerConfig;
use crate::rpc::{CleanResponse, InvokeResponse, StatusResponse};
use crate::services::containers::{containermanager::ContainerManager, IsolationFactory};
use crate::services::influx_updater::InfluxUpdater;
#[cfg(feature = "power_cap")]
use crate::services::invocation::energy_limiter::EnergyLimiter;
use crate::services::invocation::InvokerFactory;
use crate::services::registration::RegistrationService;
use crate::services::resources::{cpu::CpuResourceTracker, gpu::GpuResourceTracker};
use crate::services::status::status_service::StatusService;
use crate::services::worker_health::WorkerHealthService;
use crate::worker_api::ilúvatar_worker::IluvatarWorkerImpl;
use anyhow::Result;
use iluvatar_library::influx::InfluxClient;
use iluvatar_library::types::{Compute, Isolation, ResourceTimings};
use iluvatar_library::{bail_error, characteristics_map::CharacteristicsMap};
use iluvatar_library::{characteristics_map::AgExponential, energy::energy_logging::EnergyLogger};
use iluvatar_library::{transaction::TransactionId, types::MemSizeMb};
use std::sync::Arc;

pub mod worker_config;
pub use worker_config as config;
#[path = "./ilúvatar_worker.rs"]
pub mod ilúvatar_worker;
pub mod sim_worker;
pub mod worker_comm;

pub async fn create_worker(worker_config: WorkerConfig, tid: &TransactionId) -> Result<IluvatarWorkerImpl> {
    let cmap = Arc::new(CharacteristicsMap::new(AgExponential::new(0.6)));

    let factory = IsolationFactory::new(worker_config.clone());
    let cpu = CpuResourceTracker::new(worker_config.container_resources.clone(), tid)?;
    let gpu_resource = GpuResourceTracker::boxed(worker_config.container_resources.clone(), tid)?;
    let isos = factory
        .get_isolation_services(tid, true)
        .await
        .or_else(|e| bail_error!(tid=%tid, error=%e, "Failed to make lifecycle(s)"))?;

    let container_man = ContainerManager::boxed(
        worker_config.container_resources.clone(),
        isos.clone(),
        gpu_resource.clone(),
        tid,
    )
    .await
    .or_else(|e| bail_error!(tid=%tid, error=%e, "Failed to make container manger"))?;

    let reg = RegistrationService::new(
        container_man.clone(),
        isos.clone(),
        worker_config.limits.clone(),
        cmap.clone(),
        worker_config.container_resources.clone(),
    );

    let energy = EnergyLogger::boxed(worker_config.energy.as_ref(), tid)
        .await
        .or_else(|e| bail_error!(tid=%tid, error=%e, "Failed to make energy logger"))?;

    #[cfg(feature = "power_cap")]
    let energy_limit = EnergyLimiter::boxed(&worker_config.energy_cap, energy.clone())
        .or_else(|e| bail_error!(tid=%tid, error=%e, "Failed to make worker energy limiter"))?;

    let invoker_fact = InvokerFactory::new(
        container_man.clone(),
        worker_config.limits.clone(),
        worker_config.invocation.clone(),
        cmap.clone(),
        cpu,
        gpu_resource.clone(),
        #[cfg(feature = "power_cap")]
        energy_limit.clone(),
    );
    let invoker = invoker_fact.get_invoker_service(tid)?;
    let health = WorkerHealthService::boxed(invoker.clone(), reg.clone(), tid)
        .await
        .or_else(|e| bail_error!(tid=%tid, error=%e, "Failed to make worker health service"))?;
    let status = StatusService::boxed(
        container_man.clone(),
        worker_config.name.clone(),
        tid,
        worker_config.status.clone(),
        invoker.clone(),
        gpu_resource.clone(),
    )
    .or_else(|e| bail_error!(tid=%tid, error=%e, "Failed to make status service"))?;

    let influx_updater = match &worker_config.influx {
        Some(i_config) => {
            let client = InfluxClient::new(i_config.clone(), tid)
                .await
                .or_else(|e| bail_error!(tid=%tid, error=%e, "Failed to make influx client"))?;
            InfluxUpdater::boxed(
                client,
                i_config.clone(),
                status.clone(),
                worker_config.name.clone(),
                tid,
            )
            .or_else(|e| bail_error!(tid=%tid, error=%e, "Failed to make influx updater"))?
        }
        None => None,
    };

    Ok(IluvatarWorkerImpl::new(
        worker_config.clone(),
        container_man,
        invoker,
        status,
        health,
        energy,
        cmap,
        reg,
        influx_updater,
    ))
}

#[derive(Debug, PartialEq, Eq)]
pub enum HealthStatus {
    HEALTHY,
    UNHEALTHY,
}

#[tonic::async_trait]
pub trait WorkerAPI {
    async fn ping(&mut self, tid: TransactionId) -> Result<String>;
    async fn invoke(
        &mut self,
        function_name: String,
        version: String,
        args: String,
        tid: TransactionId,
    ) -> Result<InvokeResponse>;
    async fn invoke_async(
        &mut self,
        function_name: String,
        version: String,
        args: String,
        tid: TransactionId,
    ) -> Result<String>;
    async fn invoke_async_check(&mut self, cookie: &String, tid: TransactionId) -> Result<InvokeResponse>;
    async fn prewarm(
        &mut self,
        function_name: String,
        version: String,
        tid: TransactionId,
        compute: Compute,
    ) -> Result<String>;
    async fn register(
        &mut self,
        function_name: String,
        version: String,
        image_name: String,
        memory: MemSizeMb,
        cpus: u32,
        parallels: u32,
        tid: TransactionId,
        isolate: Isolation,
        compute: Compute,
        timings: Option<&ResourceTimings>,
    ) -> Result<String>;
    async fn status(&mut self, tid: TransactionId) -> Result<StatusResponse>;
    async fn health(&mut self, tid: TransactionId) -> Result<HealthStatus>;
    async fn clean(&mut self, tid: TransactionId) -> Result<CleanResponse>;
}
