use self::worker_config::WorkerConfig;
use crate::services::containers::{containermanager::ContainerManager, IsolationFactory};
use crate::services::influx_updater::InfluxUpdater;
#[cfg(feature = "power_cap")]
use crate::services::invocation::energy_limiter::EnergyLimiter;
use crate::services::invocation::InvokerFactory;
use crate::services::registration::RegistrationService;
use crate::services::resources::cpu::{build_load_avg_signal, get_cpu_mon};
use crate::services::resources::{cpu::CpuResourceTracker, gpu::GpuResourceTracker};
use crate::services::worker_health::WorkerHealthService;
use crate::worker_api::iluvatar_worker::IluvatarWorkerImpl;
use anyhow::Result;
use iluvatar_library::char_map::worker_char_map;
use iluvatar_library::energy::energy_logging::EnergyLogger;
use iluvatar_library::influx::InfluxClient;
use iluvatar_library::types::{Compute, ContainerServer, HealthStatus, Isolation, MemSizeMb, ResourceTimings};
use iluvatar_library::{bail_error, transaction::TransactionId};
use iluvatar_rpc::rpc::{CleanResponse, InvokeResponse, ListFunctionResponse, StatusResponse};
use std::sync::Arc;
use std::time::Duration;

pub mod worker_config;
pub use worker_config as config;
pub mod iluvatar_worker;
pub mod rpc;
pub mod sim_worker;
pub mod worker_comm;

pub async fn create_worker(worker_config: WorkerConfig, tid: &TransactionId) -> Result<IluvatarWorkerImpl> {
    let cmap = worker_char_map();
    let buff = Arc::new(iluvatar_library::ring_buff::RingBuffer::new(Duration::from_secs(60)));

    let factory = IsolationFactory::new(worker_config.clone(), cmap.clone());
    let load_avg = build_load_avg_signal();
    let cpu = CpuResourceTracker::new(&worker_config.container_resources.cpu_resource, load_avg.clone(), tid)
        .or_else(|e| bail_error!(tid=tid, error=%e, "Failed to make cpu resource tracker"))?;

    let isos = factory
        .get_isolation_services(tid, true)
        .await
        .or_else(|e| bail_error!(tid=tid, error=%e, "Failed to make lifecycle(s)"))?;
    let gpu_resource = GpuResourceTracker::boxed(
        &worker_config.container_resources.gpu_resource,
        &worker_config.container_resources,
        tid,
        &isos.get(&Isolation::DOCKER),
        &worker_config.status,
        &buff,
    )
    .await
    .or_else(|e| bail_error!(tid=tid, error=%e, "Failed to make GPU resource tracker"))?;

    let container_man = ContainerManager::boxed(
        worker_config.container_resources.clone(),
        isos.clone(),
        gpu_resource.clone(),
        &cmap,
        &buff,
        tid,
    )
    .await
    .or_else(|e| bail_error!(tid=tid, error=%e, "Failed to make container manger"))?;

    let reg = RegistrationService::new(
        container_man.clone(),
        isos.clone(),
        worker_config.limits.clone(),
        cmap.clone(),
        worker_config.container_resources.clone(),
    );

    let energy = EnergyLogger::boxed(worker_config.energy.as_ref(), tid)
        .await
        .or_else(|e| bail_error!(tid=tid, error=%e, "Failed to make energy logger"))?;

    #[cfg(feature = "power_cap")]
    let energy_limit = EnergyLimiter::boxed(&worker_config.energy_cap, energy.clone())
        .or_else(|e| bail_error!(tid=tid, error=%e, "Failed to make worker energy limiter"))?;

    let invoker_fact = InvokerFactory::new(
        container_man.clone(),
        worker_config.invocation.clone(),
        cmap.clone(),
        cpu.clone(),
        gpu_resource.clone(),
        worker_config.container_resources.gpu_resource.clone(),
        &reg,
        &buff,
        &worker_config.status,
        #[cfg(feature = "power_cap")]
        energy_limit.clone(),
    );
    let invoker = invoker_fact
        .get_invoker_service(tid)
        .or_else(|e| bail_error!(tid=tid, error=%e, "Failed to get invoker service"))?;
    let cpu_mon = get_cpu_mon(
        &worker_config.status,
        &buff,
        cpu.cores as u32,
        load_avg.clone(),
        invoker.clone(),
        tid,
    )?;
    let health = WorkerHealthService::boxed(worker_config.clone(), invoker.clone(), reg.clone(), isos.clone(), tid)
        .await
        .or_else(|e| bail_error!(tid=tid, error=%e, "Failed to make worker health service"))?;

    let influx_updater = match &worker_config.influx {
        Some(i_config) => {
            let client = InfluxClient::new(i_config.clone(), tid)
                .await
                .or_else(|e| bail_error!(tid=tid, error=%e, "Failed to make influx client"))?;
            InfluxUpdater::boxed(client, i_config.clone(), &buff, worker_config.name.clone(), tid)
                .or_else(|e| bail_error!(tid=tid, error=%e, "Failed to make influx updater"))?
        },
        None => None,
    };

    Ok(IluvatarWorkerImpl::new(
        worker_config,
        container_man,
        invoker,
        health,
        energy,
        cmap,
        reg,
        influx_updater,
        gpu_resource,
        isos,
        cpu_mon,
        cpu,
        buff,
    ))
}

#[tonic::async_trait]
pub trait WorkerAPI {
    /// Ping the worker to check if it is up and accessible.
    async fn ping(&mut self, tid: TransactionId) -> Result<String>;
    /// Invoke the specified function with json-formatted arguments.
    async fn invoke(
        &mut self,
        function_name: String,
        version: String,
        args: String,
        tid: TransactionId,
    ) -> Result<InvokeResponse>;
    /// Async invoke the specified function with json-formatted arguments.
    /// Returns a cookie to query results.
    async fn invoke_async(
        &mut self,
        function_name: String,
        version: String,
        args: String,
        tid: TransactionId,
    ) -> Result<String>;
    /// Query the async invocation.
    async fn invoke_async_check(&mut self, cookie: &str, tid: TransactionId) -> Result<InvokeResponse>;
    /// Prewarm a container for the function, can pass multiple values in `compute` and one container will be made for each.
    async fn prewarm(
        &mut self,
        function_name: String,
        version: String,
        tid: TransactionId,
        compute: Compute,
    ) -> Result<String>;
    /// Register a new function.
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
        server: ContainerServer,
        timings: Option<&ResourceTimings>,
        system_function: bool,
    ) -> Result<String>;
    /// Get worker status.
    async fn status(&mut self, tid: TransactionId) -> Result<StatusResponse>;
    /// Test worker health.
    async fn health(&mut self, tid: TransactionId) -> Result<HealthStatus>;
    /// Make worker clean up containers, etc.
    async fn clean(&mut self, tid: TransactionId) -> Result<CleanResponse>;
    async fn list_registered_funcs(&mut self, tid: TransactionId) -> Result<ListFunctionResponse>;
}
