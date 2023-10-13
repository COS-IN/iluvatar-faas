use self::queueing_dispatcher::QueueingDispatcher;
use super::containers::containermanager::ContainerManager;
use super::resources::{cpu::CpuResourceTracker, gpu::GpuResourceTracker};
#[cfg(feature = "power_cap")]
use crate::services::invocation::energy_limiter::EnergyLimiter;
use crate::services::{
    containers::structs::{ContainerState, ParsedResult},
    registration::RegisteredFunction,
};
use crate::worker_api::worker_config::{FunctionLimits, InvocationConfig};
use anyhow::Result;
use iluvatar_library::{characteristics_map::CharacteristicsMap, transaction::TransactionId, types::Compute};
use parking_lot::Mutex;
use std::{sync::Arc, time::Duration};

pub mod async_tracker;
mod completion_time_tracker;
mod cpu_q_invoke;
#[cfg(feature = "power_cap")]
pub mod energy_limiter;
mod gpu_q_invoke;
pub mod queueing;
mod queueing_dispatcher;

#[tonic::async_trait]
/// A trait representing the functionality a queue policy must implement
/// Overriding functions _must_ re-implement [tracing::info] level log statements for consistency
pub trait Invoker: Send + Sync {
    /// A synchronous invocation against this invoker
    async fn sync_invocation(
        &self,
        reg: Arc<RegisteredFunction>,
        json_args: String,
        tid: TransactionId,
    ) -> Result<InvocationResultPtr>;

    /// Launch an async invocation of the function
    /// Return a lookup cookie that can be queried for results
    fn async_invocation(&self, reg: Arc<RegisteredFunction>, json_args: String, tid: TransactionId) -> Result<String>;

    /// Check the status of the result, if found is returned destructively
    fn invoke_async_check(&self, cookie: &String, tid: &TransactionId) -> Result<crate::rpc::InvokeResponse>;
    /// Number of invocations enqueued on each compute
    fn queue_len(&self) -> std::collections::HashMap<Compute, usize>;
    /// Number of running invocations
    fn running_funcs(&self) -> u32;
}

/// A struct to create the appropriate [Invoker] from configuration at runtime.
pub struct InvokerFactory {
    cont_manager: Arc<ContainerManager>,
    function_config: Arc<FunctionLimits>,
    invocation_config: Arc<InvocationConfig>,
    cmap: Arc<CharacteristicsMap>,
    cpu: Arc<CpuResourceTracker>,
    gpu_resources: Arc<GpuResourceTracker>,
    #[cfg(feature = "power_cap")]
    energy: Arc<EnergyLimiter>,
}

impl InvokerFactory {
    pub fn new(
        cont_manager: Arc<ContainerManager>,
        function_config: Arc<FunctionLimits>,
        invocation_config: Arc<InvocationConfig>,
        cmap: Arc<CharacteristicsMap>,
        cpu: Arc<CpuResourceTracker>,
        gpu_resources: Arc<GpuResourceTracker>,
        #[cfg(feature = "power_cap")] energy: Arc<EnergyLimiter>,
    ) -> Self {
        InvokerFactory {
            cont_manager,
            function_config,
            invocation_config,
            cmap,
            cpu,
            gpu_resources,
            #[cfg(feature = "power_cap")]
            energy,
        }
    }

    pub fn get_invoker_service(&self, tid: &TransactionId) -> Result<Arc<dyn Invoker>> {
        let invoker = QueueingDispatcher::new(
            self.cont_manager.clone(),
            self.function_config.clone(),
            self.invocation_config.clone(),
            tid,
            self.cmap.clone(),
            self.cpu.clone(),
            self.gpu_resources.clone(),
            #[cfg(feature = "power_cap")]
            self.energy.clone(),
        )?;
        Ok(invoker)
    }
}

#[derive(Debug)]
/// Container for all the data about a completed invocation.
/// Including its output and execution metadata.
pub struct InvocationResult {
    /// The output from the invocation
    pub result_json: String,
    /// The E2E latency between the worker and the container
    pub duration: Duration,
    pub attempts: u32,
    pub completed: bool,
    /// The invocation time as recorded by the platform inside the container
    pub exec_time: f64,
    pub worker_result: Option<ParsedResult>,
    /// The compute the invocation was run on
    pub compute: Compute,
    /// The state of the container when the invocation was run
    pub container_state: ContainerState,
}
impl InvocationResult {
    pub fn boxed() -> InvocationResultPtr {
        Arc::new(Mutex::new(InvocationResult {
            completed: false,
            duration: Duration::from_micros(0),
            result_json: "".to_string(),
            attempts: 0,
            exec_time: 0.0,
            worker_result: None,
            compute: Compute::empty(),
            container_state: ContainerState::Error,
        }))
    }
}

pub type InvocationResultPtr = Arc<Mutex<InvocationResult>>;
