use crate::controller::controller_config::ControllerConfig;
use crate::controller::structs::internal::{RegisteredFunction, RegisteredWorker};
use anyhow::Result;
use iluvatar_library::transaction::TransactionId;
use iluvatar_worker_library::rpc::InvokeResponse;
use iluvatar_worker_library::worker_api::worker_comm::WorkerAPIFactory;
use std::sync::Arc;
use std::time::Duration;
use tracing::debug;

use super::controller_health::ControllerHealthService;
use super::load_reporting::LoadService;
mod balancers;

#[tonic::async_trait]
pub trait LoadBalancerTrait {
    /// Add a new worker to the LB pool
    /// assumed to be unhealthy until told otherwise
    fn add_worker(&self, worker: Arc<RegisteredWorker>, tid: &TransactionId);
    /// Send a synchronous invocation to a worker
    /// Returns the invocation result and the E2E duration of the invocation recorded by the load balancer  
    ///   Thus including both the time spent on the worker and the invocation time, plus networking
    async fn send_invocation(
        &self,
        func: Arc<RegisteredFunction>,
        json_args: String,
        tid: &TransactionId,
    ) -> Result<(InvokeResponse, Duration)>;
    /// Start an async invocation on a server
    /// Return the marker cookie for it, and the worker it was launched on
    ///   And the duration of the request recorded by the load balancer  
    ///   Thus including both the time spent on the worker, plus networking
    async fn send_async_invocation(
        &self,
        func: Arc<RegisteredFunction>,
        json_args: String,
        tid: &TransactionId,
    ) -> Result<(String, Arc<RegisteredWorker>, Duration)>;
    /// Prewarm the given function somewhere
    async fn prewarm(&self, func: Arc<RegisteredFunction>, tid: &TransactionId) -> Result<Duration>;
}

pub type LoadBalancer = Arc<dyn LoadBalancerTrait + Send + Sync + 'static>;

pub fn get_balancer(
    config: &ControllerConfig,
    health_svc: Arc<dyn ControllerHealthService>,
    tid: &TransactionId,
    load: Arc<LoadService>,
    worker_fact: Arc<WorkerAPIFactory>,
) -> Result<LoadBalancer> {
    if config.load_balancer.algorithm == "RoundRobin" {
        debug!(tid=%tid, "starting round robin balancer");
        Ok(Arc::new(balancers::round_robin::RoundRobinLoadBalancer::new(
            health_svc,
            worker_fact,
        )))
    } else if config.load_balancer.algorithm == "LeastLoaded" {
        debug!(tid=%tid, "starting least loaded balancer");
        Ok(balancers::least_loaded::LeastLoadedBalancer::boxed(
            health_svc,
            load,
            worker_fact,
            tid,
            config.load_balancer.clone(),
        ))
    } else {
        anyhow::bail!(
            "Unimplemented load balancing algorithm {}",
            config.load_balancer.algorithm
        )
    }
}
