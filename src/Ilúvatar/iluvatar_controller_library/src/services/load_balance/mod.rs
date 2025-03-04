use super::controller_health::ControllerHealthService;
use super::load_reporting::LoadService;
use crate::server::controller_config::ControllerConfig;
use crate::services::load_balance::balancers::rrCH::CHGLoadBalancer;
use crate::services::registration::RegisteredWorker;
use anyhow::Result;
use iluvatar_library::char_map::WorkerCharMap;
use iluvatar_library::transaction::TransactionId;
use iluvatar_rpc::rpc::InvokeResponse;
use iluvatar_worker_library::services::registration::RegisteredFunction;
use iluvatar_worker_library::worker_api::worker_comm::WorkerAPIFactory;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;

mod balancers;

#[derive(Debug, Deserialize)]
pub enum LoadBalancerAlgo {
    RoundRobin,
    LeastLoaded,
    CHGLoadBalancer,
}

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
    worker_cmap: &WorkerCharMap,
) -> Result<LoadBalancer> {
    match config.load_balancer.algorithm {
        LoadBalancerAlgo::RoundRobin => Ok(Arc::new(balancers::round_robin::RoundRobinLoadBalancer::new(
            health_svc,
            worker_fact,
        ))),
        LoadBalancerAlgo::LeastLoaded => Ok(balancers::least_loaded::LeastLoadedBalancer::boxed(
            health_svc,
            load,
            worker_fact,
            tid,
            config.load_balancer.clone(),
        )),
        LoadBalancerAlgo::CHGLoadBalancer => Ok(Arc::new(CHGLoadBalancer::new(health_svc, worker_fact, worker_cmap))),
    }
}
