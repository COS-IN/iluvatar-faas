use std::sync::Arc;
use crate::{load_balancer_api::{lb_config::ControllerConfig, structs::internal::{RegisteredWorker, RegisteredFunction, WorkerStatus}}, transaction::TransactionId};
use anyhow::Result;

mod balancers;

#[tonic::async_trait]
pub trait LoadBalancerTrait {
  /// Add a new worker to the LB pool
  /// assumed to be unhealthy until told otherwise
  fn add_worker(&self, worker: Arc<RegisteredWorker>, tid: &TransactionId);
  /// Send a synchronous invocation to a worker
  async fn send_invocation(&self, func: Arc<RegisteredFunction>, json_args: String, tid: &TransactionId) -> Result<String>;
  /// Start an async invocation on a server
  /// Return the marker cookie for it, and the worker it was launched on
  async fn send_async_invocation(&self, func: Arc<RegisteredFunction>, json_args: String, tid: &TransactionId) -> Result<(String, Arc<RegisteredWorker>)>;
  /// Update the liveliness stats of the given worker
  fn update_worker_status(&self, worker: &Arc<RegisteredWorker>, status: &WorkerStatus, tid: &TransactionId);
  /// Prewarm the given function somewhere
  async fn prewarm(&self, func: Arc<RegisteredFunction>, tid: &TransactionId) -> Result<()>;
}

pub type LoadBalancer = Arc<dyn LoadBalancerTrait + Send + Sync + 'static>;

pub fn get_balancer(config: &ControllerConfig) -> Result<LoadBalancer> {
  if config.load_balancer.algorithm == "RoundRobin" {
    Ok(Arc::new(balancers::round_robin::RoundRobinLoadBalancer::new()))
  }
  else if config.load_balancer.algorithm == "LeastLoaded" {
    Ok(Arc::new(balancers::least_loaded::LeastLoadedBalancer {}))
  }
  else {
    anyhow::bail!("Unimplemented load balancing algorithm {}", config.load_balancer.algorithm)
  }
}
