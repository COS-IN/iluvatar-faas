use std::sync::Arc;
use anyhow::Result;
use tracing::debug;
use crate::{load_balancer_api::lb_config::ControllerConfig, transaction::TransactionId};
use crate::load_balancer_api::structs::internal::{RegisteredWorker, RegisteredFunction};
use crate::services::ControllerHealthService;

use super::graphite::graphite_svc::GraphiteService;

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
  /// Prewarm the given function somewhere
  async fn prewarm(&self, func: Arc<RegisteredFunction>, tid: &TransactionId) -> Result<()>;
}

pub type LoadBalancer = Arc<dyn LoadBalancerTrait + Send + Sync + 'static>;

pub fn get_balancer(config: &ControllerConfig, health_svc: Arc<ControllerHealthService>, tid: &TransactionId) -> Result<LoadBalancer> {
  if config.load_balancer.algorithm == "RoundRobin" {
    debug!("[{}] starting round robin balancer", tid);
    Ok(Arc::new(balancers::round_robin::RoundRobinLoadBalancer::new(health_svc)))
  }
  else if config.load_balancer.algorithm == "LeastLoaded" {
    debug!("[{}] starting least loaded balancer", tid);
    let graphite_svc = GraphiteService::boxed(config.graphite.clone());
    Ok(balancers::least_loaded::LeastLoadedBalancer::boxed(health_svc, graphite_svc, tid))
  }
  else {
    anyhow::bail!("Unimplemented load balancing algorithm {}", config.load_balancer.algorithm)
  }
}
