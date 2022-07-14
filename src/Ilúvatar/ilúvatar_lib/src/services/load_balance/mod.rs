use std::sync::Arc;
use crate::{load_balancer_api::{lb_config::LoadBalancerConfig, structs::internal::{RegisteredWorker, RegisteredFunction, WorkerStatus}}, transaction::TransactionId};
use anyhow::Result;

mod balancers;

#[tonic::async_trait]
pub trait LoadBalancerTrait {
  fn add_worker(&self, worker: Arc<RegisteredWorker>, tid: &TransactionId);
  async fn send_invocation(&self, tid: &TransactionId) -> Result<()>;
  fn update_worker_status(&self, worker: &RegisteredWorker, status: WorkerStatus, tid: &TransactionId);
  async fn prewarm(&self, func: Arc<RegisteredFunction>, tid: &TransactionId) -> Result<()>;
}

pub type LoadBalancer = Arc<dyn LoadBalancerTrait + Send + Sync + 'static>;

pub fn get_balancer(config: &LoadBalancerConfig) -> Result<LoadBalancer> {
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
