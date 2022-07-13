use std::sync::Arc;

use crate::load_balancer_api::{lb_config::LoadBalancerConfig, structs::RegisterWorker};
use anyhow::Result;

mod balancers;

pub trait LoadBalancerTrait {
  fn add_worker(&self, worker: &RegisterWorker);
  fn send_invocation(&self);
  fn update_worker_status(&self);
}

pub type LoadBalancer = Arc<dyn LoadBalancerTrait + Send + Sync + 'static>;

pub fn get_balancer(config: &LoadBalancerConfig) -> Result<LoadBalancer> {
  if config.load_balancer.algorithm == "RoundRobin" {
    Ok(Arc::new(balancers::round_robin::RoundRobinLoadBalancer {}))
  }
  else if config.load_balancer.algorithm == "LeastLoaded" {
    Ok(Arc::new(balancers::least_loaded::LeastLoadedBalancer {}))
  }
  else {
    anyhow::bail!("Unimplemented load balancing algorithm {}", config.load_balancer.algorithm)
  }
}
