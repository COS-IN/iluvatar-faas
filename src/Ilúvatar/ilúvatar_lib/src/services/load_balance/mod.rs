use std::sync::Arc;

use crate::load_balancer_api::lb_config::LoadBalancerConfig;
use anyhow::Result;

mod balancers;

pub trait LoadBalancer {
  fn register_worker(&self);
  fn send_invocation(&self);
  fn update_worker_status(&self);
}

pub fn get_balancer(config: &LoadBalancerConfig) -> Result<Arc<dyn LoadBalancer + Send + Sync + 'static>> {
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
