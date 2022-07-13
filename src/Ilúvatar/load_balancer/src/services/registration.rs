use std::sync::Arc;
use anyhow::Result;
use iluvatar_lib::{services::load_balance::LoadBalancer, load_balancer_api::{structs::RegisterWorker, lb_config::LoadBalancerConfig}};

#[allow(unused)]
pub struct RegistrationService {
  pub lb: LoadBalancer,
  config: LoadBalancerConfig,
}

impl RegistrationService {
  pub fn boxed(config: LoadBalancerConfig, lb: LoadBalancer) -> Arc<Self> {
    Arc::new(RegistrationService{
      lb,
      config
    })
  }
  
  /// Register a new worker
  /// Prepare it with all registered functions too
  /// So it can be sent to load balancer
  pub fn register_worker(&self, worker: RegisterWorker) -> Result<()> {
    // TODO: register all known functions here

    self.lb.register_worker(&worker);
    todo!()
  }

  /// Register a new function with workers
  pub fn register_function() -> Result<()> {
    todo!()
  }

  /// Get an iterator over all the currently registered workers
  pub fn iter_workers(&self) {
    todo!()
  }
}