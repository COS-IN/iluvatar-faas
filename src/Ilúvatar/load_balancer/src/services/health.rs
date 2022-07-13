use std::sync::Arc;

use iluvatar_lib::{load_balancer_api::lb_config::LoadBalancerConfig, services::load_balance::LoadBalancer};

use super::registration::RegistrationService;

#[allow(unused)]
pub struct HealthService {
  reg_funcs: Arc<RegistrationService>,
  config: LoadBalancerConfig,
  lb: LoadBalancer,
}

impl HealthService {
  pub fn boxed(config: LoadBalancerConfig, reg_funcs: Arc<RegistrationService>, lb: LoadBalancer) -> Arc<Self> {
    Arc::new(HealthService {
      reg_funcs,
      config,
      lb
    })
  }
  // TODO: this service
}
