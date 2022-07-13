use std::sync::Arc;

use iluvatar_lib::load_balancer_api::lb_config::LoadBalancerConfig;

use super::registration::RegistrationService;

#[allow(unused)]
pub struct LoadService {
  pub reg_funcs: Arc<RegistrationService>,
  config: LoadBalancerConfig,
}

impl LoadService {
  pub fn boxed(config: LoadBalancerConfig, reg_funcs: Arc<RegistrationService>) -> Arc<Self> {
    Arc::new(LoadService {
      reg_funcs,
      config
    })
  }
  // TODO: this service
}
