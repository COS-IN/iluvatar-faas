use std::sync::Arc;
use iluvatar_lib::services::load_balance::LoadBalancer;

use super::registration::RegistrationService;

#[allow(unused)]
pub struct HealthService {
  reg_funcs: Arc<RegistrationService>,
  lb: LoadBalancer,
}

impl HealthService {
  pub fn boxed( reg_funcs: Arc<RegistrationService>, lb: LoadBalancer) -> Arc<Self> {
    Arc::new(HealthService {
      reg_funcs,
      lb
    })
  }
  // TODO: this service
}
