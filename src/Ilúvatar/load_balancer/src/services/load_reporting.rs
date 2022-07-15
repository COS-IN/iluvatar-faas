use std::sync::Arc;
use super::registration::RegistrationService;

#[allow(unused)]
pub struct LoadService {
  pub reg_funcs: Arc<RegistrationService>,
}

impl LoadService {
  pub fn boxed(reg_funcs: Arc<RegistrationService>) -> Arc<Self> {
    Arc::new(LoadService {
      reg_funcs,
    })
  }
  // TODO: this service
}
