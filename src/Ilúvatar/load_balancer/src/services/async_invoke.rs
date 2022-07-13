use std::sync::Arc;

use iluvatar_lib::load_balancer_api::lb_config::LoadBalancerConfig;

#[allow(unused)]
pub struct AsyncService {
  config: LoadBalancerConfig
}

impl AsyncService {
  pub fn boxed(config: LoadBalancerConfig) -> Arc<Self> {
    Arc::new(AsyncService {
      config
    })
  }
  // TODO: this service
}
