use std::sync::Arc;

use iluvatar_lib::{load_balancer_api::lb_config::LoadBalancerConfig, services::load_balance::{get_balancer, LoadBalancer}};


#[allow(unused)]
pub struct Controller {
  config: LoadBalancerConfig,
  lb: Arc<dyn LoadBalancer + Send + Sync + 'static>,
}
unsafe impl Send for Controller{}

impl Controller {
  pub fn new(config: LoadBalancerConfig) -> Self {
    let lb = get_balancer(&config).unwrap();
    Controller {
      config,
      lb
    }
  }

  pub fn index(&self) {
    println!("INDEX");
  }
  pub fn name(&self, name_str: &String) {
    println!("server: {}", name_str);
  }
}