use std::sync::Arc;

use iluvatar_lib::{load_balancer_api::lb_config::LoadBalancerConfig, services::load_balance::{get_balancer, LoadBalancer}};

use crate::services::{async_invoke::AsyncService, registration::RegistrationService, load_reporting::LoadService, health::HealthService};


#[allow(unused)]
pub struct Controller {
  config: LoadBalancerConfig,
  lb: LoadBalancer,
  async_svc: Arc<AsyncService>,
  health_svc: Arc<HealthService>,
  load_svc: Arc<LoadService>,
  registration_svc: Arc<RegistrationService>
}
unsafe impl Send for Controller{}

impl Controller {
  pub fn new(config: LoadBalancerConfig) -> Self {
    let lb: LoadBalancer = get_balancer(&config).unwrap();
    let reg_svc = RegistrationService::boxed(config.clone(), lb.clone());
    let async_svc = AsyncService::boxed(config.clone());
    let health_svc = HealthService::boxed(config.clone(), reg_svc.clone(), lb.clone());
    let load_svc = LoadService::boxed(config.clone(), reg_svc.clone());
    Controller {
      config,
      lb,
      async_svc,
      health_svc,
      load_svc,
      registration_svc: reg_svc,
    }
  }

  pub fn index(&self) {
    println!("INDEX");
  }
  pub fn name(&self, name_str: &String) {
    println!("server: {}", name_str);
  }
}