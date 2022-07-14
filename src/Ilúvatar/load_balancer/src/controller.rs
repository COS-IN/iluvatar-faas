use std::sync::Arc;

use iluvatar_lib::{services::load_balance::{get_balancer, LoadBalancer}, transaction::TransactionId, load_balancer_api::structs::json::Prewarm, utils::calculate_fqdn, bail_error};
use iluvatar_lib::load_balancer_api::{lb_config::LoadBalancerConfig, structs::json::{RegisterWorker, RegisterFunction}};
use anyhow::Result;
use log::debug;
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

  pub async fn register_function(&self, function: RegisterFunction, tid: &TransactionId) -> Result<()> {
    self.registration_svc.register_function(function, tid).await?;
    Ok(())
  }

  pub async fn register_worker(&self, worker: RegisterWorker, tid: &TransactionId) -> Result<()> {
    self.registration_svc.register_worker(worker, tid).await?;
    Ok(())
  }

  pub async fn prewarm(&self, request: Prewarm, tid: &TransactionId) -> Result<()> {
    let fqdn = calculate_fqdn(&request.function_name, &request.function_version);
    match self.registration_svc.get_function(&fqdn) {
      Some(func) => {
        debug!("[{}] found function {} for prewarm", tid, &fqdn);
        self.lb.prewarm(func, tid).await
      },
      None => bail_error!("[{}] function {} was not registered", tid, fqdn)
    }
  }

  pub fn index(&self) {
    println!("INDEX");
  }
  pub fn name(&self, name_str: &String) {
    println!("server: {}", name_str);
  }
}