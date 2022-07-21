use std::{sync::Arc, time::Duration};

use iluvatar_lib::{services::{load_balance::{get_balancer, LoadBalancer}, ControllerHealthService}, transaction::TransactionId, bail_error};
use iluvatar_lib::utils::{calculate_fqdn, config::args_to_json};
use iluvatar_lib::load_balancer_api::structs::json::{Prewarm, Invoke, RegisterWorker, RegisterFunction};
use iluvatar_lib::load_balancer_api::lb_config::ControllerConfig;
use anyhow::Result;
use tracing::{info, debug, error};
use crate::services::{async_invoke::AsyncService, registration::RegistrationService, load_reporting::LoadService};

#[allow(unused)]
pub struct Controller {
  config: ControllerConfig,
  lb: LoadBalancer,
  async_svc: Arc<AsyncService>,
  health_svc: Arc<ControllerHealthService>,
  load_svc: Arc<LoadService>,
  registration_svc: Arc<RegistrationService>
}
unsafe impl Send for Controller{}

impl Controller {
  pub fn new(config: ControllerConfig, tid: &TransactionId) -> Self {
    let health_svc = ControllerHealthService::boxed();
    let lb: LoadBalancer = get_balancer(&config, health_svc.clone(), tid).unwrap();
    let reg_svc = RegistrationService::boxed(lb.clone());
    let async_svc = AsyncService::boxed();
    let load_svc = LoadService::boxed(reg_svc.clone());
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
    let worker = self.registration_svc.register_worker(worker, tid).await?;
    self.health_svc.schedule_health_check(self.health_svc.clone(), worker, tid, Some(Duration::from_secs(5)));
    Ok(())
  }

  pub async fn prewarm(&self, request: Prewarm, tid: &TransactionId) -> Result<()> {
    let fqdn = calculate_fqdn(&request.function_name, &request.function_version);
    match self.registration_svc.get_function(&fqdn) {
      Some(func) => {
        debug!("[{}] found function {} for prewarm", tid, &fqdn);
        self.lb.prewarm(func, tid).await
      },
      None => bail_error!("[{}] function {} was not registered; could not prewarm", tid, fqdn)
    }
  }

  pub async fn invoke(&self, request: Invoke, tid: &TransactionId) -> Result<String> {
    let fqdn = calculate_fqdn(&request.function_name, &request.function_version);
    match self.registration_svc.get_function(&fqdn) {
      Some(func) => {
        info!("[{}] sending function {} to load balancer for invocation", tid, &fqdn);
        let args = match request.args {
            Some(args_vec) => args_to_json(args_vec),
            None => "{}".to_string(),
        };
        self.lb.send_invocation(func, args, tid).await
      },
      None => bail_error!("[{}] function {} was not registered; could not invoke", tid, fqdn)
    }
  }

  pub async fn invoke_async(&self, request: Invoke, tid: &TransactionId) -> Result<String> {
    let fqdn = calculate_fqdn(&request.function_name, &request.function_version);
    match self.registration_svc.get_function(&fqdn) {
      Some(func) => {
        info!("[{}] sending function {} to load balancer for invocation", tid, &fqdn);
        let args = match request.args {
            Some(args_vec) => args_to_json(args_vec),
            None => "{}".to_string(),
        };
        match self.lb.send_async_invocation(func, args, tid).await {
          Ok( (cookie, worker) ) => {
            self.async_svc.register_async_invocation(cookie.clone(), worker, tid);
            Ok(cookie)
          },
          Err(e) => {
            error!("[{}] async invocation failed because: {}", tid, e);
            Err(e)
          },
        }
      },
      None => bail_error!("[{}] function {} was not registered; could not invoke", tid, fqdn)
    }
  }

  pub async fn check_async_invocation(&self, cookie: String, tid: &TransactionId) -> Result<Option<String>> {
    self.async_svc.check_async_invocation(cookie, tid).await
  }
}
