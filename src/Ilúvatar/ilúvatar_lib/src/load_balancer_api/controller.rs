use std::{sync::Arc, time::Duration};
use crate::{services::{load_balance::{get_balancer, LoadBalancer}, ControllerHealthService, graphite::graphite_svc::GraphiteService}, transaction::TransactionId, bail_error, load_balancer_api::{registration::RegistrationService, load_reporting::LoadService}, worker_api::worker_comm::WorkerAPIFactory};
use crate::utils::{calculate_fqdn, config::args_to_json};
use crate::load_balancer_api::structs::json::{Prewarm, Invoke, RegisterWorker, RegisterFunction};
use crate::load_balancer_api::lb_config::ControllerConfig;
use crate::rpc::InvokeResponse;
use anyhow::Result;
use tracing::{info, debug, error};
use crate::load_balancer_api::async_invoke::AsyncService;

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
    let worker_fact = WorkerAPIFactory::boxed();
    let health_svc = ControllerHealthService::boxed(worker_fact.clone());
    let graphite_svc = GraphiteService::boxed(config.graphite.clone());
    let load_svc = LoadService::boxed(graphite_svc.clone(), config.load_balancer.clone(), tid);
    let lb: LoadBalancer = get_balancer(&config, health_svc.clone(), tid, graphite_svc, load_svc.clone(), worker_fact.clone()).unwrap();
    let reg_svc = RegistrationService::boxed(lb.clone(), worker_fact.clone());
    let async_svc = AsyncService::boxed(worker_fact.clone());
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

  pub async fn prewarm(&self, request: Prewarm, tid: &TransactionId) -> Result<Duration> {
    let fqdn = calculate_fqdn(&request.function_name, &request.function_version);
    match self.registration_svc.get_function(&fqdn) {
      Some(func) => {
        debug!(tid=%tid, fqdn=%fqdn, "found function for prewarm");
        self.lb.prewarm(func, tid).await
      },
      None => bail_error!(tid=%tid, fqdn=%fqdn, "Function was not registered; could not prewarm")
    }
  }

  pub async fn invoke(&self, request: Invoke, tid: &TransactionId) -> Result<(InvokeResponse, Duration)> {
    let fqdn = calculate_fqdn(&request.function_name, &request.function_version);
    match self.registration_svc.get_function(&fqdn) {
      Some(func) => {
        info!(tid=%tid, fqdn=%fqdn, "Sending function to load balancer for invocation");
        let args = match request.args {
            Some(args_vec) => args_to_json(args_vec),
            None => "{}".to_string(),
        };
        self.lb.send_invocation(func, args, tid).await
      },
      None => bail_error!(tid=%tid, fqdn=%fqdn, "Function was not registered; could not invoke")
    }
  }

  pub async fn invoke_async(&self, request: Invoke, tid: &TransactionId) -> Result<(String, Duration)> {
    let fqdn = calculate_fqdn(&request.function_name, &request.function_version);
    match self.registration_svc.get_function(&fqdn) {
      Some(func) => {
        info!(tid=%tid, fqdn=%fqdn, "Sending function to load balancer for async invocation");
        let args = match request.args {
            Some(args_vec) => args_to_json(args_vec),
            None => "{}".to_string(),
        };
        match self.lb.send_async_invocation(func, args, tid).await {
          Ok( (cookie, worker, duration) ) => {
            self.async_svc.register_async_invocation(cookie.clone(), worker, tid);
            Ok( (cookie, duration) )
          },
          Err(e) => {
            error!(tid=%tid, error=%e, "async invocation failed");
            Err(e)
          },
        }
      },
      None => bail_error!(tid=%tid, fqdn=%fqdn, "Function was not registered; could not invoke async")
    }
  }

  pub async fn check_async_invocation(&self, cookie: String, tid: &TransactionId) -> Result<Option<String>> {
    self.async_svc.check_async_invocation(cookie, tid).await
  }
}
