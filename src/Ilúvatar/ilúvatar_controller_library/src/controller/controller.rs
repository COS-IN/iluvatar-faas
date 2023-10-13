use crate::controller::controller_config::ControllerConfig;
use crate::controller::structs::json::{Invoke, Prewarm, RegisterFunction};
use crate::services::async_invoke::AsyncService;
use crate::services::controller_health::{ControllerHealthService, HealthService, SimHealthService};
use crate::services::load_balance::{get_balancer, LoadBalancer};
use crate::services::load_reporting::LoadService;
use crate::services::registration::RegistrationService;
use anyhow::Result;
use iluvatar_library::api_register::RegisterWorker;
use iluvatar_library::influx::InfluxClient;
use iluvatar_library::utils::{calculate_fqdn, config::args_to_json};
use iluvatar_library::{bail_error, transaction::TransactionId};
use iluvatar_worker_library::rpc::InvokeResponse;
use iluvatar_worker_library::worker_api::worker_comm::WorkerAPIFactory;
use std::{sync::Arc, time::Duration};
use tracing::{debug, error, info};

#[allow(unused)]
pub struct Controller {
    config: ControllerConfig,
    lb: LoadBalancer,
    async_svc: Arc<AsyncService>,
    health_svc: Arc<dyn ControllerHealthService>,
    load_svc: Arc<LoadService>,
    registration_svc: Arc<RegistrationService>,
}
unsafe impl Send for Controller {}

impl Controller {
    pub async fn new(config: ControllerConfig, tid: &TransactionId) -> Result<Self> {
        let worker_fact = WorkerAPIFactory::boxed();
        let health_svc: Arc<dyn ControllerHealthService> = match iluvatar_library::utils::is_simulation() {
            true => SimHealthService::boxed(),
            false => HealthService::boxed(worker_fact.clone()),
        };

        let influx = match InfluxClient::new(config.influx.clone(), tid).await {
            Ok(i) => i,
            Err(e) => bail_error!(tid=%tid, error=%e, "Failed to create InfluxClient"),
        };
        let load_svc = match LoadService::boxed(influx, config.load_balancer.clone(), tid, worker_fact.clone()) {
            Ok(l) => l,
            Err(e) => bail_error!(tid=%tid, error=%e, "Failed to create LoadService"),
        };
        let lb: LoadBalancer =
            match get_balancer(&config, health_svc.clone(), tid, load_svc.clone(), worker_fact.clone()) {
                Ok(lb) => lb,
                Err(e) => bail_error!(tid=%tid, error=%e, "Failed to create load balancer"),
            };
        let reg_svc = RegistrationService::boxed(lb.clone(), worker_fact.clone());
        let async_svc = AsyncService::boxed(worker_fact.clone());
        Ok(Controller {
            config,
            lb,
            async_svc,
            health_svc,
            load_svc,
            registration_svc: reg_svc,
        })
    }

    pub async fn register_function(&self, function: RegisterFunction, tid: &TransactionId) -> Result<()> {
        self.registration_svc.register_function(function, tid).await?;
        Ok(())
    }

    pub async fn register_worker(&self, worker: RegisterWorker, tid: &TransactionId) -> Result<()> {
        let worker = self.registration_svc.register_worker(worker, tid).await?;
        self.health_svc
            .schedule_health_check(self.health_svc.clone(), worker, tid, Some(Duration::from_secs(5)));
        Ok(())
    }

    pub async fn prewarm(&self, request: Prewarm, tid: &TransactionId) -> Result<Duration> {
        let fqdn = calculate_fqdn(&request.function_name, &request.function_version);
        match self.registration_svc.get_function(&fqdn) {
            Some(func) => {
                debug!(tid=%tid, fqdn=%fqdn, "found function for prewarm");
                self.lb.prewarm(func, tid).await
            }
            None => {
                bail_error!(tid=%tid, fqdn=%fqdn, "Function was not registered; could not prewarm")
            }
        }
    }

    /// Returns the invocation result and the E2E duration of the invocation recorded by the load balancer  
    ///   Thus including both the time spent on the worker and the invocation time, plus networking
    pub async fn invoke(&self, request: Invoke, tid: &TransactionId) -> Result<(InvokeResponse, Duration)> {
        let fqdn = calculate_fqdn(&request.function_name, &request.function_version);
        match self.registration_svc.get_function(&fqdn) {
            Some(func) => {
                info!(tid=%tid, fqdn=%fqdn, "Sending function to load balancer for invocation");
                let args = match request.args {
                    Some(args_vec) => args_to_json(&args_vec)?,
                    None => "{}".to_string(),
                };
                self.lb.send_invocation(func, args, tid).await
            }
            None => {
                bail_error!(tid=%tid, fqdn=%fqdn, "Function was not registered; could not invoke")
            }
        }
    }

    pub async fn invoke_async(&self, request: Invoke, tid: &TransactionId) -> Result<(String, Duration)> {
        let fqdn = calculate_fqdn(&request.function_name, &request.function_version);
        match self.registration_svc.get_function(&fqdn) {
            Some(func) => {
                info!(tid=%tid, fqdn=%fqdn, "Sending function to load balancer for async invocation");
                let args = match request.args {
                    Some(args_vec) => args_to_json(&args_vec)?,
                    None => "{}".to_string(),
                };
                match self.lb.send_async_invocation(func, args, tid).await {
                    Ok((cookie, worker, duration)) => {
                        self.async_svc.register_async_invocation(cookie.clone(), worker, tid);
                        Ok((cookie, duration))
                    }
                    Err(e) => {
                        error!(tid=%tid, error=%e, "async invocation failed");
                        Err(e)
                    }
                }
            }
            None => {
                bail_error!(tid=%tid, fqdn=%fqdn, "Function was not registered; could not invoke async")
            }
        }
    }

    pub async fn check_async_invocation(&self, cookie: String, tid: &TransactionId) -> Result<Option<String>> {
        self.async_svc.check_async_invocation(cookie, tid).await
    }
}
