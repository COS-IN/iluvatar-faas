use std::{sync::Arc, collections::HashMap};
use anyhow::Result;
use iluvatar_lib::bail_error;
use log::{info, error};
use parking_lot::RwLock;
use iluvatar_lib::{services::load_balance::LoadBalancer, utils::calculate_fqdn, transaction::TransactionId};
use iluvatar_lib::load_balancer_api::structs::{RegisterWorker, RegisterFunction};
use iluvatar_lib::load_balancer_api::lb_config::LoadBalancerConfig;

use super::worker_comm::WorkerAPIFactory;

#[allow(unused)]
pub struct RegistrationService {
  pub lb: LoadBalancer,
  config: LoadBalancerConfig,
  functions: Arc<RwLock<HashMap<String, RegisterFunction>>>,
  workers: Arc<RwLock<Vec<RegisterWorker>>>,
  worker_fact: WorkerAPIFactory,
}

impl RegistrationService {
  pub fn boxed(config: LoadBalancerConfig, lb: LoadBalancer) -> Arc<Self> {
    Arc::new(RegistrationService{
      lb,
      config,
      functions: Arc::new(RwLock::new(HashMap::new())),
      workers: Arc::new(RwLock::new(Vec::new())),
      worker_fact: WorkerAPIFactory {},
    })
  }
  
  /// Register a new worker
  /// Prepare it with all registered functions too
  /// So it can be sent to load balancer
  pub fn register_worker(&self, worker: RegisterWorker) -> Result<()> {
    // TODO: register all known functions here

    self.lb.add_worker(&worker);
    todo!()
  }

  /// Register a new function with workers
  pub async fn register_function(&self, function: RegisterFunction, tid: &TransactionId) -> Result<()> {
    let fqdn = calculate_fqdn(&function.function_name, &function.function_version);
    let mut functions = self.functions.write();
    if functions.contains_key(&fqdn) {
      bail_error!("[{}] Function {} was already registered", tid, fqdn);
    }
    else {
      for worker in self.workers.read().iter() {
        let mut api = self.worker_fact.get_worker_api(&worker, tid).await?;
        match api.register(function.function_name.clone(), function.function_version.clone(), function.image_name.clone(), function.memory, function.cpus, function.parallel_invokes, tid.clone()).await {
            Ok(_) => (),
            Err(e) => {
              error!("[{}] worker {} failed to register function because '{}'", tid, &worker.name, e)
            },
        };
      }
      functions.insert(fqdn.clone(), function);
    }

    info!("[{}] Function {} was registered", tid, fqdn);
    Ok(())
  }

  /// Get an iterator over all the currently registered workers
  pub fn iter_workers(&self) {
    todo!()
  }
}