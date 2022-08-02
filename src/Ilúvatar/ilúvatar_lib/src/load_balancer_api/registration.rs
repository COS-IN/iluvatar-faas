use std::sync::Arc;
use anyhow::Result;
use dashmap::DashMap;
use crate::bail_error;
use crate::worker_api::worker_comm::WorkerAPIFactory;
use tracing::{info, error};
use crate::{services::load_balance::LoadBalancer, utils::calculate_fqdn, transaction::TransactionId};
use crate::load_balancer_api::structs::json::{RegisterWorker, RegisterFunction};
use crate::load_balancer_api::structs::internal::{RegisteredWorker, RegisteredFunction};

#[allow(unused)]
pub struct RegistrationService {
  pub lb: LoadBalancer,
  functions: Arc<DashMap<String, Arc<RegisteredFunction>>>,
  workers: Arc<DashMap<String, Arc<RegisteredWorker>>>,
  worker_fact: WorkerAPIFactory,
}

impl RegistrationService {
  pub fn boxed(lb: LoadBalancer) -> Arc<Self> {
    Arc::new(RegistrationService{
      lb,
      functions: Arc::new(DashMap::new()),
      workers: Arc::new(DashMap::new()),
      worker_fact: WorkerAPIFactory {},
    })
  }

  /// Return the function if it's been registered
  pub fn get_function(&self, fqdn: &String) -> Option<Arc<RegisteredFunction>> {
    match self.functions.get(fqdn) {
        Some(c) => Some(c.clone()),
        None => None,
    }
  }
  
  /// Register a new worker
  /// Prepare it with all registered functions too
  /// Send to load balancer
  pub async fn register_worker(&self, worker: RegisterWorker, tid: &TransactionId) -> Result<Arc<RegisteredWorker>> {
    if self.worker_registered(&worker.name) {
      bail_error!("[{}] Worker {} was already registered", tid, &worker.name);
    }

    let reg_worker = Arc::new(RegisteredWorker::from(worker));

    let mut api = self.worker_fact.get_worker_api(&reg_worker, tid).await?;
    for item in self.functions.iter() {
      let function = item.value();
      match api.register(function.function_name.clone(), function.function_version.clone(), function.image_name.clone(), function.memory, function.cpus, function.parallel_invokes, tid.clone()).await {
        Ok(_) => (),
        Err(e) => {
          error!("[{}] new worker {} failed to register function because '{}'", tid, &reg_worker.name, e)
        },
      };
    }
    self.lb.add_worker(reg_worker.clone(), tid);
    self.workers.insert(reg_worker.name.clone(), reg_worker.clone());
    info!("[{}] worker '{}' successfully registered", tid, reg_worker.name);
    Ok(reg_worker)
  }

  /// check if worker has been registered already
  fn worker_registered(&self, name: &String) -> bool {
    self.workers.contains_key(name)
  }

  /// check if function has been registered already
  fn function_registered(&self, fqdn: &String) -> bool {
    self.functions.contains_key(fqdn)
  }

  /// Register a new function with workers
  pub async fn register_function(&self, function: RegisterFunction, tid: &TransactionId) -> Result<()> {
    let fqdn = calculate_fqdn(&function.function_name, &function.function_version);
    if self.function_registered(&fqdn) {
      bail_error!("[{}] Function {} was already registered", tid, fqdn);
    }
    else {
      for item in self.workers.iter() {
        let worker = item.value();
        let mut api = self.worker_fact.get_worker_api(&worker, tid).await?;
        match api.register(function.function_name.clone(), function.function_version.clone(), function.image_name.clone(), function.memory, function.cpus, function.parallel_invokes, tid.clone()).await {
            Ok(_) => (),
            Err(e) => {
              error!("[{}] worker {} failed to register new function because '{}'", tid, &worker.name, e)
            },
        };
      }
      let function = Arc::new(RegisteredFunction::from(function));
      self.functions.insert(fqdn.clone(), function);
    }

    info!("[{}] Function {} was registered", tid, fqdn);
    Ok(())
  }

  /// Get a lock-free iterable over all the currently registered workers
  pub fn iter_workers(&self) -> Vec<Arc<RegisteredWorker>> {
    self.workers.iter().map(|w| w.value().clone()).collect()
  }
}