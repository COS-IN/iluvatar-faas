use std::{sync::Arc, collections::HashMap};
use anyhow::Result;
use iluvatar_lib::bail_error;
use iluvatar_lib::worker_api::worker_comm::WorkerAPIFactory;
use log::{info, error};
use parking_lot::RwLock;
use iluvatar_lib::{services::load_balance::LoadBalancer, utils::calculate_fqdn, transaction::TransactionId};
use iluvatar_lib::load_balancer_api::structs::json::{RegisterWorker, RegisterFunction};
use iluvatar_lib::load_balancer_api::structs::internal::{RegisteredWorker, RegisteredFunction};

#[allow(unused)]
pub struct RegistrationService {
  pub lb: LoadBalancer,
  functions: Arc<RwLock<HashMap<String, Arc<RegisteredFunction>>>>,
  workers: Arc<RwLock<HashMap<String, Arc<RegisteredWorker>>>>,
  worker_fact: WorkerAPIFactory,
}

impl RegistrationService {
  pub fn boxed(lb: LoadBalancer) -> Arc<Self> {
    Arc::new(RegistrationService{
      lb,
      functions: Arc::new(RwLock::new(HashMap::new())),
      workers: Arc::new(RwLock::new(HashMap::new())),
      worker_fact: WorkerAPIFactory {},
    })
  }

  /// Return the function if it's been registered
  pub fn get_function(&self, fqdn: &String) -> Option<Arc<RegisteredFunction>> {
    match self.functions.read().get(fqdn) {
        Some(f) => Some(f.clone()),
        None => None,
    }
  }
  
  /// Register a new worker
  /// Prepare it with all registered functions too
  /// Send to load balancer
  pub async fn register_worker(&self, worker: RegisterWorker, tid: &TransactionId) -> Result<()> {
    if self.worker_registered(&worker.name) {
      bail_error!("[{}] Worker {} was already registered", tid, &worker.name);
    }

    let reg_worker = Arc::new(RegisteredWorker::from(worker));

    let mut api = self.worker_fact.get_worker_api(&reg_worker, tid).await?;
    for (_fqdn, function) in self.functions.read().iter() {
      match api.register(function.function_name.clone(), function.function_version.clone(), function.image_name.clone(), function.memory, function.cpus, function.parallel_invokes, tid.clone()).await {
        Ok(_) => (),
        Err(e) => {
          error!("[{}] new worker {} failed to register function because '{}'", tid, &reg_worker.name, e)
        },
      };
    }

    self.lb.add_worker(reg_worker.clone(), tid);
    let mut workers = self.workers.write();
    workers.insert(reg_worker.name.clone(), reg_worker);

    Ok(())
  }

  /// check if worker has been registered already
  fn worker_registered(&self, name: &String) -> bool {
    let workers = self.workers.read();
    workers.contains_key(name)
  }

  /// check if function has been registered already
  fn function_registered(&self, fqdn: &String) -> bool {
    let functions = self.functions.read();
    functions.contains_key(fqdn)
  }

  /// Register a new function with workers
  pub async fn register_function(&self, function: RegisterFunction, tid: &TransactionId) -> Result<()> {
    let fqdn = calculate_fqdn(&function.function_name, &function.function_version);
    if self.function_registered(&fqdn) {
      bail_error!("[{}] Function {} was already registered", tid, fqdn);
    }
    else {
      for (_name, worker) in self.workers.read().iter() {
        let mut api = self.worker_fact.get_worker_api(&worker, tid).await?;
        match api.register(function.function_name.clone(), function.function_version.clone(), function.image_name.clone(), function.memory, function.cpus, function.parallel_invokes, tid.clone()).await {
            Ok(_) => (),
            Err(e) => {
              error!("[{}] worker {} failed to register new function because '{}'", tid, &worker.name, e)
            },
        };
      }
      let function = Arc::new(RegisteredFunction::from(function));
      let mut functions = self.functions.write();
      functions.insert(fqdn.clone(), function);
    }

    info!("[{}] Function {} was registered", tid, fqdn);
    Ok(())
  }

  /// Get a lock-free iterator over all the currently registered workers
  pub fn iter_workers(&self) {
    todo!()
  }
}