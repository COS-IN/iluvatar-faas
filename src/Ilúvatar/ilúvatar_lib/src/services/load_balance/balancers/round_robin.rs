use std::sync::Arc;
use anyhow::Result;
use parking_lot::{RwLock, Mutex};
use crate::{services::load_balance::LoadBalancerTrait, transaction::TransactionId};
use crate::load_balancer_api::structs::internal::{RegisteredFunction, RegisteredWorker, WorkerStatus};
use log::*;
use crate::worker_api::worker_comm::WorkerAPIFactory;

pub struct RoundRobinLoadBalancer {
  workers: RwLock<Vec<Arc<RegisteredWorker>>>,
  next: Mutex<usize>,
  worker_fact: WorkerAPIFactory,
}

impl RoundRobinLoadBalancer {
  pub fn new() -> Self {
    RoundRobinLoadBalancer {
      workers: RwLock::new(Vec::new()),
      next: Mutex::new(0),
      worker_fact: WorkerAPIFactory {},
    }
  }

  fn get_next(&self) -> usize {
    let mut val = self.next.lock();
    *val = *val + 1 as usize;
    if *val >= self.workers.read().len() {
      *val = 0 as usize;
    }
    *val
  }
}

#[tonic::async_trait]
impl LoadBalancerTrait for RoundRobinLoadBalancer {
  fn add_worker(&self, worker: Arc<RegisteredWorker>, tid: &TransactionId) {
    info!("[{}] Registering new worker {} in RoundRobin load balancer", tid, &worker.name);
    let mut workers = self.workers.write();
    workers.push(worker);
  }

  async fn send_invocation(&self, func: Arc<RegisteredFunction>, json_args: String, tid: &TransactionId) -> Result<String> {
    let worker_idx = self.get_next();
    let worker = self.workers.read()[worker_idx].clone();
    info!("[{}] invoking function {} on worker {}", tid, &func.fqdn, &worker.name);

    let mut api = self.worker_fact.get_worker_api(&worker, tid).await?;
    let result = api.invoke(func.function_name.clone(), func.function_version.clone(), json_args, None, tid.clone()).await?;
    debug!("[{}] invocation result: {}", tid, result);
    Ok(result)
  }

  fn update_worker_status(&self, _worker: &Arc<RegisteredWorker>, _status: &WorkerStatus, _tid: &TransactionId) {
    todo!()
  }

  async fn prewarm(&self, func: Arc<RegisteredFunction>, tid: &TransactionId) -> Result<()> {
    let worker_idx = self.get_next();
    let worker = self.workers.read()[worker_idx].clone();
    info!("[{}] prewarming function {} on worker {}", tid, &func.fqdn, &worker.name);
    let mut api = self.worker_fact.get_worker_api(&worker, tid).await?;
    let result = api.prewarm(func.function_name.clone(), func.function_version.clone(), None, None, None, tid.clone()).await?;
    debug!("[{}] prewarm result: {}", tid, result);
    Ok(())
  }

  async fn send_async_invocation(&self, func: Arc<RegisteredFunction>, json_args: String, tid: &TransactionId) -> Result<(String, Arc<RegisteredWorker>)> {
    let worker_idx = self.get_next();
    let worker = self.workers.read()[worker_idx].clone();
    info!("[{}] invoking function async {} on worker {}", tid, &func.fqdn, &worker.name);

    let mut api = self.worker_fact.get_worker_api(&worker, tid).await?;
    let result = api.invoke_async(func.function_name.clone(), func.function_version.clone(), json_args, None, tid.clone()).await?;
    debug!("[{}] invocation result: {}", tid, result);
    Ok( (result, worker) )
  }
}