use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;
use parking_lot::{RwLock, Mutex};
use tracing::{info, warn, debug};
use crate::services::ControllerHealthService;
use crate::{services::load_balance::LoadBalancerTrait, transaction::TransactionId};
use crate::load_balancer_api::structs::internal::{RegisteredFunction, RegisteredWorker};
use crate::worker_api::worker_comm::WorkerAPIFactory;

pub struct RoundRobinLoadBalancer {
  workers: RwLock<Vec<Arc<RegisteredWorker>>>,
  next: Mutex<usize>,
  worker_fact: WorkerAPIFactory,
  health: Arc<ControllerHealthService>,
}

impl RoundRobinLoadBalancer {
  pub fn new(health: Arc<ControllerHealthService>) -> Self {
    RoundRobinLoadBalancer {
      workers: RwLock::new(Vec::new()),
      next: Mutex::new(0),
      worker_fact: WorkerAPIFactory {},
      health,
    }
  }

  fn get_next(&self, tid: &TransactionId) -> Arc<RegisteredWorker> {
    let mut i = 0;
    loop {
      let mut val = self.next.lock();
      *val = *val + 1 as usize;
      if *val >= self.workers.read().len() {
        *val = 0 as usize;
      }
      let worker = &self.workers.read()[*val];
      if self.health.is_healthy(worker) {
        return worker.clone();
      } else {
        if i >= self.workers.read().len() {
          warn!("[{}] Could not find a healthy worker!", tid);
          return worker.clone();
        }
      }
      i = i + 1;
    }
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
    let worker = self.get_next(tid);
    info!("[{}] invoking function {} on worker {}", tid, &func.fqdn, &worker.name);

    let mut api = self.worker_fact.get_worker_api(&worker, tid).await?;
    let result = match api.invoke(func.function_name.clone(), func.function_version.clone(), json_args, None, tid.clone()).await {
      Ok(r) => r,
      Err(e) => {
        self.health.schedule_health_check(self.health.clone(), worker, tid, Some(Duration::from_secs(1)));
        anyhow::bail!(e)
      },
    };
    debug!("[{}] invocation result: {}", tid, result);
    Ok(result)
  }

  async fn prewarm(&self, func: Arc<RegisteredFunction>, tid: &TransactionId) -> Result<()> {
    let worker = self.get_next(tid);
    info!("[{}] prewarming function {} on worker {}", tid, &func.fqdn, &worker.name);
    let mut api = self.worker_fact.get_worker_api(&worker, tid).await?;
    let result = match api.prewarm(func.function_name.clone(), func.function_version.clone(), None, None, None, tid.clone()).await {
      Ok(r) => r,
      Err(e) => {
        self.health.schedule_health_check(self.health.clone(), worker, tid, Some(Duration::from_secs(1)));
        anyhow::bail!(e)
      }
    };
    debug!("[{}] prewarm result: {}", tid, result);
    Ok(())
  }

  async fn send_async_invocation(&self, func: Arc<RegisteredFunction>, json_args: String, tid: &TransactionId) -> Result<(String, Arc<RegisteredWorker>)> {
    let worker = self.get_next(tid);
    info!("[{}] invoking function async {} on worker {}", tid, &func.fqdn, &worker.name);

    let mut api = self.worker_fact.get_worker_api(&worker, tid).await?;
    let result = match api.invoke_async(func.function_name.clone(), func.function_version.clone(), json_args, None, tid.clone()).await {
      Ok(r) => r,
      Err(e) => {
        self.health.schedule_health_check(self.health.clone(), worker, tid, Some(Duration::from_secs(1)));
        anyhow::bail!(e)
      },
    };
    debug!("[{}] invocation result: {}", tid, result);
    Ok( (result, worker) )
  }
}