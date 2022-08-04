use std::sync::Arc;
use std::time::Duration;
use anyhow::Result;
use parking_lot::{RwLock, Mutex};
use tracing::{info, warn, debug};
use crate::{send_invocation, prewarm, send_async_invocation};
use crate::services::ControllerHealthService;
use crate::{services::load_balance::LoadBalancerTrait, transaction::TransactionId};
use crate::load_balancer_api::structs::internal::{RegisteredFunction, RegisteredWorker};
use crate::{worker_api::worker_comm::WorkerAPIFactory};
use crate::utils::timing::TimedExt;
use crate::rpc::InvokeResponse;

pub struct RoundRobinLoadBalancer {
  workers: RwLock<Vec<Arc<RegisteredWorker>>>,
  next: Mutex<usize>,
  worker_fact: Arc<WorkerAPIFactory>,
  health: Arc<ControllerHealthService>,
}

impl RoundRobinLoadBalancer {
  pub fn new(health: Arc<ControllerHealthService>, worker_fact: Arc<WorkerAPIFactory>) -> Self {
    RoundRobinLoadBalancer {
      workers: RwLock::new(Vec::new()),
      next: Mutex::new(0),
      worker_fact,
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
          warn!(tid=%tid, "Could not find a healthy worker!");
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

  async fn send_invocation(&self, func: Arc<RegisteredFunction>, json_args: String, tid: &TransactionId) -> Result<(InvokeResponse, Duration)> {
    let worker = self.get_next(tid);
    send_invocation!(func, json_args, tid, self.worker_fact, self.health, worker)
  }

  async fn prewarm(&self, func: Arc<RegisteredFunction>, tid: &TransactionId) -> Result<Duration> {
    let worker = self.get_next(tid);
    prewarm!(func, tid, self.worker_fact, self.health, worker)
  }

  async fn send_async_invocation(&self, func: Arc<RegisteredFunction>, json_args: String, tid: &TransactionId) -> Result<(String, Arc<RegisteredWorker>, Duration)> {
    let worker = self.get_next(tid);
    send_async_invocation!(func, json_args, tid, self.worker_fact, self.health, worker)
  }
}