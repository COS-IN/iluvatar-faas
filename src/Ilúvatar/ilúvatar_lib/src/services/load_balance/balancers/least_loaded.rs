use std::{sync::{Arc, mpsc::{Receiver, channel}}, time::Duration, collections::HashMap};
use crate::{services::graphite::graphite_svc::GraphiteService, transaction::LEAST_LOADED_TID, bail_error, load_balancer_api::load_reporting::LoadService, send_invocation, prewarm, send_async_invocation};
use crate::worker_api::worker_comm::WorkerAPIFactory;
use crate::{services::load_balance::LoadBalancerTrait, transaction::TransactionId};
use crate::load_balancer_api::structs::internal::{RegisteredFunction, RegisteredWorker};
use anyhow::Result;
use tokio::task::JoinHandle;
use tracing::{info, debug, error, warn};
use parking_lot::RwLock;
use crate::services::ControllerHealthService;
use crate::utils::timing::TimedExt;
use crate::rpc::InvokeResponse;

#[allow(unused)]
pub struct LeastLoadedBalancer {
  workers: RwLock<HashMap<String, Arc<RegisteredWorker>>>,
  worker_fact: Arc<WorkerAPIFactory>,
  health: Arc<ControllerHealthService>,
  graphite: Arc<GraphiteService>,
  _worker_thread: JoinHandle<()>,
  assigned_worker: RwLock<Option<Arc<RegisteredWorker>>>,
  load: Arc<LoadService>,
}

impl LeastLoadedBalancer {
  fn new(health: Arc<ControllerHealthService>, graphite: Arc<GraphiteService>, worker_thread: JoinHandle<()>, load: Arc<LoadService>, worker_fact: Arc<WorkerAPIFactory>) -> Self {
    LeastLoadedBalancer {
      workers: RwLock::new(HashMap::new()),
      worker_fact,
      health,
      graphite,
      _worker_thread: worker_thread,
      assigned_worker: RwLock::new(None),
      load,
    }
  }

  pub fn boxed(health: Arc<ControllerHealthService>, graphite: Arc<GraphiteService>, load: Arc<LoadService>, worker_fact: Arc<WorkerAPIFactory>, tid: &TransactionId) -> Arc<Self> {
    let (tx, rx) = channel();
    let t = LeastLoadedBalancer::start_status_thread(rx, tid);
    let i = Arc::new(LeastLoadedBalancer::new(health, graphite, t, load, worker_fact));
    tx.send(i.clone()).unwrap();
    i
  }

  fn start_status_thread(rx: Receiver<Arc<LeastLoadedBalancer>>, tid: &TransactionId) -> JoinHandle<()> {
    debug!(tid=%tid, "Launching LeastLoadedBalancer thread");
    tokio::spawn(async move {
      let tid: &TransactionId = &LEAST_LOADED_TID;
      let svc: Arc<LeastLoadedBalancer> = match rx.recv() {
          Ok(svc) => svc,
          Err(_) => {
            error!(tid=%tid, "least loaded balancer thread failed to receive service from channel!");
            return;
          },
        };
        debug!(tid=%tid, "least loaded worker started");
        loop {
          let worker = svc.find_least_loaded(); 
          match svc.workers.read().get(&worker) {
            Some(w) => {
              *svc.assigned_worker.write() = Some(w.clone());
            },
            None => warn!(tid=%tid, worker=%worker, "Cannot update least loaded worker because it is not registered"),
          };
          info!(tid=%tid, worker=%worker, "new least loaded worker");
          tokio::time::sleep(Duration::from_secs(1)).await;
        }
      }
    )
  }

  fn find_least_loaded(&self) -> String {
    let mut least_ld = f64::MAX;
    let mut worker = &"".to_string();
    let workers = self.workers.read();
    for worker_name in workers.keys() {
      if let Some(worker_load) = self.load.get_worker(worker_name) {
        if worker_load < least_ld {
          worker = worker_name;
          least_ld = worker_load;
        }
      }
    }
    worker.clone()
  }

  fn get_worker(&self, tid: &TransactionId) -> Result<Arc<RegisteredWorker>> {
    let lck = self.assigned_worker.read();
    match &*lck {
      Some(w) => Ok(w.clone()),
      None => bail_error!(tid=%tid, "No worker has been assigned to least loaded variable yet"),
    }
  }
}

#[tonic::async_trait]
impl LoadBalancerTrait for LeastLoadedBalancer {
  fn add_worker(&self, worker: Arc<RegisteredWorker>, tid: &TransactionId) {
    info!(tid=%tid, worker=%worker.name, "Registering new worker in LeastLoaded load balancer");
    let mut workers = self.workers.write();
    workers.insert(worker.name.clone(), worker);
  }

  async fn send_invocation(&self, func: Arc<RegisteredFunction>, json_args: String, tid: &TransactionId) -> Result<(InvokeResponse, Duration)> {
    let worker = self.get_worker(tid)?;
    send_invocation!(func, json_args, tid, self.worker_fact, self.health, worker)
  }

  async fn prewarm(&self, func: Arc<RegisteredFunction>, tid: &TransactionId) -> Result<Duration> {
    let worker = self.get_worker(tid)?;
    prewarm!(func, tid, self.worker_fact, self.health, worker)
  }

  async fn send_async_invocation(&self, func: Arc<RegisteredFunction>, json_args: String, tid: &TransactionId) -> Result<(String, Arc<RegisteredWorker>, Duration)> {
    let worker = self.get_worker(tid)?;
    send_async_invocation!(func, json_args, tid, self.worker_fact, self.health, worker)
  }
}
