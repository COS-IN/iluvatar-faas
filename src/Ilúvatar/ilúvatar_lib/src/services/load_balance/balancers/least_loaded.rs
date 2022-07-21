use std::{sync::{Arc, mpsc::{Receiver, channel}}, time::Duration, collections::HashMap};
use crate::{services::graphite::graphite_svc::GraphiteService, transaction::LEAST_LOADED_TID};
use crate::worker_api::worker_comm::WorkerAPIFactory;
use crate::{services::load_balance::LoadBalancerTrait, transaction::TransactionId};
use crate::load_balancer_api::structs::internal::{RegisteredFunction, RegisteredWorker};
use anyhow::Result;
use tracing::{info, debug, error};
use parking_lot::RwLock;
use crate::services::ControllerHealthService;

#[allow(unused)]
pub struct LeastLoadedBalancer {
  workers: RwLock<Vec<Arc<RegisteredWorker>>>,
  worker_fact: WorkerAPIFactory,
  health: Arc<ControllerHealthService>,
  graphite: Arc<GraphiteService>,
  _worker_thread: std::thread::JoinHandle<()>,
}

impl LeastLoadedBalancer{
  fn new(health: Arc<ControllerHealthService>, graphite: Arc<GraphiteService>, worker_thread: std::thread::JoinHandle<()>) -> Self {
    LeastLoadedBalancer {
      workers: RwLock::new(Vec::new()),
      worker_fact: WorkerAPIFactory {},
      health,
      graphite,
      _worker_thread: worker_thread
    }
  }

  pub fn boxed(health: Arc<ControllerHealthService>, graphite: Arc<GraphiteService>, tid: &TransactionId) -> Arc<Self> {
    let (tx, rx) = channel();
    let t = LeastLoadedBalancer::start_status_thread(rx, tid);
    let i = Arc::new(LeastLoadedBalancer::new(health, graphite, t));
    tx.send(i.clone()).unwrap();
    i
  }

  fn start_status_thread(rx: Receiver<Arc<LeastLoadedBalancer>>, tid: &TransactionId) -> std::thread::JoinHandle<()> {
    debug!("[{}] Launching LeastLoadedBalancer thread", tid);
    // run on an OS thread here so we don't get blocked by our userland threading runtime
    std::thread::spawn(move || {
      let tid: &TransactionId = &LEAST_LOADED_TID;
      let svc: Arc<LeastLoadedBalancer> = match rx.recv() {
          Ok(svc) => svc,
          Err(_) => {
            error!("[{}] least loaded service thread failed to receive service from channel!", tid);
            return;
          },
        };
        let worker_rt = match tokio::runtime::Runtime::new() {
          Ok(rt) => rt,
          Err(e) => { 
            error!("[{}] least loaded worker tokio thread runtime failed to start {}", tid, e);
            return ();
          },
        };

        debug!("[{}] least loaded worker started", tid);
        worker_rt.block_on(svc.monitor_worker_status(tid));
      }
    )
  }

  async fn monitor_worker_status(&self, tid: &TransactionId) {
    loop {
      let update = self.get_status_update(tid).await;
      info!("[{}] latest worker update: {:?}", tid, update);
      std::thread::sleep(Duration::from_secs(5));
    }
  }

  async fn get_status_update(&self, tid: &TransactionId) -> HashMap<String, f64> {
    self.graphite.get_latest_metric("worker.load.loadavg", "machine", tid).await
  }
}

#[tonic::async_trait]
impl LoadBalancerTrait for LeastLoadedBalancer {
  fn add_worker(&self, worker: Arc<RegisteredWorker>, tid: &TransactionId) {
    info!("[{}] Registering new worker {} in RoundRobin load balancer", tid, &worker.name);
    let mut workers = self.workers.write();
    workers.push(worker);
  }

  async fn send_invocation(&self, _func: Arc<RegisteredFunction>, _json_args: String, _tid: &TransactionId) -> Result<String> {
    todo!()
  }

  async fn prewarm(&self, _func: Arc<RegisteredFunction>, _tid: &TransactionId) -> Result<()> {
    todo!()
  }

  async fn send_async_invocation(&self, _func: Arc<RegisteredFunction>, _json_args: String, _tid: &TransactionId) -> Result<(String, Arc<RegisteredWorker>)> {
    todo!()
  }
}
