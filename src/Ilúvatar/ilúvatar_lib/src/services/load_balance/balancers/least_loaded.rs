use std::{sync::{Arc, mpsc::{Receiver, channel}}, time::Duration, collections::HashMap};
use crate::{services::graphite::graphite_svc::GraphiteService, transaction::LEAST_LOADED_TID, bail_error};
use crate::worker_api::worker_comm::WorkerAPIFactory;
use crate::{services::load_balance::LoadBalancerTrait, transaction::TransactionId};
use crate::load_balancer_api::structs::internal::{RegisteredFunction, RegisteredWorker};
use anyhow::Result;
use tracing::{info, debug, error, warn};
use parking_lot::RwLock;
use crate::services::ControllerHealthService;

#[allow(unused)]
pub struct LeastLoadedBalancer {
  workers: RwLock<HashMap<String, Arc<RegisteredWorker>>>,
  worker_fact: WorkerAPIFactory,
  health: Arc<ControllerHealthService>,
  graphite: Arc<GraphiteService>,
  _worker_thread: std::thread::JoinHandle<()>,
  assigned_worker: RwLock<Option<Arc<RegisteredWorker>>>,
}

impl LeastLoadedBalancer{
  fn new(health: Arc<ControllerHealthService>, graphite: Arc<GraphiteService>, worker_thread: std::thread::JoinHandle<()>) -> Self {
    LeastLoadedBalancer {
      workers: RwLock::new(HashMap::new()),
      worker_fact: WorkerAPIFactory {},
      health,
      graphite,
      _worker_thread: worker_thread,
      assigned_worker: RwLock::new(None)
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
    debug!(tid=%tid, "Launching LeastLoadedBalancer thread");
    // run on an OS thread here so we don't get blocked by our userland threading runtime
    std::thread::spawn(move || {
      let tid: &TransactionId = &LEAST_LOADED_TID;
      let svc: Arc<LeastLoadedBalancer> = match rx.recv() {
          Ok(svc) => svc,
          Err(_) => {
            error!(tid=%tid, "least loaded balancer thread failed to receive service from channel!");
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

        debug!(tid=%tid, "least loaded worker started");
        worker_rt.block_on(svc.monitor_worker_status(tid));
      }
    )
  }

  async fn monitor_worker_status(&self, tid: &TransactionId) {
    loop {
      let mut least_ld = f64::MAX;
      let mut worker = &"".to_string();
      let update = self.get_status_update(tid).await;
      for (k, v) in update.iter() {
        if v < &least_ld {
          worker = k;
          least_ld = *v;
        }
      }

      match self.workers.read().get(worker) {
        Some(w) => {
          *self.assigned_worker.write() = Some(w.clone());
        },
        None => warn!(tid=%tid, worker=%worker, "Cannot update least loaded worker because it is not registered"),
      }

      info!(tid=%tid, update=?update, "latest worker update");
      std::thread::sleep(Duration::from_secs(1));
    }
  }

  async fn get_status_update(&self, tid: &TransactionId) -> HashMap<String, f64> {
    self.graphite.get_latest_metric("worker.load.loadavg", "machine", tid).await
  }

  fn get_worker(&self, tid: &TransactionId) -> Result<Arc<RegisteredWorker>> {
    let lck = self.assigned_worker.read();
    match &*lck {
      Some(w) => Ok(w.clone()),
      None => {
        bail_error!("[{}] No worker has been assigned to least loaded variable yet", tid);
      },
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

  async fn send_invocation(&self, func: Arc<RegisteredFunction>, json_args: String, tid: &TransactionId) -> Result<String> {
    let worker = self.get_worker(tid)?;
    info!("[{}] invoking function {} on worker {}", tid, &func.fqdn, &worker.name);

    let mut api = self.worker_fact.get_worker_api(&worker, tid).await?;
    let result = match api.invoke(func.function_name.clone(), func.function_version.clone(), json_args, None, tid.clone()).await {
      Ok(r) => r,
      Err(e) => {
        self.health.schedule_health_check(self.health.clone(), worker, tid, Some(Duration::from_secs(1)));
        anyhow::bail!(e)
      },
    };
    debug!("[{}] invocation result: {}", tid, result.json_result);
    Ok(result.json_result)
  }

  async fn prewarm(&self, func: Arc<RegisteredFunction>, tid: &TransactionId) -> Result<()> {
    let worker = self.get_worker(tid)?;
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
    let worker = self.get_worker(tid)?;
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
