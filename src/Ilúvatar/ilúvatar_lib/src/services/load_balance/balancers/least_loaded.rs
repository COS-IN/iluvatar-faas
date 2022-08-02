use std::{sync::{Arc, mpsc::{Receiver, channel}}, time::Duration, collections::HashMap};
use crate::{services::graphite::graphite_svc::GraphiteService, transaction::LEAST_LOADED_TID, bail_error, load_balancer_api::load_reporting::LoadService};
use crate::worker_api::worker_comm::WorkerAPIFactory;
use crate::{services::load_balance::LoadBalancerTrait, transaction::TransactionId};
use crate::load_balancer_api::structs::internal::{RegisteredFunction, RegisteredWorker};
use anyhow::Result;
use tokio::task::JoinHandle;
use tracing::{info, debug, error, warn};
use parking_lot::RwLock;
use crate::services::ControllerHealthService;

#[allow(unused)]
pub struct LeastLoadedBalancer {
  workers: RwLock<HashMap<String, Arc<RegisteredWorker>>>,
  worker_fact: WorkerAPIFactory,
  health: Arc<ControllerHealthService>,
  graphite: Arc<GraphiteService>,
  _worker_thread: JoinHandle<()>,
  assigned_worker: RwLock<Option<Arc<RegisteredWorker>>>,
  load: Arc<LoadService>,
}

impl LeastLoadedBalancer {
  fn new(health: Arc<ControllerHealthService>, graphite: Arc<GraphiteService>, worker_thread: JoinHandle<()>, load: Arc<LoadService>) -> Self {
    LeastLoadedBalancer {
      workers: RwLock::new(HashMap::new()),
      worker_fact: WorkerAPIFactory {},
      health,
      graphite,
      _worker_thread: worker_thread,
      assigned_worker: RwLock::new(None),
      load,
    }
  }

  pub fn boxed(health: Arc<ControllerHealthService>, graphite: Arc<GraphiteService>, load: Arc<LoadService>, tid: &TransactionId) -> Arc<Self> {
    let (tx, rx) = channel();
    let t = LeastLoadedBalancer::start_status_thread(rx, tid);
    let i = Arc::new(LeastLoadedBalancer::new(health, graphite, t, load));
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
