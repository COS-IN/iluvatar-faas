use std::{sync::Arc, collections::HashMap};
use iluvatar_library::{graphite::graphite_svc::GraphiteService, threading::tokio_thread};
use iluvatar_library::{transaction::TransactionId, transaction::LOAD_MONITOR_TID};
use iluvatar_worker_library::worker_api::worker_comm::WorkerAPIFactory;
use tokio::task::JoinHandle;
use tracing::{info, error, warn};
use parking_lot::RwLock;
use crate::controller::controller_config::LoadBalancingConfig;

#[allow(unused)]
pub struct LoadService {
  _worker_thread: JoinHandle<()>,
  graphite: Arc<GraphiteService>,
  workers: RwLock<HashMap<String, f64>>,
  config: Arc<LoadBalancingConfig>,
  fact: Arc<WorkerAPIFactory>,
}

impl LoadService {
  pub fn boxed(graphite: Arc<GraphiteService>, config: Arc<LoadBalancingConfig>, _tid: &TransactionId, fact: Arc<WorkerAPIFactory>) -> Arc<Self> {
    let (handle, tx) = tokio_thread(config.thread_sleep_ms, LOAD_MONITOR_TID.clone(), LoadService::monitor_worker_status);
    let ret = Arc::new(LoadService {
      _worker_thread: handle,
      graphite,
      workers: RwLock::new(HashMap::new()),
      config,
      fact,
    });
    tx.send(ret.clone()).unwrap();

    ret
  }

  async fn monitor_worker_status(service: Arc<Self>, tid: TransactionId) {
    if iluvatar_library::utils::is_simulation() {
      service.mointor_simulation(&tid).await;
    } else {
      service.monitor_live(&tid).await;
    }
  }

  #[tracing::instrument(skip(self), fields(tid=%tid))]
  async fn mointor_simulation(&self, tid: &TransactionId) {
    let mut update = HashMap::new();
    let workers = self.fact.get_cached_workers();
    for (name, mut worker) in workers {
      let status = match worker.status(tid.to_string()).await {
        Ok(s) => s,
        Err(e) => {
          warn!(error=%e, tid=%tid, "Unable to get status of simulation worker");
          continue;
        },
      };
      match self.config.load_metric.as_str() {
        "worker.load.loadavg" => update.insert(name, (status.queue_len as f64 + status.num_running_funcs as f64) / status.num_system_cores as f64),
        "worker.load.running" => update.insert(name, status.num_running_funcs as f64),
        "worker.load.cpu_pct" => update.insert(name, status.num_running_funcs as f64 / status.num_system_cores as f64),
        "worker.load.mem_pct" => update.insert(name, status.used_mem as f64 / status.total_mem as f64),
        "worker.load.queue" => update.insert(name, status.queue_len as f64),
        _ => { error!(tid=%tid, metric=%self.config.load_metric, "Unknown load metric"); return; }
      };
    }
    
    info!(tid=%tid, update=?update, "latest simulated worker update");
    *self.workers.write() = update;
  }

  #[tracing::instrument(skip(self), fields(tid=%tid))]
  async fn monitor_live(&self, tid: &TransactionId) {
    let update = self.get_live_update(tid).await;
    let mut data = self.workers.read().clone();
    for (k, v) in update.iter() {
      data.insert(k.clone(), *v);
    }
    *self.workers.write() = data;

    info!(tid=%tid, update=?update, "latest worker update");
  }

  async fn get_live_update(&self, tid: &TransactionId) -> HashMap<String, f64> {
    self.graphite.get_latest_metric(&self.config.load_metric.as_str(), "machine", tid).await
  }

  pub fn get_worker(&self, name: &String) -> Option<f64> {
    match self.workers.read().get(name) {
        Some(f) => Some(*f),
        None => None,
    }
  }
}
