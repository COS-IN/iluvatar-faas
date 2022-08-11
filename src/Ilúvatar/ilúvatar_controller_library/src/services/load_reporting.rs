use std::{sync::{Arc, mpsc::{Receiver, channel}}, time::Duration, collections::HashMap};
use iluvatar_library::graphite::graphite_svc::GraphiteService;
use iluvatar_library::{transaction::TransactionId, transaction::LOAD_MONITOR_TID};
use tokio::task::JoinHandle;
use tracing::{info, debug, error, warn};
use parking_lot::{RwLock, Mutex};
use crate::{services::worker_comm::WorkerAPIFactory, controller::controller_config::LoadBalancingConfig};

#[allow(unused)]
pub struct LoadService {
  _worker_thread: JoinHandle<()>,
  graphite: Arc<GraphiteService>,
  workers: RwLock<HashMap<String, f64>>,
  config: Arc<LoadBalancingConfig>,
  fact: Arc<WorkerAPIFactory>,
  exiting: Mutex<bool>
}

impl LoadService {
  pub fn boxed(graphite: Arc<GraphiteService>, config: Arc<LoadBalancingConfig>, tid: &TransactionId, fact: Arc<WorkerAPIFactory>, simulation: bool) -> Arc<Self> {
    let (tx, rx) = channel();
    let t = LoadService::start_thread(rx, tid, simulation);
    let ret = Arc::new(LoadService {
      _worker_thread: t,
      graphite,
      workers: RwLock::new(HashMap::new()),
      config,
      fact, 
      exiting: Mutex::new(false)
    });
    tx.send(ret.clone()).unwrap();

    ret
  }

  pub fn kill_thread(&self) {
    *self.exiting.lock() = true;
  }

  fn start_thread(rx: Receiver<Arc<LoadService>>, tid: &TransactionId, simulation: bool) -> JoinHandle<()> {
    debug!(tid=%tid, "Launching LoadService thread");
    tokio::spawn(async move {
      let tid: &TransactionId = &LOAD_MONITOR_TID;
      let svc: Arc<LoadService> = match rx.recv() {
          Ok(svc) => svc,
          Err(_) => {
            error!(tid=%tid, "LoadService thread failed to receive service from channel!");
            return;
          },
        };

        debug!(tid=%tid, "LoadService worker started");
        svc.monitor_worker_status(tid, simulation).await;
      }
    )
  }

  async fn monitor_worker_status(&self, tid: &TransactionId, simulation: bool) {
    if simulation {
      self.mointor_simulation(tid).await;
    } else {
      self.monitor_live(tid).await;
    }
  }

  async fn mointor_simulation(&self, tid: &TransactionId) {
    loop {
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
          // TODO: this load won't be right (in simulation!!)
          "worker.load.loadavg" => update.insert(name, status.load_avg_1minute),
          _ => todo!()
        };
      }
      
      info!(tid=%tid, update=?update, "latest simulated worker update");
      *self.workers.write() = update;

      std::thread::sleep(Duration::from_secs(1));
      if *self.exiting.lock() {
        return;
      }
    }
  }

  async fn monitor_live(&self, tid: &TransactionId) {
    loop {
      let update = self.get_live_update(tid).await;
      let mut data = self.workers.read().clone();
      for (k, v) in update.iter() {
        data.insert(k.clone(), *v);
      }
      *self.workers.write() = data;

      info!(tid=%tid, update=?update, "latest worker update");
      std::thread::sleep(Duration::from_secs(1));
      if *self.exiting.lock() {
        return;
      }
    }
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
