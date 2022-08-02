use std::{sync::{Arc, mpsc::{Receiver, channel}}, time::Duration, collections::HashMap};
use crate::services::graphite::graphite_svc::GraphiteService;
use crate::{transaction::TransactionId, transaction::LOAD_MONITOR_TID};
use tokio::task::JoinHandle;
use tracing::{info, debug, error};
use parking_lot::RwLock;

#[allow(unused)]
pub struct LoadService {
  _worker_thread: JoinHandle<()>,
  graphite: Arc<GraphiteService>,
  workers: RwLock<HashMap<String, f64>>,
}

impl LoadService {
  pub fn boxed(graphite: Arc<GraphiteService>, tid: &TransactionId) -> Arc<Self> {
    let (tx, rx) = channel();
    let t = LoadService::start_thread(rx, tid);
    let ret = Arc::new(LoadService {
      _worker_thread: t,
      graphite,
      workers: RwLock::new(HashMap::new()),
    });
    tx.send(ret.clone()).unwrap();

    ret
  }

  fn start_thread(rx: Receiver<Arc<LoadService>>, tid: &TransactionId) -> JoinHandle<()> {
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
        svc.monitor_worker_status(tid).await;
      }
    )
  }

  async fn monitor_worker_status(&self, tid: &TransactionId) {
    loop {
      let update = self.get_status_update(tid).await;
      let mut data = self.workers.read().clone();
      for (k, v) in update.iter() {
        data.insert(k.clone(), *v);
      }
      *self.workers.write() = data;

      info!(tid=%tid, update=?update, "latest worker update");
      std::thread::sleep(Duration::from_secs(1));
    }
  }

  async fn get_status_update(&self, tid: &TransactionId) -> HashMap<String, f64> {
    self.graphite.get_latest_metric("worker.load.loadavg", "machine", tid).await
  }

  pub fn get_worker(&self, name: &String) -> Option<f64> {
    match self.workers.read().get(name) {
        Some(f) => Some(*f),
        None => None,
    }
  }
}
