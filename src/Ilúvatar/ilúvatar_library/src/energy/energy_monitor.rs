use std::{time::SystemTime, sync::{Arc, mpsc::{Receiver, channel}}, collections::HashMap, thread::JoinHandle};
use crate::{graphite::{graphite_svc::GraphiteService, GraphiteConfig}, transaction::TransactionId};

use super::energy_layer::DataExtractorVisitor;
use dashmap::DashMap;
use parking_lot::RwLock;
use tracing::{error, debug};

const WORKER_API_TARGET: &str = "iluvatar_worker_library::worker_api::ilúvatar_worker";
const INVOKE_TARGET: &str = "iluvatar_worker_library::services::containers::containerd::containerdstructs";

pub type InvocationData = HashMap<String, (String, u128)>;

pub struct EnergyMonitor {
  invocation_spans: DashMap<u64, DataExtractorVisitor>,
  invocation_durations: Arc<RwLock<Option<InvocationData>>>,
  worker_spans: DashMap<u64, DataExtractorVisitor>,
  timing_data: DashMap<String, u128>,
  overhead_ns: Arc<RwLock<u128>>,
  graphite: Arc<GraphiteService>,
  _worker_thread: JoinHandle<()>,
}

impl EnergyMonitor {
  pub fn boxed(graphite_cfg: Arc<GraphiteConfig>) -> Arc<Self> {
    let (tx, rx) = channel();
    let handle = EnergyMonitor::launch_worker_thread(rx);

    let ret = Arc::new(EnergyMonitor {
      invocation_spans: DashMap::new(),
      invocation_durations: Arc::new(RwLock::new(Some(HashMap::new()))),
      worker_spans: DashMap::new(),
      timing_data: DashMap::new(),
      overhead_ns: Arc::new(RwLock::new(0)),
      graphite: GraphiteService::boxed(graphite_cfg),
      _worker_thread: handle,
    });
    tx.send(ret.clone()).unwrap();
    ret
  }

  fn launch_worker_thread(rx: Receiver<Arc<EnergyMonitor>>) -> JoinHandle<()> {
    std::thread::spawn(move || {
      let tid: &TransactionId = &crate::transaction::ENERGY_MONITOR_TID;
      let svc = match rx.recv() {
        Ok(svc) => svc,
        Err(_) => {
          error!(tid=%tid, "energy monitor thread failed to receive service from channel!");
          return;
        },
      };
      debug!(tid=%tid, "energy monitor worker started");
      loop {
        svc.monitor_energy(tid);
        std::thread::sleep(std::time::Duration::from_secs(20));
      }
    })
  }

  fn monitor_energy(&self, tid: &TransactionId) {
    self.graphite.publish_metrics(&vec![], vec![], tid, "".to_string());
  }

  fn get_reset_overhead(&self) -> u128 {
    let mut overhead_lock = self.overhead_ns.write();
    let ret = *overhead_lock;
    *overhead_lock = 0;
    ret
  }

  fn get_reset_invocations(&self) -> InvocationData {
    let mut lock = self.invocation_durations.write();
    let ret = lock.take();
    *lock = Some(HashMap::new());
    ret.unwrap()
  }

  pub fn get_data(&self) -> (InvocationData, u128) {
    let overhead = self.get_reset_overhead();
    let data = self.get_reset_invocations();
    (data, overhead)
  }

  pub fn span_create(&self, span_id: u64, data: DataExtractorVisitor, name: &str, target: &str) {
      // TODO: account for background work done here
      match target {
      INVOKE_TARGET => match name {
        "ContainerdContainer::invoke" => {self.invocation_spans.insert(span_id, data);},
        _ => (),
      },
      WORKER_API_TARGET => match name {
        "register" => {self.worker_spans.insert(span_id, data);},
        "invoke" => {self.worker_spans.insert(span_id, data);},
        _ => (),
      }
      _ => (),
    }
  }

  pub fn span_close(&self, span_id: u64, name: &str, target: &str) {
    // TODO: account for background work done here
    match target {
      INVOKE_TARGET => match name {
        "ContainerdContainer::invoke" => {self.remove_invoke_transaction(span_id);},
        _ => (),
      },
      WORKER_API_TARGET => match name {
        "register" => self.remove_worker_transaction(span_id),
        "invoke" => self.remove_worker_transaction(span_id),
        _ => (),
      }
      _ => (),
    }
  }

  fn remove_invoke_transaction(&self, id: u64) {
    let found_stamp = self.invocation_spans.remove(&id);
    match found_stamp {
      Some( (_id, s) ) => {
        let time_ns = match SystemTime::now().duration_since(s.timestamp) {
          Ok(t) => t.as_nanos(),
          Err(_e) => {return;},
        };
        match s.fqdn() {
          Some(f) => {
            println!("function {f:?} completed span in {time_ns} ns");
            self.invocation_durations.write().as_mut().unwrap().insert(s.transaction_id.unwrap().clone(), (f, time_ns) );
          },
          None => panic!("Completed invocation span didn't have a valid FQDN: {:?}", s),
        }
      },
      None => panic!("Tried to remove a span {} that wasn't found", id),
    }
  }
  fn remove_worker_transaction(&self, id: u64) {
    let found_stamp = self.worker_spans.remove(&id);
    match found_stamp {
      Some( (_id, s)) => {
        let time_ns = match SystemTime::now().duration_since(s.timestamp) {
          Ok(t) => t.as_nanos(),
          Err(_e) => {return;},
        };
        let overhead = match self.invocation_durations.read().as_ref().unwrap().get(&s.transaction_id.unwrap()) {
          Some( reffed ) => {
            let fqdn = &reffed.0;
            let invoke_ns = reffed.1;
            let overhead = time_ns - invoke_ns;
            match self.timing_data.get_mut(fqdn) {
              Some(mut v) => *v += invoke_ns,
              None => {self.timing_data.insert(fqdn.clone(), invoke_ns);},
            };
            overhead
          },
          None => time_ns,        
        };
        *self.overhead_ns.write() += overhead;
      },
      None => panic!("Tried to remove a span {} that wasn't found", id),
    }
  }
}
