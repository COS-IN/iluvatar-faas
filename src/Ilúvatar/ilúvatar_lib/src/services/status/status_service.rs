
use std::sync::Arc;
use std::sync::mpsc::{channel, Receiver};
use std::thread::JoinHandle;
use log::*;
use parking_lot::Mutex;
use crate::services::containers::containermanager::ContainerManager;
use crate::services::graphite::GraphiteConfig;
use crate::services::graphite::graphite_svc::GraphiteService;
use crate::services::invocation::invoker::InvokerService;
use crate::transaction::TransactionId;
use crate::utils::execute_cmd;
use super::WorkerStatus;

#[derive(Debug)]
#[allow(unused)]
pub struct StatusService {
  container_manager: Arc<ContainerManager>, 
  invoker_service: Arc<InvokerService>,
  current_status: Mutex<Arc<WorkerStatus>>,
  worker_thread: JoinHandle<()>,
  graphite: GraphiteService,
  worker_name: String,
}

impl StatusService {
  pub async fn boxed(cm: Arc<ContainerManager>, invoke: Arc<InvokerService>, graphite_cfg: Arc<GraphiteConfig>, worker_name: String) -> Arc<Self> {
    let (tx, rx) = channel();
    let handle = StatusService::launch_worker_thread(rx);

    let ret = Arc::new(StatusService { 
      container_manager: cm, 
      invoker_service: invoke,
      current_status: Mutex::new(Arc::new(WorkerStatus {
        queue_len: 0,
        used_mem: 0,
        total_mem: 0,
        cpu_us: 0,
        cpu_sy: 0,
        cpu_id: 0,
        cpu_wa: 0,
        load_avg_1minute: 0.0,
        num_system_cores: 0,
      })),
      worker_thread: handle,
      graphite: GraphiteService::new(graphite_cfg),
      worker_name,
    });
    tx.send(ret.clone()).unwrap();
    ret
  }
  
  fn launch_worker_thread(rx: Receiver<Arc<StatusService>>) -> JoinHandle<()> {
    // use an OS thread because these commands will block
    std::thread::spawn(move || {
      let tid: &TransactionId = &crate::transaction::STATUS_WORKER_TID;
      let status_svc = match rx.recv() {
        Ok(svc) => svc,
        Err(_) => {
          error!("[{}] status service thread failed to receive service from channel!", tid);
          return;
        },
      };
      debug!("[{}] status service worker started", tid);
      loop {
        status_svc.update_status(tid);
        std::thread::sleep(std::time::Duration::from_secs(5));
      }
    })
  }

  fn vmstat(&self, tid: &TransactionId) -> (i64, i64, i64, i64) {
    match execute_cmd("/usr/bin/vmstat", &vec!["1", "2", "-n", "-w"], None, tid) {
      Ok(out) => {
        let stdout = String::from_utf8_lossy(&out.stdout);
        let lines: Vec<&str> = stdout.split("\n").collect::<Vec<&str>>()[2].split(" ").filter(|str| str.len() > 0).collect();
        let us: i64 = self.parse(lines[lines.len()-5], tid);
        let sy: i64 = self.parse(lines[lines.len()-4], tid);
        let id: i64 = self.parse(lines[lines.len()-3], tid);
        let wa: i64 = self.parse(lines[lines.len()-2], tid);
        debug!("[{}] vmstat {} {} {} {}", tid, us, sy, id, wa);
        (us, sy, id, wa)
      },
      Err(e) => {
        error!("[{}] unable to call vmstat because {}", tid, e);
        (-1,-1,-1,-1)
      },
    }
  }

  fn nproc(&self, tid: &TransactionId) -> u32 {
    match execute_cmd("/usr/bin/nproc", &vec!["--all"], None, tid) {
      Ok(out) => {
        let stdout = String::from_utf8_lossy(&out.stdout).replace("\n", "");
        let nprocs = match stdout.parse::<u32>() {
          Ok(r) => r,
          Err(e) => {
            error!("[{}] error parsing u32 from nproc: '{}': {}", tid, stdout, e);
            0
          },
        };
        debug!("[{}] nprocs result: {}", tid, nprocs);
        nprocs
      },
      Err(e) => {
        error!("[{}] unable to call nproc because {}", tid, e);
        0
      },
    }
  }

  fn uptime(&self, tid: &TransactionId) -> f64 {
    match execute_cmd("/usr/bin/uptime", &vec![], None, tid) {
      Ok(out) => {
        let stdout = String::from_utf8_lossy(&out.stdout);
        let lines: Vec<&str> = stdout.split(" ").collect::<Vec<&str>>();
        let min = lines[13];
        let min = &min[..min.len()-1];
        let minute_load_avg =  match min.parse::<f64>() {
            Ok(r) => r,
            Err(e) => {
              error!("[{}] error parsing float from uptime {}: {}", tid, min, e);
              -1.0
            },
          };
        debug!("[{}] uptime {}", tid, minute_load_avg);
        minute_load_avg
      },
      Err(e) => {
        error!("[{}] unable to call uptime because {}", tid, e);
        -1.0
      },
    }
  }

  fn update_status(&self, tid: &TransactionId) {
    let _free_cs = self.container_manager.free_cores();

    let (us, sy, id, wa) = self.vmstat(tid);
    let minute_load_avg = self.uptime(tid);
    let nprocs = self.nproc(tid);

    let new_status = Arc::new(WorkerStatus {
      queue_len: self.invoker_service.queue_len() as i64,
      used_mem: self.container_manager.used_memory(),
      total_mem: self.container_manager.total_memory(),
      cpu_us: us,
      cpu_sy: sy,
      cpu_id: id,
      cpu_wa: wa,
      load_avg_1minute: minute_load_avg,
      num_system_cores: nprocs
    });
    info!("[{}] current load status: {:?}", tid, new_status);

    self.graphite.publish_metric("worker.load.loadavg", minute_load_avg.to_string(), tid, format!("machine={};type=worker", self.worker_name));

    let mut current_status = self.current_status.lock();
    *current_status = new_status;
  }

  /// parse the string to an i64, used for getting info from vmstat
  fn parse(&self, string: &str, tid: &TransactionId) -> i64 {
    match string.parse::<i64>() {
      Ok(r) => r,
      Err(e) => {
        error!("[{}] error parsing {}: {}", tid, string, e);
        -1
      },
    }
  }

  /// Returns the status and load of the worker
  pub fn get_status(&self, tid: &TransactionId) -> Arc<WorkerStatus> {
    debug!("[{}] getting current worker status", tid);
    self.current_status.lock().clone()
  }
}
