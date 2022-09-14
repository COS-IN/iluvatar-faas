
use std::sync::Arc;
use std::sync::mpsc::{channel, Receiver};
use std::thread::JoinHandle;
use std::time::{SystemTime, Duration};
use iluvatar_library::cpu_interaction::CPUService;
use tracing::{info, debug, error, warn};
use parking_lot::Mutex;
use crate::services::containers::containermanager::ContainerManager;
use iluvatar_library::graphite::{GraphiteConfig, graphite_svc::GraphiteService};
use crate::services::invocation::invoker::InvokerService;
use iluvatar_library::transaction::TransactionId;
use iluvatar_library::utils::execute_cmd;
use super::WorkerStatus;
use anyhow::Result;

#[allow(unused)]
pub struct StatusService {
  container_manager: Arc<ContainerManager>, 
  invoker_service: Arc<InvokerService>,
  current_status: Mutex<Arc<WorkerStatus>>,
  worker_thread: JoinHandle<()>,
  graphite: GraphiteService,
  tags: String,
  metrics: Vec<&'static str>,
  cpu: Arc<CPUService>,
}

impl StatusService {
  pub async fn boxed(cm: Arc<ContainerManager>, invoke: Arc<InvokerService>, graphite_cfg: Arc<GraphiteConfig>, worker_name: String, tid: &TransactionId) -> Result<Arc<Self>> {
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
        num_running_funcs: 0,
        hardware_cpu_freqs: vec![],
        kernel_cpu_freqs: vec![],
      })),
      worker_thread: handle,
      graphite: GraphiteService::new(graphite_cfg),
      tags: format!("machine={};type=worker", worker_name),
      metrics: vec!["worker.load.loadavg", "worker.load.cpu", 
                    "worker.load.queue", "worker.load.mem_pct", 
                    "worker.load.used_mem"],
      cpu: CPUService::boxed(tid)?,
    });
    tx.send(ret.clone()).unwrap();
    Ok(ret)
  }
  
  fn launch_worker_thread(rx: Receiver<Arc<StatusService>>) -> JoinHandle<()> {
    // use an OS thread because these commands will block
    std::thread::spawn(move || {
      let tid: &TransactionId = &iluvatar_library::transaction::STATUS_WORKER_TID;
      let status_svc = match rx.recv() {
        Ok(svc) => svc,
        Err(_) => {
          error!(tid=%tid, "status service thread failed to receive service from channel!");
          return;
        },
      };
      debug!(tid=%tid, "status service worker started");
      iluvatar_library::continuation::GLOB_CONT_CHECK.thread_start(tid);
      while iluvatar_library::continuation::GLOB_CONT_CHECK.check_continue() {
        let start = SystemTime::now();
        status_svc.update_status(tid);
        let sleep_t = match start.elapsed() {
          Ok(d) => std::cmp::max(0, 5000 - d.as_millis() as u64),
          Err(e) => {
            warn!(tid=%tid, error=%e, "Failed to get elapsed time of status service computation");
            5000
          },
        };
        std::thread::sleep(Duration::from_millis(sleep_t));
      }
      iluvatar_library::continuation::GLOB_CONT_CHECK.thread_exit(tid);
    })
  }

  fn vmstat(&self, tid: &TransactionId) -> (i64, i64, i64, i64) {
    match execute_cmd("/usr/bin/vmstat", &vec!["1", "2", "-n", "-w"], None, tid) {
      Ok(out) => {
        let stdout = String::from_utf8_lossy(&out.stdout);
        let split = stdout.split("\n").collect::<Vec<&str>>();
        let lines: Vec<&str> = split[split.len()-2].split(" ").filter(|str| str.len() > 0).collect();
        let us: i64 = self.parse(lines[lines.len()-5], tid);
        let sy: i64 = self.parse(lines[lines.len()-4], tid);
        let id: i64 = self.parse(lines[lines.len()-3], tid);
        let wa: i64 = self.parse(lines[lines.len()-2], tid);
        debug!(tid=%tid, "vmstat {} {} {} {}", us, sy, id, wa);
        (us, sy, id, wa)
      },
      Err(e) => {
        error!(tid=%tid, "unable to call vmstat because {}", e);
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
            error!(tid=%tid, "error parsing u32 from nproc: '{}': {}", stdout, e);
            0
          },
        };
        debug!(tid=%tid, "nprocs result: {}", nprocs);
        nprocs
      },
      Err(e) => {
        error!(tid=%tid, "unable to call nproc because {}", e);
        0
      },
    }
  }

  fn uptime(&self, tid: &TransactionId) -> f64 {
    match execute_cmd("/usr/bin/uptime", &vec![], None, tid) {
      Ok(out) => {
        let stdout = String::from_utf8_lossy(&out.stdout);
        let lines: Vec<&str> = stdout.split(" ").filter(|str| str.len() > 0).collect();
        let min = lines[lines.len()-3];
        let min = &min[..min.len()-1];
        let minute_load_avg =  match min.parse::<f64>() {
            Ok(r) => r,
            Err(e) => {
              error!(tid=%tid, "error parsing float from uptime {}: {}", min, e);
              -1.0
            },
          };
        debug!(tid=%tid, "uptime {}", minute_load_avg);
        minute_load_avg
      },
      Err(e) => {
        error!(tid=%tid, "unable to call uptime because {}", e);
        -1.0
      },
    }
  }

  fn update_status(&self, tid: &TransactionId) {
    let _free_cs = self.container_manager.free_cores();

    let (us, sy, id, wa) = self.vmstat(tid);
    let minute_load_avg = self.uptime(tid);
    let nprocs = self.nproc(tid);
    let queue_len = self.invoker_service.queue_len() as i64;
    let used_mem = self.container_manager.used_memory();
    let total_mem = self.container_manager.total_memory();
    let running = self.container_manager.running_functions();
    let hw_freqs = self.cpu.hardware_cpu_freqs(tid);
    let kernel_freqs = self.cpu.kernel_cpu_freqs(tid);

    let new_status = Arc::new(WorkerStatus {
      queue_len,
      used_mem,
      total_mem,
      cpu_us: us,
      cpu_sy: sy,
      cpu_id: id,
      cpu_wa: wa,
      load_avg_1minute: minute_load_avg,
      num_system_cores: nprocs,
      num_running_funcs: running,
      hardware_cpu_freqs: hw_freqs,
      kernel_cpu_freqs: kernel_freqs
    });
    info!(tid=%tid, status=%new_status,"current load status");

    let values = vec![(minute_load_avg / nprocs as f64), (us+sy) as f64,
                                    queue_len as f64, (used_mem as f64 / total_mem as f64), 
                                    used_mem as f64];
    self.graphite.publish_metrics(&self.metrics, values, tid, self.tags.as_str());

    let mut current_status = self.current_status.lock();
    *current_status = new_status;
  }

  /// parse the string to an i64, used for getting info from vmstat
  fn parse(&self, string: &str, tid: &TransactionId) -> i64 {
    match string.parse::<i64>() {
      Ok(r) => r,
      Err(e) => {
        error!(tid=%tid, "error parsing {}: {}", string, e);
        -1
      },
    }
  }

  /// Returns the status and load of the worker
  pub fn get_status(&self, tid: &TransactionId) -> Arc<WorkerStatus> {
    debug!(tid=%tid, "getting current worker status");
    self.current_status.lock().clone()
  }
}
