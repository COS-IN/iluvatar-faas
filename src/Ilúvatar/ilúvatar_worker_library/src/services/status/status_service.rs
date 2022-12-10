
use std::sync::Arc;
use std::thread::JoinHandle;
use iluvatar_library::cpu_interaction::CPUService;
use iluvatar_library::{nproc, threading};
use tracing::{info, debug, error, warn};
use parking_lot::Mutex;
use crate::services::containers::containermanager::ContainerManager;
use crate::services::invocation::invoker_trait::Invoker;
use crate::worker_api::worker_config::StatusConfig;
use iluvatar_library::graphite::{GraphiteConfig, graphite_svc::GraphiteService};
use iluvatar_library::transaction::TransactionId;
use iluvatar_library::utils::execute_cmd;
use super::WorkerStatus;
use anyhow::Result;

#[allow(unused)]
pub struct StatusService {
  container_manager: Arc<ContainerManager>, 
  invoker_service: Arc<dyn Invoker>,
  current_status: Mutex<Arc<WorkerStatus>>,
  worker_thread: JoinHandle<()>,
  graphite: GraphiteService,
  tags: String,
  metrics: Vec<&'static str>,
  cpu: Arc<CPUService>,
  config: Arc<StatusConfig>,
}

impl StatusService {
  pub async fn boxed(cm: Arc<ContainerManager>, invoke: Arc<dyn Invoker>, graphite_cfg: Arc<GraphiteConfig>, worker_name: String, tid: &TransactionId, config: Arc<StatusConfig>) -> Result<Arc<Self>> {
    let (handle, sender) = 
          threading::os_thread::<Self>(config.report_freq_ms, iluvatar_library::transaction::STATUS_WORKER_TID.clone(), Arc::new(StatusService::update_status));

    let ret = Arc::new(StatusService { 
      container_manager: cm, 
      invoker_service: invoke,
      current_status: Mutex::new(Arc::new(WorkerStatus {
        queue_len: 0,
        used_mem: 0,
        total_mem: 0,
        cpu_us: 0.0,
        cpu_sy: 0.0,
        cpu_id: 0.0,
        cpu_wa: 0.0,
        load_avg_1minute: 0.0,
        num_system_cores: 0,
        num_running_funcs: 0,
        hardware_cpu_freqs: vec![],
        kernel_cpu_freqs: vec![],
        num_containers: 0
      })),
      worker_thread: handle,
      graphite: GraphiteService::new(graphite_cfg),
      tags: format!("machine={};type=worker", worker_name),
      metrics: vec!["worker.load.loadavg", "worker.load.cpu", 
                    "worker.load.queue", "worker.load.mem_pct", 
                    "worker.load.used_mem"],
      cpu: CPUService::boxed(tid)?,
      config
    });
    sender.send(ret.clone()).unwrap();
    Ok(ret)
  }

  /// returns system CPU usage as reported by mpstat
  /// numbers are (user, system, idle, wait)
  fn mpstat(&self, tid: &TransactionId) -> (f64,f64,f64,f64) {
    match execute_cmd("/usr/bin/mpstat", &vec!["1", "1"], None, tid) {
      Ok(out) => {
        let stdout = String::from_utf8_lossy(&out.stdout);
        // Output will look like this:
        // Linux 5.4.0-132-generic (v-021) 	12/02/2022 	_x86_64_	(96 CPU)

        // 02:15:49 PM  CPU    %usr   %nice    %sys %iowait    %irq   %soft  %steal  %guest  %gnice   %idle
        // 02:15:50 PM  all    1.98    0.00    2.16    0.66    0.00    0.30    0.00    0.00    0.00   94.90
        // Average:     all    1.98    0.00    2.16    0.66    0.00    0.30    0.00    0.00    0.00   94.90

        let lines = stdout.split("\n").filter(|str| str.len() > 0).collect::<Vec<&str>>();
        debug!(tid=%tid, output=?lines, "mpstat output");
        if lines.len() != 4 {
          warn!(tid=%tid, "Had an unexpected number of output lines from mpstat '{}'", lines.len());
          return (-1.0,-1.0,-1.0,-1.0);
        }
        let name_parts: Vec<&str> = lines[1].split(" ").filter(|str| str.len() > 0).collect();
        let data_parts: Vec<&str> = lines.last().unwrap_or_else(||&"").split(" ").filter(|str| str.len() > 0).collect();
        debug!(tid=%tid, name_parts=?name_parts, data_parts=?data_parts, "mpstat parts");
        if data_parts[0] != "Average:" {
          warn!(tid=%tid, data_parts=?data_parts, "unexpected start to data_parts");
          return (-1.0,-1.0,-1.0,-1.0);
        }
        if data_parts.len()+1 != name_parts.len() {
          warn!(tid=%tid, "mpstat output lines were not of equal length name_parts: '{}' vs data_parts: '{}'", name_parts.len(), data_parts.len());
          return (-1.0,-1.0,-1.0,-1.0);
        }
        let mut found_data: std::collections::HashMap<&str, f64> = std::collections::HashMap::new();
        for (pos, name) in name_parts.iter().enumerate() {
          if name.starts_with("%") {
            found_data.insert(name, self.parse(data_parts[pos-1], tid));
          }
        }
        let mut user = 0.0;
        user += self.get_or_zero(tid, &found_data, "%usr");
        user += self.get_or_zero(tid, &found_data, "%nice");

        let mut sys = self.get_or_zero(tid, &found_data, "%sys");
        sys += self.get_or_zero(tid, &found_data, "%irq");
        sys += self.get_or_zero(tid, &found_data, "%soft");
        sys += self.get_or_zero(tid, &found_data, "%steal");
        sys += self.get_or_zero(tid, &found_data, "%guest");
        sys += self.get_or_zero(tid, &found_data, "%gnice");

        let wait = self.get_or_zero(tid, &found_data, "%iowait");
        let idle = self.get_or_zero(tid, &found_data, "%idle");

        debug!(tid=%tid, "mpstat {} {} {} {}", user, sys, wait, idle);
        (user,sys,idle,wait)
      },
      Err(e) => {
        error!(tid=%tid, "unable to call mpstat because {}", e);
        (-1.0,-1.0,-1.0,-1.0)
      },
    }
  }

  fn get_or_zero(&self, tid: &TransactionId, data: &std::collections::HashMap<&str, f64>, key: &str) -> f64 {
    match data.get(key) {
      Some(v) => *v,
      None => {
        debug!(tid=%tid, key=%key, "Was unable to find key in mpstat result");
        0.0
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

    let (us, sy, id, wa) = self.mpstat(tid);
    let minute_load_avg = self.uptime(tid);
    let nprocs = match nproc(tid, false) {
      Ok(n) => n,
      Err(e) => {
        error!(tid=%tid, error=%e, "Unable to get the number of processors on the system");
        0
      },
    };
    let queue_len = self.invoker_service.queue_len() as i64;
    let used_mem = self.container_manager.used_memory();
    let total_mem = self.container_manager.total_memory();
    let num_containers = self.container_manager.num_containers();
    let running = self.invoker_service.running_funcs();
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
      kernel_cpu_freqs: kernel_freqs,
      num_containers
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
  fn parse<T: std::str::FromStr + std::default::Default>(&self, string: &str, tid: &TransactionId) -> T
  where <T as std::str::FromStr>::Err: std::fmt::Display {
    match string.parse::<T>() {
      Ok(r) => r,
      Err(e) => {
        error!(tid=%tid, "error parsing {}: {}", string, e);
        T::default()
      },
    }
  }

  /// Returns the status and load of the worker
  pub fn get_status(&self, tid: &TransactionId) -> Arc<WorkerStatus> {
    debug!(tid=%tid, "getting current worker status");
    self.current_status.lock().clone()
  }
}
