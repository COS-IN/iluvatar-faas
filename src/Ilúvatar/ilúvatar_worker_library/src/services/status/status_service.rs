
use std::sync::Arc;
use std::thread::JoinHandle;
use iluvatar_library::cpu_interaction::{CPUService, CPUUtilInstant};
use iluvatar_library::{nproc, threading, load_avg};
use tracing::{info, debug, error};
use parking_lot::Mutex;
use crate::services::containers::containermanager::ContainerManager;
use crate::services::invocation::Invoker;
use crate::worker_api::worker_config::StatusConfig;
use iluvatar_library::graphite::{GraphiteConfig, graphite_svc::GraphiteService};
use iluvatar_library::transaction::{TransactionId, STATUS_WORKER_TID};
use super::WorkerStatus;
use anyhow::Result;

#[allow(unused)]
pub struct StatusService {
  container_manager: Arc<ContainerManager>, 
  current_status: Mutex<Arc<WorkerStatus>>,
  worker_thread: JoinHandle<()>,
  graphite: GraphiteService,
  tags: String,
  metrics: Vec<&'static str>,
  cpu: Arc<CPUService>,
  config: Arc<StatusConfig>,
  cpu_instant: Mutex<CPUUtilInstant>,
  invoker: Arc<dyn Invoker>,
}

impl StatusService {
  pub fn boxed(cm: Arc<ContainerManager>, graphite_cfg: Arc<GraphiteConfig>, worker_name: String, tid: &TransactionId, config: Arc<StatusConfig>, invoker: Arc<dyn Invoker>) -> Result<Arc<Self>> {
    let (handle, sender) = 
          threading::os_thread::<Self>(config.report_freq_ms, STATUS_WORKER_TID.clone(), Arc::new(StatusService::update_status))?;
    let cpu_svc = CPUService::boxed(tid)?;

    let ret = Arc::new(StatusService { 
      container_manager: cm, 
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
      cpu_instant: Mutex::new(cpu_svc.instant_cpu_util(tid)?),
      cpu: cpu_svc,
      config,
      invoker,
    });
    sender.send(ret.clone())?;
    Ok(ret)
  }

  fn update_status(&self, tid: &TransactionId) {
    let _free_cs = self.container_manager.free_cores();

    let cpu_now = match self.cpu.instant_cpu_util(tid) {
      Ok(i) => i,
      Err(e) => {
        error!(tid=%tid, error=%e, "Unable to get instant cpu utilization");
        return;
      },
    };

    let minute_load_avg = load_avg(tid);
    let nprocs = match nproc(tid, false) {
      Ok(n) => n,
      Err(e) => {
        error!(tid=%tid, error=%e, "Unable to get the number of processors on the system");
        0
      },
    };

    let mut cpu_instant_lck = self.cpu_instant.lock();
    let computed_util = self.cpu.compute_cpu_util(&cpu_now, &(*cpu_instant_lck));
    *cpu_instant_lck = cpu_now;

    let queue_len = self.invoker.queue_len() as i64;
    let used_mem = self.container_manager.used_memory();
    let total_mem = self.container_manager.total_memory();
    let num_containers = self.container_manager.num_containers();
    let running = self.container_manager.outstanding(None);
    let hw_freqs = self.cpu.hardware_cpu_freqs(tid);
    let kernel_freqs = self.cpu.kernel_cpu_freqs(tid);

    let new_status = Arc::new(WorkerStatus {
      queue_len,
      used_mem,
      total_mem,
      cpu_us: computed_util.cpu_user + computed_util.cpu_nice,
      cpu_sy: computed_util.cpu_system + computed_util.cpu_irq + computed_util.cpu_softirq + computed_util.cpu_steal + computed_util.cpu_guest + computed_util.cpu_guest_nice,
      cpu_id: computed_util.cpu_idle,
      cpu_wa: computed_util.cpu_iowait,
      load_avg_1minute: minute_load_avg,
      num_system_cores: nprocs,
      num_running_funcs: running,
      hardware_cpu_freqs: hw_freqs,
      kernel_cpu_freqs: kernel_freqs,
      num_containers
    });
    info!(tid=%tid, status=%new_status,"current load status");

    let values = vec![(minute_load_avg / nprocs as f64), (new_status.cpu_us+new_status.cpu_sy) as f64,
                                    queue_len as f64, (used_mem as f64 / total_mem as f64), 
                                    used_mem as f64];
    self.graphite.publish_metrics(&self.metrics, values, tid, self.tags.as_str());

    *self.current_status.lock() = new_status;
  }

  /// Returns the status and load of the worker
  pub fn get_status(&self, tid: &TransactionId) -> Arc<WorkerStatus> {
    debug!(tid=%tid, "getting current worker status");
    self.current_status.lock().clone()
  }
}
