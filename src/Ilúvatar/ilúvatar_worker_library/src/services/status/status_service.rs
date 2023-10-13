use super::WorkerStatus;
use crate::services::{
    containers::containermanager::ContainerManager, invocation::Invoker, resources::gpu::GpuResourceTracker,
};
use crate::worker_api::worker_config::StatusConfig;
use anyhow::Result;
use iluvatar_library::cpu_interaction::{CPUService, CPUUtilInstant};
use iluvatar_library::transaction::{TransactionId, STATUS_WORKER_TID};
use iluvatar_library::types::Compute;
use iluvatar_library::{load_avg, nproc, threading};
use parking_lot::Mutex;
use std::sync::Arc;
use std::thread::JoinHandle;
use tracing::{debug, error, info};

#[allow(unused)]
pub struct StatusService {
    container_manager: Arc<ContainerManager>,
    current_status: Mutex<Arc<WorkerStatus>>,
    worker_thread: JoinHandle<()>,
    tags: String,
    metrics: Vec<&'static str>,
    cpu: Arc<CPUService>,
    config: Arc<StatusConfig>,
    cpu_instant: Mutex<CPUUtilInstant>,
    invoker: Arc<dyn Invoker>,
    gpu: Arc<GpuResourceTracker>,
}

impl StatusService {
    pub fn boxed(
        cm: Arc<ContainerManager>,
        worker_name: String,
        tid: &TransactionId,
        config: Arc<StatusConfig>,
        invoker: Arc<dyn Invoker>,
        gpu: Arc<GpuResourceTracker>,
    ) -> Result<Arc<Self>> {
        let (handle, sender) = threading::os_thread::<Self>(
            config.report_freq_ms,
            STATUS_WORKER_TID.clone(),
            Arc::new(StatusService::update_status),
        )?;
        let cpu_svc = CPUService::boxed(tid)?;

        let ret = Arc::new(StatusService {
            container_manager: cm,
            current_status: Mutex::new(Arc::new(WorkerStatus {
                cpu_queue_len: 0,
                gpu_queue_len: 0,
                used_mem: 0,
                total_mem: 0,
                cpu_us: 0.0,
                cpu_sy: 0.0,
                cpu_id: 0.0,
                cpu_wa: 0.0,
                load_avg_1minute: 0.0,
                num_system_cores: 0,
                num_running_funcs: 0,
                num_containers: 0,
                gpu_utilization: vec![],
            })),
            worker_thread: handle,
            tags: format!("machine={};type=worker", worker_name),
            metrics: vec![
                "worker.load.loadavg",
                "worker.load.cpu",
                "worker.load.queue",
                "worker.load.mem_pct",
                "worker.load.used_mem",
            ],
            cpu_instant: Mutex::new(cpu_svc.instant_cpu_util(tid)?),
            cpu: cpu_svc,
            config,
            invoker,
            gpu,
        });
        sender.send(ret.clone())?;
        Ok(ret)
    }

    fn update_status(&self, tid: &TransactionId) {
        let cpu_now = match self.cpu.instant_cpu_util(tid) {
            Ok(i) => i,
            Err(e) => {
                error!(tid=%tid, error=%e, "Unable to get instant cpu utilization");
                return;
            }
        };

        let minute_load_avg = load_avg(tid);
        let nprocs = match nproc(tid, false) {
            Ok(n) => n,
            Err(e) => {
                error!(tid=%tid, error=%e, "Unable to get the number of processors on the system");
                0
            }
        };

        let gpu_utilization = match self.gpu.gpu_utilization(tid) {
            Ok(n) => n,
            Err(e) => {
                error!(tid=%tid, error=%e, "Unable to get gpu utilization");
                vec![]
            }
        };

        let mut cpu_instant_lck = self.cpu_instant.lock();
        let computed_util = self.cpu.compute_cpu_util(&cpu_now, &(*cpu_instant_lck));
        *cpu_instant_lck = cpu_now;

        let queue_lengths = self.invoker.queue_len();
        let used_mem = self.container_manager.used_memory();
        let total_mem = self.container_manager.total_memory();
        let num_containers = self.container_manager.num_containers();
        let running = self.invoker.running_funcs();

        let new_status = Arc::new(WorkerStatus {
            num_containers,
            gpu_utilization,
            used_mem,
            total_mem,
            cpu_queue_len: *queue_lengths.get(&Compute::CPU).unwrap_or(&0) as i64,
            gpu_queue_len: *queue_lengths.get(&Compute::GPU).unwrap_or(&0) as i64,
            cpu_us: computed_util.cpu_user + computed_util.cpu_nice,
            cpu_sy: computed_util.cpu_system
                + computed_util.cpu_irq
                + computed_util.cpu_softirq
                + computed_util.cpu_steal
                + computed_util.cpu_guest
                + computed_util.cpu_guest_nice,
            cpu_id: computed_util.cpu_idle,
            cpu_wa: computed_util.cpu_iowait,
            load_avg_1minute: minute_load_avg,
            num_system_cores: nprocs,
            num_running_funcs: running,
        });
        info!(tid=%tid, status=%new_status, "current load status");

        *self.current_status.lock() = new_status;
    }

    /// Returns the status and load of the worker
    pub fn get_status(&self, tid: &TransactionId) -> Arc<WorkerStatus> {
        debug!(tid=%tid, "getting current worker status");
        self.current_status.lock().clone()
    }
}
