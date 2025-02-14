use super::WorkerStatus;
use crate::services::invocation::InvokerLoad;
use crate::services::{
    containers::containermanager::ContainerManager, invocation::Invoker, resources::gpu::GpuResourceTracker,
};
use crate::worker_api::config::ContainerResourceConfig;
use crate::worker_api::worker_config::StatusConfig;
use anyhow::Result;
use iluvatar_library::clock::now;
use iluvatar_library::transaction::{TransactionId, STATUS_WORKER_TID};
use iluvatar_library::types::Compute;
use iluvatar_library::utils::is_simulation;
use iluvatar_library::{bail_error, threading};
use parking_lot::{Mutex, RwLock};
use std::fs::read_to_string;
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::{debug, info};

#[allow(unused)]
pub struct StatusService {
    container_manager: Arc<ContainerManager>,
    current_status: Mutex<Arc<WorkerStatus>>,
    worker_thread: JoinHandle<()>,
    tags: String,
    metrics: Vec<&'static str>,
    cpu: CpuMonitor,
    config: Arc<StatusConfig>,
    invoker: Arc<dyn Invoker>,
    gpu: Option<Arc<GpuResourceTracker>>,
    cpu_count: u32,
}

impl StatusService {
    pub fn boxed(
        cm: Arc<ContainerManager>,
        worker_name: String,
        tid: &TransactionId,
        config: Arc<StatusConfig>,
        invoker: Arc<dyn Invoker>,
        gpu: Option<Arc<GpuResourceTracker>>,
        signal: LoadAvg,
        ctr_config: &Arc<ContainerResourceConfig>,
    ) -> Result<Arc<Self>> {
        let (handle, sender) = threading::tokio_thread(
            config.report_freq_ms,
            STATUS_WORKER_TID.clone(),
            StatusService::update_status,
        );

        let cpu_count = match ctr_config.cpu_resource.count {
            0 => num_cpus::get() as u32,
            n => n,
        };
        let cpu_mon = get_cpu_mon(cpu_count, signal, invoker.clone(), tid)?;
        let ret = Arc::new(StatusService {
            container_manager: cm,
            current_status: Mutex::new(Arc::new(WorkerStatus {
                cpu_queue_len: 0,
                gpu_queue_len: 0,
                queue_load: InvokerLoad::default(),
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
            cpu: cpu_mon,
            config,
            invoker,
            gpu,
            cpu_count,
        });
        sender.send(ret.clone())?;
        Ok(ret)
    }

    async fn update_status(self: Arc<Self>, tid: TransactionId) {
        let mut gpu_utilization = vec![];
        if let Some(gpu) = &self.gpu {
            gpu_utilization = gpu.gpu_status(&tid);
        }

        let (computed_util, minute_load_avg) = match self.cpu.cpu_util(&tid) {
            Ok((c, l)) => (c, l),
            Err(_) => return,
        };

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
            cpu_queue_len: queue_lengths.get(&Compute::CPU).map_or(0, |q| q.len) as i64,
            gpu_queue_len: queue_lengths.get(&Compute::GPU).map_or(0, |q| q.len) as i64,
            queue_load: queue_lengths,
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
            num_system_cores: self.cpu_count,
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

/// A type to share the most recently computed system load average.
/// Should only have one per system.
pub type LoadAvg = Arc<RwLock<f64>>;
pub fn build_load_avg_signal() -> LoadAvg {
    Arc::new(RwLock::new(0.0))
}

fn get_cpu_mon(num_cpus: u32, signal: LoadAvg, invoker: Arc<dyn Invoker>, tid: &TransactionId) -> Result<CpuMonitor> {
    match is_simulation() {
        true => Ok(CpuSimMonitor::new(num_cpus, signal, invoker, tid)),
        false => Ok(CpuHardwareMonitor::new(signal, tid)?),
    }
}

type CpuMonitor = Arc<dyn CpuMonitorTrait + Send + Sync>;
trait CpuMonitorTrait {
    /// Returns a tuple of two items:
    /// 1) Breakdown in CPU utilization, since previous query.
    ///     More frequent querying results in more accurate utilization.
    ///     _NOT_ an instantaneous CPU utilization.
    /// 2) System [load average](https://www.man7.org/linux/man-pages/man5/proc_loadavg.5.html) over 1 minute.
    fn cpu_util(&self, tid: &TransactionId) -> Result<(CPUUtilPcts, f64)>;
}

struct CpuHardwareMonitor {
    last_instant_usage: Mutex<CPUUtilInstant>,
    signal: LoadAvg,
}
impl CpuHardwareMonitor {
    pub fn new(signal: LoadAvg, tid: &TransactionId) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            last_instant_usage: Mutex::new(CPUUtilInstant::get(tid)?),
            signal,
        }))
    }
}
#[tonic::async_trait]
impl CpuMonitorTrait for CpuHardwareMonitor {
    fn cpu_util(&self, tid: &TransactionId) -> Result<(CPUUtilPcts, f64)> {
        debug!(tid=%tid, "Computing system utilization");
        let now = CPUUtilInstant::get(tid)?;
        let mut last = self.last_instant_usage.lock();
        let diff = &now - &*last;
        *last = now;

        let buff = match read_to_string("/proc/loadavg") {
            Ok(f) => f,
            Err(e) => bail_error!(tid=%tid, error=%e, "Failed to read /proc/loadavg"),
        };
        let lines: Vec<&str> = buff.split(' ').filter(|str| !str.is_empty()).collect();
        let min = lines[0];
        let load_avg = match min.parse::<f64>() {
            Ok(r) => r,
            Err(e) => {
                bail_error!(tid=%tid, "error parsing float from uptime {}: {}", min, e);
            },
        };
        *self.signal.write() = load_avg;

        Ok((diff, load_avg))
    }
}

struct CpuSimMonitor {
    num_cpus: u32,
    signal: LoadAvg,
    invoker: Arc<dyn Invoker>,
}
impl CpuSimMonitor {
    fn new(num_cpus: u32, signal: LoadAvg, invoker: Arc<dyn Invoker>, _tid: &TransactionId) -> Arc<Self> {
        Arc::new(Self {
            num_cpus,
            signal,
            invoker,
        })
    }
}
impl CpuMonitorTrait for CpuSimMonitor {
    fn cpu_util(&self, tid: &TransactionId) -> Result<(CPUUtilPcts, f64)> {
        debug!(tid=%tid, "Computing system utilization");
        // additional 0.5 to account for system work
        let running = self.invoker.running_funcs() as f64 + 0.5;
        let mut lck = self.signal.write();
        let load_avg = (running * 0.5) + (*lck * (1.0 - 0.5));
        *lck = load_avg;
        Ok((
            CPUUtilPcts {
                read_diff: Duration::from_micros(0),
                cpu_user: load_avg / self.num_cpus as f64,
                cpu_nice: 0.0,
                cpu_system: 0.5,
                cpu_idle: 0.0,
                cpu_iowait: 0.0,
                cpu_irq: 0.0,
                cpu_softirq: 0.0,
                cpu_steal: 0.0,
                cpu_guest: 0.0,
                cpu_guest_nice: 0.0,
            },
            load_avg,
        ))
    }
}

/// The CPU usage metrics reported by /proc/stat
pub struct CPUUtilInstant {
    read_time: Instant,
    cpu_user: f64,
    cpu_nice: f64,
    cpu_system: f64,
    cpu_idle: f64,
    cpu_iowait: f64,
    cpu_irq: f64,
    cpu_softirq: f64,
    cpu_steal: f64,
    cpu_guest: f64,
    cpu_guest_nice: f64,
}
impl Default for CPUUtilInstant {
    fn default() -> Self {
        Self {
            read_time: now(),
            cpu_user: 0.0,
            cpu_nice: 0.0,
            cpu_system: 0.0,
            cpu_idle: 0.0,
            cpu_iowait: 0.0,
            cpu_irq: 0.0,
            cpu_softirq: 0.0,
            cpu_steal: 0.0,
            cpu_guest: 0.0,
            cpu_guest_nice: 0.0,
        }
    }
}
impl CPUUtilInstant {
    pub fn get(tid: &TransactionId) -> Result<Self> {
        let cpu_line = Self::read()?;
        Self::parse(cpu_line, tid)
    }
    fn read() -> Result<String> {
        let mut ret = String::new();
        let mut b = match std::fs::File::open("/proc/stat") {
            Ok(f) => BufReader::new(f),
            Err(e) => {
                return Err(e.into());
            },
        };
        loop {
            match b.read_line(&mut ret)? {
                0 => anyhow::bail!("Unable to find matching 'cpu ' line in /proc/stat"),
                _ => {
                    if ret.starts_with("cpu ") {
                        return Ok(ret);
                    }
                },
            }
        }
    }
    fn parse(mut line: String, tid: &TransactionId) -> Result<Self> {
        if line.ends_with('\n') {
            line.pop();
        }
        let strs: Vec<&str> = line.split(' ').filter(|str| !str.is_empty()).collect();
        Ok(Self {
            read_time: now(),
            cpu_user: Self::safe_get_val(&strs, 1, tid)?,
            cpu_nice: Self::safe_get_val(&strs, 2, tid)?,
            cpu_system: Self::safe_get_val(&strs, 3, tid)?,
            cpu_idle: Self::safe_get_val(&strs, 4, tid)?,
            cpu_iowait: Self::safe_get_val(&strs, 5, tid)?,
            cpu_irq: Self::safe_get_val(&strs, 6, tid)?,
            cpu_softirq: Self::safe_get_val(&strs, 7, tid)?,
            cpu_steal: Self::safe_get_val(&strs, 8, tid)?,
            cpu_guest: Self::safe_get_val(&strs, 9, tid)?,
            cpu_guest_nice: Self::safe_get_val(&strs, 10, tid)?,
        })
    }
    fn safe_get_val(split_line: &[&str], pos: usize, tid: &TransactionId) -> Result<f64> {
        if split_line.len() >= pos {
            match split_line[pos].parse::<f64>() {
                Ok(v) => Ok(v),
                Err(e) => {
                    bail_error!(error=%e, tid=%tid, line=%split_line[pos], "Unable to parse string from /proc/stat")
                },
            }
        } else {
            bail_error!(expected=pos, actual=split_line.len(), tid=%tid, "/proc/stat line was unexpectedly short!")
        }
    }
}

pub struct CPUUtilPcts {
    pub read_diff: std::time::Duration,
    pub cpu_user: f64,
    pub cpu_nice: f64,
    pub cpu_system: f64,
    pub cpu_idle: f64,
    pub cpu_iowait: f64,
    pub cpu_irq: f64,
    pub cpu_softirq: f64,
    pub cpu_steal: f64,
    pub cpu_guest: f64,
    pub cpu_guest_nice: f64,
}
impl std::ops::Sub for &CPUUtilInstant {
    type Output = CPUUtilPcts;

    fn sub(self, rhs: Self) -> Self::Output {
        let dur = self.read_time - rhs.read_time;
        let user_diff = self.cpu_user - rhs.cpu_user;
        let nice_diff = self.cpu_nice - rhs.cpu_nice;
        let system_diff = self.cpu_system - rhs.cpu_system;
        let idle_diff = self.cpu_idle - rhs.cpu_idle;
        let iowait_diff = self.cpu_iowait - rhs.cpu_iowait;
        let irq_diff = self.cpu_irq - rhs.cpu_irq;
        let softirq_diff = self.cpu_softirq - rhs.cpu_softirq;
        let steal_diff = self.cpu_steal - rhs.cpu_steal;
        let guest_diff = self.cpu_guest - rhs.cpu_guest;
        let guest_nice_diff = self.cpu_guest_nice - rhs.cpu_guest_nice;
        let tot = user_diff
            + nice_diff
            + system_diff
            + idle_diff
            + iowait_diff
            + irq_diff
            + softirq_diff
            + steal_diff
            + guest_diff
            + guest_nice_diff;
        CPUUtilPcts {
            read_diff: dur,
            cpu_user: (user_diff / tot) * 100.0,
            cpu_nice: (nice_diff / tot) * 100.0,
            cpu_system: (system_diff / tot) * 100.0,
            cpu_idle: (idle_diff / tot) * 100.0,
            cpu_iowait: (iowait_diff / tot) * 100.0,
            cpu_irq: (irq_diff / tot) * 100.0,
            cpu_softirq: (softirq_diff / tot) * 100.0,
            cpu_steal: (steal_diff / tot) * 100.0,
            cpu_guest: (guest_diff / tot) * 100.0,
            cpu_guest_nice: (guest_nice_diff / tot) * 100.0,
        }
    }
}
