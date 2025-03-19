use crate::services::invocation::Invoker;
use crate::services::registration::RegisteredFunction;
use crate::worker_api::config::StatusConfig;
use crate::worker_api::worker_config::CPUResourceConfig;
use anyhow::Result;
use iluvatar_library::clock::now;
use iluvatar_library::ring_buff::{RingBuffer, Wireable};
use iluvatar_library::threading::tokio_thread;
use iluvatar_library::transaction::TransactionId;
use iluvatar_library::utils::is_simulation;
use iluvatar_library::{bail_error, threading};
use parking_lot::{Mutex, RwLock};
use std::fs::read_to_string;
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;
use tracing::{debug, error, info};

lazy_static::lazy_static! {
  pub static ref CPU_CONCUR_WORKER_TID: TransactionId = "CPUConcurrencyMonitor".to_string();
}

/// An invoker that scales concurrency based on system load.
/// Prioritizes based on container availability.
/// Increases concurrency by 1 every [crate::worker_api::worker_config::ComputeResourceConfig::concurrency_update_check_ms].
/// If system load is above [crate::worker_api::worker_config::ComputeResourceConfig::max_load], then the concurrency is reduced by half the distance to [crate::worker_api::worker_config::ComputeResourceConfig::max_oversubscribe] rounded up.
pub struct CpuResourceTracker {
    _load_thread: Option<tokio::task::JoinHandle<()>>,
    concurrency_semaphore: Option<Arc<Semaphore>>,
    max_concur: u32,
    min_concur: u32,
    current_concur: Mutex<u32>,
    max_load: Option<f64>,
    pub cores: f64,
    load_avg: LoadAvg,
}

impl CpuResourceTracker {
    pub fn new(config: &Arc<CPUResourceConfig>, load_avg: LoadAvg, tid: &TransactionId) -> Result<Arc<Self>> {
        let mut max_concur = 0;
        let available_cores = match config.count {
            0 => num_cpus::get(),
            n => n as usize,
        };
        let sem = match config.count {
            0 => None,
            p => Some(Arc::new(Semaphore::new(p as usize))),
        };

        let (load_handle, load_tx) = match config.concurrency_update_check_ms {
            Some(0) => (None, None),
            Some(check_dur) => {
                let max_load = config
                    .max_load
                    .ok_or_else(|| anyhow::anyhow!("max_load was not present in InvocationConfig"))?;
                if max_load == 0.0 {
                    anyhow::bail!("Cannot have a 'max_load' of 0");
                }
                max_concur = config
                    .max_oversubscribe
                    .ok_or_else(|| anyhow::anyhow!("max_oversubscribe was not present in InvocationConfig"))?;
                if max_concur == 0 {
                    anyhow::bail!("Cannot have a 'max_concurrency' of 0");
                }

                let (h, tx) = tokio_thread(check_dur, CPU_CONCUR_WORKER_TID.clone(), Self::monitor_load);
                (Some(h), Some(tx))
            },
            None => (None, None),
        };
        let svc = Arc::new(CpuResourceTracker {
            concurrency_semaphore: sem,
            min_concur: config.count,
            current_concur: Mutex::new(config.count),
            max_load: config.max_load,
            max_concur,
            _load_thread: load_handle,
            cores: available_cores as f64,
            load_avg,
        });
        if let Some(load_tx) = load_tx {
            load_tx.send(svc.clone())?;
        }
        debug!(tid = tid, "Created CPUResourceMananger");
        Ok(svc)
    }

    /// Return a permit for the function to run on its registered number of cores.
    /// If the semaphore is [None], then no permits are being tracked.
    pub fn try_acquire_cores(
        &self,
        reg: &Arc<RegisteredFunction>,
        _tid: &TransactionId,
    ) -> Result<Option<OwnedSemaphorePermit>, tokio::sync::TryAcquireError> {
        if let Some(sem) = &self.concurrency_semaphore {
            return match sem.clone().try_acquire_many_owned(reg.cpus) {
                Ok(p) => Ok(Some(p)),
                Err(e) => Err(e),
            };
        }
        Ok(None)
    }

    /// Blocks until an owned permit can be acquired for the function to run on its registered number of cores.
    /// If the semaphore is [None], then no permits are being tracked.
    pub async fn acquire_cores(
        &self,
        reg: &Arc<RegisteredFunction>,
        _tid: &TransactionId,
    ) -> Result<Option<OwnedSemaphorePermit>, tokio::sync::AcquireError> {
        if let Some(sem) = &self.concurrency_semaphore {
            return match sem.clone().acquire_many_owned(reg.cpus).await {
                Ok(p) => Ok(Some(p)),
                Err(e) => Err(e),
            };
        }
        Ok(None)
    }

    pub fn available_cores(&self) -> usize {
        if let Some(sem) = &self.concurrency_semaphore {
            return sem.available_permits();
        }
        self.cores as usize
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self), fields(tid=tid)))]
    async fn monitor_load(self: &Arc<Self>, tid: &TransactionId) {
        let norm_load = *self.load_avg.read() / self.cores;
        let current = *self.current_concur.lock();
        if norm_load > self.max_load.unwrap() {
            let change = current - self.min_concur;
            let change = f64::ceil(change as f64 / 2.0) as u32;
            if change > 0 {
                if let Some(sem) = &self.concurrency_semaphore {
                    match sem.acquire_many(change).await {
                        Ok(s) => {
                            s.forget();
                            *self.current_concur.lock() = u32::max(self.min_concur, current - change);
                        },
                        Err(e) => {
                            error!(tid=tid, error=%e, "Failed to acquire concurrency semaphore")
                        },
                    };
                }
            }
        } else if current < self.max_concur {
            if let Some(sem) = &self.concurrency_semaphore {
                sem.add_permits(1);
                *self.current_concur.lock() += 1;
            }
        }
        debug!(
            tid = tid,
            concurrency = *self.current_concur.lock(),
            load = norm_load,
            "Current concurrency"
        );
    }
}

/// A type to share the most recently computed system load average.
/// Should only have one per system.
pub type LoadAvg = Arc<RwLock<f64>>;
pub fn build_load_avg_signal() -> LoadAvg {
    Arc::new(RwLock::new(0.0))
}

pub fn get_cpu_mon(
    config: &Arc<StatusConfig>,
    ring_buff: &Arc<RingBuffer>,
    num_cpus: u32,
    signal: LoadAvg,
    invoker: Arc<dyn Invoker>,
    tid: &TransactionId,
) -> Result<CpuMonitor> {
    CpuMonitorWrap::boxed(config, ring_buff, num_cpus, signal, invoker, tid)
}

#[derive(serde::Serialize, iluvatar_library::ToAny)]
pub struct CpuUtil {
    /// [cpu-time non-kernel](https://linux.die.net/man/1/mpstat)
    pub cpu_us: f64,
    /// [cpu-time kernel, hypervisor, servicing, interrupts](https://linux.die.net/man/1/mpstat)
    pub cpu_sy: f64,
    /// [cpu-time idle](https://linux.die.net/man/1/mpstat)
    pub cpu_id: f64,
    /// [cpu-time waiting](https://linux.die.net/man/1/mpstat)
    pub cpu_wa: f64,
    /// [one minute load average](https://www.man7.org/linux/man-pages/man1/uptime.1.html)
    /// NOT NORMALIZED by number of cores
    pub load_avg_1minute: f64,
    /// The number of (logical) CPU cores on the system
    /// used to normalize the load average value
    pub num_system_cores: u32,
}
impl Wireable for CpuUtil {}
impl std::fmt::Display for CpuUtil {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match serde_json::to_string::<CpuUtil>(self) {
            Ok(s) => s,
            Err(_e) => return Err(std::fmt::Error {}),
        };
        write!(f, "{}", s)
    }
}

pub const CPU_MON_TID: &str = "CPU_MON_BG";
struct CpuMonitorWrap {
    mon: CpuMonitor,
    num_cpus: u32,
}
impl CpuMonitorWrap {
    pub fn boxed(
        config: &Arc<StatusConfig>,
        ring_buff: &Arc<RingBuffer>,
        num_cpus: u32,
        signal: LoadAvg,
        invoker: Arc<dyn Invoker>,
        tid: &TransactionId,
    ) -> Result<CpuMonitor> {
        let (_handle, sender) = threading::tokio_logging_thread(
            config.report_freq_ms,
            CPU_MON_TID.to_owned(),
            ring_buff.clone(),
            Self::log,
        )?;
        let r = Arc::new(Self {
            mon: match is_simulation() {
                true => CpuSimMonitor::new(num_cpus, signal, invoker, tid),
                false => CpuHardwareMonitor::new(signal, tid)?,
            },
            num_cpus,
        });
        sender.send(r.clone())?;
        Ok(r)
    }

    async fn log(self: &Arc<Self>, tid: &TransactionId) -> Result<CpuUtil> {
        let (computed_util, minute_load_avg) = self.mon.cpu_util(tid)?;
        let r = CpuUtil {
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
            num_system_cores: self.num_cpus,
        };
        info!(tid=tid, cpu_util=%r, "CPU utilization");
        Ok(r)
    }
}
impl CpuMonitorTrait for CpuMonitorWrap {
    fn cpu_util(&self, tid: &TransactionId) -> Result<(CPUUtilPcts, f64)> {
        self.mon.cpu_util(tid)
    }
}

pub type CpuMonitor = Arc<dyn CpuMonitorTrait + Send + Sync>;
pub trait CpuMonitorTrait {
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
impl CpuMonitorTrait for CpuHardwareMonitor {
    fn cpu_util(&self, tid: &TransactionId) -> Result<(CPUUtilPcts, f64)> {
        debug!(tid = tid, "Computing system utilization");
        let now = CPUUtilInstant::get(tid)?;
        let mut last = self.last_instant_usage.lock();
        let diff = &now - &*last;
        *last = now;

        let buff = match read_to_string("/proc/loadavg") {
            Ok(f) => f,
            Err(e) => bail_error!(tid=tid, error=%e, "Failed to read /proc/loadavg"),
        };
        let lines: Vec<&str> = buff.split(' ').filter(|str| !str.is_empty()).collect();
        let min = lines[0];
        let load_avg = match min.parse::<f64>() {
            Ok(r) => r,
            Err(e) => {
                bail_error!(tid = tid, "error parsing float from uptime {}: {}", min, e);
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
        debug!(tid = tid, "Computing system utilization");
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
                    bail_error!(error=%e, tid=tid, line=%split_line[pos], "Unable to parse string from /proc/stat")
                },
            }
        } else {
            bail_error!(
                expected = pos,
                actual = split_line.len(),
                tid = tid,
                "/proc/stat line was unexpectedly short!"
            )
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
