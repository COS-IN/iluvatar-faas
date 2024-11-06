use crate::clock::now;
use crate::utils::is_simulation;
use crate::{bail_error, threading, transaction::TransactionId};
use anyhow::Result;
use parking_lot::Mutex;
use std::fs::read_to_string;
use std::io::{BufRead, BufReader, Read};
use std::thread::JoinHandle;
use std::time::Duration;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::time::Instant;
use tracing::{error, info};

pub fn get_cpu_mon(cpu_count: u32, tid: &TransactionId) -> Result<CpuMonitor> {
    match is_simulation() {
        true => Ok(CpuSimMonitor::new(cpu_count, tid)),
        false => Ok(CpuHardwareMonitor::new(cpu_count, tid)?),
    }
}

pub type CpuMonitor = Arc<dyn CpuMonitorTrait + Send + Sync>;
pub trait CpuMonitorTrait {
    /// Breakdown in CPU utilization, since previous query.
    /// More frequent querying results in more accurate utilization.
    /// _NOT_ an instantaneous CPU utilization.
    fn cpu_util(&self, tid: &TransactionId) -> Result<CPUUtilPcts>;
    /// System [load average](https://www.man7.org/linux/man-pages/man5/proc_loadavg.5.html) over 1 minute.
    fn load_average(&self, tid: &TransactionId) -> Result<f64>;
    /// Number of CPUs available to the system.
    fn nprocs(&self, tid: &TransactionId) -> Result<u32>;
}

pub struct CpuHardwareMonitor {
    last_instant_usage: Mutex<CPUUtilInstant>,
    nprocs: u32,
}
impl CpuHardwareMonitor {
    pub fn new(cpu_count: u32, tid: &TransactionId) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            last_instant_usage: Mutex::new(CPUUtilInstant::get(tid)?),
            nprocs: match cpu_count {
                0 => num_cpus::get() as u32,
                n => n,
            },
        }))
    }
}
#[async_trait::async_trait]
impl CpuMonitorTrait for CpuHardwareMonitor {
    fn cpu_util(&self, tid: &TransactionId) -> Result<CPUUtilPcts> {
        let now = CPUUtilInstant::get(tid)?;
        let mut last = self.last_instant_usage.lock();
        let diff = &now - &*last;
        *last = now;
        Ok(diff)
    }

    fn load_average(&self, tid: &TransactionId) -> Result<f64> {
        let buff = match read_to_string("/proc/loadavg") {
            Ok(f) => f,
            Err(e) => bail_error!(tid=%tid, error=%e, "Failed to read /proc/loadavg"),
        };
        let lines: Vec<&str> = buff.split(' ').filter(|str| !str.is_empty()).collect();
        let min = lines[0];
        match min.parse::<f64>() {
            Ok(r) => Ok(r),
            Err(e) => {
                bail_error!(tid=%tid, "error parsing float from uptime {}: {}", min, e);
            }
        }
    }

    fn nprocs(&self, _tid: &TransactionId) -> Result<u32> {
        Ok(self.nprocs)
    }
}

pub struct CpuSimMonitor {
    cpu_count: u32,
}
impl CpuSimMonitor {
    pub fn new(cpu_count: u32, _tid: &TransactionId) -> Arc<Self> {
        Arc::new(Self { cpu_count })
    }
}
impl CpuMonitorTrait for CpuSimMonitor {
    fn cpu_util(&self, _tid: &TransactionId) -> Result<CPUUtilPcts> {
        Ok(CPUUtilPcts {
            read_diff: Duration::from_micros(0),
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
        })
    }

    fn load_average(&self, _tid: &TransactionId) -> Result<f64> {
        Ok(0.0)
    }

    fn nprocs(&self, _tid: &TransactionId) -> Result<u32> {
        Ok(self.cpu_count)
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
            }
        };
        loop {
            match b.read_line(&mut ret)? {
                0 => anyhow::bail!("Unable to find matching 'cpu ' line in /proc/stat"),
                _ => {
                    if ret.starts_with("cpu ") {
                        return Ok(ret);
                    }
                }
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
                }
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

const BASE_CPU_DIR: &str = "/sys/devices/system/cpu";
pub struct CpuFreqMonitor {
    nprocs: usize,
    _kernel_worker: Option<JoinHandle<()>>,
    _hardware_worker: Option<JoinHandle<()>>,
}

lazy_static::lazy_static! {
  pub static ref KERNEL_FREQ_WORKER_TID: TransactionId = "KernelFreqMon".to_string();
  pub static ref HARDWARE_FREQ_WORKER_TID: TransactionId = "HardwareFreqMon".to_string();
}
impl CpuFreqMonitor {
    pub fn boxed(
        kernel_freq_ms: Option<u64>,
        hardware_freq_ms: Option<u64>,
        _tid: &TransactionId,
    ) -> Result<Arc<Self>> {
        let kernel = match kernel_freq_ms {
            None | Some(0) => (None, None),
            Some(ms) => {
                let (h, t) =
                    threading::os_thread(ms, KERNEL_FREQ_WORKER_TID.clone(), Arc::new(Self::kernel_frequencies))?;
                (Some(h), Some(t))
            }
        };
        let hardware = match hardware_freq_ms {
            None | Some(0) => (None, None),
            Some(ms) => {
                // make sure hardware data is accessible
                let (h, t) = threading::os_thread(
                    ms,
                    HARDWARE_FREQ_WORKER_TID.clone(),
                    Arc::new(Self::hardware_frequencies),
                )?;
                (Some(h), Some(t))
            }
        };
        let r = Arc::new(CpuFreqMonitor {
            _kernel_worker: kernel.0,
            _hardware_worker: hardware.0,
            nprocs: num_cpus::get_physical(),
        });
        if let Some(tx) = kernel.1 {
            tx.send(r.clone())?;
        }
        if let Some(tx) = hardware.1 {
            tx.send(r.clone())?;
        }
        Ok(r)
    }

    fn read_freq(&self, pth: PathBuf, tid: &TransactionId) -> Result<u64> {
        let mut opened = match std::fs::File::open(&pth) {
            Ok(b) => b,
            Err(e) => {
                bail_error!(error=%e, tid=%tid, file=%pth.to_string_lossy(), "Unable to open cpu freq file")
            }
        };
        let mut buff = String::new();
        match opened.read_to_string(&mut buff) {
            Ok(_) => (),
            Err(e) => {
                bail_error!(error=%e, tid=%tid, file=%pth.to_string_lossy(), "Unable to read cpu freq file into buffer")
            }
        };
        match buff[0..buff.len() - 1].parse::<u64>() {
            Ok(u) => Ok(u),
            Err(e) => {
                bail_error!(error=%e, tid=%tid, data=%buff, "Unable to parse cpu freq buffer")
            }
        }
    }

    /// Log the kernel CPU frequencies, CPU ID is the entry in the vec
    fn kernel_frequencies(&self, tid: &TransactionId) {
        let mut frequencies = Vec::new();
        let base = Path::new(BASE_CPU_DIR);

        for cpu in 0..self.nprocs {
            let shared_path = base.join(format!("cpu{}", cpu));
            let kernel_path = shared_path.join("cpufreq/scaling_cur_freq");
            let parsed = match self.read_freq(kernel_path, tid) {
                Ok(p) => p,
                Err(e) => {
                    error!(tid=%tid, error=%e, "Failed to read kernel CPU frequencies");
                    return;
                }
            };
            frequencies.push(parsed);
        }
        info!(tid=%tid, frequencies=?frequencies, "Kernel CPU Frequencies");
    }

    /// Log the hardware CPU frequencies, CPU ID is the entry in the vec
    fn hardware_frequencies(&self, tid: &TransactionId) {
        let mut frequencies = Vec::new();
        let base = Path::new(BASE_CPU_DIR);

        for cpu in 0..self.nprocs {
            let shared_path = base.join(format!("cpu{}", cpu));
            let hw_path = shared_path.join("cpufreq/cpuinfo_cur_freq");
            let parsed = match self.read_freq(hw_path, tid) {
                Ok(p) => p,
                Err(e) => {
                    error!(tid=%tid, error=%e, "Failed to read hardware CPU frequencies");
                    return;
                }
            };
            frequencies.push(parsed);
        }
        info!(tid=%tid, frequencies=?frequencies, "Hardware CPU Frequencies");
    }
}
