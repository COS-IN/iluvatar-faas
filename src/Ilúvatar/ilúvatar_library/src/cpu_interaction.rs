use crate::{bail_error, nproc, threading, transaction::TransactionId};
use anyhow::Result;
use std::thread::JoinHandle;
use std::{
    fs::File,
    io::{BufRead, Read},
    path::{Path, PathBuf},
    sync::Arc,
};
use tracing::{error, info};

const BASE_CPU_DIR: &str = "/sys/devices/system/cpu";

pub struct CPUService {
    nprocs: usize,
}

impl CPUService {
    pub fn boxed(tid: &TransactionId) -> Result<Arc<Self>> {
        let procs = nproc(tid, true)?;
        Ok(Arc::new(CPUService { nprocs: procs as usize }))
    }

    /// The frequencies of all the CPUs in the system, as reported by the hardware
    /// CPU IDs are implicit in the position, values will be 0 if an error occured
    pub fn hardware_cpu_freqs(&self, tid: &TransactionId) -> Result<Vec<u64>> {
        let mut ret = Vec::new();
        let base = Path::new(BASE_CPU_DIR);

        for cpu in 0..self.nprocs {
            let shared_path = base.join(format!("cpu{}", cpu));
            let hw_path = shared_path.join("cpufreq/cpuinfo_cur_freq");
            let parsed = self.read_freq(hw_path, tid)?;
            ret.push(parsed);
        }

        Ok(ret)
    }

    fn read_freq(&self, pth: PathBuf, tid: &TransactionId) -> Result<u64> {
        let mut opened = match File::open(&pth) {
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

    /// The frequencies of all the CPUs in the system, as reported by the kernel
    /// CPU IDs are implicit in the position, values will be 0 if an error occured
    pub fn kernel_cpu_freqs(&self, tid: &TransactionId) -> Result<Vec<u64>> {
        let mut ret = Vec::new();
        let base = Path::new(BASE_CPU_DIR);

        for cpu in 0..self.nprocs {
            let shared_path = base.join(format!("cpu{}", cpu));
            let kernel_path = shared_path.join("cpufreq/scaling_cur_freq");
            let parsed = self.read_freq(kernel_path, tid)?;
            ret.push(parsed);
        }

        Ok(ret)
    }

    pub fn instant_cpu_util(&self, tid: &TransactionId) -> Result<CPUUtilInstant> {
        CPUUtilInstant::get(tid)
    }
    pub fn compute_cpu_util(&self, inst1: &CPUUtilInstant, inst2: &CPUUtilInstant) -> CPUUtilPcts {
        if inst1.read_time > inst2.read_time {
            inst1 - inst2
        } else {
            inst2 - inst1
        }
    }
}

pub struct CpuFreqMonitor {
    cpu: Arc<CPUService>,
    _kernel_worker: Option<JoinHandle<()>>,
    _hardware_worker: Option<JoinHandle<()>>,
}

lazy_static::lazy_static! {
  pub static ref KERNEL_FREQ_WORKER_TID: TransactionId = "KernelFreqMon".to_string();
  pub static ref HARDWARE_FREQ_WORKER_TID: TransactionId = "HardwareFreqMon".to_string();
}
impl CpuFreqMonitor {
    pub fn boxed(kernel_freq_ms: Option<u64>, hardware_freq_ms: Option<u64>, tid: &TransactionId) -> Result<Arc<Self>> {
        let cpu = CPUService::boxed(tid)?;
        let kernel = match kernel_freq_ms {
            None | Some(0) => (None, None),
            Some(ms) => {
                // make sure kernel data is accessible
                cpu.kernel_cpu_freqs(tid)?;
                let (h, t) =
                    threading::os_thread(ms, KERNEL_FREQ_WORKER_TID.clone(), Arc::new(Self::kernel_frequencies))?;
                (Some(h), Some(t))
            }
        };
        let hardware = match hardware_freq_ms {
            None | Some(0) => (None, None),
            Some(ms) => {
                // make sure hardware data is accessible
                cpu.hardware_cpu_freqs(tid)?;
                let (h, t) = threading::os_thread(
                    ms,
                    HARDWARE_FREQ_WORKER_TID.clone(),
                    Arc::new(Self::hardware_frequencies),
                )?;
                (Some(h), Some(t))
            }
        };
        let r = Arc::new(CpuFreqMonitor {
            cpu,
            _kernel_worker: kernel.0,
            _hardware_worker: hardware.0,
        });
        if let Some(tx) = kernel.1 {
            tx.send(r.clone())?;
        }
        if let Some(tx) = hardware.1 {
            tx.send(r.clone())?;
        }
        Ok(r)
    }

    /// Log the kernel CPU frequencies, CPU ID is the entry in the vec
    pub fn kernel_frequencies(&self, tid: &TransactionId) {
        match self.cpu.kernel_cpu_freqs(tid) {
            Ok(h) => info!(tid=%tid, frequencies=?h, "Kernel CPU Frequencies"),
            Err(e) => error!(tid=%tid, error=%e, "Failed to read kernel CPU frequencies"),
        }
    }

    /// Log the hardware CPU frequencies, CPU ID is the entry in the vec
    pub fn hardware_frequencies(&self, tid: &TransactionId) {
        match self.cpu.hardware_cpu_freqs(tid) {
            Ok(h) => info!(tid=%tid, frequencies=?h, "Hardware CPU Frequencies"),
            Err(e) => error!(tid=%tid, error=%e, "Failed to read hardware CPU frequencies"),
        }
    }
}

/// The CPU usage metrics reported by /proc/stat
pub struct CPUUtilInstant {
    read_time: time::Instant,
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
impl CPUUtilInstant {
    pub fn get(tid: &TransactionId) -> Result<Self> {
        let cpu_line = Self::read()?;
        Self::parse(cpu_line, tid)
    }
    fn read() -> Result<String> {
        let mut ret = String::new();
        let mut b = match File::open("/proc/stat") {
            Ok(f) => std::io::BufReader::new(f),
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
        let strs: Vec<&str> = line.split(" ").filter(|str| str.len() > 0).collect();
        Ok(Self {
            read_time: time::Instant::now(),
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
    fn safe_get_val(split_line: &Vec<&str>, pos: usize, tid: &TransactionId) -> Result<f64> {
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
    pub read_diff: time::Duration,
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
