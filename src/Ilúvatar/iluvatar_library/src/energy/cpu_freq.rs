use crate::threading;
use crate::transaction::TransactionId;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::JoinHandle;
use tracing::{error, info};

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
    ) -> anyhow::Result<Arc<Self>> {
        let kernel = match kernel_freq_ms {
            None | Some(0) => (None, None),
            Some(ms) => {
                let (h, t) =
                    threading::os_thread(ms, KERNEL_FREQ_WORKER_TID.clone(), Arc::new(Self::kernel_frequencies))?;
                (Some(h), Some(t))
            },
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
            },
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

    fn read_freq(&self, pth: PathBuf, tid: &TransactionId) -> anyhow::Result<u64> {
        let mut opened = match std::fs::File::open(&pth) {
            Ok(b) => b,
            Err(e) => {
                bail_error!(error=%e, tid=tid, file=%pth.to_string_lossy(), "Unable to open cpu freq file")
            },
        };
        let mut buff = String::new();
        match opened.read_to_string(&mut buff) {
            Ok(_) => (),
            Err(e) => {
                bail_error!(error=%e, tid=tid, file=%pth.to_string_lossy(), "Unable to read cpu freq file into buffer")
            },
        };
        match buff[0..buff.len() - 1].parse::<u64>() {
            Ok(u) => Ok(u),
            Err(e) => {
                bail_error!(error=%e, tid=tid, data=%buff, "Unable to parse cpu freq buffer")
            },
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
                    error!(tid=tid, error=%e, "Failed to read kernel CPU frequencies");
                    return;
                },
            };
            frequencies.push(parsed);
        }
        info!(tid=tid, frequencies=?frequencies, "Kernel CPU Frequencies");
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
                    error!(tid=tid, error=%e, "Failed to read hardware CPU frequencies");
                    return;
                },
            };
            frequencies.push(parsed);
        }
        info!(tid=tid, frequencies=?frequencies, "Hardware CPU Frequencies");
    }
}
