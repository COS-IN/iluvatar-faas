use std::{path::{Path, PathBuf}, fs::File, io::Read, sync::Arc};
use tracing::{error, debug};
use anyhow::Result;
use crate::{transaction::TransactionId, nproc};

const BASE_CPU_DIR: &str = "/sys/devices/system/cpu";

pub struct CPUService {
  nprocs: usize,
}

impl CPUService {
  pub fn boxed(tid: &TransactionId) -> Result<Arc<Self>> {
    let procs = nproc(tid, true)?;
    Ok(Arc::new(CPUService {
      nprocs: procs as usize
    }))
  }

  /// The frequencies of all the CPUs in the system, as reported by the hardware
  /// CPU IDs are implicit in the position, values will be 0 if an error occured
  pub fn hardware_cpu_freqs(&self, tid: &TransactionId) -> Vec<u64> {
    let mut ret = Vec::new();
    let base = Path::new(BASE_CPU_DIR);

    for cpu in 0..self.nprocs {
      let shared_path = base.join(format!("cpu{}", cpu));
      let hw_path = shared_path.join("cpufreq/cpuinfo_cur_freq");
      let parsed = self.read_freq(hw_path, tid, false);
      ret.push(parsed);
    }

    ret
  }

  fn read_freq(&self, pth: PathBuf, tid: &TransactionId, log_error: bool) -> u64 {
    let mut opened = match File::open(&pth) {
      Ok(b) => b,
      Err(e) => {
        if log_error {
          error!(error=%e, tid=%tid, file=%pth.to_string_lossy(), "Unable to open cpu freq file into buffer");
        } else {
          debug!(error=%e, tid=%tid, file=%pth.to_string_lossy(), "Unable to open cpu freq file into buffer");
        }
        return 0;
      },
    };
    let mut buff = String::new();
    match opened.read_to_string(&mut buff) {
      Ok(_) => (),
      Err(e) => {
        error!(error=%e, tid=%tid, file=%pth.to_string_lossy(), "Unable to read cpu freq file into buffer");
        return 0;      },
    };
    match buff[0..buff.len()-1].parse::<u64>() {
      Ok(u) => u,
      Err(e) => {
        error!(error=%e, tid=%tid, data=%buff, "Unable to parse cpu freq buffer");
        0
      },
    }
  }

  /// The frequencies of all the CPUs in the system, as reported by the kernel
  /// CPU IDs are implicit in the position, values will be 0 if an error occured
  pub fn kernel_cpu_freqs(&self, tid: &TransactionId) -> Vec<u64> {
    let mut ret = Vec::new();
    let base = Path::new(BASE_CPU_DIR);

    for cpu in 0..self.nprocs {
      let shared_path = base.join(format!("cpu{}", cpu));
      let kernel_path = shared_path.join("cpufreq/scaling_cur_freq");
      let parsed = self.read_freq(kernel_path, tid, true);
      ret.push(parsed);
    }

    ret
  }
}
