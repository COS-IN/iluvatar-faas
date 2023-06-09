use std::{sync::Arc, path::Path};
use crate::{transaction::TransactionId, energy::{perf::start_perf_stat, EnergyConfig}, cpu_interaction::CpuFreqMonitor};
use tracing::{error, debug};
use anyhow::Result;
use super::{ipmi::IPMIMonitor, rapl::RaplMonitor, process_pct::ProcessMonitor};

/// Struct that repeatedly checks energy usage from various sources
/// They are then stored in a timestamped log file
/// Optionally can inject additional information to be included in each line
#[allow(unused)]
pub struct EnergyLogger {
  rapl: Option<Arc<RaplMonitor>>,
  ipmi: Option<Arc<IPMIMonitor>>,
  proc: Option<Arc<ProcessMonitor>>,
  _perf_child: Option<std::process::Child>,
  cpu: Option<Arc<CpuFreqMonitor>>,
}

impl EnergyLogger {
  pub async fn boxed(config: Option<&Arc<EnergyConfig>>, tid: &TransactionId) -> Result<Arc<Self>> {
    let (perf_child, ipmi, rapl, proc, cpu) = match config {
      Some(config) => {
        let perf_child = match config.perf_enabled() {
          true => {
            let perf_file = Path::new(&config.log_folder);
            let perf_file = perf_file.join("energy-perf.log");
            debug!(tid=%tid, "Starting perf energy monitoring");
            let f = match perf_file.to_str() {
              Some(f) => f,
              None => {
                anyhow::bail!("Failed to start perf because the log file could not be formatted properly");
              },
            };
            if let Some(ms) = config.perf_freq_ms {
              Some(start_perf_stat(&f, tid, ms).await?)  
            } else {
              None
            }
          },
          false => None
        };
    
        let ipmi = match config.ipmi_enabled() {
          true => {
            debug!(tid=%tid, "Starting IPMI energy monitoring");
            Some(IPMIMonitor::boxed(config.clone(), tid)?)
          },
          false => None,
        };
    
        let rapl = match config.rapl_enabled() {
          true => {
            debug!(tid=%tid, "Starting rapl energy monitoring");
            Some(RaplMonitor::boxed(config.clone(), tid)?)
          },
          false => None,
        };
    
        let proc = match config.process_enabled() {
          true => {
            debug!(tid=%tid, "Starting process energy monitoring");
            Some(ProcessMonitor::boxed(config.clone(), tid)?)
          },
          false => None,
        };
        
        let cpu_mon = match config.cpu_freqs_enabled() {
          true => {
            debug!(tid=%tid, "Starting cpu frequency monitoring");
            Some(CpuFreqMonitor::boxed(config.kernel_cpu_frequencies_freq_ms, config.hardware_cpu_frequencies_freq_ms, tid)?)
          },
          false => None,
        };
        (perf_child, ipmi, rapl, proc, cpu_mon)
      },
      None => (None,None,None,None,None),
    };

    Ok(Arc::new(EnergyLogger {
      rapl, ipmi, proc, _perf_child: perf_child, cpu
    }))
  }
}

impl Drop for EnergyLogger {
  fn drop(&mut self) {
    match self._perf_child.take() {
      Some(mut c) => match c.kill() {
        Ok(_) => (),
        Err(e) => error!(error=%e, "Failed to kill perf child!"),
      },
      None => (),
    }
  }
}
