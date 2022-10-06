use std::{sync::Arc, path::Path};
use crate::{transaction::TransactionId, energy::{perf::start_perf_stat, EnergyConfig}};
use tracing::{error, debug};
use anyhow::Result;
use super::{ipmi::IPMIMonitor, rapl::RaplMonitor, process_pct::ProcessMonitor};

/// Struct that repeatedly checks energy usage from various sources
/// They are then stored in a timestamped log file
/// Optionall can inject additional information to be included in each line
#[allow(unused)]
pub struct EnergyLogger {
  config: Arc<EnergyConfig>,
  rapl: Option<Arc<RaplMonitor>>,
  ipmi: Option<Arc<IPMIMonitor>>,
  proc: Option<Arc<ProcessMonitor>>,
  _perf_child: Option<std::process::Child>,
}

impl EnergyLogger {
  pub async fn boxed(config: Arc<EnergyConfig>, tid: &TransactionId) -> Result<Arc<Self>> {
    let parf_child = match config.perf_enabled() {
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
        Some(start_perf_stat(&f, tid, config.perf_freq_ms).await?)  
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

    let i = Arc::new(EnergyLogger {
      config,
      rapl,
      ipmi,
      proc,
      _perf_child: parf_child,
    });
    Ok(i)
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
