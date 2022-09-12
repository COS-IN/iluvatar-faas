use std::{sync::Arc, path::Path};
use crate::{transaction::TransactionId, energy::{perf::start_perf_stat, EnergyConfig}};
use tracing::error;
use anyhow::Result;
use super::{ipmi::IPMIMonitor, rapl::RaplMonitor};

/// Struct that repeatedly checks energy usage from various sources
/// They are then stored in a timestamped log file
/// Optionall can inject additional information to be included in each line
#[allow(unused)]
pub struct EnergyLogger {
  config: Arc<EnergyConfig>,
  rapl: Option<Arc<RaplMonitor>>,
  ipmi: Option<Arc<IPMIMonitor>>,
  _perf_child: Option<std::process::Child>,
}

impl EnergyLogger {
  pub fn boxed(config: Arc<EnergyConfig>, tid: &TransactionId) -> Result<Arc<Self>> {
    let parf_child = match config.perf_enabled() {
      true => {
        let perf_file = Path::new(&config.log_folder);
        let perf_file = perf_file.join("energy-perf.log");
        Some(start_perf_stat(&perf_file.to_str().unwrap(), tid, config.perf_freq_ms)?)  
      },
      false => None
    };

    let ipmi = match config.ipmi_enabled() {
      true => Some(IPMIMonitor::boxed(config.clone(), tid)?),
      false => None,
    };

    let rapl = match config.rapl_enabled() {
      true => Some(RaplMonitor::boxed(config.clone(), tid)?),
      false => None,
    };

    let i = Arc::new(EnergyLogger {
      config,
      rapl,
      _perf_child: parf_child,
      ipmi
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
