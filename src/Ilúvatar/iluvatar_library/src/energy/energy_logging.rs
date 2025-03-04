use super::{ipmi::IPMIMonitor, process_pct::ProcessMonitor, rapl::RaplMonitor};
use crate::energy::cpu_freq::CpuFreqMonitor;
use crate::{
    energy::{perf::start_perf_stat, tegrastats::start_tegrastats, EnergyConfig},
    transaction::TransactionId,
};
use anyhow::Result;
use std::{path::Path, sync::Arc};
use tracing::{debug, error};

/// Struct that repeatedly checks energy usage from various sources
/// They are then stored in a timestamped log file
/// Optionally can inject additional information to be included in each line
#[allow(unused)]
pub struct EnergyLogger {
    rapl: Option<Arc<RaplMonitor>>,
    ipmi: Option<Arc<IPMIMonitor>>,
    proc: Option<Arc<ProcessMonitor>>,
    _perf_child: Option<std::process::Child>,
    _tegra_child: Option<std::process::Child>,
    cpu: Option<Arc<CpuFreqMonitor>>,
    config: Option<Arc<EnergyConfig>>,
}

impl EnergyLogger {
    pub async fn boxed(config: Option<&Arc<EnergyConfig>>, tid: &TransactionId) -> Result<Arc<Self>> {
        let (perf_child, tegra_child, ipmi, rapl, proc, cpu) = match config {
            Some(config) => {
                let perf_child = match config.perf_enabled() {
                    true => {
                        let perf_file = Path::new(&config.log_folder);
                        let perf_file = perf_file.join("energy-perf.log");
                        debug!(tid = tid, "Starting perf energy monitoring");
                        let f = match perf_file.to_str() {
                            Some(f) => f,
                            None => {
                                anyhow::bail!(
                                    "Failed to start perf because the log file could not be formatted properly"
                                );
                            },
                        };
                        if let Some(ms) = config.perf_freq_ms {
                            Some(start_perf_stat(&f, tid, ms).await?)
                        } else {
                            None
                        }
                    },
                    false => None,
                };

                let tegra_child = match config.tegra_enabled() {
                    true => {
                        let tegra_file = Path::new(&config.log_folder);
                        let tegra_file = tegra_file.join("tegrastats.log");
                        debug!(tid = tid, "Starting tegra monitoring");
                        let f = match tegra_file.to_str() {
                            Some(f) => f,
                            None => {
                                anyhow::bail!(
                                    "Failed to start tegra because the log file could not be formatted properly"
                                );
                            },
                        };
                        if let Some(ms) = config.tegra_freq_ms {
                            Some(start_tegrastats(&f, tid, ms).await?)
                        } else {
                            None
                        }
                    },
                    false => None,
                };

                let ipmi = match config.ipmi_enabled() {
                    true => {
                        debug!(tid = tid, "Starting IPMI energy monitoring");
                        Some(IPMIMonitor::boxed(config.clone(), tid)?)
                    },
                    false => None,
                };

                let rapl = match config.rapl_enabled() {
                    true => {
                        debug!(tid = tid, "Starting rapl energy monitoring");
                        Some(RaplMonitor::boxed(config.clone(), tid)?)
                    },
                    false => None,
                };

                let proc = match config.process_enabled() {
                    true => {
                        debug!(tid = tid, "Starting process energy monitoring");
                        Some(ProcessMonitor::boxed(config.clone(), tid)?)
                    },
                    false => None,
                };

                let cpu_mon = match config.cpu_freqs_enabled() {
                    true => {
                        debug!(tid = tid, "Starting cpu frequency monitoring");
                        Some(CpuFreqMonitor::boxed(
                            config.kernel_cpu_frequencies_freq_ms,
                            config.hardware_cpu_frequencies_freq_ms,
                            tid,
                        )?)
                    },
                    false => None,
                };
                (perf_child, tegra_child, ipmi, rapl, proc, cpu_mon)
            },
            None => (None, None, None, None, None, None),
        };

        Ok(Arc::new(EnergyLogger {
            rapl,
            ipmi,
            proc,
            _perf_child: perf_child,
            _tegra_child: tegra_child,
            cpu,
            config: config.cloned(),
        }))
    }

    pub fn get_reading_time_ms(&self) -> u64 {
        if let Some(c) = &self.config {
            if let Some(ms) = c.ipmi_freq_ms {
                if ms > 0 {
                    return ms;
                }
            }
            if let Some(ms) = c.rapl_freq_ms {
                if ms > 0 {
                    return ms;
                }
            }
        }
        0
    }

    pub fn readings_enabled(&self) -> bool {
        self.ipmi.is_some() || self.rapl.is_some()
    }

    /// Return the latest energy reading in (timestamp_ns, Joules)
    pub fn get_latest_reading(&self) -> (i128, f64) {
        if let Some(ipmi) = &self.ipmi {
            return ipmi.get_latest_reading();
        }
        if let Some(rapl) = &self.rapl {
            return rapl.get_latest_reading();
        }
        (0, 0.0)
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
