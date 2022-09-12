pub mod energy_layer;
pub mod energy_service;
pub mod rapl;
pub mod perf;
pub mod energy_logging;
pub mod ipmi;

#[derive(Debug, serde::Deserialize, clap::Parser)]
#[clap(author, version, about)]
/// Energy monitoring configuring
pub struct EnergyConfig {
  /// Log RAPL energy consumption
  /// If 0 then RAPL is disabled
  #[clap(long, action, default_value_t=0)]
  pub rapl_freq_ms: u64,

  /// Log instantaneous power usage from IPMI
  /// If 0 then IPMI is disabled
  #[clap(long, action, default_value_t=0)]
  pub ipmi_freq_ms: u64,

  /// Log energy usage as monitored via `perf`
  /// If 0 then perf is disabled
  #[clap(long, action, default_value_t=0)]
  pub perf_freq_ms: u64,

  /// File path containing the IPMI password
  #[clap(long)]
  pub ipmi_pass_file: Option<String>,

  /// IP Address for the local IPMI endpoint
  #[clap(long)]
  pub ipmi_ip_addr: Option<String>,

  /// Folder to log energy metrics to
  #[clap(long)]
  pub log_folder: String,
}

impl EnergyConfig {
  pub fn perf_enabled(&self) -> bool {
    self.perf_freq_ms != 0
  }
  pub fn rapl_enabled(&self) -> bool {
    self.rapl_freq_ms != 0
  }
  pub fn ipmi_enabled(&self) -> bool {
    self.ipmi_freq_ms != 0
  }
}
