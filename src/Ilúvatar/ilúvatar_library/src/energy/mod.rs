pub mod energy_layer;
pub mod energy_service;
pub mod rapl;
pub mod perf;
pub mod energy_logging;

#[derive(Debug, serde::Deserialize, clap::Parser)]
#[clap(author, version, about)]
/// Energy monitoring configuring
pub struct EnergyConfig {
  /// Log RAPL energy consumption
  #[clap(long, action)]
  pub enable_rapl: bool,

  /// Log instantaneous power usage from IPMI
  #[clap(long, action)]
  pub enable_ipmi: bool,

  /// Log energy usage as monitored via `perf`
  #[clap(long, action)]
  pub enable_perf: bool,

  /// Frequecy in seconds to track perf energy metric
  #[clap(long)]
  pub perf_stat_duration_sec: Option<u64>,

  /// File path containing the IPMI password
  #[clap(long)]
  pub ipmi_pass_file: Option<String>,

  /// Frequency of energy logging in milliseconds
  #[clap(long)]
  pub log_freq_ms: u64,

  /// Folder to log energy and function metrics to
  #[clap(long)]
  pub log_folder: String,
}
