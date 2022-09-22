use std::sync::Arc;
use iluvatar_library::{transaction::{TransactionId, ENERGY_MONITOR_TID}, energy::{EnergyConfig, energy_logging::EnergyLogger}, logging::{LoggingConfig, start_tracing}, graphite::GraphiteConfig};
use clap::Parser;
use signal_hook::{consts::signal::{SIGINT, SIGTERM, SIGUSR1, SIGUSR2, SIGQUIT}, iterator::Signals};

pub mod read;
pub mod structs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  let tid: &TransactionId = &ENERGY_MONITOR_TID;
  let config = Arc::new(EnergyConfig::parse());

  let log_config = Arc::new(LoggingConfig {
    level: "info".to_string(),
    directory: config.log_folder.clone(),
    basename: "energy_monitor".to_string(),
    spanning: "NONE".to_string(),
    flame: "".to_string(),
    span_energy_monitoring: false,
  });
  let graphite_cfg = Arc::new(GraphiteConfig {
    address: "".to_string(),
    api_port: 0,
    ingestion_port: 0,
    ingestion_udp: false,
    enabled: false,
  });
  let _guard = start_tracing(log_config, graphite_cfg,&"energy_monitor".to_string())?;

  let sigs = vec![SIGINT, SIGTERM, SIGUSR1, SIGUSR2, SIGQUIT];
  let mut signals = Signals::new(&sigs)?;

  let _mon = EnergyLogger::boxed(config, tid).await?;

  'outer: for signal in &mut signals {
    match signal {
      _term_sig => { // got a termination signal
        break 'outer;
      }
    }
  }
  iluvatar_library::continuation::GLOB_CONT_CHECK.signal_application_exit(tid);

  Ok(())
}
