use std::sync::Arc;
use iluvatar_library::{transaction::{TransactionId, ENERGY_MONITOR_TID}, energy::{EnergyConfig, energy_logging::EnergyLogger}, logging::{LoggingConfig, start_tracing}, graphite::GraphiteConfig, utils::wait_for_exit_signal};
use clap::Parser;

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
  let _guard = start_tracing(log_config, graphite_cfg,&"energy_monitor".to_string(), tid)?;

  let _mon = EnergyLogger::boxed(config, tid).await?;
  wait_for_exit_signal(tid).await?;
  Ok(())
}
