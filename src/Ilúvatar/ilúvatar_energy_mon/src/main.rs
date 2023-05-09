use std::sync::Arc;
use iluvatar_library::{transaction::{TransactionId, ENERGY_MONITOR_TID}, energy::{EnergyConfig, energy_logging::EnergyLogger}, logging::{LoggingConfig, start_tracing}, utils::wait_for_exit_signal};
use clap::Parser;

pub mod read;
pub mod structs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  let tid: &TransactionId = &ENERGY_MONITOR_TID;
  let config = Arc::new(EnergyConfig::parse());

  let log_config = Arc::new(LoggingConfig {
    level: "debug".to_string(),
    directory: Some(config.log_folder.clone()),
    basename: "energy_monitor".to_string(),
    spanning: "NONE".to_string(),
    flame: None,
    span_energy_monitoring: false,
  });
  let _guard = start_tracing(log_config, &"energy_monitor".to_string(), tid)?;

  let _mon = EnergyLogger::boxed(Some(&config), tid).await?;
  wait_for_exit_signal(tid).await?;
  Ok(())
}
