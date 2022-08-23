use std::sync::Arc;
use iluvatar_library::{transaction::{TransactionId, ENERGY_MONITOR_TID}, energy::{EnergyConfig, energy_logging::EnergyLogger}};
use clap::Parser;

pub mod read;
pub mod structs;

fn main() -> anyhow::Result<()> {
  let tid: &TransactionId = &ENERGY_MONITOR_TID;
  let config = Arc::new(EnergyConfig::parse());

  let _mon = EnergyLogger::boxed(config, tid)?;

  loop {
    // sleep forever
    std::thread::sleep(std::time::Duration::MAX);
  }
}
