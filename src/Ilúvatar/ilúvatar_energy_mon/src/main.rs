use clap::ArgMatches;
use iluvatar_library::{utils::config::get_val, transaction::{TransactionId, ENERGY_MONITOR_TID}, energy::energy_log::log_energy_usage};
use read::analyze_logs;
use anyhow::Result;

pub mod config;
pub mod read;
pub mod structs;

fn main() -> anyhow::Result<()> {
  let matches = config::parse();

  match matches.subcommand() {
    ("analyze", Some(submatches)) => analyze_logs(&matches, submatches),
    ("monitor", Some(submatches)) => energy_monitor(&matches, submatches),
    (text,_) => anyhow::bail!("Unknown command {}, try --help", text),
  }
}


fn energy_monitor(_matches: &ArgMatches, submatches: &ArgMatches) -> Result<()> {
  let outdir: String = get_val("outdir", submatches)?;
  let poll_sec: u64 = get_val("poll", submatches)?;
  let tid: &TransactionId = &ENERGY_MONITOR_TID;
  log_energy_usage(outdir.as_str(), tid, poll_sec)?;
  Ok(())
}
