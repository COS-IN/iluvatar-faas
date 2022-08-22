use std::time::SystemTime;
use std::fs::read_to_string;
use anyhow::Result;
use crate::bail_error;
use crate::transaction::TransactionId;

const RAPL_PTH: &str = "/sys/devices/virtual/powercap/intel-rapl/intel-rapl:0/energy_uj";

/// Basic interactions with (RAPL)[https://lwn.net/Articles/545745/] 
#[derive(Debug)]
pub struct RAPL {
  start: SystemTime,
  start_uj: u128,
}
impl RAPL {
  pub fn record() -> Result<Self> {
    Ok(RAPL {
      start: SystemTime::now(),
      start_uj: get_uj()?,
    })
  }

  /// Return the elapsed time and used uj since 
  ///   this was started or poll was called
  pub fn difference(&self, other: &RAPL, tid: &TransactionId) -> Result<(u128, u128)> {
    let elapsed = self.start.duration_since(other.start)?.as_micros();
    if self.start_uj < other.start_uj {
      bail_error!(tid=%tid, left=?self, right=?other, "Got a reduction in uj between two RAPL calls");
    }
    let uj = self.start_uj - other.start_uj;
    Ok((elapsed, uj))
  }
}

fn get_uj() -> Result<u128> {
  Ok(read_to_string(RAPL_PTH)?.strip_suffix("\n").unwrap().parse::<u128>()?)
}
