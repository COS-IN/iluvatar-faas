use std::time::SystemTime;
use std::fs::read_to_string;
use anyhow::Result;
use crate::bail_error;
use crate::transaction::TransactionId;

const RAPL_PTH: &str = "/sys/devices/virtual/powercap/intel-rapl/intel-rapl:0/energy_uj";
const MAX_ENERGY_PTH: &str = "/sys/devices/virtual/powercap/intel-rapl/intel-rapl:0/max_energy_range_uj";

/// Basic interactions with (RAPL)[https://lwn.net/Articles/545745/] 
pub struct RAPL {
  max_uj: u128,
}
impl RAPL {
  pub fn new() -> Result<Self> {
    Ok(RAPL {
      max_uj: RAPL::max_uj()?
    })
  }

  pub fn record(&self) -> Result<RAPLQuery> {
    Ok(RAPLQuery {
      start: SystemTime::now(),
      start_uj: RAPL::get_uj()?,
    })
  }

  /// Return the elapsed time and used uj between the two queries
  ///   right must have happened before left or it will error
  pub fn difference(&self, left: &RAPLQuery, right: &RAPLQuery, tid: &TransactionId) -> Result<(u128, u128)> {
    let elapsed = match left.start.duration_since(right.start) {
        Ok(t) => t,
        Err(e) => bail_error!(tid=%tid, error=%e, "Clock error reading RAPL information"),
     }.as_micros();
    let uj: u128;
    if left.start_uj < right.start_uj {
      uj = left.start_uj + (self.max_uj - right.start_uj);
      println!("going around the energy horn {uj} = {} + ({} - {})", left.start_uj, self.max_uj, right.start_uj);
    } else {
      uj = left.start_uj - right.start_uj;
    }
    Ok((elapsed, uj))
  }

  fn get_uj() -> Result<u128> {
    RAPL::read_uj(RAPL_PTH)
  }
  fn max_uj() -> Result<u128> {
    RAPL::read_uj(MAX_ENERGY_PTH)
  }

  fn read_uj(pth: &str) -> Result<u128> {
    Ok(read_to_string(pth)?.strip_suffix("\n").unwrap().parse::<u128>()?)
  }
}

pub struct RAPLQuery {
  pub start: SystemTime,
  pub start_uj: u128,
}
