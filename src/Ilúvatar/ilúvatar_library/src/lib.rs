use transaction::TransactionId;
use utils::execute_cmd;

pub mod utils;
pub mod transaction;
pub mod macros;
pub mod logging;
pub mod types;
pub mod graphite;
pub mod api_register;
pub mod energy;
pub mod cpu_interaction;
pub mod continuation;

/// The number of logical processors on the system
pub fn nproc(tid: &TransactionId) -> anyhow::Result<u32> {
  let nproc = execute_cmd("/usr/bin/nproc", &vec!["--all"], None, tid)?;
  let stdout = String::from_utf8_lossy(&nproc.stdout);
  match stdout[0..stdout.len()-1].parse::<u32>() {
    Ok(u) => Ok(u),
    Err(e) => anyhow::bail!("Unable to parse nproc result because of error: '{}'", e),
  }
}
