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
pub mod threading;

/// The number of logical processors on the system
/// [all] returns all the processors on the system when true
/// When false, only those the process is allowed to use
pub fn nproc(tid: &TransactionId, all: bool) -> anyhow::Result<u32> {
  let args = match all {
    true => vec!["--all"],
    false => vec![],
  };
  let nproc = execute_cmd("/usr/bin/nproc", &args, None, tid)?;
  let stdout = String::from_utf8_lossy(&nproc.stdout);
  match stdout[0..stdout.len()-1].parse::<u32>() {
    Ok(u) => Ok(u),
    Err(e) => anyhow::bail!("Unable to parse nproc result because of error: '{}'", e),
  }
}
