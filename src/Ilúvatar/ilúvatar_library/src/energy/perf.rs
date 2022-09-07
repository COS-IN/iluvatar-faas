use crate::{utils::execute_cmd_nonblocking, transaction::TransactionId};
use std::{process::Child, time::{Duration, SystemTime}};
use anyhow::Result;
use tracing::{warn, debug};

/// Start perf stat tracking of [power/energy-pkg/](https://stackoverflow.com/questions/55956287/perf-power-consumption-measure-how-does-it-work)
/// The csv results will be put into [outfile](https://manpages.ubuntu.com/manpages/bionic/man1/perf-stat.1.html)
pub fn start_perf_stat<S>(outfile: &S, tid: &TransactionId, stat_duration_ms: u64) -> Result<Child> 
  where S: AsRef<str> + ?Sized + std::fmt::Display {

  let st = stat_duration_ms.to_string();
  let mut args = vec!["stat", "-I", &st.as_str(), "-x", ",", "--output", outfile.as_ref()];
  if include_instructions(tid)? {
    debug!(tid=%tid, "Enabling retired instructions perf metric");
    args.push("-M");
    args.push("Instructions");
  }
  if include_energy_ram(tid)? {
    debug!(tid=%tid, "Enabling energy-ram perf counter");
    args.push("-e");
    args.push("power/energy-ram/");
  }
  if include_energy_pkg(tid)? {
    debug!(tid=%tid, "Enabling energy-ram perf counter");
    args.push("-e");
    args.push("power/energy-pkg/");
  }
  execute_cmd_nonblocking("/usr/bin/perf", &args, None, tid)
}

fn include_instructions(tid: &TransactionId) -> Result<bool> {
  let args = vec!["stat", "-M", "Instructions", "-I", "100"];
  test_args(tid, &args)
}

fn include_energy_pkg(tid: &TransactionId) -> Result<bool> {
  let args = vec!["stat", "-e", "power/energy-pkg/", "-I", "100"];
  test_args(tid, &args)
}

fn include_energy_ram(tid: &TransactionId) -> Result<bool> {
  let args = vec!["stat", "-e", "power/energy-ram/", "-I", "100"];
  test_args(tid, &args)
}

fn test_args(tid: &TransactionId, args: &Vec<&str>)-> Result<bool> {
  let mut child = execute_cmd_nonblocking("/usr/bin/perf", args, None, tid)?;
  let start = SystemTime::now();
  
  let timeout = Duration::from_secs(1);
  while start.elapsed()? < timeout {
    match child.try_wait() {
      Ok(exit) => match exit {
        // an exit means the metric doesn't exist
        Some(_) => return Ok(false),
        None => {
          // didn't exit yet
          std::thread::sleep(Duration::from_millis(5));
          continue;
        },
      },
      Err(e) => {
        warn!(tid=%tid, error=%e, "Checking if `{:?}` args existed encountered an error", args);
        return Ok(false);
      },
    };
  }
  // probably would have errored out after a second
  // safe to assume metric exists
  child.kill()?;
  Ok(true)
}
