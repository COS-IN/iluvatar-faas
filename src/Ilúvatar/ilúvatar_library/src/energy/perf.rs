use crate::{utils::execute_cmd_nonblocking, transaction::TransactionId};
use std::process::Child;

/// Start perf stat tracking of [power/energy-pkg/](https://stackoverflow.com/questions/55956287/perf-power-consumption-measure-how-does-it-work)
/// The csv results will be put into [outfile](https://manpages.ubuntu.com/manpages/bionic/man1/perf-stat.1.html)
pub fn start_perf_stat(outfile: &String, tid: &TransactionId, stat_duration_sec: u64) -> anyhow::Result<Child> {
  let dur = (stat_duration_sec * 1000).to_string();
  let args = vec!["stat", "-e", "power/energy-pkg/", "-I", &dur.as_str(), "-x", ",", "--output", outfile.as_str()];
  execute_cmd_nonblocking("/usr/bin/perf", &args, None, tid)
}
