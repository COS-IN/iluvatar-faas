//! Ilúvatar Library
//!
//! This crate is for shared code and utilities that are not specific to any executable in the Ilúvatar stack.

use std::{fs::File, io::Read};
use tracing::error;
use transaction::TransactionId;
use utils::execute_cmd_checked;

pub mod transaction;
pub mod utils;
#[macro_use]
pub mod macros;
pub mod api_register;
pub mod characteristics_map;
pub mod continuation;
pub mod cpu_interaction;
pub mod energy;
pub mod influx;
pub mod logging;
pub mod threading;
pub mod types;

/// The number of logical processors on the system
/// * `all` - returns all the processors on the system when true
/// When false, only those the process is allowed to use
pub fn nproc(tid: &TransactionId, all: bool) -> anyhow::Result<u32> {
    let args = match all {
        true => vec!["--all"],
        false => vec![],
    };
    let nproc = execute_cmd_checked("/usr/bin/nproc", &args, None, tid)?;
    let stdout = String::from_utf8_lossy(&nproc.stdout);
    if stdout.len() == 0 {
        anyhow::bail!("`nproc` output was empty");
    }
    match stdout[0..stdout.len() - 1].parse::<u32>() {
        Ok(u) => Ok(u),
        Err(e) => anyhow::bail!("Unable to parse nproc result because of error: '{}'", e),
    }
}

/// Returns the one-minute system load average
/// If an error occurs, -1.0 is returned
pub fn load_avg(tid: &TransactionId) -> f64 {
    let mut file = match File::open("/proc/loadavg") {
        Ok(f) => f,
        Err(e) => {
            error!(tid=%tid, error=%e, "Failed to open /proc/loadavg");
            return -1.0;
        }
    };
    let mut buff = String::new();
    match file.read_to_string(&mut buff) {
        Ok(f) => f,
        Err(e) => {
            error!(tid=%tid, error=%e, "Failed to read /proc/loadavg");
            return -1.0;
        }
    };
    let lines: Vec<&str> = buff.split(" ").filter(|str| str.len() > 0).collect();
    let min = lines[0];
    match min.parse::<f64>() {
        Ok(r) => r,
        Err(e) => {
            error!(tid=%tid, "error parsing float from uptime {}: {}", min, e);
            -1.0
        }
    }
}
