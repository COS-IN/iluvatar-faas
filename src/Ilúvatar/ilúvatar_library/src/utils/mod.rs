pub mod file_utils;
pub use file_utils as file;
pub mod port_utils;
pub use port_utils as port;
pub mod config_utils;
pub use config_utils as config;
pub mod cgroup_utils;
pub use cgroup_utils as cgroup;
pub mod timing_utils;
pub use timing_utils as timing; 

use crate::utils::port::Port;
use crate::transaction::TransactionId;
use crate::bail_error;
use std::collections::HashMap;
use std::process::{Command, Output, Child, Stdio};
use tracing::debug;
use anyhow::Result;

/// get the fully qualified domain name for a function from its name and version
pub fn calculate_fqdn(function_name: &String, function_version: &String) -> String {
  format!("{}-{}", function_name, function_version)
}

pub fn calculate_invoke_uri(address: &str, port: Port) -> String {
  format!("http://{}:{}/invoke", address, port)
}

pub fn calculate_base_uri(address: &str, port: Port) -> String {
  format!("http://{}:{}/", address, port)
}

fn prepare_cmd<S>(cmd_pth: &S, args: &Vec<&str>, env: Option<&HashMap<String, String>>, tid: &TransactionId) -> Result<Command>
  where S: AsRef<std::ffi::OsStr> + ?Sized + std::fmt::Display {
  
  debug!(tid=%tid, command=%cmd_pth, args=?args, environment=?env, "executing host command");
  if ! std::path::Path::new(&cmd_pth).exists() {
    bail_error!(tid=%tid, command=%cmd_pth, "Command does not exists");
  }
  let mut cmd = Command::new(cmd_pth);
  cmd.args(args);
  if let Some(env) = env {
    cmd.envs(env);
  }
  Ok(cmd)
}

/// Executes the specified executable with args and environment
/// cmd_pth **must** be an absolute path
pub fn execute_cmd<S>(cmd_pth: &S, args: &Vec<&str>, env: Option<&HashMap<String, String>>, tid: &TransactionId) -> Result<Output> 
  where S: AsRef<std::ffi::OsStr> + ?Sized + std::fmt::Display {
  
  let mut cmd = prepare_cmd(cmd_pth, args, env, tid)?;
  match cmd.output() {
    Ok(out) => Ok(out),
    Err(e) => bail_error!(tid=%tid, command=%cmd_pth, error=%e, "Running command failed")
  }
}

/// Executes the specified executable with args and environment
/// cmd_pth **must** be an absolute path
/// All std* pipes will be sent to null for the process
pub fn execute_cmd_nonblocking<S>(cmd_pth: &S, args: &Vec<&str>, env: Option<&HashMap<String, String>>, tid: &TransactionId) -> Result<Child>
  where S: AsRef<std::ffi::OsStr> + ?Sized + std::fmt::Display {

  debug!(tid=%tid, command=%cmd_pth, args=?args, environment=?env, "executing host command");
  let mut cmd = prepare_cmd(cmd_pth, args, env, tid)?;
  cmd.stdout(Stdio::null())
      .stdin(Stdio::null())
      .stderr(Stdio::null());

  match cmd.spawn() {
    Ok(out) => Ok(out),
    Err(e) => bail_error!(tid=%tid, command=%cmd_pth, error=%e, "Spawning non-blocking command failed")
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use rstest::rstest;

  #[rstest]
  #[case("localhost", 8080, "http://localhost:8080/invoke")]
  #[case("localhost", 8081, "http://localhost:8081/invoke")]
  #[case("localhost", 19840, "http://localhost:19840/invoke")]
  #[case("0.0.0.0", 8080, "http://0.0.0.0:8080/invoke")]
  fn format_invoke_correctly(#[case] addr: &str, #[case] port: Port, #[case] expected: &str){
    let ans = calculate_invoke_uri(addr, port);
    assert_eq!(expected, ans);
  }

  #[rstest]
  #[case("localhost", 8080, "http://localhost:8080/")]
  #[case("localhost", 8081, "http://localhost:8081/")]
  #[case("localhost", 19840, "http://localhost:19840/")]
  #[case("0.0.0.0", 8080, "http://0.0.0.0:8080/")]
  fn format_base_correctly(#[case] addr: &str, #[case] port: Port, #[case] expected: &str){
    let ans = calculate_base_uri(addr, port);
    assert_eq!(expected, ans);
  }

  #[rstest]
  #[case("hello", "080", "hello-080")]
  #[case("cnn", "1.0.2", "cnn-1.0.2")]
  #[case("video", "1.5.2", "video-1.5.2")]
  #[case("alpine", "0.0.1", "alpine-0.0.1")]
  fn format_fqdn(#[case] name: &str, #[case] version: &str, #[case] expected: &str){
    let ans = calculate_fqdn(&name.to_string(), &version.to_string());
    assert_eq!(expected, ans);
  }
}