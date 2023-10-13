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

use crate::bail_error;
use crate::transaction::TransactionId;
use crate::utils::port::Port;
use anyhow::Result;
use std::collections::HashMap;
use std::process::{Child, Command, Output, Stdio};
use tokio::signal::unix::{signal, Signal, SignalKind};
use tracing::{debug, info};

lazy_static::lazy_static! {
  pub static ref SIMULATION_CHECK: parking_lot::Mutex<bool>  = parking_lot::Mutex::new(false);
}
/// Set globally that the system is being run as a simulation
pub fn set_simulation() {
    *SIMULATION_CHECK.lock() = true;
}
/// A method for anyone to check if the system is being run as a simulation
pub fn is_simulation() -> bool {
    // false
    *SIMULATION_CHECK.lock()
}

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

fn prepare_cmd<S, S2, I>(
    cmd_pth: &S,
    args: I,
    env: Option<&HashMap<String, String>>,
    tid: &TransactionId,
) -> Result<Command>
where
    I: IntoIterator<Item = S2> + std::fmt::Debug,
    S2: AsRef<std::ffi::OsStr> + std::fmt::Debug + std::fmt::Display,
    S: AsRef<std::ffi::OsStr> + std::fmt::Display + ?Sized,
{
    debug!(tid=%tid, command=%cmd_pth, args=?args, environment=?env, "executing host command");
    if !std::path::Path::new(&cmd_pth).exists() {
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
pub fn execute_cmd<S, S2, I>(
    cmd_pth: &S,
    args: I,
    env: Option<&HashMap<String, String>>,
    tid: &TransactionId,
) -> Result<Output>
where
    I: IntoIterator<Item = S2> + std::fmt::Debug,
    S2: AsRef<std::ffi::OsStr> + std::fmt::Debug + std::fmt::Display,
    S: AsRef<std::ffi::OsStr> + std::fmt::Display + ?Sized,
{
    let mut cmd = prepare_cmd(cmd_pth, args, env, tid)?;
    match cmd.output() {
        Ok(out) => Ok(out),
        Err(e) => bail_error!(tid=%tid, command=%cmd_pth, error=%e, "Running command failed"),
    }
}

/// Executes the specified executable with args and environment
/// Raises an error if the exit code isn't `0`
pub fn execute_cmd_checked<S, S2, I>(
    cmd_pth: &S,
    args: I,
    env: Option<&HashMap<String, String>>,
    tid: &TransactionId,
) -> Result<Output>
where
    I: IntoIterator<Item = S2> + std::fmt::Debug,
    S2: AsRef<std::ffi::OsStr> + std::fmt::Debug + std::fmt::Display,
    S: AsRef<std::ffi::OsStr> + std::fmt::Display + ?Sized,
{
    match execute_cmd(cmd_pth, args, env, tid) {
        Ok(out) => match out.status.success() {
            true => Ok(out),
            false => {
                bail_error!(tid=%tid, exe=%cmd_pth, stdout=%String::from_utf8_lossy(&out.stdout), stderr=%String::from_utf8_lossy(&out.stderr), code=out.status.code(), "Bad error code executing command")
            }
        },
        Err(e) => Err(e),
    }
}

/// Executes the specified executable with args and environment
/// cmd_pth **must** be an absolute path
/// All std* pipes will be sent to null for the process
pub fn execute_cmd_nonblocking<S, S2, I>(
    cmd_pth: &S,
    args: I,
    env: Option<&HashMap<String, String>>,
    tid: &TransactionId,
) -> Result<Child>
where
    I: IntoIterator<Item = S2> + std::fmt::Debug,
    S2: AsRef<std::ffi::OsStr> + std::fmt::Debug + std::fmt::Display,
    S: AsRef<std::ffi::OsStr> + std::fmt::Display + ?Sized,
{
    debug!(tid=%tid, command=%cmd_pth, args=?args, environment=?env, "executing host command");
    let mut cmd = prepare_cmd(cmd_pth, args, env, tid)?;
    cmd.stdout(Stdio::null()).stdin(Stdio::null()).stderr(Stdio::null());

    match cmd.spawn() {
        Ok(out) => Ok(out),
        Err(e) => {
            bail_error!(tid=%tid, command=%cmd_pth, error=%e, "Spawning non-blocking command failed")
        }
    }
}

/// Waits for an expected exit signal from the OS
/// Any of these: sigint, sig_term, sig_usr1, sig_usr2, sig_quit
/// Notifies [static@crate::continuation::GLOB_CONT_CHECK] of the impending exit
pub async fn wait_for_exit_signal(tid: &TransactionId) -> Result<()> {
    let mut sig_int = try_create_signal(tid, SignalKind::interrupt())?;
    let mut sig_term = try_create_signal(tid, SignalKind::terminate())?;
    let mut sig_usr1 = try_create_signal(tid, SignalKind::user_defined1())?;
    let mut sig_usr2 = try_create_signal(tid, SignalKind::user_defined2())?;
    let mut sig_quit = try_create_signal(tid, SignalKind::quit())?;

    info!(tid=%tid, "Waiting on exit signal");
    tokio::select! {
      res = sig_int.recv() => res,
      res = sig_term.recv() => res,
      res = sig_usr1.recv() => res,
      res = sig_usr2.recv() => res,
      res = sig_quit.recv() => res,
    };
    crate::continuation::GLOB_CONT_CHECK.signal_application_exit(tid);
    Ok(())
}
fn try_create_signal(tid: &TransactionId, kind: SignalKind) -> Result<Signal> {
    match signal(kind) {
        Ok(s) => Ok(s),
        Err(e) => {
            bail_error!(error=%e, tid=%tid, kind=kind.as_raw_value(), "Failed to create signal")
        }
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
    fn format_invoke_correctly(#[case] addr: &str, #[case] port: Port, #[case] expected: &str) {
        let ans = calculate_invoke_uri(addr, port);
        assert_eq!(expected, ans);
    }

    #[rstest]
    #[case("localhost", 8080, "http://localhost:8080/")]
    #[case("localhost", 8081, "http://localhost:8081/")]
    #[case("localhost", 19840, "http://localhost:19840/")]
    #[case("0.0.0.0", 8080, "http://0.0.0.0:8080/")]
    fn format_base_correctly(#[case] addr: &str, #[case] port: Port, #[case] expected: &str) {
        let ans = calculate_base_uri(addr, port);
        assert_eq!(expected, ans);
    }

    #[rstest]
    #[case("hello", "080", "hello-080")]
    #[case("cnn", "1.0.2", "cnn-1.0.2")]
    #[case("video", "1.5.2", "video-1.5.2")]
    #[case("alpine", "0.0.1", "alpine-0.0.1")]
    fn format_fqdn(#[case] name: &str, #[case] version: &str, #[case] expected: &str) {
        let ans = calculate_fqdn(&name.to_string(), &version.to_string());
        assert_eq!(expected, ans);
    }
}
#[cfg(test)]
mod signal_tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(SignalKind::interrupt())]
    #[case(SignalKind::terminate())]
    #[case(SignalKind::user_defined1())]
    #[case(SignalKind::user_defined2())]
    #[case(SignalKind::quit())]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_create_signal(#[case] kind: SignalKind) {
        let _ = try_create_signal(&"TEST".to_string(), kind).unwrap();
    }

    #[rstest]
    #[case(SignalKind::interrupt())]
    #[case(SignalKind::terminate())]
    #[case(SignalKind::user_defined1())]
    #[case(SignalKind::user_defined2())]
    #[case(SignalKind::quit())]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn should_exit_on_signal(#[case] kind: SignalKind) {
        let tid = "TEST".to_string();
        let t = tokio::spawn(async move { wait_for_exit_signal(&tid.clone()).await });
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let nix_signal = nix::sys::signal::Signal::try_from(kind.as_raw_value()).unwrap();
        nix::sys::signal::kill(nix::unistd::Pid::from_raw(std::process::id() as i32), nix_signal).unwrap();
        t.await.unwrap().unwrap();
    }
}
