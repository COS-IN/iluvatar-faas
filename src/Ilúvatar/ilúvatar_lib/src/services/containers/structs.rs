use std::{sync::Arc, time::SystemTime, fs};
use crate::{bail_error, transaction::TransactionId, types::MemSizeMb, services::network::network_structs::Namespace};
use crate::utils::{port::Port, file::temp_file_pth, calculate_invoke_uri, calculate_base_uri};
use inotify::{Inotify, WatchMask};
use parking_lot::{RwLock, Mutex};

use crate::services::containers::containermanager::ContainerManager;
//use log::{debug};
use anyhow::{Result, Context};
use tracing::instrument; 
use tracing::{debug, info,warn};
use log::error; 

#[derive(Debug)]
#[allow(unused)]
pub struct Container {
  pub container_id: String,
  pub task: Task,
  pub port: Port,
  pub address: String,
  pub invoke_uri: String,
  pub base_uri: String,
  /// Mutex guard used to limit number of open requests to a single container
  // TODO: implement real in-container parallelism
  //    run multiple tasks in each? -> what about port setup then?
  //    web server handles parallelism?
  pub mutex: Mutex<u32>,
  // TODO: reference to function somehow?
  pub fqdn: String,
  pub function: Arc<RegisteredFunction>,
  last_used: RwLock<SystemTime>,
  pub namespace: Arc<Namespace>,
  invocations: Mutex<u32>,
  mem_usage: RwLock<MemSizeMb>,
  pub healthy: Mutex<bool>
}


#[allow(unused)]
impl Container {

  pub fn new(container_id: String, task: Task, port: Port, address: String, parallel_invokes: u32, fqdn: &String, function: &Arc<RegisteredFunction>, ns: Arc<Namespace>) -> Self {
    let invoke_uri = calculate_invoke_uri(&address, port);
    let base_uri = calculate_base_uri(&address, port);
    Container {
      container_id,
      task,
      port,
      address,
      invoke_uri,
      base_uri,
      mutex: Mutex::new(parallel_invokes),
      fqdn: fqdn.clone(),
      function: function.clone(),
      last_used: RwLock::new(SystemTime::now()),
      namespace: ns,
      invocations: Mutex::new(0),
      mem_usage: RwLock::new(function.memory),
      healthy: Mutex::new(true),
    }
  }

  pub fn stdout(&self) -> String {
    temp_file_pth(&self.container_id, "stdout")
  }
  pub fn read_stdout(&self, tid: &TransactionId) -> String {
    let path = self.stdout();
    match std::fs::read_to_string(path) {
      Ok(s) => str::replace(&s, "\n", "\\n"),
      Err(e) =>  {
        log::error!("[{}] error reading container '{}' stdout: {}", tid, self.container_id, e);
        format!("STDOUT_READ_ERROR: {}", e)
      },
    }
  }
  pub fn stderr(&self) -> String {
    temp_file_pth(&self.container_id, "stderr")
  }
  pub fn read_stderr(&self, tid: &TransactionId) -> String {
    let path = self.stderr();
    match std::fs::read_to_string(path) {
      Ok(s) => str::replace(&s, "\n", "\\n"),
      Err(e) =>  {
        log::error!("[{}] error reading container '{}' stdout: {}", tid, self.container_id, e);
        format!("STDERR_READ_ERROR: {}", e)
      },
    }
  }
  pub fn stdin(&self) -> String {
    temp_file_pth(&self.container_id, "stdin")
  }

  /// wait_startup
  /// Waits for the startup message for a container to come through
  /// Really the task inside, the web server should write (something) to stdout when it is ready
  pub async fn wait_startup(&self, timout_ms: u64, tid: &TransactionId) -> Result<()> {
    debug!("[{}] Waiting for startup of container {}", tid, &self.container_id);
    let stderr = self.stderr();

    let start = SystemTime::now();

    let mut inotify = Inotify::init().context("Init inotify watch failed")?;
    let dscriptor = inotify
        .add_watch(&stderr, WatchMask::MODIFY).context("Adding inotify watch failed")?;
    let mut buffer = [0; 256];

    loop {
      match inotify.read_events(&mut buffer) {
        Ok(events) => {
          inotify.rm_watch(dscriptor).context("Deleting inotify watch failed")?;
          break
        }, // stderr was written to, gunicorn server is either up or crashed
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
          if start.elapsed()?.as_millis() as u64 >= timout_ms {
            let stdout = self.read_stdout(tid);
            let stderr = self.read_stderr(tid);
            bail_error!("[{}] Timeout while reading inotify events for container {}; stdout: '{}'; stderr '{}'", tid, &self.container_id, stdout, stderr);
          }
          continue;
        },
        _ => bail_error!("[{}] Error while reading inotify events for container {}", &tid, self.container_id),
      }
      tokio::time::sleep(std::time::Duration::from_micros(100)).await;
    }
    Ok(())
  }

  pub fn touch(&self) {
    let mut lock = self.last_used.write();
    *lock = SystemTime::now();
    *self.invocations.lock() += 1;
  }

  pub fn last_used(&self) -> SystemTime {
    *self.last_used.read()
  }

  pub fn invocations(&self) -> u32 {
    *self.invocations.lock()
  }

  pub fn curr_mem_usage(&self) -> MemSizeMb {
    *self.mem_usage.read()
  }

  /// update_memory_usage_mb
  /// Update the current resident memory size of the container
  pub fn update_memory_usage_mb(&self, tid: &TransactionId) -> MemSizeMb {
    let contents = match fs::read_to_string(format!("/proc/{}/statm", self.task.pid)) {
        Ok(c) => c,
        Err(e) => { 
          log::warn!("[{}] Error trying to read container '{}' /proc/<pid>/statm: {}", e, self.container_id, tid);
          *self.healthy.lock() = false;
          *self.mem_usage.write() = self.function.memory; 
          return *self.mem_usage.read(); 
        },
    };
    let split: Vec<&str> = contents.split(" ").collect();
    // https://linux.die.net/man/5/proc
    // virtual memory resident set size
    let vmrss = split[1];
    *self.mem_usage.write() = match vmrss.parse::<MemSizeMb>() {
      // multiply page size in bytes by number pages, then convert to mb
      Ok(size_pages) => (size_pages * 4096) / (1024*1024),
      Err(e) => {
        log::warn!("[{}] Error trying to parse vmrss of '{}': {}", e, vmrss, tid);
        self.function.memory
      },
    };
    *self.mem_usage.read()
  }
}

#[derive(Debug)]
#[allow(unused)]
pub struct Task {
  pub pid: u32,
  pub container_id: Option<String>,
  pub running: bool,
}

#[derive(Debug)]
#[allow(unused)]
pub struct RegisteredFunction {
  pub function_name: String,
  pub function_version: String,
  pub image_name: String,
  pub memory: MemSizeMb,
  pub cpus: u32,
  pub snapshot_base: String,
  pub parallel_invokes: u32,
}

pub struct ContainerLock<'a> {
  pub container: Arc<Container>,
  container_mrg: &'a ContainerManager,
  transaction_id: &'a TransactionId,
}

impl<'a> ContainerLock<'a> {
  pub fn new(container: Arc<Container>, container_mrg: &'a ContainerManager, tid: &'a TransactionId) -> Self {
    ContainerLock {
      container,
      container_mrg,
      transaction_id: tid
    }
  }
}

impl<'a> Drop for ContainerLock<'a> {
  fn drop(&mut self) {
    debug!("[{}] Dropping container lock for '{}'!", self.transaction_id, self.container.container_id);
    self.container_mrg.return_container(&self.container);
  }
}

#[derive(Debug)]
pub struct InsufficientMemoryError {
  pub needed: MemSizeMb,
  pub used: MemSizeMb,
  pub available: MemSizeMb,
}
impl std::fmt::Display for InsufficientMemoryError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
    write!(f, "Not enough memory to launch container")?;
    Ok(())
  }
}
impl std::error::Error for InsufficientMemoryError {

}

#[derive(Debug)]
pub struct InsufficientCoresError {
}
impl std::fmt::Display for InsufficientCoresError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
    write!(f, "No available cores to service container")?;
    Ok(())
  }
}
impl std::error::Error for InsufficientCoresError {

}

#[derive(Debug)]
pub struct ContainerStartupError {
  pub message: String
}
impl std::fmt::Display for ContainerStartupError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
    write!(f, "Startup error: {}", self.message)?;
    Ok(())
  }
}
impl std::error::Error for ContainerStartupError {

}

#[derive(Debug)]
pub struct ContainerLockedError {
}
impl std::fmt::Display for ContainerLockedError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
    write!(f, "Someone has a lock on this container")?;
    Ok(())
  }
}
impl std::error::Error for ContainerLockedError {

}
