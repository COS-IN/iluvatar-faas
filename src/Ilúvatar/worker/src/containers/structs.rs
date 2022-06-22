use std::{sync::Arc, time::SystemTime};

use iluvatar_lib::utils::{Port, temp_file, calculate_invoke_uri, calculate_base_uri};
use inotify::{Inotify, WatchMask};
use parking_lot::RwLock;
use super::containermanager::ContainerManager;
use log::debug;
use anyhow::Result;

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
  pub mutex: parking_lot::Mutex<u32>,
  // TODO: reference to function somehow?
  pub fqdn: String,
  pub function: Arc<RegisteredFunction>,
  last_used: RwLock<SystemTime>,
}

#[allow(unused)]
impl Container {

  pub fn new(container_id: String, task: Task, port: Port, address: String, parallel_invokes: u32, fqdn: &String, function: &Arc<RegisteredFunction>) -> Self {
    let invoke_uri = calculate_invoke_uri(&address, port);
    let base_uri = calculate_base_uri(&address, port);
    Container {
      container_id,
      task,
      port,
      address,
      invoke_uri,
      base_uri,
      mutex: parking_lot::Mutex::new(parallel_invokes),
      fqdn: fqdn.clone(),
      function: function.clone(),
      last_used: RwLock::new(SystemTime::now()),
    }
  }

  pub fn stdout(&self) -> Result<String> {
    temp_file(&self.container_id, "stdout")
  }
  pub fn stderr(&self) -> Result<String> {
    temp_file(&self.container_id, "stderr")
  }
  pub fn stdin(&self) -> Result<String> {
    temp_file(&self.container_id, "stdin")
  }

  /// wait_startup
  /// Waits for the startup message for a container to come through
  /// Really the task inside, the web server should write (something) to stdout when it is ready
  pub fn wait_startup(&self) -> Result<()> {
    // TODO: timeout for this wait
    debug!("Waiting for startup of container {}", &self.container_id);
    let mut inotify = Inotify::init()?;
    inotify
    .add_watch(self.stdout()?, WatchMask::MODIFY)?;
    let mut buffer = [0; 128];
    inotify.read_events_blocking(&mut buffer)?;
    Ok(())
  }

  pub fn touch(&self) {
    let mut lock = self.last_used.write();
    *lock = SystemTime::now();
  }

  pub fn last_used(&self) -> SystemTime {
    *self.last_used.read()
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
  pub memory: u32,
  pub cpus: u32,
  pub snapshot_base: String,
  pub parallel_invokes: u32,
}

#[derive(Debug)]
#[allow(unused)]
pub struct ContainerLock<'a> {
  pub container: Arc<Container>,
  pub container_mrg: &'a ContainerManager,
}

impl<'a> ContainerLock<'a> {
  pub fn new(container: Arc<Container>, container_mrg: &'a ContainerManager) -> Self {
    ContainerLock {
      container,
      container_mrg
    }
  }
}

impl<'a> Drop for ContainerLock<'a> {
  fn drop(&mut self) {
    debug!("Dropping container lock!");
    self.container_mrg.return_container(&self.container);
  }
}

#[derive(Debug)]
pub struct InsufficientMemoryError {
  pub needed: u32,
  pub used: u32,
  pub available: u32,
}
impl std::fmt::Display for InsufficientMemoryError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
    write!(f, "Not enough memory to launch container").unwrap();
    Ok(())
  }
}
impl std::error::Error for InsufficientMemoryError {

}

#[derive(Debug)]
pub struct ContainerStartupError {
  pub message: String
}
impl std::fmt::Display for ContainerStartupError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
    write!(f, "Startup error: {}", self.message).unwrap();
    Ok(())
  }
}
impl std::error::Error for ContainerStartupError {

}