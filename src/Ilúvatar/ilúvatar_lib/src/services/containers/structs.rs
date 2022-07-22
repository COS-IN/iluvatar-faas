use std::{sync::Arc, time::SystemTime};
use crate::{transaction::TransactionId, types::MemSizeMb, services::network::network_structs::Namespace};
use crate::utils::{port::Port, calculate_invoke_uri, calculate_base_uri};
use parking_lot::{RwLock, Mutex};
use crate::services::containers::containermanager::ContainerManager;
use anyhow::Result;
use tracing::debug;

#[derive(Debug)]
#[allow(unused)]
// TODO: move this behind a trait for simulation
pub struct Container {
  pub container_id: String,
  /// The containerd task in the container
  pub task: Task,
  pub port: Port,
  pub address: String,
  pub invoke_uri: String,
  pub base_uri: String,
  /// Mutex guard used to limit number of open requests to a single container
  pub mutex: Mutex<u32>,
  pub fqdn: String,
  /// the associated function inside the container
  pub function: Arc<RegisteredFunction>,
  last_used: RwLock<SystemTime>,
  /// The namespace container has been put in
  pub namespace: Arc<Namespace>,
  /// number of invocations a container has performed
  invocations: Mutex<u32>,
  /// Most recently clocked memory usage
  pub mem_usage: RwLock<MemSizeMb>,
  /// Is container healthy?
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
