use std::sync::Arc;

use iluvatar_lib::utils::Port;

use super::containermanager::ContainerManager;
use log::debug;

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
  pub mutex: parking_lot::Mutex<i32>,
  // TODO: reference to function somehow?
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