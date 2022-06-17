use std::sync::Arc;

use iluvatar_lib::utils::{Port, temp_file};
use inotify::{Inotify, WatchMask};

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
  pub mutex: parking_lot::Mutex<i32>,
  // TODO: reference to function somehow?
  pub function: Option<Arc<RegisteredFunction>>,
}

#[allow(unused)]
impl Container {
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
    let mut inotify = Inotify::init()?;
    inotify
    .add_watch(self.stdout()?, WatchMask::MODIFY)?;
    let mut buffer = [0; 128];
    inotify.read_events_blocking(&mut buffer)?;
    Ok(())
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