use std::{time::SystemTime, sync::Arc};

use parking_lot::{RwLock, Mutex};

use crate::{types::MemSizeMb, utils::{calculate_invoke_uri, port_utils::Port, calculate_base_uri}, services::{containers::structs::{RegisteredFunction, ContainerT}, network::network_structs::Namespace}, bail_error, transaction::TransactionId};

#[derive(Debug)]
pub struct Task {
  pub pid: u32,
  pub container_id: Option<String>,
  pub running: bool,
}

#[derive(Debug)]
#[allow(unused)]
pub struct ContainerdContainer {
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

impl ContainerdContainer {
  pub fn new(container_id: String, task: Task, port: Port, address: String, parallel_invokes: u32, fqdn: &String, function: &Arc<RegisteredFunction>, ns: Arc<Namespace>) -> Self {
    let invoke_uri = calculate_invoke_uri(&address, port);
    let base_uri = calculate_base_uri(&address, port);
    ContainerdContainer {
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
}

#[tonic::async_trait]
impl ContainerT for ContainerdContainer {
  #[tracing::instrument(skip(self, json_args))]
  async fn invoke(&self, json_args: &String, tid: &TransactionId) -> anyhow::Result<String> {
    *self.invocations.lock() += 1;

    self.touch();
    let client = reqwest::Client::new();
    let result = match client.post(&self.invoke_uri)
      .body(json_args.to_owned())
      .header("Content-Type", "application/json")
      .send()
      .await {
        Ok(r) => r,
        Err(e) =>{
          self.mark_unhealthy();
          bail_error!(tid=%tid, error=%e, container_id=%self.container_id, "HTTP error when trying to connect to container");
        },
      };
    match result.text().await {
      Ok(r) => Ok(r),
      Err(e) => bail_error!(tid=%tid, error=%e, container_id=%self.container_id, "Error reading text data from container"),
    }
  }

  fn container_id(&self) ->  &String {
    &self.container_id
  }

  fn last_used(&self) -> SystemTime {
    *self.last_used.read()
  }

  fn invocations(&self) -> u32 {
    *self.invocations.lock()
  }

  fn touch(&self) {
    let mut lock = self.last_used.write();
    *lock = SystemTime::now();
  }

  fn get_curr_mem_usage(&self) -> MemSizeMb {
    *self.mem_usage.read()
  }

  fn set_curr_mem_usage(&self, usage:MemSizeMb) {
    *self.mem_usage.write() = usage;
  }

  fn function(&self) -> Arc<RegisteredFunction>  {
    self.function.clone()
  }

  fn fqdn(&self) ->  &String {
    &self.fqdn
  }

  fn is_healthy(&self) -> bool {
    *self.healthy.lock()
  }
  fn mark_unhealthy(&self) {
    *self.healthy.lock() = false;
  }

  fn acquire(&self) {
    let mut m = self.mutex.lock();
    *m -= 1;
  }
  fn try_acquire(&self) -> bool {
    let mut m = self.mutex.lock();
    if *m > 0 {
      *m -= 1;
      return true;
    }
    return false;
  }
  fn release(&self) {
    let mut m = self.mutex.lock();
    *m += 1;
  }
  fn try_seize(&self) -> bool {
    let mut cont_lock = self.mutex.lock();
    if *cont_lock != self.function().parallel_invokes {
      return false;
    }
    *cont_lock = 0;
    true
  }
  fn being_held(&self) -> bool {
    *self.mutex.lock() != self.function().parallel_invokes
  }
}

impl crate::services::containers::structs::ToAny for ContainerdContainer {
  fn as_any(&self) -> &dyn std::any::Any {
      self
  }
}
