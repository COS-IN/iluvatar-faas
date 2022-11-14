use std::{sync::Arc, time::{SystemTime, Duration}};
use iluvatar_library::{transaction::TransactionId, types::MemSizeMb, utils::port::Port, bail_error};
use crate::{services::{containers::structs::{ContainerT, RegisteredFunction, ParsedResult}}, };
use anyhow::Result;
use parking_lot::{Mutex, RwLock};

#[derive(Debug)]
#[allow(unused)]
pub struct DockerContainer {
  pub container_id: String,
  /// Mutex guard used to limit number of open requests to a single container
  pub mutex: Mutex<u32>,
  pub fqdn: String,
  /// the associated function inside the container
  pub function: Arc<RegisteredFunction>,
  pub last_used: RwLock<SystemTime>,
  /// number of invocations a container has performed
  pub invocations: Mutex<u32>,
  pub port: Port,
  pub invoke_uri: String,
  pub base_uri: String,
  /// Is container healthy?
  pub healthy: Mutex<bool>
}

#[tonic::async_trait]
impl ContainerT for DockerContainer {
  #[tracing::instrument(skip(self, json_args, timeout_sec), fields(tid=%tid), name="DockerContainer::invoke")]
  async fn invoke(&self, json_args: &String, tid: &TransactionId, timeout_sec: u64) ->  Result<(ParsedResult, Duration)> {
    *self.invocations.lock() += 1;

    self.touch();
    let client = reqwest::Client::builder()
            .pool_max_idle_per_host(0)
            .pool_idle_timeout(None)
                                    // tiny buffer to allow for network delay from possibly full system
            .connect_timeout(Duration::from_secs(timeout_sec+2))
            .build()?;
    let build = client.post(&self.invoke_uri)
                        .body(json_args.to_owned())
                        .header("Content-Type", "application/json");

    let start = SystemTime::now();
    let result = match build.send()
      .await {
        Ok(r) => r,
        Err(e) =>{
          self.mark_unhealthy();
          bail_error!(tid=%tid, error=%e, container_id=%self.container_id, "HTTP error when trying to connect to container");
        },
      };
    let duration = match start.elapsed() {
      Ok(dur) => dur,
      Err(e) => bail_error!(tid=%tid, error=%e, "Timer error recording invocation duration"),
    };
    let r = match result.text().await {
      Ok(r) => r,
      Err(e) => bail_error!(tid=%tid, error=%e, container_id=%self.container_id, "Error reading text data from container"),
    };
    let result = ParsedResult::parse(r, tid)?;
    Ok( (result,duration) )
  }

  fn touch(&self) {
    let mut lock = self.last_used.write();
    *lock = SystemTime::now();
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

  fn get_curr_mem_usage(&self) -> MemSizeMb {
    self.function.memory
  }

  fn set_curr_mem_usage(&self, _usage:MemSizeMb) {
    // TODO: proper memory tracking for these containers
  }

  fn function(&self) -> Arc<crate::services::containers::structs::RegisteredFunction>  {
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

impl crate::services::containers::structs::ToAny for DockerContainer {
  fn as_any(&self) -> &dyn std::any::Any {
    self
  }
}
