use std::{sync::Arc, time::SystemTime};
use crate::{services::{containers::structs::{ContainerT, RegisteredFunction}}, transaction::TransactionId, types::MemSizeMb, bail_error};
use anyhow::Result;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use tracing::error;

#[derive(Debug)]
#[allow(unused)]
pub struct SimulatorContainer {
  pub container_id: String,
  /// Mutex guard used to limit number of open requests to a single container
  pub mutex: Mutex<u32>,
  pub fqdn: String,
  /// the associated function inside the container
  pub function: Arc<RegisteredFunction>,
  last_used: RwLock<SystemTime>,
  /// number of invocations a container has performed
  invocations: Mutex<u32>,
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
/// struct used to control "invocation" pattern of simulated function
pub struct SimulationInvocation {
  pub warm_time_duration_ms: u64,
  pub cold_time_duration_ms: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[allow(unused)]
/// output of "invocation" in simulated function
pub struct SimulationResult {
  pub was_cold: bool,
  pub duration_ms: u64,
}

#[tonic::async_trait]
impl ContainerT for SimulatorContainer {
  async fn invoke(&self, json_args: &String, tid: &TransactionId) ->  Result<String> {
    // just sleep for a while based on data from json args
    // TODO: get info in here if function was pre-warmed
    let data = match serde_json::from_str::<SimulationInvocation>(json_args) {
      Ok(d) => d,
      Err(e) => bail_error!("[{}] unable to deserialize run time information because of: {}", tid, e),
    };

    let (duration_ms, was_cold) = match self.invocations() {
      0 => (data.cold_time_duration_ms, true),
      _ => (data.warm_time_duration_ms, false)
    };
    tokio::time::sleep(std::time::Duration::from_millis(duration_ms)).await;
    let ret = SimulationResult {
      was_cold,
      duration_ms
    };
    Ok(serde_json::to_string(&ret)?)
  }

  fn touch(&self) {
    let mut lock = self.last_used.write();
    *lock = SystemTime::now();
    *self.invocations.lock() += 1;
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
    // do nothing
  }

  fn function(&self) -> Arc<crate::services::containers::structs::RegisteredFunction>  {
    self.function.clone()
  }

  fn fqdn(&self) ->  &String {
    &self.fqdn
  }

  fn is_healthy(&self) -> bool {
    true
  }

  fn mark_unhealthy(&self) {
    error!(container_id=%self.container_id, "Cannot mark simulation container unhealthy!")
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

impl crate::services::containers::structs::ToAny for SimulatorContainer {
  fn as_any(&self) -> &dyn std::any::Any {
      self
  }
}
