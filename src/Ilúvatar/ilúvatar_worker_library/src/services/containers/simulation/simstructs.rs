use std::{sync::Arc, time::{SystemTime, Duration}};
use iluvatar_library::{transaction::TransactionId, types::MemSizeMb, bail_error, logging::LocalTime};
use crate::{services::{containers::structs::{ContainerT, RegisteredFunction, ParsedResult}}, };
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
  pub last_used: RwLock<SystemTime>,
  /// number of invocations a container has performed
  pub invocations: Mutex<u32>,
}

#[derive(Debug, Deserialize, Serialize)]
#[allow(unused)]
/// struct used to control "invocation" pattern of simulated function
pub struct SimulationInvocation {
  #[serde(deserialize_with = "cust_deserialize")]
  pub warm_dur_ms: u64,
  #[serde(deserialize_with = "cust_deserialize")]
  pub cold_dur_ms: u64,
}
/// custom deserialized on this, beceause sometimes values for [SimulationInvocation] are strings, not actually [u64]
fn cust_deserialize<'de, D>(deser: D) -> Result<u64, D::Error> where D: serde::Deserializer<'de> {
  String::deserialize(deser)?.parse::<u64>().map_err(serde::de::Error::custom)
}

#[derive(Debug, Deserialize, Serialize)]
#[allow(unused)]
/// output of "invocation" in simulated function
pub struct SimulationResult {
  pub was_cold: bool,
  pub duration_us: u128,
  pub function_name: String,
}

#[tonic::async_trait]
impl ContainerT for SimulatorContainer {
  #[tracing::instrument(skip(self, json_args), fields(tid=%tid), name="SimulatorContainer::invoke")]
  async fn invoke(&self, json_args: &String, tid: &TransactionId) ->  Result<(ParsedResult, Duration)> {
    // just sleep for a while based on data from json args
    let data = match serde_json::from_str::<SimulationInvocation>(json_args) {
      Ok(d) => d,
      Err(e) => bail_error!(tid=%tid, error=%e, args=%json_args, "Unable to deserialize run time information"),
    };

    let (duration_us, was_cold) = match self.invocations() {
      0 => (data.cold_dur_ms * 1000, true),
      _ => (data.warm_dur_ms * 1000, false)
    };
    *self.invocations.lock() += 1;
    let timer = LocalTime::new(tid)?;
    let start = timer.now_str()?;
    tokio::time::sleep(Duration::from_micros(duration_us)).await;
    let end = timer.now_str()?;
    let ret = SimulationResult {
      was_cold,
      duration_us: duration_us as u128,
      function_name: self.function.function_name.clone()
    };
    let d = Duration::from_micros(duration_us);
    let user_result = serde_json::to_string(&ret)?;
    let result = ParsedResult { 
      user_result: Some(user_result), 
      user_error: None, 
      start,
      end,
      was_cold,
      duration_sec: Duration::from_micros(duration_us).as_secs_f64() };
    Ok( (result,d) )
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
