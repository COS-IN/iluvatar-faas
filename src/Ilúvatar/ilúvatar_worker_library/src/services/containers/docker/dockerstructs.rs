use std::{sync::Arc, time::{SystemTime, Duration}, num::NonZeroU32};
use iluvatar_library::{transaction::TransactionId, types::{MemSizeMb, Isolation}, utils::{port::Port, calculate_invoke_uri, calculate_base_uri}, bail_error};
use reqwest::Client;
use crate::services::{containers::structs::{ContainerT, RegisteredFunction, ParsedResult, ContainerState}};
use anyhow::Result;
use parking_lot::{Mutex, RwLock};

#[derive(Debug)]
#[allow(unused)]
pub struct DockerContainer {
  pub container_id: String,
  pub fqdn: String,
  /// the associated function inside the container
  pub function: Arc<RegisteredFunction>,
  pub last_used: RwLock<SystemTime>,
  /// number of invocations a container has performed
  pub invocations: Mutex<u32>,
  pub port: Port,
  pub invoke_uri: String,
  pub base_uri: String,
  state: Mutex<ContainerState>,
  client: Client,
}

impl DockerContainer {
  pub fn new(container_id: String, port: Port, address: String, _parallel_invokes: NonZeroU32, fqdn: &String, function: &Arc<RegisteredFunction>, invoke_timeout: u64, state: ContainerState) -> Result<Self> {
    let client = match reqwest::Client::builder()
      .pool_max_idle_per_host(0)
      .pool_idle_timeout(None)
                              // tiny buffer to allow for network delay from possibly full system
      .connect_timeout(Duration::from_secs(invoke_timeout+2))
      .build() {
        Ok(c) => c,
        Err(e) => bail_error!(error=%e, "Unable to build reqwest HTTP client"),
      };
    let r = DockerContainer {
      container_id: container_id,
      fqdn: fqdn.clone(),
      function: function.clone(),
      last_used: RwLock::new(SystemTime::now()),
      invocations: Mutex::new(0),
      port,
      invoke_uri: calculate_invoke_uri(address.as_str(), port),
      base_uri: calculate_base_uri(address.as_str(), port),
      state: Mutex::new(state),
      client,
    };
    Ok(r)
  }
}

#[tonic::async_trait]
impl ContainerT for DockerContainer {
  #[tracing::instrument(skip(self, json_args), fields(tid=%tid), name="DockerContainer::invoke")]
  async fn invoke(&self, json_args: &String, tid: &TransactionId) ->  Result<(ParsedResult, Duration)> {
    *self.invocations.lock() += 1;
    self.touch();
    let build = self.client.post(&self.invoke_uri)
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
    self.state() != ContainerState::Unhealthy
  }
  fn mark_unhealthy(&self) {
    self.set_state(ContainerState::Unhealthy);
  }
  fn state(&self) -> ContainerState {
    *self.state.lock()
  }
  fn set_state(&self, state: ContainerState) {
    *self.state.lock() = state;
  }
  fn container_type(&self) -> Isolation { Isolation::DOCKER }
}

impl crate::services::containers::structs::ToAny for DockerContainer {
  fn as_any(&self) -> &dyn std::any::Any {
    self
  }
}
