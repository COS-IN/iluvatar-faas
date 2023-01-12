use std::sync::Arc;
use iluvatar_library::{transaction::TransactionId, types::MemSizeMb};
use self::simstructs::SimulatorContainer;
use super::{structs::{RegisteredFunction, Container, ContainerState}, LifecycleService};
use anyhow::Result;
use guid_create::GUID;
use tracing::warn;

pub mod simstructs;

#[derive(Debug)]
pub struct SimulatorLifecycle {

}

impl SimulatorLifecycle {
  pub fn new() -> Self {
    SimulatorLifecycle { }
  }
}

#[tonic::async_trait]
#[allow(unused)]
impl LifecycleService for SimulatorLifecycle {
  /// creates and starts the entrypoint for a container based on the given image
  /// Run inside the specified namespace
  /// returns a new, unique ID representing it
  async fn run_container(&self, fqdn: &String, image_name: &String, _parallel_invokes: u32, namespace: &str, mem_limit_mb: MemSizeMb, cpus: u32, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> Result<Container> {
    let cid = format!("{}-{}", fqdn, GUID::rand());
    Ok(Arc::new(SimulatorContainer::new(cid, fqdn, reg, ContainerState::Cold)))
  }

  /// Removed the specified container in the containerd namespace
  async fn remove_container(&self, container: Container, ctd_namespace: &str, tid: &TransactionId) -> Result<()> {
    Ok(())
  }

  /// Read through an image's digest to find it's snapshot base
  async fn prepare_function_registration(&self, function_name: &String, function_version: &String, image_name: &String, memory: MemSizeMb, cpus: u32, parallel_invokes: u32, _fqdn: &String, _tid: &TransactionId) -> Result<RegisteredFunction> {
    Ok(RegisteredFunction {
      function_name: function_name.clone(),
      function_version: function_version.clone(),
      image_name: image_name.clone(),
      memory,
      cpus,
      snapshot_base: "".to_string(),
      parallel_invokes,
    })
  }
  
  async fn clean_containers(&self, ctd_namespace: &str, self_src: Arc<dyn LifecycleService>, tid: &TransactionId) -> Result<()> {
    Ok(())
  }

  async fn wait_startup(&self, container: &Container, timout_ms: u64, tid: &TransactionId) -> Result<()> {
   Ok(())
  }

  fn update_memory_usage_mb(&self, container: &Container, tid: &TransactionId) -> MemSizeMb {
    let cast_container = match crate::services::containers::structs::cast::<SimulatorContainer>(&container, tid) {
      Ok(c) => c,
      Err(e) => { 
        warn!(tid=%tid, error=%e, "Error casting container to SimulatorContainer");
        return container.get_curr_mem_usage();
      },
    };
    cast_container.function.memory
  }

  fn read_stdout(&self, container: &Container, tid: &TransactionId) -> String {
    "".to_string()
  }
  fn read_stderr(&self, container: &Container, tid: &TransactionId) -> String {
    "".to_string()
  }
}
impl crate::services::containers::structs::ToAny for SimulatorLifecycle {
  fn as_any(&self) -> &dyn std::any::Any {
      self
  }
}
