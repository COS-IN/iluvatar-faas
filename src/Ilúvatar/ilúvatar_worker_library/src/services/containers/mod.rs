use std::{sync::Arc, collections::HashMap};
use tonic::async_trait;
use anyhow::Result;
use iluvatar_library::{types::{MemSizeMb, Isolation, Compute}, transaction::TransactionId};
use tracing::info;
use crate::{worker_api::worker_config::{ContainerResourceConfig, NetworkingConfig, FunctionLimits}};
use crate::services::{containers::{structs::{Container}, containerd::ContainerdLifecycle, simulation::SimulatorLifecycle}};
use crate::services::network::namespace_manager::NamespaceManager;
use self::{structs::ToAny, docker::DockerLifecycle, resources::gpu::GPU};
use super::registration::RegisteredFunction;

pub mod structs;
pub mod containermanager;
#[path ="./containerd/containerdlife.rs"]
pub mod containerd;
#[path ="./simulation/simulatorlife.rs"]
pub mod simulation;
#[path ="./docker/dockerlife.rs"]
pub mod docker;
mod container_pool;
mod resources;

#[async_trait]
pub trait LifecycleService: ToAny + Send + Sync + std::fmt::Debug {
  /// Return a container that has been started with the given settings
  /// NOTE: you will have to ask the lifetime again to wait on the container to be started up
  async fn run_container(&self, fqdn: &String, image_name: &String, parallel_invokes: u32, namespace: &str, mem_limit_mb: MemSizeMb, cpus: u32, reg: &Arc<RegisteredFunction>, iso: Isolation, compute: Compute, device_resource: Option<Arc<GPU>>, tid: &TransactionId) -> Result<Container>;

  /// removes a specific container, and all the related resources
  async fn remove_container(&self, container_id: Container, ctd_namespace: &str, tid: &TransactionId) -> Result<()>;

  // async fn search_image_digest(&self, image: &String, namespace: &str, tid: &TransactionId) -> Result<String>;
  async fn prepare_function_registration(&self, rf: &mut RegisteredFunction, fqdn: &String, tid: &TransactionId) -> Result<()>;
  
  /// Removes _all_ containers owned by the lifecycle
  async fn clean_containers(&self, namespace: &str, self_src: Arc<dyn LifecycleService>, tid: &TransactionId) -> Result<()>;

  /// Waits for the startup message for a container to come through
  /// Really the task inside, the web server should write (something) to stdout when it is ready
  async fn wait_startup(&self, container: &Container, timout_ms: u64, tid: &TransactionId) -> Result<()>;

  /// Update the current resident memory size of the container
  fn update_memory_usage_mb(&self, container: &Container, tid: &TransactionId) -> MemSizeMb;

  /// get the contents of the container's stdout as a string
  /// or an error message string if something went wrong
  fn read_stdout(&self, container: &Container, tid: &TransactionId) -> String;
  /// get the contents of the container's stderr as a string
  /// or an error message string if something went wrong
  fn read_stderr(&self, container: &Container, tid: &TransactionId) -> String;

  /// The backend type for this lifecycle
  /// Real backends should only submit one Isolation type, returning a vector here allows the simulation to "act" as multiple backends
  fn backend(&self) -> Vec<Isolation>;
}
pub type LifecycleCollection = Arc<std::collections::HashMap<Isolation, Arc<dyn LifecycleService>>>;

pub struct LifecycleFactory {
  containers: Arc<ContainerResourceConfig>,
  networking: Arc<NetworkingConfig>,
  limits_config: Arc<FunctionLimits>,
}

impl LifecycleFactory {
  pub fn new(containers: Arc<ContainerResourceConfig>, networking: Arc<NetworkingConfig>, limits_config: Arc<FunctionLimits>) -> Self {
    LifecycleFactory {
      containers,
      networking,
      limits_config
    }
  }

  pub async fn get_lifecycle_services(&self, tid: &TransactionId, ensure_bridge: bool) -> Result<LifecycleCollection> {
    let mut ret = HashMap::new();
    if iluvatar_library::utils::is_simulation() {
      info!(tid=%tid, "Creating 'simulation' backend");
      let c = SimulatorLifecycle::new();
      self.insert_cycle(&mut ret, Arc::new(c))?;
    } else {
      if ContainerdLifecycle::supported(tid).await {
        info!(tid=%tid, "Creating 'containerd' backend");
        let netm = NamespaceManager::boxed(self.networking.clone(), tid, ensure_bridge)?;
        let mut lifecycle = ContainerdLifecycle::new(netm, self.containers.clone(), self.limits_config.clone());
        lifecycle.connect().await?;
        self.insert_cycle(&mut ret, Arc::new(lifecycle))?;
      }
      if DockerLifecycle::supported(tid) {
        info!(tid=%tid, "Creating 'docker' backend");
        let d = Arc::new(DockerLifecycle::new(self.containers.clone(), self.limits_config.clone()));
        self.insert_cycle(&mut ret, d)?;
      } 
    }
    if ret.len() < 1 {
      anyhow::bail!("No lifecycles were able to be made");
    }
  
    Ok(Arc::new(ret))
  }

  fn insert_cycle(&self, map: &mut HashMap<Isolation, Arc<dyn LifecycleService>>, life: Arc<dyn LifecycleService>) -> Result<()> {
    for iso in life.backend() {
      if map.contains_key(&iso) {
        anyhow::bail!("Got multiple backend registrations for Isolation {:?}", iso);
      }
      map.insert(iso, life.clone());
    }
    Ok(())
  }
}
