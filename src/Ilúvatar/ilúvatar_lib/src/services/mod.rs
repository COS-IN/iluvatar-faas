use std::sync::Arc;
use tonic::async_trait;
use anyhow::Result;
use crate::{types::MemSizeMb, transaction::TransactionId, worker_api::worker_config::{ContainerResources, NetworkingConfig}};
use self::{containers::{structs::{Container, RegisteredFunction, ContainerT}, containerd::{ContainerdLifecycle, containerdstructs::ContainerdContainer}}, network::namespace_manager::NamespaceManager};

pub mod containers;
pub mod invocation;
pub mod network;
pub mod status;
pub mod load_balance;
pub mod worker_health;
pub use worker_health::WorkerHealthService as WorkerHealthService;
pub mod controller_health;
pub use controller_health::HealthService as ControllerHealthService;
pub mod graphite;

#[async_trait]
pub trait LifecycleService<C = Container>: Send + Sync + std::fmt::Debug {
  /// Create a container, but do not start it / the process inside it
  // async fn create_container(&self, fqdn: &String, image_name: &String, namespace: &str, parallel_invokes: u32, mem_limit_mb: MemSizeMb, cpus: u32, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> Result<Container>;

  /// Return a container that has been started with the given settings
  /// NOTE: you will have to ask the lifetime again to wait on the container to be started up
  async fn run_container(&self, fqdn: &String, image_name: &String, parallel_invokes: u32, namespace: &str, mem_limit_mb: MemSizeMb, cpus: u32, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> Result<C>;

  /// removes a specific container, and all the related resources
  async fn remove_container(&self, container_id: Container, ctd_namespace: &str, tid: &TransactionId) -> Result<()>;

  // async fn search_image_digest(&self, image: &String, namespace: &str, tid: &TransactionId) -> Result<String>;
  async fn prepare_function_registration(&self, function_name: &String, function_version: &String, image_name: &String, memory: MemSizeMb, cpus: u32, parallel_invokes: u32, fqdn: &String, tid: &TransactionId) -> Result<RegisteredFunction>;
  
  /// Removes _all_ containers owned by the lifecycle
  async fn clean_containers(&self, namespace: &str, tid: &TransactionId) -> Result<()>;

  /// Waits for the startup message for a container to come through
  /// Really the task inside, the web server should write (something) to stdout when it is ready
  async fn wait_startup(&self, container: &Container, timout_ms: u64, tid: &TransactionId) -> Result<()>;

  /// Update the current resident memory size of the container
  fn update_memory_usage_mb(&self, container: &Container, tid: &TransactionId) -> MemSizeMb;

  fn stdout_pth(&self, container: &Container) -> String;
  fn stderr_pth(&self, container: &Container) -> String;
  fn stdin_pth(&self, container: &Container) -> String;

  fn read_stdout(&self, container: &Container, tid: &TransactionId) -> String;
  fn read_stderr(&self, container: &Container, tid: &TransactionId) -> String;
}

pub struct LifecycleFactory {
  containers: Arc<ContainerResources>,
  networking: Arc<NetworkingConfig>,
}

impl LifecycleFactory {
  pub fn new(containers: Arc<ContainerResources>, networking: Arc<NetworkingConfig>) -> Self {
    LifecycleFactory {
      containers,
      networking
    }
  }

  pub async fn get_lifecycle_service(&self, tid: &TransactionId, bridge: bool) -> Result<Arc<dyn LifecycleService>> {
    if self.containers.backend == "containerd" {
      let netm = NamespaceManager::boxed(self.networking.clone(), tid);
      if bridge {
        netm.ensure_bridge(tid)?;
      }
      
      let mut lifecycle = ContainerdLifecycle::new(netm);
      lifecycle.connect().await?;
      Ok(Arc::new(lifecycle))
    } else if self.containers.backend == "docker" {
      todo!();
    } else {
      panic!("Unknown lifecycle backend '{}'", self.containers.backend);
    }
  }
}
