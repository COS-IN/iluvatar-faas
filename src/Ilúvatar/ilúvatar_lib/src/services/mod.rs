use std::sync::Arc;
use tonic::async_trait;
use anyhow::Result;
use crate::{types::MemSizeMb, transaction::TransactionId, worker_api::worker_config::{ContainerResources, NetworkingConfig}};
use self::{containers::{structs::{Container, RegisteredFunction}, containerdlife::ContainerdLifecycle}, network::{network_structs::Namespace, namespace_manager::NamespaceManager}};

pub mod containers;
pub mod invocation;
pub mod network;
pub mod status;
pub mod load_balance;
pub mod worker_health;
pub use worker_health::WorkerHealthService as WorkerHealthService;

#[async_trait]
pub trait LifecycleService: Send + Sync + std::fmt::Debug {
  async fn create_container(&self, fqdn: &String, image_name: &String, namespace: &str, parallel_invokes: u32, mem_limit_mb: MemSizeMb, cpus: u32, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> Result<Container>;
  async fn run_container(&self, fqdn: &String, image_name: &String, parallel_invokes: u32, namespace: &str, mem_limit_mb: MemSizeMb, cpus: u32, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> Result<Container>;
  async fn remove_container(&self, container_id: &String, net_namespace: &Arc<Namespace>, ctd_namespace: &str, tid: &TransactionId) -> Result<()>;
  async fn ensure_image(&self, image_name: &String) -> Result<()>;
  async fn search_image_digest(&self, image: &String, namespace: &str, tid: &TransactionId) -> Result<String>;
  async fn clean_containers(&self, namespace: &str, tid: &TransactionId) -> Result<()>;
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
