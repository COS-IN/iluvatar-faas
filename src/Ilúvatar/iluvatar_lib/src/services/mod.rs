use std::sync::Arc;

use tonic::async_trait;
use anyhow::Result;

use crate::{types::MemSizeMb, transaction::TransactionId, worker_api::config::WorkerConfig};

use self::{containers::{structs::{Container, RegisteredFunction}, containerdlife::ContainerdLifecycle}, network::{network_structs::Namespace, namespace_manager::NamespaceManager}};

pub mod containers;
pub mod network;

#[async_trait]
// #[derive(Debug)]
pub trait LifecycleService: Send + Sync {
  async fn create_container(&self, fqdn: &String, image_name: &String, namespace: &str, parallel_invokes: u32, mem_limit_mb: MemSizeMb, cpus: u32, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> Result<Container>;
  async fn run_container(&self, fqdn: &String, image_name: &String, parallel_invokes: u32, namespace: &str, mem_limit_mb: MemSizeMb, cpus: u32, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> Result<Container>;
  async fn remove_container(&self, container_id: &String, net_namespace: &Arc<Namespace>, ctd_namespace: &str, tid: &TransactionId) -> Result<()>;
  async fn ensure_image(&self, image_name: &String) -> Result<()>;
  async fn search_image_digest(&self, image: &String, namespace: &str, tid: &TransactionId) -> Result<String>;
}

pub struct LifecycleFactory {
  config: WorkerConfig
}

impl LifecycleFactory {
  pub fn new(config: WorkerConfig) -> Self {
    LifecycleFactory {
      config
    }
  }

  pub async fn get_lifecycle_service(&self, tid: &TransactionId) -> Result<Arc<dyn LifecycleService>> {
    if self.config.container_resources.backend == "containerd" {
      let netm = NamespaceManager::boxed(self.config.clone(), tid);
      netm.ensure_bridge(tid)?;
      
      let mut lifecycle = ContainerdLifecycle::new(netm);
      lifecycle.connect().await?;
      Ok(Arc::new(lifecycle))
    } else if self.config.container_resources.backend == "docker" {
      todo!();
    } else {
      panic!("Unknown lifecycle backend '{}'", self.config.container_resources.backend);
    }
  }
}