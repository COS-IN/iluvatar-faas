use std::any::Any;
use std::{sync::Arc, time::SystemTime};
use crate::{transaction::TransactionId, types::MemSizeMb};
use crate::services::containers::containermanager::ContainerManager;
use anyhow::Result;
use tracing::debug;

#[tonic::async_trait]
pub trait ContainerT: ToAny + std::fmt::Debug + Send + Sync {
  // type Implementer;

  async fn invoke(&self, json_args: &String, tid: &TransactionId) -> Result<String>;

  /// indicate that the container as been "used" or internal datatsructures should be updated such that it has
  fn touch(&self);
  /// the unique ID for this container
  fn container_id(&self) -> &String;
  /// The time at which the container last served an invocation
  fn last_used(&self) -> SystemTime;
  /// Number of invocations this container has served
  fn invocations(&self) -> u32;
  /// Current memory usage of this container
  fn get_curr_mem_usage(&self) -> MemSizeMb;
  /// Update the memory usage of this container
  fn set_curr_mem_usage(&self, usage: MemSizeMb);
  /// the function this container serves
  fn function(&self) -> Arc<RegisteredFunction>;
  /// the fully qualified domain name of the function
  fn fqdn(&self) -> &String;

  /// true if the container is healthy
  fn is_healthy(&self) -> bool;
  /// set the container as unhealthy
  fn mark_unhealthy(&self);

  /// acquire a lock for invocation
  fn acquire(&self);
  /// true if a lock was acquired
  fn try_acquire(&self) -> bool;
  /// release invocation lock
  fn release(&self);
  /// return true if all locks for invocations can be acquired
  fn try_seize(&self) -> bool;
  /// return true if anyone has a lock for invocation
  fn being_held(&self) -> bool;
}

/// 
pub fn cast<'a, T>(c: &'a Container, tid: &TransactionId) -> Result<&'a T>
  where T : ContainerT {
  match c.as_any().downcast_ref::<T>() {
    Some(i) => Ok(i),
    None => {
      anyhow::bail!("[{}] Failed to cast ContainerT type {} to {:?}", tid, std::any::type_name::<Container>(), std::any::type_name::<T>());
    },
  }
}

pub trait ToAny: 'static {
  fn as_any(&self) -> &dyn Any;
}
impl<T: 'static> ToAny for T {
  fn as_any(&self) -> &dyn Any {
      self
  }
}

// pub type Container = Arc<dyn ContainerT>;
pub type Container = Arc<super::containerd::containerdstructs::ContainerdContainer>;

#[derive(Debug)]
pub struct RegisteredFunction {
  pub function_name: String,
  pub function_version: String,
  pub image_name: String,
  pub memory: MemSizeMb,
  pub cpus: u32,
  pub snapshot_base: String,
  pub parallel_invokes: u32,
}

pub struct ContainerLock<'a> {
  pub container: Container,
  container_mrg: &'a ContainerManager,
  transaction_id: &'a TransactionId,
}

impl<'a> ContainerLock<'a> {
  pub fn new(container: Container, container_mrg: &'a ContainerManager, tid: &'a TransactionId) -> Self {
    ContainerLock {
      container,
      container_mrg,
      transaction_id: tid
    }
  }

  pub async fn invoke(&self, json_args: &String) -> Result<String> {
    self.container.invoke(json_args, self.transaction_id).await
  }
}

impl<'a> Drop for ContainerLock<'a> {
  fn drop(&mut self) {
    debug!("[{}] Dropping container lock for '{}'!", self.transaction_id, self.container.container_id());
    self.container_mrg.return_container(&self.container);
  }
}

#[derive(Debug)]
pub struct InsufficientMemoryError {
  pub needed: MemSizeMb,
  pub used: MemSizeMb,
  pub available: MemSizeMb,
}
impl std::fmt::Display for InsufficientMemoryError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
    write!(f, "Not enough memory to launch container")?;
    Ok(())
  }
}
impl std::error::Error for InsufficientMemoryError {

}

#[derive(Debug)]
pub struct InsufficientCoresError {
}
impl std::fmt::Display for InsufficientCoresError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
    write!(f, "No available cores to service container")?;
    Ok(())
  }
}
impl std::error::Error for InsufficientCoresError {

}

#[derive(Debug)]
pub struct ContainerStartupError {
  pub message: String
}
impl std::fmt::Display for ContainerStartupError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
    write!(f, "Startup error: {}", self.message)?;
    Ok(())
  }
}
impl std::error::Error for ContainerStartupError {

}

#[derive(Debug)]
pub struct ContainerLockedError {
}
impl std::fmt::Display for ContainerLockedError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
    write!(f, "Someone has a lock on this container")?;
    Ok(())
  }
}
impl std::error::Error for ContainerLockedError {

}
