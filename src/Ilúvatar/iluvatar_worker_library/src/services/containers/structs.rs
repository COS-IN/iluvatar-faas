use crate::services::resources::gpu::ProtectedGpuRef;
use crate::services::{containers::containermanager::ContainerManager, registration::RegisteredFunction};
use anyhow::Result;
use iluvatar_library::types::ToAny;
use iluvatar_library::{
    bail_error,
    transaction::TransactionId,
    types::{Compute, DroppableToken, Isolation, MemSizeMb},
};
pub use iluvatar_rpc::rpc::ContainerState;
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;
use tracing::debug;

#[tonic::async_trait]
#[allow(dyn_drop)]
pub trait ContainerT: ToAny + Send + Sync {
    /// Invoke the function within the container, passing the json args to it
    async fn invoke(&self, json_args: &str, tid: &TransactionId) -> Result<(ParsedResult, Duration)>;

    /// indicate that the container as been "used" or internal datatsructures should be updated such that it has
    fn touch(&self);
    /// the unique ID for this container
    fn container_id(&self) -> &String;
    /// The time at which the container last served an invocation
    fn last_used(&self) -> Instant;
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
    /// Get the current state of the container
    fn state(&self) -> ContainerState;
    fn set_state(&self, state: ContainerState);
    fn container_type(&self) -> Isolation;
    fn compute_type(&self) -> Compute;
    /// Returned with [Some(&GPU)] if the container has extra resources
    fn device_resource(&self) -> ProtectedGpuRef<'_>;
    /// Update amount of device memory the container uses.
    fn set_device_memory(&self, size: MemSizeMb);
    /// Get amount of device memory the container uses, and if memory was set to be _on_ device currently.
    fn device_memory(&self) -> (MemSizeMb, bool);
    /// Remove the attached device if it exists
    fn revoke_device(&self) -> Option<crate::services::resources::gpu::GPU>;
    async fn move_to_device(&self, tid: &TransactionId) -> Result<()>;
    async fn move_from_device(&self, tid: &TransactionId) -> Result<()>;
    /// Perform any actions that might improve performance before invocation(s) are sent
    async fn prewarm_actions(&self, _tid: &TransactionId) -> Result<()> {
        Ok(())
    }
    /// Perform any actions to reduce resource usage knowing that function will not be called in the near future
    async fn cooldown_actions(&self, _tid: &TransactionId) -> Result<()> {
        Ok(())
    }
    /// Add an item to be 'held' by the container until it's removal by the container manager
    fn add_drop_on_remove(&self, item: DroppableToken, tid: &TransactionId);
    /// Release all held virtual resources, called on container removal
    fn remove_drop(&self, tid: &TransactionId);
}

/// Cast a container pointer to a concrete type
/// ```let contd: &ContainerdContainer = cast::<ContainerdContainer>(&container, tid)```
pub fn cast<T>(c: &Container) -> Result<&T>
where
    T: ContainerT + ToAny,
{
    match c.as_any().downcast_ref::<T>() {
        Some(i) => Ok(i),
        None => {
            anyhow::bail!(
                "Failed to cast ContainerT type {} to {:?}",
                std::any::type_name::<Container>(),
                std::any::type_name::<T>()
            );
        },
    }
}

pub type Container = Arc<dyn ContainerT>;

#[derive(Debug, serde::Deserialize)]
pub struct ParsedResult {
    /// The result string from the user execution
    pub user_result: Option<String>,
    /// The error returned by user execution, if an error occurred
    pub user_error: Option<String>,
    /// The timestamp when user code started
    pub start: String,
    /// The timestamp when user code ended
    pub end: String,
    /// If the container tracked that the invocation was run for the first time
    pub was_cold: bool,
    /// Duration in seconds for how long the execution took
    /// As recorded by the platform _inside_ the container
    pub duration_sec: f64,
    /// Amount of GPU memory allocated by the device.
    #[serde(default)]
    pub gpu_allocation_mb: MemSizeMb,
}
impl ParsedResult {
    pub fn parse_slice(from: &[u8], tid: &TransactionId) -> Result<Self> {
        match serde_json::from_slice(from) {
            Ok(p) => Ok(p),
            Err(e) => {
                bail_error!(error=%e, tid=tid, "Failed to parse u8 from invocation return")
            },
        }
    }

    pub fn parse(from: &str, tid: &TransactionId) -> Result<Self> {
        match serde_json::from_str(from) {
            Ok(p) => Ok(p),
            Err(e) => {
                bail_error!(error=%e, tid=tid, value=%from, "Failed to parse json from invocation return")
            },
        }
    }

    pub fn result_string(&self) -> Result<String> {
        match &self.user_result {
            Some(s) => Ok(s.clone()),
            None => match &self.user_error {
                Some(s) => Ok(s.clone()),
                None => anyhow::bail!("ParsedResult had neither a user result or error, somehow!"),
            },
        }
    }
}

/// A struct denoting that the owner has a lock on the container to invoke with
pub struct ContainerLock {
    pub container: Container,
    container_mrg: Arc<ContainerManager>,
    transaction_id: TransactionId,
}

impl ContainerLock {
    pub fn new(container: Container, container_mrg: &Arc<ContainerManager>, tid: &TransactionId) -> Self {
        ContainerLock {
            container,
            container_mrg: container_mrg.clone(),
            transaction_id: tid.clone(),
        }
    }

    /// Ask the internal container to invoke the function
    /// Returns
    /// [ParsedResult] A result representing the function output, the user result plus some platform tracking
    /// [Duration]: The E2E latency between the worker and the container
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, json_args), fields(tid=self.transaction_id), name="ContainerLock::invoke"))]
    pub async fn invoke(&self, json_args: &str) -> Result<(ParsedResult, Duration)> {
        self.container.invoke(json_args, &self.transaction_id).await
    }
}

/// Automatically release the lock on the container when the lock is dropped
impl Drop for ContainerLock {
    fn drop(&mut self) {
        debug!(tid=self.transaction_id, container_id=%self.container.container_id(), "Dropping container lock");
        self.container_mrg
            .return_container(&self.container, &self.transaction_id);
    }
}

#[derive(Debug)]
/// An container start failed because the system did not have enough memory
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
impl std::error::Error for InsufficientMemoryError {}

#[derive(Debug)]
/// An container start failed because the system did not have a free GPU
pub struct InsufficientGPUError {}
impl std::fmt::Display for InsufficientGPUError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "No GPU available to launch container")?;
        Ok(())
    }
}
impl std::error::Error for InsufficientGPUError {}

#[derive(Debug)]
/// An container start failed with a platform error
pub struct ContainerStartupError {
    pub message: String,
}
impl std::fmt::Display for ContainerStartupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "Startup error: {}", self.message)?;
        Ok(())
    }
}
impl std::error::Error for ContainerStartupError {}
