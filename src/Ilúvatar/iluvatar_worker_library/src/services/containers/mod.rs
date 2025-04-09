use self::docker::DockerIsolation;
use super::{registration::RegisteredFunction, resources::gpu::GPU};
use crate::services::containers::{containerd::ContainerdIsolation, simulator::SimulatorIsolation, structs::Container};
use crate::services::network::namespace_manager::NamespaceManager;
use crate::worker_api::worker_config::WorkerConfig;
use anyhow::Result;
use iluvatar_library::char_map::WorkerCharMap;
use iluvatar_library::types::{ResultErrorVal, ToAny};
use iluvatar_library::{
    transaction::TransactionId,
    types::{Compute, Isolation, MemSizeMb},
};
use std::{collections::HashMap, sync::Arc};
use tonic::async_trait;
use tracing::{info, warn};

mod clients;
mod container_pool;
#[path = "./containerd/containerd.rs"]
pub mod containerd;
pub mod containermanager;
#[path = "./docker/docker.rs"]
pub mod docker;
pub mod eviction;
#[path = "./simulation/simulator.rs"]
pub mod simulator;
pub mod structs;

/// Run, manage, and interact-with containers.
#[async_trait]
pub trait ContainerIsolationService: ToAny + Send + Sync {
    /// Return a container that has been started with the given settings
    /// NOTE: you will have to ask the lifetime again to wait on the container to be started up
    async fn run_container(
        &self,
        fqdn: &str,
        image_name: &str,
        parallel_invokes: u32,
        namespace: &str,
        mem_limit_mb: MemSizeMb,
        cpus: u32,
        reg: &Arc<RegisteredFunction>,
        iso: Isolation,
        compute: Compute,
        device_resource: Option<GPU>,
        tid: &TransactionId,
    ) -> ResultErrorVal<Container, Option<GPU>>;

    /// removes a specific container, and all the related resources
    async fn remove_container(&self, container_id: Container, ctd_namespace: &str, tid: &TransactionId) -> Result<()>;

    async fn prepare_function_registration(
        &self,
        rf: &mut RegisteredFunction,
        namespace: &str,
        tid: &TransactionId,
    ) -> Result<()>;

    /// Removes _all_ containers owned by the lifecycle, returns the number removed
    async fn clean_containers(
        &self,
        namespace: &str,
        self_src: Arc<dyn ContainerIsolationService>,
        tid: &TransactionId,
    ) -> Result<()>;

    /// Waits for the startup message for a container to come through
    /// Really the task inside, the web server should write (something) to stdout when it is ready
    async fn wait_startup(&self, container: &Container, timout_ms: u64, tid: &TransactionId) -> Result<()>;

    /// Update the current resident memory size of the container
    /// If an error occurs, the memory usage will not be change and the container will be marked unhealthy
    async fn update_memory_usage_mb(&self, container: &Container, tid: &TransactionId) -> MemSizeMb;

    /// get the contents of the container's stdout as a string
    /// or an error message string if something went wrong
    async fn read_stdout(&self, container: &Container, tid: &TransactionId) -> String;
    /// get the contents of the container's stderr as a string
    /// or an error message string if something went wrong
    async fn read_stderr(&self, container: &Container, tid: &TransactionId) -> String;

    /// The backend type for this lifecycle
    /// Real backends should only submit one Isolation type, returning a vector here allows the simulation to "act" as multiple backends
    fn backend(&self) -> Vec<Isolation>;
}

/// Different containerization
pub type ContainerIsolationCollection = Arc<std::collections::HashMap<Isolation, Arc<dyn ContainerIsolationService>>>;

pub struct IsolationFactory {
    worker_config: WorkerConfig,
    cmap: WorkerCharMap,
}

impl IsolationFactory {
    pub fn new(worker_config: WorkerConfig, cmap: WorkerCharMap) -> Self {
        IsolationFactory { worker_config, cmap }
    }

    pub async fn get_isolation_services(
        &self,
        tid: &TransactionId,
        ensure_bridge: bool,
    ) -> Result<ContainerIsolationCollection> {
        let mut ret = HashMap::new();
        if iluvatar_library::utils::is_simulation() {
            info!(tid = tid, "Creating 'simulation' backend");
            let c = SimulatorIsolation::new(self.cmap.clone());
            self.insert_cycle(&mut ret, Arc::new(c))?;
        } else {
            if ContainerdIsolation::supported(tid).await {
                info!(tid = tid, "Creating 'containerd' backend");
                if let Some(networking) = self.worker_config.networking.as_ref() {
                    let netm = NamespaceManager::boxed(networking.clone(), tid, ensure_bridge)?;
                    let mut lifecycle = ContainerdIsolation::new(
                        netm,
                        self.worker_config.container_resources.clone(),
                        self.worker_config.limits.clone(),
                        self.worker_config.container_resources.docker_config.clone(),
                    );
                    lifecycle.connect().await?;
                    self.insert_cycle(&mut ret, Arc::new(lifecycle))?;
                } else {
                    warn!(
                        tid = tid,
                        "Containerd not supported because no networking config provided"
                    );
                }
            }
            if DockerIsolation::supported(tid).await {
                info!(tid = tid, "Creating 'docker' backend");
                let d = Arc::new(DockerIsolation::new(
                    self.worker_config.container_resources.clone(),
                    self.worker_config.limits.clone(),
                    self.worker_config.container_resources.docker_config.clone(),
                    tid,
                )?);
                self.insert_cycle(&mut ret, d)?;
            }
        }
        if ret.is_empty() {
            anyhow::bail!("No lifecycles were able to be made");
        }

        Ok(Arc::new(ret))
    }

    fn insert_cycle(
        &self,
        map: &mut HashMap<Isolation, Arc<dyn ContainerIsolationService>>,
        life: Arc<dyn ContainerIsolationService>,
    ) -> Result<()> {
        for iso in life.backend() {
            if map.contains_key(&iso) {
                anyhow::bail!("Got multiple backend registrations for Isolation {:?}", iso);
            }
            map.insert(iso, life.clone());
        }
        Ok(())
    }
}
