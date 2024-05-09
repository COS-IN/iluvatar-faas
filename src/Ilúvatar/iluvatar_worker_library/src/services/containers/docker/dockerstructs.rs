use crate::services::containers::http_client::HttpContainerClient;
use crate::services::registration::RegisteredFunction;
use crate::services::{
    containers::structs::{ContainerState, ContainerT, ParsedResult},
    resources::gpu::GPU,
};
use anyhow::Result;
use iluvatar_library::types::DroppableToken;
use iluvatar_library::{
    transaction::TransactionId,
    types::{Compute, Isolation, MemSizeMb},
    utils::port::Port,
};
use parking_lot::{Mutex, RwLock};
use std::{
    num::NonZeroU32,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tracing::{debug, warn};

#[allow(unused, dyn_drop)]
pub struct DockerContainer {
    pub container_id: String,
    fqdn: String,
    /// the associated function inside the container
    pub function: Arc<RegisteredFunction>,
    last_used: RwLock<SystemTime>,
    /// number of invocations a container has performed
    invocations: Mutex<u32>,
    port: Port,
    state: Mutex<ContainerState>,
    pub client: HttpContainerClient,
    compute: Compute,
    device: Option<Arc<GPU>>,
    mem_usage: RwLock<MemSizeMb>,
    drop_on_remove: Mutex<Vec<DroppableToken>>,
}

impl DockerContainer {
    pub fn new(
        container_id: String,
        port: Port,
        address: String,
        _parallel_invokes: NonZeroU32,
        fqdn: &str,
        function: &Arc<RegisteredFunction>,
        invoke_timeout: u64,
        state: ContainerState,
        compute: Compute,
        device: Option<Arc<GPU>>,
        tid: &TransactionId,
    ) -> Result<Self> {
        let client = HttpContainerClient::new(&container_id, port, &address, invoke_timeout, tid)?;
        let r = DockerContainer {
            mem_usage: RwLock::new(function.memory),
            container_id,
            fqdn: fqdn.to_owned(),
            function: function.clone(),
            last_used: RwLock::new(SystemTime::now()),
            invocations: Mutex::new(0),
            port,
            client,
            compute,
            state: Mutex::new(state),
            device,
            drop_on_remove: Mutex::new(vec![]),
        };
        Ok(r)
    }
}

#[tonic::async_trait]
impl ContainerT for DockerContainer {
    #[tracing::instrument(skip(self, json_args), fields(tid=%tid), name="DockerContainer::invoke")]
    async fn invoke(&self, json_args: &str, tid: &TransactionId) -> Result<(ParsedResult, Duration)> {
        *self.invocations.lock() += 1;
        self.touch();
        match self.client.invoke(json_args, tid, &self.container_id).await {
            Ok(r) => Ok(r),
            Err(e) => {
                warn!(tid=%tid, container_id=%self.container_id(), "Marking container unhealthy");
                self.mark_unhealthy();
                Err(e)
            }
        }
    }

    async fn prewarm_actions(&self, tid: &TransactionId) -> Result<()> {
        self.client.move_to_device(tid, &self.container_id).await
    }

    async fn cooldown_actions(&self, tid: &TransactionId) -> Result<()> {
        self.client.move_from_device(tid, &self.container_id).await
    }

    fn touch(&self) {
        let mut lock = self.last_used.write();
        *lock = SystemTime::now();
    }

    fn container_id(&self) -> &String {
        &self.container_id
    }

    fn last_used(&self) -> SystemTime {
        *self.last_used.read()
    }

    fn invocations(&self) -> u32 {
        *self.invocations.lock()
    }

    fn get_curr_mem_usage(&self) -> MemSizeMb {
        *self.mem_usage.read()
    }

    fn set_curr_mem_usage(&self, usage: MemSizeMb) {
        *self.mem_usage.write() = usage;
    }

    fn function(&self) -> Arc<RegisteredFunction> {
        self.function.clone()
    }

    fn fqdn(&self) -> &String {
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
    fn container_type(&self) -> Isolation {
        Isolation::DOCKER
    }
    fn compute_type(&self) -> Compute {
        self.compute
    }
    fn device_resource(&self) -> &Option<Arc<GPU>> {
        &self.device
    }
    fn add_drop_on_remove(&self, item: DroppableToken, tid: &TransactionId) {
        debug!(tid=%tid, container_id=%self.container_id(), "Adding token to drop on remove");
        self.drop_on_remove.lock().push(item);
    }
    fn remove_drop(&self, tid: &TransactionId) {
        let mut lck = self.drop_on_remove.lock();
        let to_drop = std::mem::take(&mut *lck);
        debug!(tid=%tid, container_id=%self.container_id(), num_tokens=to_drop.len(), "Dropping tokens");
        for i in to_drop.into_iter() {
            drop(i);
        }
    }
}

impl crate::services::containers::structs::ToAny for DockerContainer {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
