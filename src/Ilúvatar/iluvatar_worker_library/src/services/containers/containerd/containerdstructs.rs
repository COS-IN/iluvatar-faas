use crate::services::containers::clients::{create_container_client, ContainerClient};
use crate::services::resources::gpu::ProtectedGpuRef;
use crate::services::{
    containers::structs::{ContainerState, ContainerT, ParsedResult},
    network::network_structs::Namespace,
    registration::RegisteredFunction,
    resources::gpu::GPU,
};
use anyhow::Result;
use iluvatar_library::clock::now;
use iluvatar_library::types::{err_val, ResultErrorVal};
use iluvatar_library::{
    transaction::TransactionId,
    types::{Compute, DroppableToken, Isolation, MemSizeMb},
    utils::port_utils::Port,
    ToAny,
};
use parking_lot::{Mutex, RwLock};
use std::{num::NonZeroU32, sync::Arc, time::Duration};
use tokio::time::Instant;
use tracing::{debug, warn};

pub struct Task {
    pub pid: u32,
    pub container_id: Option<String>,
    pub running: bool,
}

#[allow(unused)]
#[derive(ToAny)]
pub struct ContainerdContainer {
    pub container_id: String,
    /// The containerd task in the container
    pub task: Task,
    pub port: Port,
    pub address: String,
    fqdn: String,
    /// the associated function inside the container
    pub function: Arc<RegisteredFunction>,
    last_used: RwLock<Instant>,
    /// The namespace container has been put in
    pub namespace: Arc<Namespace>,
    /// number of invocations a container has performed
    invocations: Mutex<u32>,
    /// Most recently clocked memory usage
    mem_usage: RwLock<MemSizeMb>,
    state: Mutex<ContainerState>,
    client: Box<dyn ContainerClient>,
    compute: Compute,
    device: RwLock<Option<GPU>>,
    /// Most recently clocked memory usage
    dev_mem_usage: RwLock<(MemSizeMb, bool)>,
    drop_on_remove: Mutex<Vec<DroppableToken>>,
}

impl ContainerdContainer {
    pub async fn new(
        container_id: String,
        task: Task,
        port: Port,
        address: String,
        _parallel_invokes: NonZeroU32,
        fqdn: &str,
        function: &Arc<RegisteredFunction>,
        ns: Arc<Namespace>,
        invoke_timeout: u64,
        state: ContainerState,
        compute: Compute,
        device: Option<GPU>,
        tid: &TransactionId,
    ) -> ResultErrorVal<Self, Option<GPU>> {
        let client = match create_container_client(function, &container_id, port, &address, invoke_timeout, tid).await {
            Ok(c) => c,
            Err(e) => return err_val(e, device),
        };
        Ok(ContainerdContainer {
            container_id,
            task,
            port,
            address,
            client,
            compute,
            fqdn: fqdn.to_owned(),
            function: function.clone(),
            last_used: RwLock::new(now()),
            namespace: ns,
            invocations: Mutex::new(0),
            mem_usage: RwLock::new(function.memory),
            dev_mem_usage: RwLock::new((0, true)),
            state: Mutex::new(state),
            device: RwLock::new(device),
            drop_on_remove: Mutex::new(vec![]),
        })
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self), fields(tid=_tid, fqdn=%self.fqdn)))]
    fn update_metadata_on_invoke(&self, _tid: &TransactionId) {
        *self.invocations.lock() += 1;
        self.touch();
    }
}

#[tonic::async_trait]
impl ContainerT for ContainerdContainer {
    #[tracing::instrument(skip(self, json_args), fields(tid=tid, fqdn=%self.fqdn), name="ContainerdContainer::invoke")]
    async fn invoke(&self, json_args: &str, tid: &TransactionId) -> Result<(ParsedResult, Duration)> {
        self.update_metadata_on_invoke(tid);
        match self.client.invoke(json_args, tid, &self.container_id).await {
            Ok(r) => Ok(r),
            Err(e) => {
                warn!(tid=tid, container_id=%self.container_id(), "Marking container unhealthy");
                self.mark_unhealthy();
                Err(e)
            },
        }
    }
    fn touch(&self) {
        let mut lock = self.last_used.write();
        *lock = now();
    }

    fn container_id(&self) -> &String {
        &self.container_id
    }

    fn last_used(&self) -> Instant {
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
        Isolation::CONTAINERD
    }
    fn compute_type(&self) -> Compute {
        self.compute
    }
    fn device_resource(&self) -> ProtectedGpuRef<'_> {
        self.device.read()
    }
    fn set_device_memory(&self, size: MemSizeMb) {
        let mut lck = self.dev_mem_usage.write();
        *lck = (size, lck.1);
    }
    async fn move_to_device(&self, tid: &TransactionId) -> Result<()> {
        {
            let mut lck = self.dev_mem_usage.write();
            *lck = (lck.0, true);
            drop(lck);
        }
        self.client.move_to_device(tid, &self.container_id).await
    }
    async fn move_from_device(&self, tid: &TransactionId) -> Result<()> {
        {
            let mut lck = self.dev_mem_usage.write();
            *lck = (lck.0, false);
            drop(lck);
        }
        self.client.move_from_device(tid, &self.container_id).await
    }
    fn device_memory(&self) -> (MemSizeMb, bool) {
        *self.dev_mem_usage.read()
    }
    fn revoke_device(&self) -> Option<GPU> {
        *self.dev_mem_usage.write() = (0, false);
        self.device.write().take()
    }

    fn add_drop_on_remove(&self, item: DroppableToken, tid: &TransactionId) {
        debug!(tid=tid, container_id=%self.container_id(), "Adding token to drop on remove");
        self.drop_on_remove.lock().push(item);
    }
    fn remove_drop(&self, tid: &TransactionId) {
        let mut lck = self.drop_on_remove.lock();
        let to_drop = std::mem::take(&mut *lck);
        debug!(tid=tid, container_id=%self.container_id(), num_tokens=to_drop.len(), "Dropping tokens");
        for i in to_drop.into_iter() {
            drop(i);
        }
    }
}
