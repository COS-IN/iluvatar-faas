use crate::services::containers::clients::{create_container_client, ContainerClient};
use crate::services::registration::RegisteredFunction;
use crate::services::resources::gpu::ProtectedGpuRef;
use crate::services::{
    containers::structs::{ContainerState, ContainerT, ParsedResult},
    resources::gpu::GPU,
};
use anyhow::Result;
use iluvatar_library::clock::now;
use iluvatar_library::types::{err_val, DroppableToken, ResultErrorVal};
use iluvatar_library::utils::execute_cmd;
use iluvatar_library::{
    transaction::TransactionId,
    types::{Compute, Isolation, MemSizeMb, Utilization},
    utils::port::Port,
};
use parking_lot::{Mutex, RwLock};
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::path::Path;
use std::{num::NonZeroU32, sync::Arc, time::Duration};
use tokio::time::Instant;
use tracing::{debug, error, warn};

lazy_static::lazy_static! {
    pub static ref DOCKER_INSPECT_TID: TransactionId = "DockerInspectCall".to_string();
}

fn inspect_container(container_id: &str, field: &str) -> String {
    let pargs = vec!["inspect", "-f", field, container_id];
    // just an inspect cmd no need for env
    if let Ok(output) = execute_cmd("/usr/bin/docker", pargs, None, &DOCKER_INSPECT_TID) {
        return String::from_utf8_lossy(&output.stdout)
            .trim()
            .trim_matches('\'')
            .to_string();
    }
    "".to_string()
}

fn docker_container_cgroup_id(container_id: &str) -> String {
    let container_full_id = inspect_container(&container_id, "'{{.Id}}'");
    "docker-".to_string() + &container_full_id + ".scope"
}

#[allow(unused, dyn_drop)]
#[derive(iluvatar_library::ToAny)]
pub struct DockerContainer {
    pub container_id: String,
    pub cgroup_id: String,
    fqdn: String,
    /// the associated function inside the container
    pub function: Arc<RegisteredFunction>,
    last_used: RwLock<Instant>,
    /// number of invocations a container has performed
    invocations: Mutex<u32>,
    port: Port,
    state: Mutex<ContainerState>,
    pub client: Box<dyn ContainerClient>,
    compute: Compute,
    last_invoke_cpu_usage_usec: Mutex<u64>,
    last_invoke_cpu_utilization: Mutex<Utilization>,
    device: RwLock<Option<GPU>>,
    dev_mem_usage: RwLock<(MemSizeMb, bool)>,
    mem_usage: RwLock<MemSizeMb>,
    drop_on_remove: Mutex<Vec<DroppableToken>>,
    last_domain: std::sync::atomic::AtomicI32,
}

impl DockerContainer {
    pub async fn new(
        container_id: String,
        port: Port,
        address: String,
        _parallel_invokes: NonZeroU32,
        fqdn: &str,
        function: &Arc<RegisteredFunction>,
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
        let r = DockerContainer {
            mem_usage: RwLock::new(function.memory),
            dev_mem_usage: RwLock::new((0, true)),
            cgroup_id: docker_container_cgroup_id(&container_id),
            container_id,
            fqdn: fqdn.to_owned(),
            function: function.clone(),
            last_used: RwLock::new(now()),
            invocations: Mutex::new(0),
            port,
            client,
            compute,
            last_invoke_cpu_usage_usec: Mutex::new(0),
            last_invoke_cpu_utilization: Mutex::new(0),
            state: Mutex::new(state),
            device: RwLock::new(device),
            drop_on_remove: Mutex::new(vec![]),
            last_domain: std::sync::atomic::AtomicI32::new(-1),
        };
        Ok(r)
    }

    fn cpu_stat(&self, stat_name: &String) -> u64 {
        let scx_path = format!("/sys/fs/cgroup/scx.slice/{}/cpu.stat", self.cgroup_id);
        let system_path = format!("/sys/fs/cgroup/system.slice/{}/cpu.stat", self.cgroup_id);
        let filepath = if Path::new(&scx_path).is_file() { &scx_path } else { &system_path };
        let filepath = Path::new(filepath);

        if filepath.is_file() {
            let file = match File::open(filepath) {
                Ok(f) => f,
                Err(e) => {
                    error!(container_id=self.container_id, error=%e, cpu_stat=%filepath.display(), "Error opening container cpu_stat file");
                    return 0;
                },
            };

            let buf_reader = BufReader::new(file);
            for line in buf_reader.lines() {
                let line = line.unwrap_or("none none".to_string());
                let splits: Vec<&str> = line.split(" ").collect();
                if splits[0] == stat_name {
                    match u64::from_str_radix(splits[1], 10) {
                        Ok(stat_value) => return stat_value,
                        Err(e) => {
                            error!(container_id=self.container_id, stat_name=%splits[0], stat_value=%splits[1], error=%e, cpu_stat=%filepath.display(), "Error converting stat_value to u64");
                            return 0;
                        },
                    }
                }
            }
        } else {
            error!(container_id=self.container_id, cpu_stat=%filepath.display(), "Error container cpu_stat file does not exist");
        }

        0
    }

    fn stats_invoke_start(&self) {
        *self.last_invoke_cpu_usage_usec.lock() = self.cpu_stat(&"usage_usec".to_string());
    }

    fn stats_invoke_complete(&self) {
        let invoke_time = self.last_used.read().elapsed().as_micros() as u64;

        let cpu_usage_usec = self.cpu_stat(&"usage_usec".to_string());
        let last_invoke_cpu_usage_usec = self.last_invoke_cpu_usage_usec.lock();
        let cpu_time = cpu_usage_usec - *last_invoke_cpu_usage_usec;

        let mut last_invoke_cpu_utilization = self.last_invoke_cpu_utilization.lock();
        *last_invoke_cpu_utilization = (cpu_time * 100) / invoke_time;
    }
}

#[tonic::async_trait]
impl ContainerT for DockerContainer {
    #[tracing::instrument(skip(self, json_args), fields(tid=tid), name="DockerContainer::invoke")]
    async fn invoke(&self, json_args: &str, tid: &TransactionId) -> Result<(ParsedResult, Duration)> {
        *self.invocations.lock() += 1;
        self.touch();
        self.stats_invoke_start();
        match self.client.invoke(json_args, tid, &self.container_id).await {
            Ok(r) => {
                self.stats_invoke_complete();
                Ok(r)
            },
            Err(e) => {
                warn!(tid=tid, container_id=%self.container_id(), "Marking container unhealthy");
                self.mark_unhealthy();
                self.stats_invoke_complete();
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

    fn cgroup_id(&self) -> &String {
        &self.cgroup_id
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
        Isolation::DOCKER
    }
    fn compute_type(&self) -> Compute {
        self.compute
    }
    fn cpu_utilization(&self) -> Utilization {
        *self.last_invoke_cpu_utilization.lock()
    }
    fn last_domain(&self) -> Option<usize> {
        let raw = self.last_domain.load(std::sync::atomic::Ordering::Relaxed);
        (raw >= 0).then_some(raw as usize)
    }
    fn set_last_domain(&self, domain: Option<usize>) {
        let raw = domain.and_then(|d| i32::try_from(d).ok()).unwrap_or(-1);
        self.last_domain.store(raw, std::sync::atomic::Ordering::Relaxed);
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
    async fn prewarm_actions(&self, tid: &TransactionId) -> Result<()> {
        self.client.move_to_device(tid, &self.container_id).await
    }
    async fn cooldown_actions(&self, tid: &TransactionId) -> Result<()> {
        self.client.move_from_device(tid, &self.container_id).await
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
