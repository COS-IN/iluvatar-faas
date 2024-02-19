use crate::services::containers::structs::CtrResources;
use crate::services::{
    containers::{
        http_client::HttpContainerClient,
        structs::{ContainerState, ContainerT, ParsedResult},
    },
    network::network_structs::Namespace,
    registration::RegisteredFunction,
    resources::gpu::GPU,
};
use anyhow::Result;
use iluvatar_library::{
    bail_error, utils,
    utils::{execute_cmd, execute_cmd_checked},
};
use iluvatar_library::{
    transaction::TransactionId,
    types::{Compute, DroppableToken, Isolation, MemSizeMb},
    utils::port_utils::Port,
};
use parking_lot::{Mutex, RwLock};
use std::{
    num::NonZeroU32,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tracing::{debug, error, info, warn};

use serde_json::{from_str, Error, Value};
use std::collections::HashMap;

type JsonMap = HashMap<String, serde_json::Value>;

#[derive(Debug)]
pub struct Task {
    pub pid: u32,
    pub container_id: Option<String>,
    pub running: bool,
}

#[derive(Debug)]
#[allow(unused)]
pub struct ContainerdContainer {
    pub container_id: String,
    /// The containerd task in the container
    pub task: Task,
    pub port: Port,
    /// IP Address assigned
    pub address: String,
    /// Network interface
    pub net_iface_name: String,
    fqdn: String,
    /// the associated function inside the container
    pub function: Arc<RegisteredFunction>,
    last_used: RwLock<SystemTime>,
    /// The namespace container has been put in
    pub namespace: Arc<Namespace>,
    /// number of invocations a container has performed
    invocations: Mutex<u32>,
    /// Most recently clocked memory usage
    mem_usage: RwLock<MemSizeMb>,
    state: Mutex<ContainerState>,
    client: HttpContainerClient,
    compute: Compute,
    device: Option<Arc<GPU>>,
    ctr_resources: RwLock<CtrResources>,
}

impl ContainerdContainer {
    pub fn new(
        container_id: String,
        task: Task,
        port: Port,
        address: String,
        net_iface_name: String,
        _parallel_invokes: NonZeroU32,
        fqdn: &str,
        function: &Arc<RegisteredFunction>,
        ns: Arc<Namespace>,
        invoke_timeout: u64,
        state: ContainerState,
        compute: Compute,
        device: Option<Arc<GPU>>,
        tid: &TransactionId,
    ) -> Result<Self> {
        let client = HttpContainerClient::new(&container_id, port, &address, invoke_timeout, tid)?;
        Ok(ContainerdContainer {
            container_id,
            task,
            port,
            address,
            net_iface_name,
            client,
            compute,
            fqdn: fqdn.to_owned(),
            function: function.clone(),
            last_used: RwLock::new(SystemTime::now()),
            namespace: ns,
            invocations: Mutex::new(0),
            mem_usage: RwLock::new(function.memory),
            state: Mutex::new(state),
            device,
            ctr_resources: RwLock::new(CtrResources {
                cpu: 0.0,
                mem: 0.0,
                disk: 0.0,
                cumul_disk: 0.0,
                net: 0.0,
                cumul_net: 0.0,
            }),
        })
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self), fields(tid=%_tid, fqdn=%self.fqdn)))]
    fn update_metadata_on_invoke(&self, _tid: &TransactionId) {
        *self.invocations.lock() += 1;
        self.touch();
    }
}

#[tonic::async_trait]
impl ContainerT for ContainerdContainer {
    #[tracing::instrument(skip(self, json_args), fields(tid=%tid, fqdn=%self.fqdn), name="ContainerdContainer::invoke")]
    async fn invoke(&self, json_args: &str, tid: &TransactionId) -> Result<(ParsedResult, Duration)> {
        self.update_metadata_on_invoke(tid);
        match self.client.invoke(json_args, tid, &self.container_id).await {
            Ok(r) => Ok(r),
            Err(e) => {
                warn!(tid=%tid, container_id=%self.container_id(), "Marking container unhealthy");
                self.mark_unhealthy();
                Err(e)
            }
        }
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

    fn touch(&self) {
        let mut lock = self.last_used.write();
        *lock = SystemTime::now();
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
    fn device_resource(&self) -> &Option<Arc<GPU>> {
        &self.device
    }

    fn update_ctr_resources(&self) {
        // Networking:
        let vethname = self.net_iface_name.clone();
        // ip -s -j link {}
        // json output, filter stats64.rx.bytes and stats64.tx.bytes
        let tid: TransactionId = String::from("Na");
        let ipout = match execute_cmd_checked(
            "/usr/sbin/ip",
            vec!["-s", "-j", "link", "show", vethname.as_str()],
            None,
            &tid,
        ) {
            Ok(out) => out,
            Err(e) => return,
        };

        let stdout = String::from_utf8_lossy(&ipout.stdout);

        debug!(?stdout, "Output from ip link");

        let json_out = serde_json::from_str::<Vec<Value>>(&stdout).unwrap();

        let j = &json_out[0];

        debug!(?j, "Parsed JSON");

        let rx = &j["stats64"]["rx"];
        let rb = &rx["bytes"];

        debug!(?rb, "Parsed RB");

        let read_bytes = match rb.as_f64() {
            Some(v) => v,
            _ => 0.0,
        };

        debug!(?read_bytes, "read bytes");

        let wx = &j["stats64"]["wx"];
        let wb = &wx["bytes"];
        let write_bytes = match wb.as_f64() {
            Some(v) => v,
            _ => 0.0,
        };

        let total_bytes = (read_bytes + write_bytes) as f32;

        let old_r = self.ctr_resources.read().unwrap().clone();
        let delta_n = total_bytes - old_r.cumul_net;
        let mlock = self.ctr_resources.write();
        *mlock.cumul_net = total_bytes;
        *mlock.net = delta_n;

        old_r.net = delta_n;
        old_r.cumul_net = total_bytes;

        // let old_c = self.ctr_resources.cumul_net.clone();
        // let diff = total_bytes - old_c ;
        // self.ctr_resources.cumul_net = total_bytes ;
        // self.ctr_resources.net = diff ;

        //self.ctr_resources.unwrap().cumul_net = total_bytes as f32;

        debug!(vethname = vethname, bytes = total_bytes, "Read network bytes");
        // return old_r
    }

    fn add_drop_on_remove(&self, _item: DroppableToken, _tid: &TransactionId) {
        todo!("Containerd containers are CPU-only and shouldn't be given anything to drop on remove!");
    }
    fn remove_drop(&self, _tid: &TransactionId) {}
}

impl crate::services::containers::structs::ToAny for ContainerdContainer {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
