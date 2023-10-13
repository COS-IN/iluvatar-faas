use crate::services::{
    containers::structs::{ContainerState, ContainerT, ParsedResult},
    network::network_structs::Namespace,
    registration::RegisteredFunction,
    resources::gpu::GPU,
};
use anyhow::Result;
use iluvatar_library::{
    bail_error,
    transaction::TransactionId,
    types::{Compute, Isolation, MemSizeMb},
    utils::{calculate_base_uri, calculate_invoke_uri, port_utils::Port},
};
use parking_lot::{Mutex, RwLock};
use reqwest::{Client, Response};
use std::{
    num::NonZeroU32,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tracing::warn;

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
    pub address: String,
    pub invoke_uri: String,
    pub base_uri: String,
    pub fqdn: String,
    /// the associated function inside the container
    pub function: Arc<RegisteredFunction>,
    last_used: RwLock<SystemTime>,
    /// The namespace container has been put in
    pub namespace: Arc<Namespace>,
    /// number of invocations a container has performed
    invocations: Mutex<u32>,
    /// Most recently clocked memory usage
    pub mem_usage: RwLock<MemSizeMb>,
    state: Mutex<ContainerState>,
    client: Client,
    compute: Compute,
    device: Option<Arc<GPU>>,
}

impl ContainerdContainer {
    pub fn new(
        container_id: String,
        task: Task,
        port: Port,
        address: String,
        _parallel_invokes: NonZeroU32,
        fqdn: &String,
        function: &Arc<RegisteredFunction>,
        ns: Arc<Namespace>,
        invoke_timeout: u64,
        state: ContainerState,
        compute: Compute,
        device: Option<Arc<GPU>>,
    ) -> Result<Self> {
        let invoke_uri = calculate_invoke_uri(&address, port);
        let base_uri = calculate_base_uri(&address, port);
        let client = match reqwest::Client::builder()
            .pool_max_idle_per_host(0)
            .pool_idle_timeout(None)
            // tiny buffer to allow for network delay from possibly full system
            .connect_timeout(Duration::from_secs(invoke_timeout + 2))
            .build()
        {
            Ok(c) => c,
            Err(e) => bail_error!(error=%e, "Unable to build reqwest HTTP client"),
        };
        Ok(ContainerdContainer {
            container_id,
            task,
            port,
            address,
            invoke_uri,
            base_uri,
            client,
            compute,
            fqdn: fqdn.clone(),
            function: function.clone(),
            last_used: RwLock::new(SystemTime::now()),
            namespace: ns,
            invocations: Mutex::new(0),
            mem_usage: RwLock::new(function.memory),
            state: Mutex::new(state),
            device,
        })
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self), fields(tid=%_tid, fqdn=%self.fqdn)))]
    fn update_metadata_on_invoke(&self, _tid: &TransactionId) {
        *self.invocations.lock() += 1;
        self.touch();
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, json_args), fields(tid=%tid, fqdn=%self.fqdn)))]
    async fn call_container(&self, json_args: &String, tid: &TransactionId) -> Result<(Response, Duration)> {
        let builder = self
            .client
            .post(&self.invoke_uri)
            .body(json_args.to_owned())
            .header("Content-Type", "application/json");
        let start = SystemTime::now();
        let response = match builder.send().await {
            Ok(r) => r,
            Err(e) => {
                self.mark_unhealthy();
                bail_error!(tid=%tid, error=%e, container_id=%self.container_id, "HTTP error when trying to connect to container");
            }
        };
        let duration = match start.elapsed() {
            Ok(dur) => dur,
            Err(e) => bail_error!(tid=%tid, error=%e, "Timer error recording invocation duration"),
        };
        Ok((response, duration))
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, response), fields(tid=%tid, fqdn=%self.fqdn)))]
    async fn download_text(&self, response: Response, tid: &TransactionId) -> Result<ParsedResult> {
        let r = match response.text().await {
            Ok(r) => r,
            Err(e) => {
                bail_error!(tid=%tid, error=%e, container_id=%self.container_id, "Error reading text data from container")
            }
        };
        ParsedResult::parse(r, tid)
    }
}

#[tonic::async_trait]
impl ContainerT for ContainerdContainer {
    #[tracing::instrument(skip(self, json_args), fields(tid=%tid, fqdn=%self.fqdn), name="ContainerdContainer::invoke")]
    async fn invoke(&self, json_args: &String, tid: &TransactionId) -> Result<(ParsedResult, Duration)> {
        self.update_metadata_on_invoke(tid);
        let (response, duration) = self.call_container(json_args, tid).await?;
        let status = response.status();
        let result = self.download_text(response, tid).await?;
        match status {
            reqwest::StatusCode::OK => (),
            reqwest::StatusCode::UNPROCESSABLE_ENTITY => {
                self.mark_unhealthy();
                warn!(tid=%tid, status=422, result=?result, "A user code error occured in the container, marking for removal");
            }
            reqwest::StatusCode::INTERNAL_SERVER_ERROR => {
                self.mark_unhealthy();
                bail_error!(tid=%tid, status=500, result=?result, "A platform error occured in the container, making for removal");
            }
            other => {
                self.mark_unhealthy();
                bail_error!(tid=%tid, status=%other, "Unknown status code from container call");
            }
        };

        Ok((result, duration))
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
}

impl crate::services::containers::structs::ToAny for ContainerdContainer {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
