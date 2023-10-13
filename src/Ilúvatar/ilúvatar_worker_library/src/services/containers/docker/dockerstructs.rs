use crate::services::registration::RegisteredFunction;
use crate::services::{
    containers::structs::{ContainerState, ContainerT, ParsedResult},
    resources::gpu::GPU,
};
use anyhow::Result;
use iluvatar_library::{
    bail_error,
    transaction::TransactionId,
    types::{Compute, Isolation, MemSizeMb},
    utils::{calculate_base_uri, calculate_invoke_uri, port::Port},
};
use parking_lot::{Mutex, RwLock};
use reqwest::Client;
use std::{
    num::NonZeroU32,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tracing::warn;

#[derive(Debug)]
#[allow(unused)]
pub struct DockerContainer {
    pub container_id: String,
    pub fqdn: String,
    /// the associated function inside the container
    pub function: Arc<RegisteredFunction>,
    pub last_used: RwLock<SystemTime>,
    /// number of invocations a container has performed
    pub invocations: Mutex<u32>,
    pub port: Port,
    pub invoke_uri: String,
    pub base_uri: String,
    state: Mutex<ContainerState>,
    client: Client,
    compute: Compute,
    device: Option<Arc<GPU>>,
    mem_usage: RwLock<MemSizeMb>,
}

impl DockerContainer {
    pub fn new(
        container_id: String,
        port: Port,
        address: String,
        _parallel_invokes: NonZeroU32,
        fqdn: &String,
        function: &Arc<RegisteredFunction>,
        invoke_timeout: u64,
        state: ContainerState,
        compute: Compute,
        device: Option<Arc<GPU>>,
    ) -> Result<Self> {
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
        let r = DockerContainer {
            mem_usage: RwLock::new(function.memory),
            container_id: container_id,
            fqdn: fqdn.clone(),
            function: function.clone(),
            last_used: RwLock::new(SystemTime::now()),
            invocations: Mutex::new(0),
            port,
            client,
            compute,
            invoke_uri: calculate_invoke_uri(address.as_str(), port),
            base_uri: calculate_base_uri(address.as_str(), port),
            state: Mutex::new(state),
            device,
        };
        Ok(r)
    }
}

#[tonic::async_trait]
impl ContainerT for DockerContainer {
    #[tracing::instrument(skip(self, json_args), fields(tid=%tid), name="DockerContainer::invoke")]
    async fn invoke(&self, json_args: &String, tid: &TransactionId) -> Result<(ParsedResult, Duration)> {
        *self.invocations.lock() += 1;
        self.touch();
        let build = self
            .client
            .post(&self.invoke_uri)
            .body(json_args.to_owned())
            .header("Content-Type", "application/json");

        let start = SystemTime::now();
        let response = match build.send().await {
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
        let status = response.status();
        let r = match response.text().await {
            Ok(r) => r,
            Err(e) => {
                bail_error!(tid=%tid, error=%e, container_id=%self.container_id, "Error reading text data from container")
            }
        };
        match status {
            reqwest::StatusCode::OK => (),
            reqwest::StatusCode::UNPROCESSABLE_ENTITY => {
                self.mark_unhealthy();
                warn!(tid=%tid, status=422, "A user code error occured in the container, marking for removal");
            }
            reqwest::StatusCode::INTERNAL_SERVER_ERROR => {
                self.mark_unhealthy();
                bail_error!(tid=%tid, status=500, result=%r, "A platform error occured in the container, making for removal");
            }
            other => {
                self.mark_unhealthy();
                bail_error!(tid=%tid, status=%other, result=%r, "Unknown status code from container call");
            }
        }
        let result = ParsedResult::parse(r, tid)?;
        Ok((result, duration))
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
}

impl crate::services::containers::structs::ToAny for DockerContainer {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
