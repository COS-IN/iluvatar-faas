use iluvatar_library::types::{Compute, ContainerServer};
use iluvatar_library::utils::port_utils::Port;
use iluvatar_library::{
    types::{CommunicationMethod, Isolation, ResourceTimings},
    utils::calculate_fqdn,
};
use iluvatar_rpc::rpc::{RegisterRequest, RegisterWorkerRequest};
use serde::{Deserialize, Serialize};

#[allow(unused)]
#[derive(Deserialize, Serialize, Debug)]
pub struct RegisteredWorker {
    pub name: String,
    pub isolation: Isolation,
    pub communication_method: CommunicationMethod,
    pub host: String,
    pub port: Port,
    pub memory: i64,
    pub cpus: u32,
}
impl RegisteredWorker {
    pub fn from(req: RegisterWorkerRequest) -> anyhow::Result<Self> {
        Ok(RegisteredWorker {
            name: req.name,
            isolation: Isolation::from(req.isolation),
            communication_method: u32::try_into(req.communication_method)?,
            host: req.host,
            port: req.port as Port,
            memory: req.memory,
            cpus: req.cpus,
        })
    }
}

#[allow(unused)]
#[derive(Deserialize, Serialize, Debug)]
pub struct RegisteredFunction {
    pub fqdn: String,
    pub function_name: String,
    pub function_version: String,
    pub image_name: String,
    pub memory: i64,
    pub cpus: u32,
    pub parallel_invokes: u32,
    pub isolate: Isolation,
    pub compute: Compute,
    pub server: ContainerServer,
    pub timings: Option<ResourceTimings>,
}
impl RegisteredFunction {
    pub fn from(req: RegisterRequest) -> Self {
        RegisteredFunction {
            fqdn: calculate_fqdn(&req.function_name, &req.function_version),
            function_name: req.function_name,
            function_version: req.function_version,
            image_name: req.image_name,
            memory: req.memory,
            cpus: req.cpus,
            parallel_invokes: req.parallel_invokes,
            compute: Compute::from(req.compute),
            isolate: Isolation::from(req.isolate),
            server: ContainerServer::try_from(req.container_server).unwrap_or(ContainerServer::default()),
            timings: match req.resource_timings_json.is_empty() {
                true => None,
                _ => match serde_json::from_str::<ResourceTimings>(&req.resource_timings_json) {
                    Ok(t) => Some(t),
                    Err(_) => None,
                },
            },
        }
    }
}
