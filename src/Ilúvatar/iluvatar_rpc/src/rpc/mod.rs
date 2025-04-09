#![allow(clippy::derive_partial_eq_without_eq)]
tonic::include_proto!("iluvatar_rpc");
use iluvatar_library::types::ContainerServer;
use iluvatar_library::{
    transaction::TransactionId,
    types::{Compute, Isolation, MemSizeMb, ResourceTimings},
};

impl InvokeResponse {
    pub fn error(message: &str) -> Self {
        InvokeResponse {
            json_result: format!("{{ \"Error\": \"{}\" }}", message),
            success: false,
            duration_us: 0,
            compute: Compute::empty().bits(),
            container_state: ContainerState::Error.into(),
        }
    }
}

impl RegisterRequest {
    pub fn new(
        name: &str,
        version: &str,
        image: &str,
        cpus: u32,
        memory: MemSizeMb,
        timings: Option<&ResourceTimings>,
        language: LanguageRuntime,
        compute: Compute,
        isolation: Isolation,
        server: ContainerServer,
        parallel_invokes: u32,
        tid: &TransactionId,
        system_function: bool,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            function_name: name.to_owned(),
            function_version: version.to_owned(),
            image_name: image.to_owned(),
            memory,
            cpus,
            parallel_invokes,
            transaction_id: tid.to_owned(),
            language: language.into(),
            compute: compute.bits(),
            isolate: isolation.bits(),
            container_server: server as u32,
            resource_timings_json: match timings {
                Some(r) => serde_json::to_string(r)?,
                None => "{}".to_string(),
            },
            system_function,
        })
    }
}
