#![allow(clippy::derive_partial_eq_without_eq)]
tonic::include_proto!("iluvatar_rpc");

use anyhow::Error;
use iluvatar_library::types::ContainerServer;
use iluvatar_library::{
    transaction::TransactionId,
    types::{Compute, Isolation, MemSizeMb, ResourceTimings},
};
use std::fmt::{Display, Formatter};

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
        runtime: Runtime,
        compute: Compute,
        isolation: Isolation,
        server: ContainerServer,
        parallel_invokes: u32,
        tid: &TransactionId,
        system_function: bool,
        code_zip: Vec<u8>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            function_name: name.to_owned(),
            function_version: version.to_owned(),
            image_name: image.to_owned(),
            memory,
            cpus,
            parallel_invokes,
            transaction_id: tid.to_owned(),
            runtime: runtime.into(),
            compute: compute.bits(),
            isolate: isolation.bits(),
            container_server: server as u32,
            resource_timings_json: match timings {
                Some(r) => serde_json::to_string(r)?,
                None => "".to_string(),
            },
            system_function,
            code_zip,
        })
    }
}

impl TryFrom<u32> for Runtime {
    type Error = Error;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Runtime::Nolang),
            1 => Ok(Runtime::Python3),
            2 => Ok(Runtime::Python3gpu),
            _ => anyhow::bail!("Cannot parse {} for Runtime", value),
        }
    }
}
impl Display for Runtime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self.as_str_name()))?;
        // match self {
        //     Runtime::Nolang => f.write_fmt(format_args!("Nolang"))?,
        //     Runtime::Python3 => f.write_fmt(format_args!("Python3"))?,
        //     Runtime::Python3gpu => f.write_fmt(format_args!("Python3gpu"))?,
        // };
        Ok(())
    }
}
