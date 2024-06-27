#![allow(clippy::derive_partial_eq_without_eq)]
tonic::include_proto!("iluvatar_rpc");
use iluvatar_library::types::Compute;


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
