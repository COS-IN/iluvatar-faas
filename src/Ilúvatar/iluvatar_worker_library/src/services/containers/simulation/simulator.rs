use self::simstructs::SimulatorContainer;
use super::{
    structs::{Container, ContainerState},
    ContainerIsolationService,
};
use crate::services::registration::RegisteredFunction;
use crate::services::resources::gpu::GPU;
use anyhow::Result;
use guid_create::GUID;
use iluvatar_library::char_map::{Chars, WorkerCharMap};
use iluvatar_library::transaction::gen_tid;
use iluvatar_library::types::ResultErrorVal;
use iluvatar_library::{
    error_value,
    transaction::TransactionId,
    types::{Compute, Isolation, MemSizeMb},
};
use std::sync::Arc;

pub mod simstructs;

#[derive(iluvatar_library::ToAny)]
pub struct SimulatorIsolation {
    cmap: WorkerCharMap,
}

impl SimulatorIsolation {
    pub fn new(cmap: WorkerCharMap) -> Self {
        SimulatorIsolation { cmap }
    }
}

#[tonic::async_trait]
#[allow(unused)]
impl ContainerIsolationService for SimulatorIsolation {
    fn backend(&self) -> Vec<Isolation> {
        vec![Isolation::CONTAINERD, Isolation::DOCKER]
    }

    async fn run_container(
        &self,
        fqdn: &str,
        image_name: &str,
        _parallel_invokes: u32,
        namespace: &str,
        mem_limit_mb: MemSizeMb,
        cpus: u32,
        reg: &Arc<RegisteredFunction>,
        iso: Isolation,
        compute: Compute,
        device_resource: Option<GPU>,
        tid: &TransactionId,
    ) -> ResultErrorVal<Container, Option<GPU>> {
        let cid = format!("{}-{}", fqdn, GUID::rand());
        // 'cold' container start delay
        // TODO: fixing this is in TODO.md
        tokio::time::sleep(tokio::time::Duration::from_secs(
            (self.cmap.get_avg(fqdn, Chars::CpuColdTime) * 0.75).round() as u64,
        ))
        .await;
        match SimulatorContainer::new(
            &gen_tid(),
            cid,
            fqdn,
            reg,
            ContainerState::Cold,
            iso,
            compute,
            device_resource,
        ) {
            Ok(c) => Ok(Arc::new(c)),
            Err((e, d)) => error_value!("{:?}", e, d),
        }
    }

    async fn remove_container(&self, container: Container, ctd_namespace: &str, tid: &TransactionId) -> Result<()> {
        Ok(())
    }

    async fn prepare_function_registration(
        &self,
        rf: &mut RegisteredFunction,
        namespace: &str,
        _tid: &TransactionId,
    ) -> Result<()> {
        Ok(())
    }

    async fn clean_containers(
        &self,
        ctd_namespace: &str,
        self_src: Arc<dyn ContainerIsolationService>,
        tid: &TransactionId,
    ) -> Result<()> {
        Ok(())
    }

    async fn wait_startup(&self, container: &Container, timout_ms: u64, tid: &TransactionId) -> Result<()> {
        Ok(())
    }

    async fn update_memory_usage_mb(&self, container: &Container, tid: &TransactionId) -> MemSizeMb {
        container.get_curr_mem_usage()
    }

    async fn read_stdout(&self, container: &Container, tid: &TransactionId) -> String {
        "".to_string()
    }
    async fn read_stderr(&self, container: &Container, tid: &TransactionId) -> String {
        "".to_string()
    }
}
