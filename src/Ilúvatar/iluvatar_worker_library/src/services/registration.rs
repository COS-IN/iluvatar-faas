use super::containers::{containermanager::ContainerManager, ContainerIsolationCollection};
use crate::worker_api::worker_config::{ContainerResourceConfig, FunctionLimits};
use anyhow::Result;
use iluvatar_library::char_map::{add_registration_timings, WorkerCharMap};
use iluvatar_library::types::ContainerServer;
use iluvatar_library::{
    transaction::TransactionId,
    types::{Compute, Isolation, MemSizeMb, ResourceTimings},
    utils::calculate_fqdn,
};
use iluvatar_rpc::rpc::RegisterRequest;
use parking_lot::RwLock;
use std::{collections::HashMap, sync::Arc};
use tracing::{debug, info};

/// A registered function is ready to be run if invoked later. Resource configuration is set here (CPU, mem, isolation, compute-device.
#[derive(Debug, Default)]
pub struct RegisteredFunction {
    pub function_name: String,
    pub function_version: String,
    pub fqdn: String,
    pub image_name: String,
    pub memory: MemSizeMb,
    pub cpus: u32,
    pub snapshot_base: String,
    pub parallel_invokes: u32,
    pub isolation_type: Isolation,
    pub supported_compute: Compute,
    pub container_server: ContainerServer,
    pub all_resource_timings: Option<ResourceTimings>,
    pub historical_runtime_data_sec: HashMap<Compute, Vec<f64>>,
    pub system_function: bool,
}

impl RegisteredFunction {
    #[inline(always)]
    pub fn cpu_only(&self) -> bool {
        self.supported_compute == Compute::CPU
    }
    #[inline(always)]
    pub fn gpu_only(&self) -> bool {
        self.supported_compute == Compute::GPU
    }
    #[inline(always)]
    pub fn polymorphic(&self) -> bool {
        self.supported_compute.contains(Compute::GPU) && self.supported_compute.contains(Compute::CPU)
    }
}
impl TryFrom<RegisterRequest> for RegisteredFunction {
    type Error = anyhow::Error;

    fn try_from(req: RegisterRequest) -> Result<Self> {
        Ok(RegisteredFunction {
            fqdn: calculate_fqdn(&req.function_name, &req.function_version),
            function_name: req.function_name,
            function_version: req.function_version,
            image_name: req.image_name,
            memory: req.memory,
            cpus: req.cpus,
            snapshot_base: "".to_string(),
            parallel_invokes: req.parallel_invokes,
            isolation_type: Isolation::from(req.isolate),
            supported_compute: Compute::from(req.compute),
            container_server: ContainerServer::try_from(req.container_server).unwrap_or(ContainerServer::default()),
            all_resource_timings: match req.resource_timings_json.is_empty() {
                true => None,
                _ => match serde_json::from_str::<ResourceTimings>(&req.resource_timings_json) {
                    Ok(t) => Some(t),
                    Err(e) => anyhow::bail!("failed to parse resource_timings_json: '{}'", e),
                },
            },
            historical_runtime_data_sec: Default::default(),
            system_function: req.system_function,
        })
    }
}

pub struct RegistrationService {
    reg_map: RwLock<HashMap<String, Arc<RegisteredFunction>>>,
    cm: Arc<ContainerManager>,
    lifecycles: ContainerIsolationCollection,
    limits_config: Arc<FunctionLimits>,
    cmap: WorkerCharMap,
    resources: Arc<ContainerResourceConfig>,
}

impl RegistrationService {
    pub fn new(
        cm: Arc<ContainerManager>,
        lifecycles: ContainerIsolationCollection,
        limits_config: Arc<FunctionLimits>,
        characteristics_map: WorkerCharMap,
        resources: Arc<ContainerResourceConfig>,
    ) -> Arc<Self> {
        Arc::new(RegistrationService {
            reg_map: RwLock::new(HashMap::new()),
            cm,
            lifecycles,
            limits_config,
            cmap: characteristics_map,
            resources,
        })
    }

    pub async fn register(&self, request: RegisterRequest, tid: &TransactionId) -> Result<Arc<RegisteredFunction>> {
        let mut reg: RegisteredFunction = request.try_into()?;

        if reg.function_name.is_empty() {
            anyhow::bail!("Invalid function name");
        }
        if reg.function_version.is_empty() {
            anyhow::bail!("Invalid function version");
        }
        if reg.memory < self.limits_config.mem_min_mb || reg.memory > self.limits_config.mem_max_mb {
            anyhow::bail!("Illegal memory allocation request '{}'", reg.memory);
        }
        if reg.cpus < 1 || reg.cpus > self.limits_config.cpu_max {
            anyhow::bail!("Illegal cpu allocation request '{}'", reg.cpus);
        }
        if reg.parallel_invokes != 1 {
            anyhow::bail!("Illegal parallel invokes set, must be 1");
        }
        if reg.function_name.contains('/') || reg.function_name.contains('\\') {
            anyhow::bail!("Illegal characters in function name: cannot container any \\,/");
        }
        if reg.isolation_type.is_empty() {
            anyhow::bail!("Could not register function with no specified isolation!");
        }
        if reg.supported_compute.is_empty() {
            anyhow::bail!("Could not register function with no specified compute!");
        }

        for specific_compute in reg.supported_compute {
            if (specific_compute == Compute::GPU && self.resources.gpu_resource.as_ref().map_or(0, |c| c.count) == 0)
                || (specific_compute != Compute::CPU && specific_compute != Compute::GPU)
            {
                anyhow::bail!(
                    "Could not register function for compute {} because the worker has no devices of that type!",
                    specific_compute
                );
            }
            if let Some(timings) = &reg.all_resource_timings {
                if let Some(warm) = timings.get(&specific_compute) {
                    reg.historical_runtime_data_sec
                        .insert(specific_compute, warm.warm_results_sec.clone());
                }
            }
        }

        if self.reg_map.read().contains_key(&reg.fqdn) {
            anyhow::bail!("Function {} is already registered!", reg.fqdn);
        }

        let mut isolations = reg.isolation_type;
        for (lifecycle_iso, lifecycle) in self.lifecycles.iter() {
            if !isolations.contains(*lifecycle_iso) {
                continue;
            }
            isolations.remove(*lifecycle_iso);
            lifecycle
                .prepare_function_registration(&mut reg, "default", tid)
                .await?;
        }
        if !isolations.is_empty() {
            anyhow::bail!("Could not register function with isolation(s): {:?}", isolations);
        }
        add_registration_timings(
            &self.cmap,
            reg.supported_compute,
            &reg.all_resource_timings,
            &reg.fqdn,
            tid,
        )?;

        let ret = Arc::new(reg);
        debug!(
            tid = tid,
            function_name = ret.function_name,
            function_version = ret.function_version,
            fqdn = ret.fqdn,
            "Adding new registration to registered_functions map"
        );
        self.reg_map.write().insert(ret.fqdn.clone(), ret.clone());
        self.cm.register(&ret, tid)?;
        info!(
            tid = tid,
            function_name = ret.function_name,
            function_version = ret.function_version,
            fqdn = ret.fqdn,
            "function was successfully registered"
        );
        Ok(ret)
    }

    pub fn registered_fqdns(&self) -> Vec<String> {
        self.reg_map.read().keys().cloned().collect()
    }

    pub fn num_registered(&self) -> usize {
        self.reg_map.read().len()
    }

    pub fn get_registration(&self, fqdn: &str) -> Option<Arc<RegisteredFunction>> {
        self.reg_map.read().get(fqdn).cloned()
    }

    pub fn get_all_registered_functions(&self) -> HashMap<String, Arc<RegisteredFunction>> {
        self.reg_map.read().clone()
    }
}
