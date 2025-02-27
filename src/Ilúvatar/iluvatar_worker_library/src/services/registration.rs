use super::containers::{containermanager::ContainerManager, ContainerIsolationCollection};
use crate::worker_api::worker_config::{ContainerResourceConfig, FunctionLimits};
use anyhow::Result;
use iluvatar_library::types::ContainerServer;
use iluvatar_library::{
    characteristics_map::{CharacteristicsMap, Values},
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
    pub supported_compute: Compute, // TODO: Rename Compute to ComputeDevice
    pub container_server: ContainerServer,
    pub historical_runtime_data_sec: HashMap<Compute, Vec<f64>>,
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

pub struct RegistrationService {
    reg_map: RwLock<HashMap<String, Arc<RegisteredFunction>>>,
    cm: Arc<ContainerManager>,
    lifecycles: ContainerIsolationCollection,
    limits_config: Arc<FunctionLimits>,
    characteristics_map: Arc<CharacteristicsMap>,
    resources: Arc<ContainerResourceConfig>,
}

impl RegistrationService {
    pub fn new(
        cm: Arc<ContainerManager>,
        lifecycles: ContainerIsolationCollection,
        limits_config: Arc<FunctionLimits>,
        characteristics_map: Arc<CharacteristicsMap>,
        resources: Arc<ContainerResourceConfig>,
    ) -> Arc<Self> {
        Arc::new(RegistrationService {
            reg_map: RwLock::new(HashMap::new()),
            cm,
            lifecycles,
            limits_config,
            characteristics_map,
            resources,
        })
    }

    pub async fn register(&self, request: RegisterRequest, tid: &TransactionId) -> Result<Arc<RegisteredFunction>> {
        if request.function_name.is_empty() {
            anyhow::bail!("Invalid function name");
        }
        if request.function_version.is_empty() {
            anyhow::bail!("Invalid function version");
        }
        if request.memory < self.limits_config.mem_min_mb || request.memory > self.limits_config.mem_max_mb {
            anyhow::bail!("Illegal memory allocation request '{}'", request.memory);
        }
        if request.cpus < 1 || request.cpus > self.limits_config.cpu_max {
            anyhow::bail!("Illegal cpu allocation request '{}'", request.cpus);
        }
        if request.parallel_invokes != 1 {
            anyhow::bail!("Illegal parallel invokes set, must be 1");
        }
        if request.function_name.contains('/') || request.function_name.contains('\\') {
            anyhow::bail!("Illegal characters in function name: cannot container any \\,/");
        }
        let mut isolation: Isolation = request.isolate.into();
        if isolation.is_empty() {
            anyhow::bail!("Could not register function with no specified isolation!");
        }

        let compute: Compute = request.compute.into();
        if compute.is_empty() {
            anyhow::bail!("Could not register function with no specified compute!");
        }

        for specific_compute in compute {
            if (specific_compute == Compute::GPU && self.resources.gpu_resource.as_ref().map_or(0, |c| c.count) == 0)
                || (specific_compute != Compute::CPU && specific_compute != Compute::GPU)
            {
                anyhow::bail!(
                    "Could not register function for compute {} because the worker has no devices of that type!",
                    specific_compute
                );
            }
        }

        let fqdn = calculate_fqdn(&request.function_name, &request.function_version);
        if self.reg_map.read().contains_key(&fqdn) {
            anyhow::bail!("Function {} is already registered!", fqdn);
        }

        let mut rf = RegisteredFunction {
            function_name: request.function_name,
            function_version: request.function_version,
            fqdn: fqdn.clone(),
            image_name: request.image_name,
            memory: request.memory,
            cpus: request.cpus,
            snapshot_base: "".to_string(),
            parallel_invokes: request.parallel_invokes,
            isolation_type: isolation,
            supported_compute: compute,
            historical_runtime_data_sec: HashMap::new(),
            container_server: request.container_server.try_into()?,
        };
        for (lifecycle_iso, lifecycle) in self.lifecycles.iter() {
            if !isolation.contains(*lifecycle_iso) {
                continue;
            }
            isolation.remove(*lifecycle_iso);
            lifecycle
                .prepare_function_registration(&mut rf, &fqdn, "default", tid)
                .await?;
        }
        if !isolation.is_empty() {
            anyhow::bail!("Could not register function with isolation(s): {:?}", isolation);
        }

        if !request.resource_timings_json.is_empty() {
            match serde_json::from_str::<ResourceTimings>(&request.resource_timings_json) {
                Ok(r) => {
                    for dev_compute in compute.into_iter() {
                        if let Some(timings) = r.get(&dev_compute) {
                            debug!(tid=%tid, compute=%dev_compute, from_compute=%compute, fqdn=%fqdn, timings=?r, "Registering timings for function");
                            let (cold, warm, prewarm, exec, e2e, _) =
                                self.characteristics_map.get_characteristics(&dev_compute)?;
                            for v in timings.cold_results_sec.iter() {
                                self.characteristics_map.add(&fqdn, exec, Values::F64(*v), true);
                            }
                            for v in timings.warm_results_sec.iter() {
                                self.characteristics_map.add(&fqdn, exec, Values::F64(*v), true);
                            }
                            for v in timings.cold_worker_duration_us.iter() {
                                self.characteristics_map
                                    .add(&fqdn, cold, Values::F64(*v as f64 / 1_000_000.0), true);
                                self.characteristics_map
                                    .add(&fqdn, e2e, Values::F64(*v as f64 / 1_000_000.0), true);
                            }
                            for v in timings.warm_worker_duration_us.iter() {
                                self.characteristics_map
                                    .add(&fqdn, warm, Values::F64(*v as f64 / 1_000_000.0), true);
                                self.characteristics_map.add(
                                    &fqdn,
                                    prewarm,
                                    Values::F64(*v as f64 / 1_000_000.0),
                                    true,
                                );
                                self.characteristics_map
                                    .add(&fqdn, e2e, Values::F64(*v as f64 / 1_000_000.0), true);
                            }
                            rf.historical_runtime_data_sec
                                .insert(dev_compute, timings.warm_results_sec.clone());
                        }
                    }
                },
                Err(e) => anyhow::bail!("Failed to parse resource timings because {:?}", e),
            };
        }
        let ret = Arc::new(rf);
        debug!(tid=%tid, function_name=%ret.function_name, function_version=%ret.function_version, fqdn=%ret.fqdn, "Adding new registration to registered_functions map");
        self.reg_map.write().insert(fqdn.clone(), ret.clone());
        self.cm.register(&ret, tid)?;
        info!(tid=%tid, function_name=%ret.function_name, function_version=%ret.function_version, fqdn=%ret.fqdn, "function was successfully registered");
        Ok(ret)
    }

    pub fn registered_funcs(&self) -> Vec<String> {
        self.reg_map.read().keys().cloned().collect()
    }

    pub fn get_registration(&self, fqdn: &str) -> Option<Arc<RegisteredFunction>> {
        self.reg_map.read().get(fqdn).cloned()
    }

    pub fn get_all_registered_functions(&self) -> Vec<Arc<RegisteredFunction>> {
        self.reg_map.read().values().cloned().collect()
    }
}
