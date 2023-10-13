use super::containers::{containermanager::ContainerManager, ContainerIsolationCollection};
use crate::worker_api::worker_config::FunctionLimits;
use crate::{rpc::RegisterRequest, worker_api::worker_config::ContainerResourceConfig};
use anyhow::Result;
use iluvatar_library::{
    characteristics_map::{Characteristics, CharacteristicsMap, Values},
    transaction::TransactionId,
    types::{Compute, Isolation, MemSizeMb, ResourceTimings},
    utils::calculate_fqdn,
};
use parking_lot::RwLock;
use std::{collections::HashMap, sync::Arc};
use tracing::{debug, info};

#[derive(Debug)]
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
        if request.function_name.len() < 1 {
            anyhow::bail!("Invalid function name");
        }
        if request.function_version.len() < 1 {
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
        if request.function_name.contains("/") || request.function_name.contains("\\") {
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
            let compute_config = match self.resources.resource_map.get(&(&specific_compute).try_into()?) {
                Some(c) => Some(c.clone()),
                None => None,
            };
            if let Some(compute_config) = compute_config {
                // TODO: Abstract away compute types to use resource trackers (e.g. CpuResourceTracker, GpuResourceTracker) to do this check
                if compute_config.count == 0 {
                    if specific_compute != Compute::CPU {
                        anyhow::bail!(
                            "Could not register function for compute {:?} because the worker has no devices of that type!",
                            specific_compute
                        );
                    }
                }
            } else {
                anyhow::bail!(
                    "Could not register function for compute {:?} because the worker was not configured for it!",
                    specific_compute
                );
            }
        }

        let fqdn = calculate_fqdn(&request.function_name, &request.function_version);
        if self.reg_map.read().contains_key(&fqdn) {
            anyhow::bail!(format!("Function {} is already registered!", fqdn));
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
        };
        for (lifecycle_iso, lifecycle) in self.lifecycles.iter() {
            if !isolation.contains(*lifecycle_iso) {
                continue;
            }
            isolation.remove(*lifecycle_iso);
            lifecycle.prepare_function_registration(&mut rf, &fqdn, tid).await?;
        }
        if !isolation.is_empty() {
            anyhow::bail!("Could not register function with isolation(s): {:?}", isolation);
        }
        let ret = Arc::new(rf);
        debug!(tid=%tid, function_name=%ret.function_name, function_version=%ret.function_version, fqdn=%ret.fqdn, "Adding new registration to registered_functions map");
        self.reg_map.write().insert(fqdn.clone(), ret.clone());

        if request.resource_timings_json.len() > 0 {
            match serde_json::from_str::<ResourceTimings>(&request.resource_timings_json) {
                Ok(r) => {
                    for dev_compute in compute {
                        if let Some(timings) = r.get(&((&dev_compute).try_into()?)) {
                            let (cold, warm, exec) = Self::get_characteristics(dev_compute)?;
                            for v in timings.cold_results_sec.iter() {
                                self.characteristics_map.add(&fqdn, exec, Values::F64(*v), true);
                            }
                            for v in timings.warm_results_sec.iter() {
                                self.characteristics_map.add(&fqdn, exec, Values::F64(*v), true);
                            }
                            for v in timings.warm_worker_duration_us.iter() {
                                self.characteristics_map.add(&fqdn, warm, Values::F64(*v as f64), true);
                            }
                            for v in timings.cold_worker_duration_us.iter() {
                                self.characteristics_map.add(&fqdn, cold, Values::F64(*v as f64), true);
                            }
                        }
                    }
                }
                Err(e) => anyhow::bail!("Failed to parse resource timings because {:?}", e),
            };
        }

        self.cm.register(&ret, tid)?;

        info!(tid=%tid, function_name=%ret.function_name, function_version=%ret.function_version, fqdn=%ret.fqdn, "function was successfully registered");
        Ok(ret)
    }

    /// Get the Cold, Warm, and Execution time [Characteristics] specific to the given compute
    fn get_characteristics(compute: Compute) -> Result<(Characteristics, Characteristics, Characteristics)> {
        if compute == Compute::CPU {
            return Ok((
                Characteristics::ColdTime,
                Characteristics::WarmTime,
                Characteristics::ExecTime,
            ));
        } else if compute == Compute::GPU {
            return Ok((
                Characteristics::GpuColdTime,
                Characteristics::GpuWarmTime,
                Characteristics::GpuExecTime,
            ));
        } else {
            anyhow::bail!("Unknown compute to get characteristics for registration: {:?}", compute)
        }
    }

    pub fn get_registration(&self, fqdn: &String) -> Option<Arc<RegisteredFunction>> {
        match self.reg_map.read().get(fqdn) {
            Some(val) => Some(val.clone()),
            None => None,
        }
    }
}
