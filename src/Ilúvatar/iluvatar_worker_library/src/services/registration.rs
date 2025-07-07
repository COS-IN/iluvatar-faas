use super::containers::{containermanager::ContainerManager, ContainerIsolationCollection};
use crate::worker_api::config::BaseImages;
use crate::worker_api::worker_config::{ContainerResourceConfig, FunctionLimits};
use anyhow::Result;
use iluvatar_library::char_map::{add_registration_timings, WorkerCharMap};
use iluvatar_library::types::ContainerServer;
use iluvatar_library::utils::file::temp_pth;
use iluvatar_library::{
    bail_error,
    transaction::TransactionId,
    types::{Compute, Isolation, MemSizeMb, ResourceTimings},
    utils::calculate_fqdn,
};
use iluvatar_rpc::rpc::{RegisterRequest, Runtime};
use parking_lot::RwLock;
use std::{collections::HashMap, sync::Arc};
use tracing::{debug, info};

#[derive(Debug, Clone)]
pub enum RunFunction {
    Image {},
    Runtime { packages_dir: String, main_dir: String },
}
impl Default for RunFunction {
    fn default() -> Self {
        Self::Image {}
    }
}

/// A registered function is ready to be run if invoked later. Resource configuration is set here (CPU, mem, isolation, compute-device.
#[derive(Debug, Default, Clone)]
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
    pub historical_runtime_data_sec: HashMap<Compute, Vec<f64>>,
    pub system_function: bool,
    pub runtime: Runtime,
    pub run_info: RunFunction,
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
    cmap: WorkerCharMap,
    resources: Arc<ContainerResourceConfig>,
    base_images: Arc<BaseImages>,
}

impl RegistrationService {
    pub fn new(
        cm: Arc<ContainerManager>,
        lifecycles: ContainerIsolationCollection,
        limits_config: Arc<FunctionLimits>,
        characteristics_map: WorkerCharMap,
        resources: Arc<ContainerResourceConfig>,
        base_images: Arc<BaseImages>,
    ) -> Arc<Self> {
        Arc::new(RegistrationService {
            reg_map: RwLock::new(HashMap::new()),
            cm,
            lifecycles,
            limits_config,
            cmap: characteristics_map,
            resources,
            base_images,
        })
    }

    pub async fn register(&self, request: RegisterRequest, tid: &TransactionId) -> Result<Arc<RegisteredFunction>> {
        let (mut reg, all_resource_timings) = self.convert_registration(request).await?;

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
        add_registration_timings(&self.cmap, reg.supported_compute, &all_resource_timings, &reg.fqdn, tid)?;

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

    async fn convert_registration(
        &self,
        req: RegisterRequest,
    ) -> Result<(RegisteredFunction, Option<ResourceTimings>)> {
        let all_resource_timings = match req.resource_timings_json.is_empty() {
            true => None,
            _ => match serde_json::from_str::<ResourceTimings>(&req.resource_timings_json) {
                Ok(t) => Some(t),
                Err(e) => anyhow::bail!("failed to parse resource_timings_json: '{}'", e),
            },
        };
        let mut runtime_data = HashMap::new();
        if let Some(timings) = &all_resource_timings {
            for (compute, c_times) in timings.iter() {
                runtime_data.insert(*compute, c_times.warm_results_sec.clone());
            }
        }
        let runtime = req.runtime.try_into()?;
        let fqdn = calculate_fqdn(&req.function_name, &req.function_version);
        let (run_info, image_name, container_server) = match runtime {
            Runtime::Python3gpu | Runtime::Python3 => match self.prepare_on_disk(&fqdn, runtime, &req).await {
                Ok((packages, main)) => (
                    RunFunction::Runtime {
                        packages_dir: packages,
                        main_dir: main,
                    },
                    match runtime {
                        Runtime::Python3 => self.base_images.python_cpu.clone(),
                        Runtime::Python3gpu => self.base_images.python_gpu.clone(),
                        _ => unreachable!(),
                    },
                    ContainerServer::UnixSocket,
                ),
                Err(e) => bail_error!(error=%e, tid=req.transaction_id, "prepare_on_disk failed"),
            },
            Runtime::Nolang => (
                RunFunction::Image {},
                req.image_name,
                ContainerServer::try_from(req.container_server).unwrap_or(ContainerServer::default()),
            ),
        };
        let r = RegisteredFunction {
            fqdn,
            function_name: req.function_name,
            function_version: req.function_version,
            image_name,
            memory: req.memory,
            cpus: req.cpus,
            snapshot_base: "".to_string(),
            parallel_invokes: req.parallel_invokes,
            isolation_type: Isolation::from(req.isolate),
            supported_compute: Compute::from(req.compute),
            container_server,
            historical_runtime_data_sec: runtime_data,
            system_function: req.system_function,
            runtime,
            run_info,
        };

        Ok((r, all_resource_timings))
    }

    async fn prepare_on_disk(&self, fqdn: &str, _runtime: Runtime, req: &RegisterRequest) -> Result<(String, String)> {
        let storage = temp_pth(fqdn);
        match std::fs::exists(&storage) {
            Ok(true) => {
                // old instance of matching fqdn
                // we check for dupe function registration before this, so this is safe
                if let Err(e) = tokio::fs::remove_dir_all(&storage).await {
                    bail_error!(error=%e, d=storage, "rmdir failed")
                };
            },
            Ok(false) => (),
            Err(e) => bail_error!(error=%e, d=storage, "exists failed"),
        }
        let code = format!("{storage}/code");
        let packages = format!("{storage}/packages");
        if let Err(e) = std::fs::create_dir_all(&code) {
            bail_error!(error=%e, d=code, "mkdirs code failed");
        };
        if let Err(e) = std::fs::create_dir_all(&packages) {
            bail_error!(error=%e, c=packages, "mkdirs packages failed");
        };

        crate::unpack_tar(&code, req.code_zip.as_slice(), &req.transaction_id)?;
        Ok((packages, code))
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
