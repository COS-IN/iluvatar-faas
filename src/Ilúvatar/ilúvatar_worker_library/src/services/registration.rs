use std::{collections::HashMap, sync::Arc};
use iluvatar_library::{types::{Isolation, MemSizeMb}, transaction::TransactionId, utils::calculate_fqdn};
use parking_lot::RwLock;
use tracing::{debug, info};
use crate::rpc::RegisterRequest;
use crate::worker_api::worker_config::FunctionLimits;
use super::containers::{containermanager::ContainerManager, LifecycleCollection};
use anyhow::Result;

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
  pub isolation_type: Isolation
}

pub struct RegistrationService {
  reg_map: RwLock<HashMap<String, Arc<RegisteredFunction>>>,
  cm: Arc<ContainerManager>, 
  lifecycles: LifecycleCollection,
  limits_config: Arc<FunctionLimits>,
}

impl RegistrationService {
  pub fn new(cm: Arc<ContainerManager>, lifecycles: LifecycleCollection, limits_config: Arc<FunctionLimits>,) -> Arc<Self> {
    Arc::new(RegistrationService {
      reg_map: RwLock::new(HashMap::new()),
      cm, lifecycles, limits_config
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
      anyhow::bail!("Illegal memory allocation request");
    }
    if request.cpus < 1 || request.cpus > self.limits_config.cpu_max {
      anyhow::bail!("Illegal cpu allocation request");
    }
    if request.parallel_invokes != 1 {
      anyhow::bail!("Illegal parallel invokes set, must be 1");
    }
    if request.function_name.contains("/") || request.function_name.contains("\\") {
      anyhow::bail!("Illegal characters in function name: cannot container any \\,/");
    }
    let mut isolation = request.isolate.into();
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
    };
    for (iso, lifecycle) in self.lifecycles.iter() {
      if isolation.contains(*iso) {
        isolation.remove(isolation);
      }
      lifecycle.prepare_function_registration(&mut rf, &fqdn, tid).await?;
    }
    if !isolation.is_empty() {
      anyhow::bail!("Could not register function with isolation(s): {:?}", isolation);
    }
    let ret = Arc::new(rf);
    debug!(tid=%tid, function_name=%ret.function_name, function_version=%ret.function_version, fqdn=%ret.fqdn, "Adding new registration to registered_functions map");
    self.reg_map.write().insert(fqdn.clone(), ret.clone());

    self.cm.register(&ret, tid)?;
    
    info!(tid=%tid, function_name=%ret.function_name, function_version=%ret.function_version, fqdn=%ret.fqdn, "function was successfully registered");
    Ok(ret)
  }

  pub fn get_registration(&self, fqdn: &String) -> Option<Arc<RegisteredFunction>> {
    match self.reg_map.read().get(fqdn) {
      Some(val) => Some(val.clone()),
      None => None,
    }
  }
}
