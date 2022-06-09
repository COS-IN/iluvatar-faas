use crate::containers::containerlife::ContainerLifecycle;

use iluvatar_lib::rpc::{RegisterRequest, PrewarmRequest};
use iluvatar_lib::utils::calculate_fqdn;
use anyhow::Result;
use std::collections::HashMap; 
use std::sync::Arc;
use parking_lot::RwLock;
use crate::config::WorkerConfig;

#[derive(Debug)]
#[allow(unused)]
struct RegisteredFunction {
  pub function_name: String,
  pub function_version: String,
  pub image_name: String,
  pub memory: u32,
  pub cpus: u32,
  pub snapshot_base: String,
}

#[derive(Debug)]
#[allow(unused)]
struct Container {
  id: String
  // TODO: reference to function somehow
}

#[derive(Debug)]
pub struct ContainerManager {
  registered_functions: Arc<RwLock<HashMap<String, Arc<RegisteredFunction>>>>,
  active_containers: Arc<RwLock<Vec<Arc<Container>>>>,
  config: WorkerConfig
}

impl ContainerManager {
  pub fn new(config: WorkerConfig) -> ContainerManager {
    ContainerManager {
      registered_functions: Arc::new(RwLock::new(HashMap::new())),
      active_containers: Arc::new(RwLock::new(Vec::new())),
      config
    }
  }

  /// Prewarm a container for the requested function
  /// 
  /// # Errors
  /// Can error if not already registered and full info isn't provided
  /// Other errors caused by starting/registered the function apply
  pub async fn prewarm(&self, request: &PrewarmRequest) -> Result<()> {
    let fqdn = calculate_fqdn(&request.function_name, &request.function_version);
    let reg = match self.get_registration(&fqdn) {
        Ok(r) => r,
        Err(_) => {
          match self.register_internal(&request.function_name, &request.function_version, &request.image_name, request.memory, request.cpu, &fqdn).await {
            Ok(_) => self.get_registration(&fqdn)?,
            Err(sub_e) => anyhow::bail!("Function {} was not registered! Attempted registration failed because '{}'", fqdn, sub_e),
          }          
        },
    };

    let mut lifecycle = ContainerLifecycle::new();
    // TODO: memory limits
    // TODO: cpu limits
    // TODO: overrides for cpu and mem request overrides registration
    let cid = lifecycle.run_container(&reg.image_name, "default").await?;
    {
      let mut conts = self.active_containers.write();
      conts.push(Arc::new(Container {
        id: cid
      }));
    }
    Ok(())
  }

  /// Registerrs a function using the given request
  /// 
  /// # Errors
  /// Can error if the function is already registers, the image is invalid, or many other reasons
  pub async fn register(&self, request: &RegisterRequest) -> Result<()> {
    let fqdn = calculate_fqdn(&request.function_name, &request.function_version);

    self.check_registration(&fqdn)?;
    return self.register_internal(&request.function_name, &request.function_version, &request.image_name, request.memory, request.cpus, &fqdn).await;
  }

  /// Returns the function registration identified by `fqdn` if it exists, an error otherwise
  fn get_registration(&self, fqdn: &String) -> Result<Arc<RegisteredFunction>> {
    { // read lock
      let acquired_reg = self.registered_functions.read();
      match acquired_reg.get(fqdn) {
        Some(val) => Ok(val.clone()),
        None => anyhow::bail!("Function {} was not registered!", fqdn),
      }
    }
  }

  /// check_registration
  /// 
  /// Returns an error if the function identified by `fqdn`
  fn check_registration(&self, fqdn: &String) -> Result<()> {
    { // read lock
      let acquired_reg = self.registered_functions.read();
      if acquired_reg.contains_key(fqdn) {
        anyhow::bail!("Function {} is already registered!", fqdn);
      }
    }
    Ok(())
  }

  async fn register_internal(&self, function_name: &String, function_version: &String, image_name: &String, memory: u32, cpus: u32, fqdn: &String) -> Result<()> {
    let mut lifecycle = ContainerLifecycle::new();

    if function_name.len() < 1 {
      anyhow::bail!("Invalid function name");
    }
    if function_version.len() < 1 {
      anyhow::bail!("Invalid function version");
    }
    if memory < self.config.limits.mem_min_mb || memory > self.config.limits.mem_max_mb {
      anyhow::bail!("Illegal memory allocation request");
    }
    if cpus < 1 || cpus > self.config.limits.cpu_max {
      anyhow::bail!("Illegal cpu allocation request");
    }

    lifecycle.ensure_image(&image_name).await?;
    let snapshot_base = lifecycle.search_image_digest(&image_name, "default").await?;
    let registration = RegisteredFunction {
      function_name: function_name.clone(),
      function_version: function_version.clone(),
      image_name: image_name.clone(),
      memory: memory,
      cpus: cpus,
      snapshot_base
    };

    { // write lock
      let mut acquired_reg = self.registered_functions.write();
      acquired_reg.insert(fqdn.clone(), Arc::new(registration));
    }
    println!("function '{}'; version '{}' was successfully registered", function_name, function_version);
    Ok(())
  }
}