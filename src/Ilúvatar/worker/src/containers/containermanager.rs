use crate::containers::containerlife::ContainerLifecycle;
use crate::network::namespace_manager::NamespaceManager;

use iluvatar_lib::rpc::{RegisterRequest, PrewarmRequest};
use iluvatar_lib::utils::calculate_fqdn;
use anyhow::Result;
use log::*;
use std::collections::HashMap; 
use std::sync::Arc;
use parking_lot::{RwLock, Mutex};
use crate::config::WorkerConfig;
use super::structs::{Container, RegisteredFunction, ContainerLock};

type ContainerPool = HashMap<String, Arc<RwLock<Vec<Arc<Container>>>>>;

#[derive(Debug)]
pub struct ContainerManager {
  registered_functions: Arc<RwLock<HashMap<String, Arc<RegisteredFunction>>>>,
  active_containers: Arc<RwLock<ContainerPool>>,
  config: WorkerConfig,
  namespace_man: Arc<NamespaceManager>,
  used_mem_mb: Mutex<u32>,
}

impl ContainerManager {
  // TODO: implement removing container

  pub fn new(config: WorkerConfig, ns_man: Arc<NamespaceManager>) -> ContainerManager {
    ContainerManager {
      registered_functions: Arc::new(RwLock::new(HashMap::new())),
      active_containers: Arc::new(RwLock::new(HashMap::new())),
      config,
      namespace_man: ns_man,
      used_mem_mb: Mutex::new(0),
    }
  }

  pub async fn acquire_container<'a>(&'a self, fqdn: &String) -> Result<Option<ContainerLock<'a>>> {
    match self.try_acquire_container(fqdn) {
      Some(l) => Ok(Some(l)),
      None => {
        // not available container, cold start
        // TODO: cold start container needs time to start web server
        //    poll? wait? what to do...
        Ok(self.cold_start(fqdn).await?)
      },
    } 
  }

  fn try_acquire_container<'a>(&'a self, fqdn: &String) -> Option<ContainerLock<'a>> {
    let conts = self.active_containers.read();
    let opt = conts.get(fqdn);
    match opt {
      Some(pool) => {
        let pool = pool.read();
        if pool.len() > 0 {
          for container in pool.iter() {
            match self.try_lock_container(container) {
              Some(c) => return Some(c),
              None => continue,
            }
          }
        }
        None
      },
      None => {
        // 'should' not get here
        error!("active_containers had key for fqdn '{}' but not a real value", fqdn);
        None
      },
    }
  }

  async fn cold_start<'a>(&'a self, fqdn: &String) -> Result<Option<ContainerLock<'a>>> {
    let container = self.launch_container(fqdn).await?;
    {
      // claim this for ourselves before it touches the pool
      let mut m = container.mutex.lock();
      *m -= 1;
    }
    let container = self.add_container_to_pool(fqdn, container)?;
    Ok(Some(ContainerLock {
      container: container.clone(),
      container_mrg: self,
    }))
  }

  fn try_lock_container(&self, container: &Arc<Container>) -> Option<ContainerLock> {
    unsafe {
      if *container.mutex.data_ptr() > 0 {
        let mut m = container.mutex.lock();
        if *m > 0 {
          *m -= 1;
          return Some(ContainerLock {
            container: container.clone(),
            container_mrg: self,
          });
        }
      }
    }
    None
  }

  pub fn return_container(&self, container: &Arc<Container>) {
    let mut m = container.mutex.lock();
    *m += 1;
  }

  pub async fn launch_container(&self, fqdn: &String) -> Result<Container> {
    let reg = match self.get_registration(&fqdn) {
      Ok(r) => r,
      Err(_) => {
        warn!("function {} was attempted to be prewarmed before registering. Attempting register...", fqdn);
        anyhow::bail!("Function {} was not registered! Launching new container for it failed", fqdn);
      },
    };
    let container = self.launch_container_internal(&reg).await?;
    Ok(container)
  }

  fn add_container_to_pool(&self, fqdn: &String, container: Container) -> Result<Arc<Container>> {
    // acquire read lock to see if that function already has a pool entry
    let conts = self.active_containers.read();
    if conts.contains_key(fqdn) {
      let pool = conts.get(fqdn);
      match pool {
          Some(pool) => {
            let mut locked_pool = pool.write();
            let ret = Arc::new(container);
            locked_pool.push(ret.clone());
            return Ok(ret);
          },
          None => anyhow::bail!("Function '{}' was supposed to be readable in pool but could not be found", fqdn),
      }
    } 
    else {
      // acquire global write lock on containers
      drop(conts);
      let mut conts = self.active_containers.write();
      conts.insert(fqdn.clone(), Arc::new(RwLock::new(Vec::new())));
      let pool = conts.get(fqdn);
      match pool {
          Some(pool) => {
            let mut locked_pool = pool.write();
            let ret = Arc::new(container);
            locked_pool.push(ret.clone());
            return Ok(ret);
          },
          None => anyhow::bail!("Function '{}' was supposed to be just added in pool but could not be found", fqdn),
      }
    }
  }

  async fn launch_container_internal(&self, reg: &Arc<RegisteredFunction>) -> Result<Container> {
    let mut lifecycle = ContainerLifecycle::new(self.namespace_man.clone());
    // TODO: cpu and mem prewarm request overrides registration?
    unsafe {
      let curr_mem = *self.used_mem_mb.data_ptr();
      if curr_mem >= self.config.memory_mb {
        anyhow::bail!("Insufficient memory, already allocated {} of {}", curr_mem, self.config.memory_mb);
      }  
    }
    let cont = lifecycle.run_container(&reg.image_name, reg.parallel_invokes, "default", reg.memory, reg.cpus).await;
    let mut cont = match cont {
        Ok(cont) => {
          let mut locked = self.used_mem_mb.lock();
          *locked += reg.memory;
      
          cont
        },
        Err(e) => return Err(e),
    };

    cont.function = Some(reg.clone());
    cont.wait_startup()?;
    info!("container with image '{}' was launched", reg.image_name);
    Ok(cont)
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
          warn!("function {} was attempted to be prewarmed before registering. Attempting register...", fqdn);
          match self.register_internal(&request.function_name, &request.function_version, &request.image_name, request.memory, request.cpu, 1, &fqdn).await {
            Ok(_) => self.get_registration(&fqdn)?,
            Err(sub_e) => {
              error!("Prewarm of function {} was not registered because it was not registered! Attempted registration failed because '{}'", fqdn, sub_e);
              anyhow::bail!("Function {} was not registered! Attempted registration failed because '{}'", fqdn, sub_e)
            }
          }
        },
    };

    let container = self.launch_container_internal(&reg).await?;
    self.add_container_to_pool(&fqdn, container)?;
    info!("function '{}' was successfully prewarmed", fqdn);
    Ok(())
  }

  /// Registerrs a function using the given request
  /// 
  /// # Errors
  /// Can error if the function is already registers, the image is invalid, or many other reasons
  pub async fn register(&self, request: &RegisterRequest) -> Result<()> {
    let fqdn = calculate_fqdn(&request.function_name, &request.function_version);

    self.check_registration(&fqdn)?;
    return self.register_internal(&request.function_name, &request.function_version, &request.image_name, request.memory, request.cpus, request.parallel_invokes, &fqdn).await;
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

  async fn register_internal(&self, function_name: &String, function_version: &String, image_name: &String, memory: u32, cpus: u32, parallel_invokes: u32, fqdn: &String) -> Result<()> {
    let mut lifecycle = ContainerLifecycle::new(self.namespace_man.clone());

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
    if parallel_invokes != 1 {
      anyhow::bail!("Illegal parallel invokes set, must be 1");
    }

    lifecycle.ensure_image(&image_name).await?;
    let snapshot_base = lifecycle.search_image_digest(&image_name, "default").await?;
    let registration = RegisteredFunction {
      function_name: function_name.clone(),
      function_version: function_version.clone(),
      image_name: image_name.clone(),
      memory,
      cpus,
      snapshot_base,
      parallel_invokes,
    };

    { // write lock
      let mut acquired_reg = self.registered_functions.write();
      acquired_reg.insert(fqdn.clone(), Arc::new(registration));
    }
    info!("function '{}'; version '{}' was successfully registered", function_name, function_version);
    Ok(())
  }
}