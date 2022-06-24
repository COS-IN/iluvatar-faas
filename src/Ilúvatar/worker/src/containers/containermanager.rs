use crate::containers::containerlife::ContainerLifecycle;
use crate::containers::structs::InsufficientMemoryError;
use crate::network::namespace_manager::NamespaceManager;

use iluvatar_lib::bail_error;
use iluvatar_lib::rpc::{RegisterRequest, PrewarmRequest};
use iluvatar_lib::utils::calculate_fqdn;
use anyhow::{Result, bail};
use log::*;
use std::cmp::Ordering;
use std::collections::HashMap; 
use std::sync::Arc;
use parking_lot::{RwLock, Mutex};
use crate::config::WorkerConfig;
use super::structs::{Container, RegisteredFunction, ContainerLock};

type ContainerList = Arc<RwLock<Vec<Arc<Container>>>>;
type ContainerPool = HashMap<String, ContainerList>;

#[derive(Debug)]
pub struct ContainerManager {
  registered_functions: Arc<RwLock<HashMap<String, Arc<RegisteredFunction>>>>,
  active_containers: Arc<RwLock<ContainerPool>>,
  config: WorkerConfig,
  used_mem_mb: Arc<Mutex<u32>>,
  cont_lifecycle: Arc<ContainerLifecycle>,
  prioritized_list: ContainerList,
}

impl ContainerManager {
  // TODO: implement CPU restrictions

  pub async fn new(config: WorkerConfig, ns_man: Arc<NamespaceManager>) -> Result<ContainerManager> {
    let mut lifecycle = ContainerLifecycle::new(ns_man.clone());
    lifecycle.connect().await?;
    Ok(ContainerManager {
      registered_functions: Arc::new(RwLock::new(HashMap::new())),
      active_containers: Arc::new(RwLock::new(HashMap::new())),
      config,
      used_mem_mb: Arc::new(Mutex::new(0)),
      cont_lifecycle: Arc::new(lifecycle),
      prioritized_list: Arc::new(RwLock::new(Vec::new())),
    })
  }

  pub async fn boxed(config: WorkerConfig, ns_man: Arc<NamespaceManager>) -> Result<Arc<ContainerManager>> {
    let cm = Arc::new(ContainerManager::new(config.clone(), ns_man).await?);
    let cm_clone = cm.clone();
    let _handle = tokio::spawn(async move {
      ContainerManager::monitor_pool(config, cm_clone).await;
    });
    Ok(cm)
  }

  async fn monitor_pool(config: WorkerConfig, cm: Arc<ContainerManager>) {
    loop {
      cm.compute_eviction_priorities();
      if config.container_resources.memory_buffer_mb > 0 {
        let reclaim = config.container_resources.memory_buffer_mb - cm.free_memory();
        if reclaim > 0 {
          match cm.reclaim_memory(reclaim).await {
            Ok(_) => {},
            Err(e) => error!("Error while trying to remove containers '{}'", e),
          };
        }
      }
      tokio::time::sleep(std::time::Duration::from_secs(config.container_resources.pool_freq_sec)).await;
    }
  }

  pub fn free_memory(&self) -> u32 {
    self.config.container_resources.memory_mb - *self.used_mem_mb.lock()
  }

  pub async fn acquire_container<'a>(&'a self, fqdn: &String) -> Result<Option<ContainerLock<'a>>> {
    let cont = self.try_acquire_container(fqdn);
    match cont {
      Some(l) => Ok(Some(l)),
      None => {
        // not available container, cold start
        self.cold_start(fqdn).await
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
              Some(c) => {
                c.container.touch();
                return Some(c)
              },
              None => continue,
            }
          }
        }
        None
      },
      None => {
        // 'should' not get here
        error!("fqdn '{}' has not been registered", fqdn);
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

  async fn launch_container(&self, fqdn: &String) -> Result<Container> {
    let reg = match self.get_registration(&fqdn) {
      Ok(r) => r,
      Err(_) => {
        anyhow::bail!("Function {} was not registered! Cannot launch a container for it", fqdn);
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
      anyhow::bail!("Function '{}' was not registered yet", fqdn);
    }
  }

  async fn try_launch_container(&self, reg: &Arc<RegisteredFunction>) -> Result<Container> {
    // TODO: cpu and mem prewarm request overrides registration?
    {
      let mut curr_mem = self.used_mem_mb.lock();
      if *curr_mem + reg.memory > self.config.container_resources.memory_mb {
        let avail = self.config.container_resources.memory_mb-*curr_mem;
        anyhow::bail!(InsufficientMemoryError{ needed: reg.memory-avail, used: *curr_mem, available: avail});
      } else {
        *curr_mem += reg.memory;
      }
    }
    let fqdn = calculate_fqdn(&reg.function_name, &reg.function_version);
    let cont = self.cont_lifecycle.run_container(&fqdn, &reg.image_name, reg.parallel_invokes, "default", reg.memory, reg.cpus, reg).await;
    let cont = match cont {
        Ok(cont) => {
          cont
        },
        Err(e) => {
          let mut locked = self.used_mem_mb.lock();
          *locked -= reg.memory;
          return Err(e);
        },
    };

    match cont.wait_startup(self.config.container_resources.startup_timeout_ms) {
        Ok(_) => (),
        Err(e) => {
          {
            let mut locked = self.used_mem_mb.lock();
            *locked -= reg.memory;
          }
          match self.cont_lifecycle.remove_container(&cont.container_id, &cont.namespace, "default").await {
            Ok(_) => { return Err(e); },
            Err(inner_e) => anyhow::bail!("Encountered a second error after startup failed. Primary error: '{}'; inner error: '{}'", e, inner_e),
          };
        },
    };
    info!("container '{}' with image '{}' was launched", cont.container_id, reg.image_name);
    Ok(cont)
  }

  /// launch_container_internal
  /// 
  /// Does a best effort to ensure a container is launched
  /// If various known errors happen, it will re-try to start it
  async fn launch_container_internal(&self, reg: &Arc<RegisteredFunction>) -> Result<Container> {
    match self.try_launch_container(&reg).await {
            Ok(c) => Ok(c),
            Err(cause) => 
              match cause.downcast_ref::<InsufficientMemoryError>() {
                Some(mem) => {
                  self.reclaim_memory(mem.needed).await?;
                  self.try_launch_container(&reg).await
                },
                None => Err(cause),
              },
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
          warn!("function {} was attempted to be prewarmed before registering. Attempting register...", fqdn);
          match self.register_internal(&request.function_name, &request.function_version, &request.image_name, request.memory, request.cpu, 1, &fqdn).await {
            Ok(_) => self.get_registration(&fqdn)?,
            Err(sub_e) => {
              bail_error!("Prewarm of function {} was not registered because it was not registered! Attempted registration failed because '{}'", fqdn, sub_e);
            }
          }
        },
    };

    let container = match self.launch_container_internal(&reg).await {
        Ok(c) => Ok(c),
        Err(cause) => 
          match cause.downcast_ref::<InsufficientMemoryError>() {
            Some(mem) => {
              self.reclaim_memory(mem.needed).await?;
              self.launch_container_internal(&reg).await
            },
            None => Err(cause),
          },
    }?;
    self.add_container_to_pool(&fqdn, container)?;
    info!("function '{}' was successfully prewarmed", fqdn);
    Ok(())
  }

  /// Registers a function using the given request
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
    if function_name.contains("/") || function_name.contains("\\") {
      anyhow::bail!("Illegal characters in function name: cannot container any \\,/");
    }

    self.cont_lifecycle.ensure_image(&image_name).await?;
    let snapshot_base = self.cont_lifecycle.search_image_digest(&image_name, "default").await?;
    let registration = RegisteredFunction {
      function_name: function_name.clone(),
      function_version: function_version.clone(),
      image_name: image_name.clone(),
      memory,
      cpus,
      snapshot_base,
      parallel_invokes,
    };

    { // write lock on registered_functions
      let mut acquired_reg = self.registered_functions.write();
      acquired_reg.insert(fqdn.clone(), Arc::new(registration));
    }
    { // write lock on active_containers
      let mut conts = self.active_containers.write();
      conts.insert(fqdn.clone(), Arc::new(RwLock::new(Vec::new())));
    }
    info!("function '{}'; version '{}' was successfully registered", function_name, function_version);
    Ok(())
  }

  pub async fn remove_container(&self, container: Arc<Container>, lock_check: bool) -> Result<()> {
    if lock_check {
      let mut cont_lock = container.mutex.lock();
      if *cont_lock != container.function.parallel_invokes {
        bail!("Someone is still holding a lock on container '{}'; cannot remove", container.container_id)
      }
      *cont_lock = 0;
    }

    match self.get_container_vec(&container.fqdn) {
      Some(pool) => {
        let (pos, pool_len) = self.find_container_pos(&container, pool.clone());
        if pos < pool_len {
          {
            let mut wlocked_pool = pool.write();
            let dropped_cont = wlocked_pool.remove(pos);
            let mut locked = self.used_mem_mb.lock();
            *locked -= dropped_cont.function.memory;
          }
          self.cont_lifecycle.remove_container(&container.container_id, &container.namespace, "default").await?;
          return Ok(());
        } else {
          anyhow::bail!("Was unable to find container {} to remove it", container.container_id);
        }
      },
      None => anyhow::bail!("Function '{}' was supposed to be readable in pool but could not be found", container.fqdn),
    }
  }

  async fn reclaim_memory(&self, amount_mb: u32) -> Result<()> {
    if amount_mb <= 0 {
      bail!("Cannot reclaim '{}' amount of memory", amount_mb);
    }
    let mut reclaimed = 0;
    let mut to_remove = Vec::new();
    for container in self.prioritized_list.read().iter() {
      if self.try_seize_container(container) {
        to_remove.push(container.clone());
        reclaimed += container.function.memory;
        if reclaimed >= amount_mb {
          break;
        }
      }
    }
    for container in to_remove {
      self.remove_container(container, false).await?;
    }
    Ok(())
  }

  fn compute_eviction_priorities(&self) {
    let mut ordered = Vec::new();
    for (_fqdn, cont_list) in self.active_containers.read().iter() {
      for container in cont_list.read().iter() {
        ordered.push(container.clone());
      }
    }
    let comparator = match self.config.container_resources.eviction.as_str() {
      "LRU" => ContainerManager::lru_eviction,
      _ => { 
          error!("Unkonwn eviction algorithm '{}'", self.config.container_resources.eviction);
          return;
      }
    };
    ordered.sort_by(|c1, c2| comparator(c1,c2));
    let mut lock = self.prioritized_list.write();
    *lock = ordered;
  }

  fn lru_eviction(c1: &Arc<Container>, c2: &Arc<Container>) -> Ordering {
    c1.last_used().cmp(&c2.last_used())
  }

  fn try_seize_container(&self, container: &Arc<Container>) -> bool {
    let mut cont_lock = container.mutex.lock();
    if *cont_lock != container.function.parallel_invokes {
      return false;
    }
    *cont_lock = 0;
    true
  }

  fn find_container_pos(&self, container: &Arc<Container>, pool: ContainerList) -> (usize,usize) {
    let rlocked_pool = pool.read();
    let pool_len = rlocked_pool.len();
    let mut pos = pool_len + 100;
    for (i, iter_cont) in rlocked_pool.iter().enumerate() {
      if container.container_id == iter_cont.container_id {
        pos = i;
        break;
      }
    }
    return (pos, pool_len);
  }

  fn get_container_vec(&self, fqdn: &String) -> Option<ContainerList> {
    let lock = self.active_containers.read();
    match lock.get(fqdn) {
        Some(v) => Some(v.clone()),
        None => None,
    }
  }
}