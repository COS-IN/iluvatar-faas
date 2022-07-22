use crate::services::LifecycleService;
use crate::services::containers::structs::{InsufficientMemoryError, InsufficientCoresError, ContainerLockedError};
use crate::bail_error;
use crate::rpc::{RegisterRequest, PrewarmRequest};
use crate::transaction::{TransactionId, CTR_MGR_WORKER_TID};
use crate::types::MemSizeMb;
use crate::utils::calculate_fqdn;
use crate::worker_api::worker_config::{FunctionLimits, ContainerResources};
use anyhow::{Result, bail};
use dashmap::DashMap;
use std::cmp::Ordering;
use std::collections::HashMap; 
use std::sync::{Arc, mpsc::{Receiver, channel}};
use parking_lot::{RwLock, Mutex};
use super::structs::{Container, RegisteredFunction, ContainerLock};
use tracing::{info, warn, debug, error};

type ContainerList = Arc<RwLock<Vec<Arc<Container>>>>;
type ContainerPool = HashMap<String, ContainerList>;

#[derive(Debug)]
pub struct ContainerManager {
  registered_functions: Arc<DashMap<String, Arc<RegisteredFunction>>>,
  active_containers: Arc<RwLock<ContainerPool>>,
  limits_config: Arc<FunctionLimits>, 
  resources: Arc<ContainerResources>,
  used_mem_mb: Arc<Mutex<MemSizeMb>>,
  running_funcs: Arc<Mutex<u32>>,
  cont_lifecycle: Arc<dyn LifecycleService>,
  prioritized_list: ContainerList,
  _worker_thread: std::thread::JoinHandle<()>,
}

impl ContainerManager {
  #[tracing::instrument]  
  async fn new(limits_config: Arc<FunctionLimits>, resources: Arc<ContainerResources>, cont_lifecycle: Arc<dyn LifecycleService>, worker_thread: std::thread::JoinHandle<()>) -> Result<ContainerManager> {

    Ok(ContainerManager {
      registered_functions: Arc::new(DashMap::new()),
      active_containers: Arc::new(RwLock::new(HashMap::new())),
      limits_config,
      resources,
      used_mem_mb: Arc::new(Mutex::new(0)),
      running_funcs: Arc::new(Mutex::new(0)),
      cont_lifecycle,
      prioritized_list: Arc::new(RwLock::new(Vec::new())),
      _worker_thread: worker_thread,
    })
  }

  pub async fn boxed(limits_config: Arc<FunctionLimits>, resources: Arc<ContainerResources>, cont_lifecycle: Arc<dyn LifecycleService>, tid: &TransactionId) -> Result<Arc<ContainerManager>> {
    let (tx, rx) = channel();
    let worker = ContainerManager::start_thread(rx, tid);

    let cm = Arc::new(ContainerManager::new(limits_config, resources.clone(), cont_lifecycle, worker).await?);
    tx.send(cm.clone()).unwrap();
    Ok(cm)
  }

  fn start_thread(rx: Receiver<Arc<ContainerManager>>, tid: &TransactionId) -> std::thread::JoinHandle<()> {
    debug!("[{}] Launching ContainerManager thread", tid);
    // run on an OS thread here
    // If this thread crashes, we'll never know and the worker will not have a good time
    std::thread::spawn(move || {
      let tid: &TransactionId = &CTR_MGR_WORKER_TID;
      let cm: Arc<ContainerManager> = match rx.recv() {
        Ok(cm) => cm,
        Err(_) => {
          error!("[{}] invoker service thread failed to receive service from channel!", tid);
          return;
        },
      };

      let worker_rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => { 
          error!("[{}] tokio thread runtime failed to start {}", tid, e);
          return ();
        },
      };
      debug!("[{}] container manager worker started", tid);
      worker_rt.block_on(cm.monitor_pool());
    })
  }

  async fn monitor_pool(&self) {
    let tid: &TransactionId = &CTR_MGR_WORKER_TID;
    loop {
      self.update_memory_usages(tid).await;

      self.compute_eviction_priorities(tid);
      if self.resources.memory_buffer_mb > 0 {
        let reclaim = self.resources.memory_buffer_mb - self.free_memory();
        if reclaim > 0 {
          match self.reclaim_memory(reclaim, tid).await {
            Ok(_) => {},
            Err(e) => error!("[{}] Error while trying to remove containers '{}'", tid, e),
          };
        }
      }
      std::thread::sleep(std::time::Duration::from_secs(self.resources.pool_freq_sec));
    }
  }

  pub fn free_memory(&self) -> MemSizeMb {
    self.resources.memory_mb - *self.used_mem_mb.lock()
  }
  pub fn used_memory(&self) -> MemSizeMb {
    *self.used_mem_mb.lock()
  }
  pub fn total_memory(&self) -> MemSizeMb {
    self.resources.memory_mb
  }

  pub fn free_cores(&self) -> u32 {
    self.resources.cores - *self.running_funcs.lock()
  }

  async fn update_memory_usages(&self, tid: &TransactionId) {
    debug!("[{}] updating container memory usages", tid);
    let old_total_mem = *self.used_mem_mb.lock();
    let mut to_remove = vec![];
    unsafe {
      for (_fqdn, cont_list) in (*self.active_containers.data_ptr()).iter() {
        let read_lock = cont_list.read();
        let mut sum_change = 0;
        for container in read_lock.iter() {
          if *container.healthy.lock() {
            let old_usage = container.curr_mem_usage();
            let new_usage = container.update_memory_usage_mb(tid);
            let diff = new_usage - old_usage;
            debug!("[{}] container '{}' new: {}; old: {}; diff:{}", tid, container.container_id, new_usage, old_usage, diff);
            sum_change += diff;
          } else {
            to_remove.push(container.clone());
          }
        }
        *self.used_mem_mb.lock() += sum_change;
      }
    }

    for container in to_remove {
      let stdout = container.read_stdout(tid);
      let stderr = container.read_stderr(tid);
      warn!("[{}] Removing an unhealthy container {} stdout: '{}' stderr: '{}'", tid, container.container_id, stdout, stderr);
      match self.remove_container(container.clone(), true, tid).await {
        Ok(_) => (),
        Err(cause) => {
          if let Some(_core_err) = cause.downcast_ref::<ContainerLockedError>() {
            error!("[{}] tried to remove an unhealthy container someone was using", tid);
          } else {
            error!("[{}] got an unknown error trying to remove an unhealthy container '{}'", tid, cause);
          }
        },
      };
    }

    let new_total_mem = *self.used_mem_mb.lock();
    debug!("[{}] Total container memory usage old: {}; new: {}", tid, old_total_mem, new_total_mem);
  }

  /// acquire_container
  /// get a lock on a container for the specified function
  /// will start a function if one is not available
    /// Can return a custom InsufficientCoresError if an invocation cannot be started now

  #[tracing::instrument]
  pub async fn acquire_container<'a>(&'a self, fqdn: &String, tid: &'a TransactionId) -> Result<ContainerLock<'a>> {
    let cont = self.try_acquire_container(fqdn, tid);
    let cont = match cont {
      Some(l) => Ok(l),
      None => {
        // not available container, cold start
        self.cold_start(fqdn, tid).await
      },
    };
    match cont {
        Ok(cont) =>  {
          let mut running_funcs = self.running_funcs.lock();
          if self.resources.cores > 0 && *running_funcs < self.resources.cores {
            *running_funcs += 1;
          } else {
            debug!("[{}] Not enough available cores to run something right now", tid);
            anyhow::bail!(InsufficientCoresError{})
          }
          Ok(cont)
        },
        Err(e) => Err(e),
    }
  }

  #[tracing::instrument]  
  fn try_acquire_container<'a>(&'a self, fqdn: &String, tid: &'a TransactionId) -> Option<ContainerLock<'a>> {
    let conts = self.active_containers.read();
    let opt = conts.get(fqdn);
    match opt {
      Some(pool) => {
        let pool = pool.read();
        if pool.len() > 0 {
          for container in pool.iter() {
            match self.try_lock_container(container, tid) {
              Some(c) => {
                debug!("[{}] Container '{}' acquired", tid, c.container.container_id);
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
        error!("[{}] fqdn '{}' has not been registered", tid, fqdn);
        None
      },
    }
  }

  #[tracing::instrument]  
  async fn cold_start<'a>(&'a self, fqdn: &String, tid: &'a TransactionId) -> Result<ContainerLock<'a>> {
    let container = self.launch_container(fqdn, tid).await?;
    {
      // claim this for ourselves before it touches the pool
      let mut m = container.mutex.lock();
      *m -= 1;
    }
    let container = self.add_container_to_pool(fqdn, container)?;
    Ok(ContainerLock::new(container.clone(), self, tid))
  }

  #[tracing::instrument]  
  fn try_lock_container<'a>(&'a self, container: &Arc<Container>, tid: &'a TransactionId) -> Option<ContainerLock<'a>> {
    unsafe {
      if *container.mutex.data_ptr() > 0 && *container.healthy.lock() {
        let mut m = container.mutex.lock();
        if *m > 0 {
          *m -= 1;
          return Some(ContainerLock::new(container.clone(), self, tid));
        }
      }
    }
    None
  }

  pub fn return_container(&self, container: &Arc<Container>) {
    let mut m = container.mutex.lock();
    *m += 1;
    let mut running_funcs = self.running_funcs.lock();
    if self.resources.cores > 0 && *running_funcs >= self.resources.cores {
      *running_funcs -= 1;
    }
  }

  #[tracing::instrument]  
  async fn launch_container(&self, fqdn: &String, tid: &TransactionId) -> Result<Container> {
    let reg = match self.get_registration(&fqdn) {
      Ok(r) => r,
      Err(_) => {
        anyhow::bail!("Function {} was not registered! Cannot launch a container for it", fqdn);
      },
    };
    let container = self.launch_container_internal(&reg, tid).await?;
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

  #[tracing::instrument]  
  async fn try_launch_container(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> Result<Container> {
    // TODO: cpu and mem prewarm request overrides registration?
    {
      let mut curr_mem = self.used_mem_mb.lock();
      if *curr_mem + reg.memory > self.resources.memory_mb {
        let avail = self.resources.memory_mb-*curr_mem;
        debug!("[{}] Can't launch container due to insufficient memory. needed: {}; used: {}; available: {}", tid, reg.memory-avail, *curr_mem, avail);
        anyhow::bail!(InsufficientMemoryError{ needed: reg.memory-avail, used: *curr_mem, available: avail });
      } else {
        *curr_mem += reg.memory;
      }
    }
    let fqdn = calculate_fqdn(&reg.function_name, &reg.function_version);
    let cont = self.cont_lifecycle.run_container(&fqdn, &reg.image_name, reg.parallel_invokes, "default", reg.memory, reg.cpus, reg, tid).await;
    let cont = match cont {
        Ok(cont) => {
          cont
        },
        Err(e) => {
          *self.used_mem_mb.lock() -= reg.memory;
          return Err(e);
        },
    };

    match cont.wait_startup(self.resources.startup_timeout_ms, tid).await {
        Ok(_) => (),
        Err(e) => {
          {
            *self.used_mem_mb.lock() -= reg.memory;
          }
          match self.cont_lifecycle.remove_container(&cont.container_id, &cont.namespace, "default", tid).await {
            Ok(_) => { return Err(e); },
            Err(inner_e) => anyhow::bail!("Encountered a second error after startup failed. Primary error: '{}'; inner error: '{}'", e, inner_e),
          };
        },
    };
    info!("[{}] container '{}' with image '{}' was launched", tid, cont.container_id, reg.image_name);
    Ok(cont)
  }

  /// launch_container_internal
  /// 
  /// Does a best effort to ensure a container is launched
  /// If various known errors happen, it will re-try to start it
  async fn launch_container_internal(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> Result<Container> {
    match self.try_launch_container(&reg, tid).await {
            Ok(c) => Ok(c),
            Err(cause) => 
              match cause.downcast_ref::<InsufficientMemoryError>() {
                Some(mem) => {
                  self.reclaim_memory(mem.needed, tid).await?;
                  self.try_launch_container(&reg, tid).await
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
  #[tracing::instrument]  
  pub async fn prewarm(&self, request: &PrewarmRequest) -> Result<()> {
    let fqdn = calculate_fqdn(&request.function_name, &request.function_version);
    let reg = match self.get_registration(&fqdn) {
        Ok(r) => r,
        Err(_) => {
          warn!("[{}] function {} was attempted to be prewarmed before registering. Attempting register...", request.transaction_id, fqdn);
          match self.register_internal(&request.function_name, &request.function_version, &request.image_name, request.memory, request.cpu, 1, &fqdn, &request.transaction_id).await {
            Ok(_) => self.get_registration(&fqdn)?,
            Err(sub_e) => {
              bail_error!("[{}] Prewarm of function {} was not registered because it was not registered! Attempted registration failed because '{}'", &request.transaction_id, fqdn, sub_e);
            }
          }
        },
    };

    let container = match self.launch_container_internal(&reg, &request.transaction_id).await {
        Ok(c) => Ok(c),
        Err(cause) => {
            if let Some(mem_err) = cause.downcast_ref::<InsufficientMemoryError>() {
              self.reclaim_memory(mem_err.needed, &request.transaction_id).await?;
              self.launch_container_internal(&reg, &request.transaction_id).await
            } else {
              Err(cause)
            }
          },
    }?;
    self.add_container_to_pool(&fqdn, container)?;
    info!("[{}] function '{}' was successfully prewarmed", &request.transaction_id, fqdn);
    Ok(())
  }

  /// Registers a function using the given request
  /// 
  /// # Errors
  /// Can error if the function is already registers, the image is invalid, or many other reasons
  pub async fn register(&self, request: &RegisterRequest) -> Result<()> {
    let fqdn = calculate_fqdn(&request.function_name, &request.function_version);

    self.check_registration(&fqdn)?;
    return self.register_internal(&request.function_name, &request.function_version, &request.image_name, request.memory, request.cpus, request.parallel_invokes, &fqdn, &request.transaction_id).await;
  }

  /// Returns the function registration identified by `fqdn` if it exists, an error otherwise
  fn get_registration(&self, fqdn: &String) -> Result<Arc<RegisteredFunction>> {
    match self.registered_functions.get(fqdn) {
      Some(val) => Ok(val.clone()),
      None => anyhow::bail!("Function {} was not registered!", fqdn),
    }
  }

  /// Returns an error if the function identified by `fqdn`
  fn check_registration(&self, fqdn: &String) -> Result<()> {
    if self.registered_functions.contains_key(fqdn) {
      anyhow::bail!("Function {} is already registered!", fqdn);
    }
    Ok(())
  }

  async fn register_internal(&self, function_name: &String, function_version: &String, image_name: &String, memory: MemSizeMb, cpus: u32, parallel_invokes: u32, fqdn: &String, tid: &TransactionId) -> Result<()> {
    if function_name.len() < 1 {
      anyhow::bail!("Invalid function name");
    }
    if function_version.len() < 1 {
      anyhow::bail!("Invalid function version");
    }
    if memory < self.limits_config.mem_min_mb || memory > self.limits_config.mem_max_mb {
      anyhow::bail!("Illegal memory allocation request");
    }
    if cpus < 1 || cpus > self.limits_config.cpu_max {
      anyhow::bail!("Illegal cpu allocation request");
    }
    if parallel_invokes != 1 {
      anyhow::bail!("Illegal parallel invokes set, must be 1");
    }
    if function_name.contains("/") || function_name.contains("\\") {
      anyhow::bail!("Illegal characters in function name: cannot container any \\,/");
    }

    self.cont_lifecycle.ensure_image(&image_name).await?;
    let snapshot_base = self.cont_lifecycle.search_image_digest(&image_name, "default", tid).await?;
    let registration = RegisteredFunction {
      function_name: function_name.clone(),
      function_version: function_version.clone(),
      image_name: image_name.clone(),
      memory,
      cpus,
      snapshot_base,
      parallel_invokes,
    };

    debug!("[{}] Adding new registration to registered_functions map: {} {}", tid, function_name, function_version);
    self.registered_functions.insert(fqdn.clone(), Arc::new(registration));
    debug!("[{}] Adding new registration to active_containers map: {} {}", tid, function_name, function_version);
    { // write lock on active_containers
      let mut conts = self.active_containers.write();
      conts.insert(fqdn.clone(), Arc::new(RwLock::new(Vec::new())));
    }
    info!("[{}] function '{}'; version '{}' was successfully registered", tid, function_name, function_version);
    Ok(())
  }

  pub fn mark_unhealthy(&self, container: &Arc<Container>, tid: &TransactionId) {
    info!("[{}] Marking container '{}' as unhealthy", tid, container.container_id);
    *container.healthy.lock() = false;
  }

  pub async fn remove_container(&self, container: Arc<Container>, lock_check: bool, tid: &TransactionId) -> Result<()> {
    if lock_check {
      let mut cont_lock = container.mutex.lock();
      if *cont_lock != container.function.parallel_invokes {
        bail!(ContainerLockedError{})
      }
      *cont_lock = 0;
    }

    match self.get_container_vec(&container.fqdn) {
      Some(pool) => {
        let (pos, pool_len) = self.find_container_pos(&container, pool.clone());
        if pos < pool_len {
          info!("[{}] Removing container {}", tid, &container.container_id);
          {
            let mut wlocked_pool = pool.write();
            let dropped_cont = wlocked_pool.remove(pos);
            *self.used_mem_mb.lock() -= dropped_cont.curr_mem_usage();
          }
          self.cont_lifecycle.remove_container(&container.container_id, &container.namespace, "default", tid).await?;
          return Ok(());
        } else {
          anyhow::bail!("Was unable to find container {} to remove it", container.container_id);
        }
      },
      None => anyhow::bail!("Function '{}' was supposed to be readable in pool but could not be found", container.fqdn),
    }
  }

  async fn reclaim_memory(&self, amount_mb: MemSizeMb, tid: &TransactionId) -> Result<()> {
    debug!("[{}] Trying to reclaim {} memory", tid, amount_mb);
    if amount_mb <= 0 {
      bail!("Cannot reclaim '{}' amount of memory", amount_mb);
    }
    let mut reclaimed: MemSizeMb = 0;
    let mut to_remove = Vec::new();
    for container in self.prioritized_list.read().iter() {
      if self.try_seize_container(container) {
        to_remove.push(container.clone());
        reclaimed += container.curr_mem_usage();
        if reclaimed >= amount_mb {
          break;
        }
      }
    }
    for container in to_remove {
      self.remove_container(container, false, tid).await?;
    }
    Ok(())
  }

  fn compute_eviction_priorities(&self, tid: &TransactionId) {
    debug!("[{}] Computing eviction priorities", tid);
    let mut ordered = Vec::new();
    unsafe {
      for (_fqdn, cont_list) in (*self.active_containers.data_ptr()).iter() {
        for container in cont_list.read().iter() {
          ordered.push(container.clone());
        }
      }
    }
    let comparator = match self.resources.eviction.as_str() {
      "LRU" => ContainerManager::lru_eviction,
      _ => { 
          error!("[{}] Unkonwn eviction algorithm '{}'", tid, self.resources.eviction);
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
