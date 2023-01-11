use crate::services::containers::structs::{InsufficientMemoryError, ContainerLockedError};
use futures::Future;
use iluvatar_library::bail_error;
use iluvatar_library::threading::{tokio_runtime, EventualItem};
use tokio::sync::{Semaphore, OwnedSemaphorePermit};
use crate::rpc::{RegisterRequest, PrewarmRequest};
use iluvatar_library::transaction::{TransactionId, CTR_MGR_WORKER_TID};
use iluvatar_library::types::MemSizeMb;
use iluvatar_library::utils::calculate_fqdn;
use crate::worker_api::worker_config::{FunctionLimits, ContainerResources};
use anyhow::{Result, bail};
use dashmap::DashMap;
use std::cmp::Ordering;
use std::sync::Arc;
use parking_lot::RwLock;
use super::LifecycleService;
use super::container_pool::{ContainerPool, Subpool};
use super::structs::{Container, RegisteredFunction, ContainerLock};
use tracing::{info, warn, debug, error};

pub struct ContainerManager {
  registered_functions: Arc<DashMap<String, Arc<RegisteredFunction>>>,
  idle_containers: ContainerPool,
  running_containers: ContainerPool,
  limits_config: Arc<FunctionLimits>, 
  resources: Arc<ContainerResources>,
  used_mem_mb: Arc<RwLock<MemSizeMb>>,
  cont_lifecycle: Arc<dyn LifecycleService>,
  prioritized_list: RwLock<Subpool>,
  _worker_thread: std::thread::JoinHandle<()>,
  core_sem: Option<Arc<Semaphore>>,
}

impl ContainerManager {
  async fn new(limits_config: Arc<FunctionLimits>, resources: Arc<ContainerResources>, cont_lifecycle: Arc<dyn LifecycleService>, worker_thread: std::thread::JoinHandle<()>) -> ContainerManager {
    let core_sem = match resources.cores {
      0 => None,
      c => Some(Arc::new(Semaphore::new(c as usize))),
    };
    ContainerManager {
      core_sem,
      registered_functions: Arc::new(DashMap::new()),
      limits_config,
      resources,
      used_mem_mb: Arc::new(RwLock::new(0)),
      cont_lifecycle,
      prioritized_list: RwLock::new(Vec::new()),
      _worker_thread: worker_thread,
      idle_containers: ContainerPool::new("idle"),
      running_containers: ContainerPool::new("running"),
    }
  }

  pub async fn boxed(limits_config: Arc<FunctionLimits>, resources: Arc<ContainerResources>, cont_lifecycle: Arc<dyn LifecycleService>, _tid: &TransactionId) -> Result<Arc<ContainerManager>> {
    let (handle, tx) = tokio_runtime(resources.pool_freq_ms, CTR_MGR_WORKER_TID.clone(), 
          ContainerManager::monitor_pool, None::<fn(Arc<ContainerManager>, TransactionId) -> tokio::sync::futures::Notified<'static>>, None)?;
    let cm = Arc::new(ContainerManager::new(limits_config, resources.clone(), cont_lifecycle, handle).await);
    tx.send(cm.clone()).unwrap();
    Ok(cm)
  }

  #[tracing::instrument(skip(service), fields(tid=%tid))]
  async fn monitor_pool<'r, 's>(service: Arc<Self>, tid: TransactionId) {
    service.update_memory_usages(&tid).await;

    service.compute_eviction_priorities(&tid);
    if service.resources.memory_buffer_mb > 0 {
      let reclaim = service.resources.memory_buffer_mb - service.free_memory();
      if reclaim > 0 {
        match service.reclaim_memory(reclaim, &tid).await {
          Ok(_) => {},
          Err(e) => error!(tid=%tid, error=%e, "Error while trying to remove containers"),
        };
      }
    }
  }

  pub fn free_memory(&self) -> MemSizeMb {
    self.resources.memory_mb - *self.used_mem_mb.read()
  }
  pub fn used_memory(&self) -> MemSizeMb {
    *self.used_mem_mb.read()
  }
  pub fn total_memory(&self) -> MemSizeMb {
    self.resources.memory_mb
  }
  pub fn free_cores(&self) -> u32 {
    match &self.core_sem {
      Some(s) => s.available_permits() as u32,
      None => 0
    }
  }
  pub fn num_containers(&self) -> u32 {
    self.running_containers.len() + self.idle_containers.len()
    // let mut cnt = 0;
    // for cont_list in self.active_containers.iter() {
    //   cnt += cont_list.value().len() as u32;
    // }
    // cnt
  }

  /// Return a permit for the function to run on its registered number of cores
  /// If the semaphore is [None], then no permits are being tracked
  pub fn try_acquire_cores(&self, fqdn: &String) -> Result<Option<OwnedSemaphorePermit>, tokio::sync::TryAcquireError> {
    if let Ok(reg) = self.get_registration(fqdn) {
      if let Some(sem) = &self.core_sem {
        return match sem.clone().try_acquire_many_owned(reg.cpus) {
          Ok(p) => Ok(Some(p)),
          Err(e) => Err(e),
        };
      }
      return Ok(None);
    }
    // function was not registered
    Err(tokio::sync::TryAcquireError::Closed)
  }

  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self), fields(tid=%tid)))]
  async fn update_memory_usages(&self, tid: &TransactionId) {
    debug!(tid=%tid, "updating container memory usages");
    let old_total_mem = *self.used_mem_mb.read();
    let mut to_remove = vec![];
    let mut all_ctrs = self.idle_containers.iter();
    all_ctrs.append(&mut self.running_containers.iter());
    for container in all_ctrs {
      let mut sum_change = 0;
      if container.is_healthy() {
        let old_usage = container.get_curr_mem_usage();
        let new_usage = self.cont_lifecycle.update_memory_usage_mb(&container, tid);
        let diff = new_usage - old_usage;
        debug!(tid=%tid, container_id=%container.container_id(), new_usage=new_usage, old=old_usage, diff=diff, "updated container memory usage");
        sum_change += diff;
      } else {
        to_remove.push(container);
      }
      *self.used_mem_mb.write() += sum_change;
    }

    for container in to_remove {
      let stdout = self.cont_lifecycle.read_stdout(&container, tid);
      let stderr = self.cont_lifecycle.read_stderr(&container, tid);
      warn!(tid=%tid, container_id=%container.container_id(), stdout=%stdout, stderr=%stderr, "Removing an unhealthy container");
      match self.remove_container(container.clone(), true, tid).await {
        Ok(_) => (),
        Err(cause) => {
          if let Some(_core_err) = cause.downcast_ref::<ContainerLockedError>() {
            error!(tid=%tid, "Tried to remove an unhealthy container someone was using");
          } else {
            error!(tid=%tid, error=%cause, "Got an unknown error trying to remove an unhealthy container");
          }
        },
      };
    }

    let new_total_mem = *self.used_mem_mb.read();
    debug!(tid=%tid, old_total=old_total_mem, total=new_total_mem, "Total container memory usage");
  }

  /// acquire_container
  /// get a lock on a container for the specified function
  /// will start a function if one is not available
  /// A return type [EventualItem::Future] means a container will have to be started to run the invocation.
  ///    The process to start the container has not begun, and will not until the future is awaited on. A product of Rust's implementation of async/futures.
  /// A return type [EventualItem::Now] means an existing container has been acquired
  /// Can return a custom InsufficientCoresError if an invocation cannot be started now
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, fqdn), fields(tid=%tid)))]
  pub fn acquire_container<'a>(&'a self, fqdn: &String, tid: &'a TransactionId) -> EventualItem<impl Future<Output=Result<ContainerLock<'a>>>> {
    let cont = self.try_acquire_container(fqdn, tid);
    let cont = match cont {
      Some(l) => EventualItem::Now(Ok(l)),
      None => {
        // not available container, cold start
        EventualItem::Future(self.cold_start(fqdn.clone(), tid))
      },
    };
    return cont;
  }

  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, fqdn), fields(tid=%tid)))]
  fn try_acquire_container<'a>(&'a self, fqdn: &String, tid: &'a TransactionId) -> Option<ContainerLock<'a>> {
    match self.idle_containers.get_random_container(fqdn, tid) {
      Some(c) => {
        self.try_lock_container(c, tid)
      },
      None => None,
    }
    // match self.active_containers.get(fqdn) {
    //   Some(pool) => {
    //     let pool = pool.value();
    //     for (i, container) in pool.iter().enumerate() {
    //       match self.try_lock_container(container, tid) {
    //         Some(c) => {
    //           debug!(tid=%tid, container_id=%c.container.container_id(), size=pool.len(), number=i, "Container acquired");
    //           c.container.touch();
    //           return Some(c);
    //         },
    //         None => continue,
    //       }
    //     }
    //     drop(pool);
    //     return None;
    //   },
    //   None => {
    //     // 'should' not get here
    //     error!(tid=%tid, fqdn=%fqdn, "Tried to acquire a container for an fqdn that has not been registered");
    //     return None;
    //   },
    // }
  }

  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, fqdn), fields(tid=%tid)))]
  async fn cold_start<'a>(&'a self, fqdn: String, tid: &'a TransactionId) -> Result<ContainerLock<'a>> {
    debug!(tid=%tid, fqdn=%fqdn, "Trying to cold start a new container");
    let container = self.launch_container(&fqdn, tid).await?;
    // claim this for ourselves before it touches the pool
    container.acquire();
    info!(tid=%tid, container_id=%container.container_id(), "Container cold start completed");

    self.add_container_to_pool(&self.running_containers, container.clone(), tid)?;
    Ok(ContainerLock::new(container, self, tid))
  }

  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, container), fields(tid=%tid)))]
  fn try_lock_container<'a>(&'a self, container: Container, tid: &'a TransactionId) -> Option<ContainerLock<'a>> {
    if container.try_acquire() {
      if container.is_healthy() {
        debug!(tid=%tid, container_id=%container.container_id(), "Container acquired");
        container.touch();
        return Some(ContainerLock::new(container.clone(), self, tid));
      }
      else {
        container.release();
      }
    }
    None
  }

  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, container), fields(tid=%_tid)))]
  pub fn return_container(&self, container: &Container, _tid: &TransactionId) {
    // TODO: update this part
    container.release();
  }

  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, fqdn), fields(tid=%tid)))]
  async fn launch_container(&self, fqdn: &String, tid: &TransactionId) -> Result<Container> {
    let reg = match self.get_registration(&fqdn) {
      Ok(r) => r,
      Err(_) => {
        anyhow::bail!("Function {} was not registered! Cannot launch a container for it", fqdn);
      },
    };
    self.launch_container_internal(&reg, tid).await
  }

  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, fqdn, container), fields(tid=%_tid)))]
  fn add_container_to_pool(&self, pool: &ContainerPool, container: Container, tid: &TransactionId) -> Result<()> {
    // acquire read lock to see if that function already has a pool entry
    pool.add_container(container, tid)
    // match self.active_containers.get_mut(fqdn) {
    //   Some(mut pool) => {
    //     (*pool).push(container.clone());
    //     info!(tid=%tid, container_id=%container.container_id(), "Container added to pool");
    //     Ok(())
    //   },
    //   None => anyhow::bail!("Function '{}' was not registered yet", fqdn)
    // }
  }

  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, reg), fields(tid=%tid)))]
  async fn try_launch_container(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> Result<Container> {
    // TODO: cpu and mem prewarm request overrides registration?
    let curr_mem = *self.used_mem_mb.read();
    if curr_mem + reg.memory > self.resources.memory_mb {
      let avail = self.resources.memory_mb-curr_mem;
      debug!(tid=%tid, needed=reg.memory-avail, used=curr_mem, available=avail, "Can't launch container due to insufficient memory");
      anyhow::bail!(InsufficientMemoryError{ needed: reg.memory-avail, used: curr_mem, available: avail });
    } else {
      *self.used_mem_mb.write() += reg.memory;
    }
    let fqdn = calculate_fqdn(&reg.function_name, &reg.function_version);
    let cont = self.cont_lifecycle.run_container(&fqdn, &reg.image_name, reg.parallel_invokes, "default", reg.memory, reg.cpus, reg, tid).await;
    let cont = match cont {
      Ok(cont) => {
        cont
      },
      Err(e) => {
        *self.used_mem_mb.write() -= reg.memory;
        return Err(e);
      },
    };

    match self.cont_lifecycle.wait_startup(&cont, self.resources.startup_timeout_ms, tid).await {
      Ok(_) => (),
      Err(e) => {
        {
          *self.used_mem_mb.write() -= reg.memory;
        }
        match self.cont_lifecycle.remove_container(cont, "default", tid).await {
          Ok(_) => { return Err(e); },
          Err(inner_e) => anyhow::bail!("Encountered a second error after startup failed. Primary error: '{}'; inner error: '{}'", e, inner_e),
        };
      },
    };
    info!(tid=%tid, image=%reg.image_name, container_id=%cont.container_id(), "container was launched");
    Ok(cont)
  }

  /// launch_container_internal
  /// 
  /// Does a best effort to ensure a container is launched
  /// If various known errors happen, it will re-try to start it
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, reg), fields(tid=%tid)))]
  async fn launch_container_internal(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> Result<Container> {
    match self.try_launch_container(&reg, tid).await {
      Ok(c) => Ok(c),
      Err(cause) => match cause.downcast_ref::<InsufficientMemoryError>() {
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
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, request), fields(tid=%request.transaction_id)))]
  pub async fn prewarm(&self, request: &PrewarmRequest) -> Result<()> {
    let fqdn = calculate_fqdn(&request.function_name, &request.function_version);
    let reg = match self.get_registration(&fqdn) {
        Ok(r) => r,
        Err(_) => {
          warn!(tid=%request.transaction_id, fqdn=%fqdn, "Function was attempted to be prewarmed before registering. Attempting register...");
          match self.register_internal(&request.function_name, &request.function_version, &request.image_name, request.memory, request.cpu, 1, &fqdn, &request.transaction_id).await {
            Ok(_) => self.get_registration(&fqdn)?,
            Err(sub_e) => bail_error!(tid=%request.transaction_id, fqdn=%fqdn, error=%sub_e, "Prewarm of function was registered because it was not registered! Attempted registration failed")
          }
        },
    };

    let container = self.launch_container_internal(&reg, &request.transaction_id).await?;
    self.add_container_to_pool(&self.idle_containers, container, &request.transaction_id)?;
    info!(tid=%request.transaction_id, fqdn=%fqdn, "function was successfully prewarmed");
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

    let registration = self.cont_lifecycle.prepare_function_registration(function_name, function_version, image_name, memory, cpus, parallel_invokes, fqdn, tid).await?;

    debug!(tid=%tid, function_name=%function_name, function_version=%function_version, fqdn=%fqdn, "Adding new registration to registered_functions map");
    self.registered_functions.insert(fqdn.clone(), Arc::new(registration));
    debug!(tid=%tid, function_name=%function_name, function_version=%function_version, fqdn=%fqdn, "Adding new registration to active_containers map");

    self.running_containers.register_fqdn(fqdn.clone());
    self.idle_containers.register_fqdn(fqdn.clone());
    // self.active_containers.insert(fqdn.clone(), Vec::new());
    info!(tid=%tid, function_name=%function_name, function_version=%function_version, fqdn=%fqdn, "function was successfully registered");
    Ok(())
  }

  pub fn mark_unhealthy(&self, container: &Container, tid: &TransactionId) {
    info!(tid=%tid, container_id=%container.container_id(), "Marking container as unhealthy");
    container.mark_unhealthy();
  }

  /// Delete a container and releases tracked resources for it
  /// Container **must not** be in any container pool
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, container, lock_check), fields(tid=%tid)))]
  async fn remove_container(&self, container: Container, lock_check: bool, tid: &TransactionId) -> Result<()> {
    if lock_check && container.being_held() {
      bail!(ContainerLockedError{})
    }

    // self.idle_containers.remove_container(&container, tid)

    // let dropped_cont = match self.active_containers.get_mut(container.fqdn()) {
    //   Some(mut pool) => {
    //     let pool = pool.value_mut();
    //     let (pos, pool_len) = self.find_container_pos(&container, &pool);
    //     if pos < pool_len {
    //       info!(tid=%tid, container_id=%container.container_id(), "Removing container");
    //       pool.remove(pos)
    //     } else {
    //       anyhow::bail!("Was unable to find container {} to remove it", container.container_id())
    //     }
    //   },
    //   None => anyhow::bail!("Function '{}' was supposed to be readable in pool but could not be found", container.fqdn()),
    // };
    *self.used_mem_mb.write() -= container.get_curr_mem_usage();
    self.cont_lifecycle.remove_container(container, "default", tid).await?;
    Ok(())
  }

  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, amount_mb), fields(tid=%tid)))]
  async fn reclaim_memory(&self, amount_mb: MemSizeMb, tid: &TransactionId) -> Result<()> {
    debug!(tid=%tid, amount=amount_mb, "Trying to reclaim memory");
    if amount_mb <= 0 {
      bail!("Cannot reclaim '{}' amount of memory", amount_mb);
    }
    let mut reclaimed: MemSizeMb = 0;
    let mut to_remove = Vec::new();
    for container in self.prioritized_list.read().iter() {
      if let Some(removed_ctr) = self.idle_containers.remove_container(container, tid) {
      // if container.try_seize() {
        to_remove.push(removed_ctr.clone());
        reclaimed += removed_ctr.get_curr_mem_usage();
        if reclaimed >= amount_mb {
          break;
        }
        // }  
      }
    }
    for container in to_remove {
      self.remove_container(container, false, tid).await?;
    }
    Ok(())
  }

  fn compute_eviction_priorities(&self, tid: &TransactionId) {
    debug!(tid=%tid, "Computing eviction priorities");
    // let mut ordered = Vec::new();
    let mut ordered = self.idle_containers.iter();
    ordered.append(&mut self.running_containers.iter());
    // for container in all_ctrs.iter() {
    //   ordered.push(container.clone());
    // }
    let comparator = match self.resources.eviction.as_str() {
      "LRU" => ContainerManager::lru_eviction,
      _ => { 
          error!(tid=%tid, algorithm=%self.resources.eviction, "Unkonwn eviction algorithm");
          return;
      }
    };
    ordered.sort_by(|c1, c2| comparator(c1,c2));
    let mut lock = self.prioritized_list.write();
    *lock = ordered;
  }

  fn lru_eviction(c1: &Container, c2: &Container) -> Ordering {
    c1.last_used().cmp(&c2.last_used())
  }

  // fn find_container_pos(&self, container: &Container, pool: &ContainerList) -> (usize,usize) {
  //   let pool_len = pool.len();
  //   let mut pos = usize::MAX;
  //   for (i, iter_cont) in pool.iter().enumerate() {
  //     if container.container_id() == iter_cont.container_id() {
  //       pos = i;
  //       break;
  //     }
  //   }
  //   return (pos, pool_len);
  // }
}
