use super::container_pool::{ContainerPool, ResourcePool, Subpool};
use super::structs::{Container, ContainerLock, ContainerState};
use super::ContainerIsolationCollection;
use crate::services::containers::structs::{InsufficientGPUError, InsufficientMemoryError};
use crate::services::{registration::RegisteredFunction, resources::gpu::GpuResourceTracker};
use crate::worker_api::worker_config::ContainerResourceConfig;
use anyhow::{bail, Result};
use dashmap::DashMap;
use futures::Future;
use iluvatar_library::threading::{tokio_runtime, EventualItem};
use iluvatar_library::types::{Compute, Isolation, MemSizeMb};
use iluvatar_library::{bail_error, transaction::TransactionId, utils::calculate_fqdn};
use parking_lot::RwLock;
use std::cmp::Ordering;
use std::sync::{atomic::AtomicU32, Arc};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tracing::{debug, error, info, warn};

lazy_static::lazy_static! {
  pub static ref CTR_MGR_WORKER_TID: TransactionId = "CtrMrgWorker".to_string();
  pub static ref CTR_MGR_HEALTH_WORKER_TID: TransactionId = "CtrMrgHealthWorker".to_string();
  pub static ref CTR_MGR_REMOVER_TID: TransactionId = "CtrMrgUnhealthyRemoved".to_string();
}

/// A struct to manage and control access to containers and system resources
pub struct ContainerManager {
    /// Containers that only use CPU compute resources
    cpu_containers: ResourcePool,
    /// Containers that have GPU compute resources (and CPU naturally)
    gpu_containers: ResourcePool,
    resources: Arc<ContainerResourceConfig>,
    used_mem_mb: Arc<RwLock<MemSizeMb>>,
    cont_isolations: ContainerIsolationCollection,
    prioritized_list: RwLock<Subpool>,
    _worker_thread: std::thread::JoinHandle<()>,
    _health_thread: tokio::task::JoinHandle<()>,
    gpu_resources: Arc<GpuResourceTracker>,
    /// A channel to send unhealthy containers to to be removed async to the sender
    /// Containers must not be in a pool when sent here
    unhealthy_removal_rx: UnboundedSender<Container>,
    outstanding_containers: DashMap<String, AtomicU32>,
}

impl ContainerManager {
    fn new(
        resources: Arc<ContainerResourceConfig>,
        cont_isolations: ContainerIsolationCollection,
        worker_thread: std::thread::JoinHandle<()>,
        health_thread: tokio::task::JoinHandle<()>,
        gpu_resources: Arc<GpuResourceTracker>,
        removal_rx: UnboundedSender<Container>,
    ) -> Self {
        ContainerManager {
            resources,
            cont_isolations,
            gpu_resources,
            used_mem_mb: Arc::new(RwLock::new(0)),
            cpu_containers: ResourcePool::new(Compute::CPU),
            gpu_containers: ResourcePool::new(Compute::GPU),
            prioritized_list: RwLock::new(Vec::new()),
            _worker_thread: worker_thread,
            _health_thread: health_thread,
            unhealthy_removal_rx: removal_rx,
            outstanding_containers: DashMap::new(),
        }
    }

    fn deletion_thread() -> (
        tokio::task::JoinHandle<()>,
        std::sync::mpsc::Sender<Arc<Self>>,
        UnboundedSender<Container>,
    ) {
        let (tx, rx) = std::sync::mpsc::channel();
        let (del_tx, del_rx) = tokio::sync::mpsc::unbounded_channel::<Container>();
        let handle = tokio::spawn(async move {
            let tid: &TransactionId = &CTR_MGR_REMOVER_TID;
            let service: Arc<Self> = match rx.recv() {
                Ok(cm) => cm,
                Err(e) => {
                    error!(tid=%tid, error=%e, "Tokio service thread failed to receive service from channel!");
                    return;
                }
            };
            service.cull_unhealthy(tid, del_rx).await;
        });

        (handle, tx, del_tx)
    }

    pub async fn boxed(
        resources: Arc<ContainerResourceConfig>,
        cont_isolations: ContainerIsolationCollection,
        gpu_resources: Arc<GpuResourceTracker>,
        _tid: &TransactionId,
    ) -> Result<Arc<Self>> {
        let (handle, tx) = tokio_runtime(
            resources.pool_freq_ms,
            CTR_MGR_WORKER_TID.clone(),
            ContainerManager::monitor_pool,
            None::<fn(Arc<ContainerManager>, TransactionId) -> tokio::sync::futures::Notified<'static>>,
            None,
        )?;
        let (health_handle, health_tx, del_ctr_tx) = Self::deletion_thread();
        let cm = Arc::new(ContainerManager::new(
            resources.clone(),
            cont_isolations,
            handle,
            health_handle,
            gpu_resources,
            del_ctr_tx,
        ));
        tx.send(cm.clone())?;
        health_tx.send(cm.clone())?;
        Ok(cm)
    }

    #[tracing::instrument(skip(service), fields(tid=%tid))]
    async fn monitor_pool<'r, 's>(service: Arc<Self>, tid: TransactionId) {
        service.update_memory_usages(&tid).await;
        service.compute_eviction_priorities(&tid);
        if service.resources.memory_buffer_mb > 0 {
            let reclaim = service.resources.memory_buffer_mb - service.free_memory();
            if reclaim > 0 {
                info!(tid=%tid, amount=reclaim, "Trying to reclaim memory for monitor pool");
                match service.reclaim_memory(reclaim, &tid).await {
                    Ok(_) => {}
                    Err(e) => error!(tid=%tid, error=%e, "Error while trying to remove containers"),
                };
            }
        }
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, rx), fields(tid=%tid)))]
    async fn cull_unhealthy(&self, tid: &TransactionId, mut rx: UnboundedReceiver<Container>) {
        loop {
            let to_remove = match rx.recv().await {
                Some(c) => c,
                None => return,
            };
            let cont_lifecycle = match self.cont_isolations.get(&to_remove.container_type()) {
                Some(c) => c,
                None => {
                    error!(tid=%tid, iso=?to_remove.container_type(), "Lifecycle for container not supported");
                    continue;
                }
            };
            let stdout = cont_lifecycle.read_stdout(&to_remove, &tid);
            let stderr = cont_lifecycle.read_stderr(&to_remove, &tid);
            warn!(tid=%tid, container_id=%to_remove.container_id(), stdout=%stdout, stderr=%stderr, "Removing an unhealthy container");
            match self.remove_container(to_remove, &tid).await {
                Ok(_) => (),
                Err(cause) => {
                    error!(tid=%tid, error=%cause, "Got an unknown error trying to remove an unhealthy container")
                }
            };
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
    pub fn num_containers(&self) -> u32 {
        self.cpu_containers.running_containers.len()
            + self.cpu_containers.idle_containers.len()
            + self.gpu_containers.running_containers.len()
            + self.gpu_containers.idle_containers.len()
    }
    /// Returns the best possible idle container's [ContainerState] at this time
    /// Not a guarantee it will be available
    pub fn container_available(&self, fqdn: &String, compute: Compute) -> ContainerState {
        if compute == Compute::CPU {
            let x = self.cpu_containers.idle_containers.has_container(fqdn);
            return x;
        }
        if compute == Compute::GPU {
            let x = self.gpu_containers.idle_containers.has_container(fqdn);
            return x;
        }
        ContainerState::Cold
    }
    /// Returns the best possible idle container's [ContainerState] at this time
    /// Can be either running or idle, if [ContainerState::Cold], then possibly no container found
    pub fn container_exists(&self, fqdn: &String, compute: Compute) -> ContainerState {
        let mut ret = ContainerState::Cold;
        if compute == Compute::CPU {
            let idle = self.cpu_containers.idle_containers.has_container(fqdn);
            let run = self.cpu_containers.running_containers.has_container(fqdn);
            ret = std::cmp::max(idle, run);
        } else if compute == Compute::GPU {
            let idle = self.gpu_containers.idle_containers.has_container(fqdn);
            let run = self.gpu_containers.running_containers.has_container(fqdn);
            ret = std::cmp::max(idle, run);
        }
        if ret == ContainerState::Unhealthy {
            return ContainerState::Cold;
        }
        return ret;
    }

    /// The number of containers for the given FQDN that are not idle
    /// I.E. they are executing an invocation
    /// 0 if the fqdn is unknown
    pub fn outstanding(&self, fqdn: &String) -> u32 {
        match self.outstanding_containers.get(fqdn) {
            Some(cnt) => (*cnt).load(std::sync::atomic::Ordering::Relaxed),
            None => 0,
        }
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self), fields(tid=%tid)))]
    async fn update_memory_usages(&self, tid: &TransactionId) {
        debug!(tid=%tid, "updating container memory usages");
        let old_total_mem = *self.used_mem_mb.read();
        let mut all_ctrs = self.cpu_containers.idle_containers.iter();
        all_ctrs.extend(self.cpu_containers.running_containers.iter());
        all_ctrs.extend(self.gpu_containers.running_containers.iter());
        all_ctrs.extend(self.gpu_containers.idle_containers.iter());
        all_ctrs = all_ctrs.into_iter().filter(|x| x.is_healthy()).collect();
        let mut sum_change = 0;
        for container in all_ctrs {
            if !container.is_healthy() {
                continue; // don't update unhealthy containers, they will be remove soon
            }
            let old_usage = container.get_curr_mem_usage();
            let cont_lifecycle = match self.cont_isolations.get(&container.container_type()) {
                Some(c) => c,
                None => {
                    error!(tid=%tid, iso=?container.container_type(), "Lifecycle for container not supported");
                    continue;
                }
            };
            let new_usage = cont_lifecycle.update_memory_usage_mb(&container, tid);
            let diff = new_usage - old_usage;
            debug!(tid=%tid, container_id=%container.container_id(), new_usage=new_usage, old=old_usage, diff=diff, "updated container memory usage");
            sum_change += diff;
        }
        *self.used_mem_mb.write() += sum_change;

        let new_total_mem = *self.used_mem_mb.read();
        debug!(tid=%tid, old_total=old_total_mem, total=new_total_mem, "Total container memory usage");
        if new_total_mem < 0 {
            error!(tid=%tid, old_total=old_total_mem, total=new_total_mem, "Container memory usage has gone negative");
        }
    }

    /// acquire_container
    /// get a lock on a container for the specified function
    /// will start a function if one is not available
    /// A return type [EventualItem::Future] means a container will have to be started to run the invocation.
    ///    The process to start the container has not begun, and will not until the future is awaited on. A product of Rust's implementation of async/futures.
    /// A return type [EventualItem::Now] means an existing container has been acquired
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, reg, compute), fields(tid=%tid)))]
    pub fn acquire_container<'a>(
        &'a self,
        reg: &Arc<RegisteredFunction>,
        tid: &'a TransactionId,
        compute: Compute,
    ) -> EventualItem<impl Future<Output = Result<ContainerLock<'a>>>> {
        let cont = self.try_acquire_container(&reg.fqdn, tid, compute);
        let cont = match cont {
            Some(l) => EventualItem::Now(Ok(l)),
            None => EventualItem::Future(self.cold_start(reg.clone(), tid, compute)), // no available container, cold start
        };
        return cont;
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, fqdn), fields(tid=%tid)))]
    /// Returns an warmed container if one is available
    fn try_acquire_container<'a>(
        &'a self,
        fqdn: &String,
        tid: &'a TransactionId,
        compute: Compute,
    ) -> Option<ContainerLock<'a>> {
        if let Ok(pool) = self.get_resource_pool(compute) {
            return self.acquire_container_from_pool(&pool, fqdn, tid);
        }
        None
    }

    fn acquire_container_from_pool<'a>(
        &'a self,
        pool: &'a ResourcePool,
        fqdn: &String,
        tid: &'a TransactionId,
    ) -> Option<ContainerLock<'a>> {
        loop {
            match pool.idle_containers.get_random_container(fqdn, tid) {
                Some(c) => {
                    if c.is_healthy() {
                        return self.try_lock_container(c, tid);
                    } else {
                        if let Err(e) = self.unhealthy_removal_rx.send(c) {
                            error!(tid=%tid, error=%e, "Failed to send unhealthy container for removal");
                        }
                    }
                }
                None => return None,
            }
        }
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, reg), fields(tid=%tid)))]
    /// Starts a new container and returns a [ContainerLock] for it to be used
    async fn cold_start<'a>(
        &'a self,
        reg: Arc<RegisteredFunction>,
        tid: &'a TransactionId,
        compute: Compute,
    ) -> Result<ContainerLock<'a>> {
        debug!(tid=%tid, fqdn=%reg.fqdn, "Trying to cold start a new container");
        let container = self.launch_container_internal(&reg, tid, compute).await?;
        info!(tid=%tid, container_id=%container.container_id(), "Container cold start completed");
        container.set_state(ContainerState::Cold);
        self.try_lock_container(container, tid)
            .ok_or(anyhow::anyhow!("Encountered an error making conatiner lock"))
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, container), fields(tid=%tid)))]
    /// Adds the container to the running pool and returns a [ContainerLock] for it
    /// Returns [None] if the container is unhealthy or an error occurs
    fn try_lock_container<'a>(&'a self, container: Container, tid: &'a TransactionId) -> Option<ContainerLock<'a>> {
        if container.is_healthy() {
            debug!(tid=%tid, container_id=%container.container_id(), "Container acquired");
            container.touch();
            let rpool = match self.get_resource_pool(container.compute_type()) {
                Ok(r) => r,
                Err(_) => return None,
            };
            return match self.add_container_to_pool(&rpool.running_containers, container.clone(), tid) {
                Ok(_) => {
                    if let Some(cnt) = self.outstanding_containers.get(container.fqdn()) {
                        (*cnt).fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    Some(ContainerLock::new(container, self, tid))
                }
                Err(e) => {
                    error!(error=%e, container_id=%container.container_id(), "Failed to add container to running containers");
                    None
                }
            };
        } else {
            None
        }
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, container), fields(tid=%tid)))]
    pub fn return_container(&self, container: &Container, tid: &TransactionId) {
        if let Some(cnt) = self.outstanding_containers.get(container.fqdn()) {
            (*cnt).fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
        let resource_pool = match self.get_resource_pool(container.compute_type()) {
            Ok(r) => r,
            Err(_) => {
                error!(tid=%tid, container_id=%container.container_id(), compute=?container.compute_type(), "Unknonwn compute for container");
                return;
            }
        };

        if let Some(removed_ctr) = resource_pool.running_containers.remove_container(container, tid) {
            if removed_ctr.is_healthy() {
                removed_ctr.set_state(ContainerState::Warm);
                match self.add_container_to_pool(&resource_pool.idle_containers, removed_ctr, tid) {
                    Ok(_) => return,
                    Err(e) => {
                        error!(tid=%tid, error=%e, container_id=%container.container_id(), "Encountered an error trying to return a container to the idle pool")
                    }
                };
            }
        } else {
            error!(tid=%tid, container_id=%container.container_id(), "Tried to return a container that wasn't running");
        }
        container.mark_unhealthy();
        warn!(tid=%tid, container_id=%container.container_id(), "Marking unhealthy container for removal");
        if let Err(e) = self.unhealthy_removal_rx.send(container.clone()) {
            error!(tid=%tid, error=%e, "Failed to send container for removal on return");
        }
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, pool, container), fields(tid=%tid)))]
    fn add_container_to_pool(&self, pool: &ContainerPool, container: Container, tid: &TransactionId) -> Result<()> {
        pool.add_container(container, tid)
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, reg), fields(tid=%tid)))]
    async fn try_launch_container(
        &self,
        reg: &Arc<RegisteredFunction>,
        tid: &TransactionId,
        compute: Compute,
    ) -> Result<Container> {
        let chosen_iso = self.get_optimal_lifecycle(reg, tid, compute)?;
        let cont_lifecycle = match self.cont_isolations.get(&chosen_iso) {
            Some(c) => c,
            None => {
                bail_error!(tid=%tid, iso=?chosen_iso, "Lifecycle(s) for container not supported")
            }
        };

        let counter = if compute == Compute::GPU {
            match self.gpu_resources.acquire_gpu() {
                Some(g) => {
                    info!(tid=%tid, uuid=%g.gpu_uuid, "Assigning GPU to container");
                    Some(g)
                }
                None => anyhow::bail!(InsufficientGPUError {}),
            }
        } else {
            None
        };
        let curr_mem = *self.used_mem_mb.read();
        if curr_mem + reg.memory > self.resources.memory_mb {
            let avail = self.resources.memory_mb - curr_mem;
            debug!(tid=%tid, needed=reg.memory-avail, used=curr_mem, available=avail, "Can't launch container due to insufficient memory");
            anyhow::bail!(InsufficientMemoryError {
                needed: reg.memory - avail,
                used: curr_mem,
                available: avail
            });
        } else {
            *self.used_mem_mb.write() += reg.memory;
        }
        let fqdn = calculate_fqdn(&reg.function_name, &reg.function_version);
        let cont = cont_lifecycle
            .run_container(
                &fqdn,
                &reg.image_name,
                reg.parallel_invokes,
                "default",
                reg.memory,
                reg.cpus,
                reg,
                chosen_iso,
                compute,
                counter,
                tid,
            )
            .await;
        let cont = match cont {
            Ok(cont) => cont,
            Err(e) => {
                *self.used_mem_mb.write() -= reg.memory;
                return Err(e);
            }
        };

        match cont_lifecycle
            .wait_startup(&cont, self.resources.startup_timeout_ms, tid)
            .await
        {
            Ok(_) => (),
            Err(e) => {
                *self.used_mem_mb.write() -= reg.memory;
                match cont_lifecycle.remove_container(cont, "default", tid).await {
                    Ok(_) => {
                        return Err(e);
                    }
                    Err(inner_e) => anyhow::bail!(
                        "Encountered a second error after startup failed. Primary error: '{}'; inner error: '{}'",
                        e,
                        inner_e
                    ),
                };
            }
        };
        info!(tid=%tid, image=%reg.image_name, container_id=%cont.container_id(), "Container was launched");
        Ok(cont)
    }

    /// Get the optimal lifecycle for starting a new container based on it's registration information
    /// Consider both startup time (i.e. Containerd > Docker) and other resource options (i.e. GPU)
    fn get_optimal_lifecycle(
        &self,
        reg: &Arc<RegisteredFunction>,
        tid: &TransactionId,
        compute: Compute,
    ) -> Result<Isolation> {
        if !reg.supported_compute.contains(compute) {
            bail_error!(tid=%tid, iso=?reg.supported_compute, compute=?compute, "Registration did not contain requested compute")
        }
        if compute.contains(Compute::GPU) {
            if reg.isolation_type.contains(Isolation::DOCKER) {
                return Ok(Isolation::DOCKER);
            } else {
                bail_error!(tid=%tid, iso=?reg.isolation_type, compute=?compute, "GPU only supported with Docker isolation")
            }
        }
        if reg.isolation_type.contains(Isolation::CONTAINERD) {
            return Ok(Isolation::CONTAINERD);
        }
        if reg.isolation_type.contains(Isolation::DOCKER) {
            return Ok(Isolation::DOCKER);
        }
        Ok(reg.isolation_type)
    }

    /// Does a best effort to ensure a container is launched
    /// If various known errors happen, it will re-try to start it
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, reg), fields(tid=%tid)))]
    async fn launch_container_internal(
        &self,
        reg: &Arc<RegisteredFunction>,
        tid: &TransactionId,
        compute: Compute,
    ) -> Result<Container> {
        match self.try_launch_container(&reg, tid, compute).await {
            Ok(c) => Ok(c),
            Err(cause) => {
                if let Some(mem) = cause.downcast_ref::<InsufficientMemoryError>() {
                    debug!(tid=%tid, amount=mem.needed, "Trying to reclaim memory to cold-start a container");
                    self.reclaim_memory(mem.needed, tid).await?;
                    return self.try_launch_container(&reg, tid, compute).await;
                } else if let Some(_) = cause.downcast_ref::<InsufficientGPUError>() {
                    self.reclaim_gpu(tid).await?;
                    return self.try_launch_container(&reg, tid, compute).await;
                } else {
                    return Err(cause);
                }
            }
        }
    }

    /// Prewarm a container for the requested function
    ///
    /// # Errors
    /// Can error if not already registered and full info isn't provided
    /// Other errors caused by starting/registered the function apply
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, reg), fields(tid=%tid)))]
    pub async fn prewarm(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId, compute: Compute) -> Result<()> {
        let container = self.launch_container_internal(&reg, tid, compute).await?;
        container.set_state(ContainerState::Prewarm);
        let pool = self.get_resource_pool(compute)?;
        self.add_container_to_pool(&pool.idle_containers, container, &tid)?;
        info!(tid=%tid, fqdn=%reg.fqdn, "function was successfully prewarmed");
        Ok(())
    }

    /// Registers a function using the given request
    pub fn register(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> Result<()> {
        debug!(tid=%tid, function_name=%reg.function_name, function_version=%reg.function_version, fqdn=%reg.fqdn, "Adding new registration to active_containers map");
        self.cpu_containers.running_containers.register_fqdn(reg.fqdn.clone());
        self.cpu_containers.idle_containers.register_fqdn(reg.fqdn.clone());
        self.gpu_containers.running_containers.register_fqdn(reg.fqdn.clone());
        self.gpu_containers.idle_containers.register_fqdn(reg.fqdn.clone());
        self.outstanding_containers.insert(reg.fqdn.clone(), AtomicU32::new(0));
        Ok(())
    }

    /// Delete a container and releases tracked resources for it
    /// Container **must** have already been removed from the container pool
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, container), fields(tid=%tid)))]
    async fn remove_container(&self, container: Container, tid: &TransactionId) -> Result<()> {
        info!(tid=%tid, container_id=%container.container_id(), "Removing container");
        let cont_lifecycle = match self.cont_isolations.get(&container.container_type()) {
            Some(c) => c,
            None => {
                bail_error!(tid=%tid, iso=?container.container_type(), "Lifecycle for container not supported")
            }
        };
        *self.used_mem_mb.write() -= container.get_curr_mem_usage();
        if let Some(dev) = container.device_resource() {
            self.gpu_resources.return_gpu(dev.clone());
        }
        cont_lifecycle.remove_container(container, "default", tid).await?;
        Ok(())
    }

    fn get_resource_pool(&self, compute: Compute) -> Result<&ResourcePool> {
        if compute == Compute::CPU {
            return Ok(&self.cpu_containers);
        }
        if compute == Compute::GPU {
            return Ok(&self.gpu_containers);
        }
        anyhow::bail!("No pool for compute: {:?}", compute)
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self), fields(tid=%tid)))]
    /// Reclaim a single GPU from a container via eviction
    /// If any are free to be removed
    async fn reclaim_gpu(&self, tid: &TransactionId) -> Result<()> {
        // TODO: Eviction ordering, have list sorted
        let mut killable = self.gpu_containers.idle_containers.iter();
        loop {
            match killable.pop() {
                Some(chosen) => {
                    if let Some(_) = self.gpu_containers.idle_containers.remove_container(&chosen, tid) {
                        self.remove_container(chosen, tid).await?;
                        break;
                    }
                }
                None => break,
            }
        }
        Ok(())
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, amount_mb), fields(tid=%tid)))]
    /// Reclaim at least the specified amount of memory by evicting containers
    /// Not guaranteed to do so, as all containers could be busy
    async fn reclaim_memory(&self, amount_mb: MemSizeMb, tid: &TransactionId) -> Result<()> {
        if amount_mb <= 0 {
            bail!("Cannot reclaim '{}' amount of memory", amount_mb);
        }
        let mut reclaimed: MemSizeMb = 0;
        let mut to_remove = Vec::new();
        for container in self.prioritized_list.read().iter() {
            if let Some(removed_ctr) = self.cpu_containers.idle_containers.remove_container(container, tid) {
                to_remove.push(removed_ctr.clone());
                reclaimed += removed_ctr.get_curr_mem_usage();
                if reclaimed >= amount_mb {
                    break;
                }
            }
        }
        debug!(tid=%tid, memory=reclaimed, "Memory to be reclaimed");
        for container in to_remove {
            self.remove_container(container, tid).await?;
        }
        Ok(())
    }

    fn compute_eviction_priorities(&self, tid: &TransactionId) {
        debug!(tid=%tid, "Computing eviction priorities");
        let mut ordered = self.cpu_containers.idle_containers.iter();
        ordered.append(&mut self.cpu_containers.running_containers.iter());
        let comparator = match self.resources.eviction.as_str() {
            "LRU" => ContainerManager::lru_eviction,
            _ => {
                error!(tid=%tid, algorithm=%self.resources.eviction, "Unkonwn eviction algorithm");
                return;
            }
        };
        ordered.sort_by(|c1, c2| comparator(c1, c2));
        let mut lock = self.prioritized_list.write();
        *lock = ordered;
    }

    fn lru_eviction(c1: &Container, c2: &Container) -> Ordering {
        c1.last_used().cmp(&c2.last_used())
    }

    pub async fn remove_idle_containers(&self, tid: &TransactionId) -> Result<std::collections::HashMap<Compute, i32>> {
        let mut ret = std::collections::HashMap::new();
        let mut conts = self.gpu_containers.idle_containers.iter();
        conts.extend(self.cpu_containers.idle_containers.iter());
        for c in conts {
            let compute = c.compute_type();
            let pool = self.get_resource_pool(compute)?;
            if let Some(c) = pool.idle_containers.remove_container(&c, tid) {
                self.remove_container(c, tid).await?;
                match ret.get_mut(&compute) {
                    Some(v) => *v += 1,
                    None => {
                        ret.insert(compute, 1);
                    }
                }
            }
        }
        Ok(ret)
    }
}
