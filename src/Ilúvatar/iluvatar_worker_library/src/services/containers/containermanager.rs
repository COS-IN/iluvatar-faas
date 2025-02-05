use super::container_pool::{ContainerPool, Subpool};
use super::structs::{Container, ContainerLock, ContainerState, ContainerT};
use super::ContainerIsolationCollection;
use crate::services::containers::docker::dockerstructs::DockerContainer;
use crate::services::containers::structs::{InsufficientGPUError, InsufficientMemoryError};
use crate::services::resources::gpu::{GpuToken, GPU};
use crate::services::{registration::RegisteredFunction, resources::gpu::GpuResourceTracker};
use crate::worker_api::worker_config::ContainerResourceConfig;
use anyhow::{bail, Result};
use dashmap::DashMap;
use futures::Future;
use iluvatar_library::threading::{tokio_notify_thread, tokio_runtime, tokio_sender_thread, EventualItem};
use iluvatar_library::types::{Compute, Isolation, MemSizeMb};
use iluvatar_library::{bail_error, transaction::TransactionId, utils::calculate_fqdn};
use parking_lot::RwLock;
use std::cmp::Ordering;
use std::sync::{atomic::AtomicU32, Arc};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

lazy_static::lazy_static! {
  pub static ref CTR_MGR_WORKER_TID: TransactionId = "CtrMrgWorker".to_string();
  pub static ref CTR_MGR_HEALTH_WORKER_TID: TransactionId = "CtrMrgHealthWorker".to_string();
  pub static ref CTR_MGR_REMOVER_TID: TransactionId = "CtrMrgUnhealthyRemoved".to_string();
  pub static ref CTR_MGR_PRI_TID: TransactionId = "CtrMrgPriorities".to_string();
}

/// Manage and control access to containers and system resources. CPU and GPU resource pools. Primary container state-tracking.
pub struct ContainerManager {
    /// Containers that only use CPU compute resources
    cpu_containers: ContainerPool,
    /// Containers that have GPU compute resources (and CPU naturally)
    gpu_containers: ContainerPool,
    resources: Arc<ContainerResourceConfig>,
    used_mem_mb: Arc<RwLock<MemSizeMb>>,
    cont_isolations: ContainerIsolationCollection,
    /// For keep-alive eviction
    pub prioritized_list: RwLock<Subpool>,
    pub prioritized_gpu_list: RwLock<Subpool>,
    _worker_thread: std::thread::JoinHandle<()>,
    _health_thread: tokio::task::JoinHandle<()>,
    _priorities_thread: tokio::task::JoinHandle<()>,
    /// Signal to tell to re-compute eviction priorities
    prioritiy_notify: Arc<Notify>,
    gpu_resources: Option<Arc<GpuResourceTracker>>,
    /// A channel to send unhealthy containers to to be removed async to the sender
    /// Containers must not be in a pool when sent here
    unhealthy_removal_rx: UnboundedSender<Container>,
    /// Currently executing a function
    outstanding_containers: DashMap<String, AtomicU32>,
}

impl ContainerManager {
    pub async fn boxed(
        resources: Arc<ContainerResourceConfig>,
        cont_isolations: ContainerIsolationCollection,
        gpu_resources: Option<Arc<GpuResourceTracker>>,
        _tid: &TransactionId,
    ) -> Result<Arc<Self>> {
        let (worker_handle, tx) = tokio_runtime(
            resources.pool_freq_ms,
            CTR_MGR_WORKER_TID.clone(),
            ContainerManager::monitor_pool,
            None::<fn(Arc<ContainerManager>, TransactionId) -> tokio::sync::futures::Notified<'static>>,
            None,
        )?;
        let (health_handle, health_tx, del_ctr_tx) =
            tokio_sender_thread(CTR_MGR_REMOVER_TID.clone(), Arc::new(Self::cull_unhealthy));
        let pri_notif = Arc::new(Notify::new());
        let (pri_handle, pri_tx) = tokio_notify_thread(
            CTR_MGR_PRI_TID.clone(),
            pri_notif.clone(),
            Arc::new(Self::recompute_eviction_priorities),
        );
        let cm = Arc::new(ContainerManager {
            resources,
            cont_isolations,
            gpu_resources,
            used_mem_mb: Arc::new(RwLock::new(0)),
            cpu_containers: ContainerPool::new(Compute::CPU),
            gpu_containers: ContainerPool::new(Compute::GPU),
            prioritized_list: RwLock::new(Vec::new()),
            prioritized_gpu_list: RwLock::new(Vec::new()),
            _worker_thread: worker_handle,
            _health_thread: health_handle,
            _priorities_thread: pri_handle,
            unhealthy_removal_rx: del_ctr_tx,
            outstanding_containers: DashMap::new(),
            prioritiy_notify: pri_notif,
        });
        tx.send(cm.clone())?;
        health_tx.send(cm.clone())?;
        pri_tx.send(cm.clone())?;
        Ok(cm)
    }

    #[tracing::instrument(skip(self), fields(tid=%tid))]
    fn recompute_eviction_priorities(&self, tid: &TransactionId) {
        self.compute_eviction_priorities(tid);
        self.compute_gpu_eviction_priorities(tid);
    }

    #[tracing::instrument(skip(service), fields(tid=%tid))]
    async fn monitor_pool<'r, 's>(service: Arc<Self>, tid: TransactionId) {
        service.update_memory_usages(&tid).await;
        service.prioritiy_notify.notify_waiters();
        if service.resources.memory_buffer_mb > 0 {
            let reclaim = service.resources.memory_buffer_mb - service.free_memory();
            if reclaim > 0 {
                info!(tid=%tid, amount=reclaim, "Trying to reclaim memory for monitor pool");
                match service.reclaim_memory(reclaim, &tid).await {
                    Ok(_) => {},
                    Err(e) => error!(tid=%tid, error=%e, "Error while trying to remove containers"),
                };
            }
        }
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, to_remove), fields(tid=%tid)))]
    async fn cull_unhealthy(self: Arc<Self>, tid: TransactionId, to_remove: Container) {
        if let Ok(pool) = self.get_resource_pool(to_remove.compute_type()) {
            if pool.remove_container(&to_remove, &tid).is_none() {
                warn!(tid=%tid, container_id=%to_remove.container_id(), compute=%to_remove.compute_type(),
                      "Failed to remove container from container pool before cull");
            }
        }
        let cont_lifecycle = match self.cont_isolations.get(&to_remove.container_type()) {
            Some(c) => c,
            None => {
                error!(tid=%tid, iso=?to_remove.container_type(), "Lifecycle for container not supported");
                return;
            },
        };
        let stdout = cont_lifecycle.read_stdout(&to_remove, &tid).await;
        let stderr = cont_lifecycle.read_stderr(&to_remove, &tid).await;
        warn!(tid=%tid, container_id=%to_remove.container_id(), stdout=%stdout, stderr=%stderr, "Removing an unhealthy container");
        match self.purge_container(to_remove, &tid).await {
            Ok(_) => (),
            Err(cause) => {
                error!(tid=%tid, error=%cause, "Got an unknown error trying to remove an unhealthy container")
            },
        };
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
        self.cpu_containers.len() + self.gpu_containers.len()
    }

    /// Returns the best possible idle container's [ContainerState] at this time
    /// Not a guarantee it will be available
    pub fn container_available(&self, fqdn: &str, compute: Compute) -> ContainerState {
        if compute == Compute::CPU {
            return self.cpu_containers.has_idle_container(fqdn);
        }
        if compute == Compute::GPU {
            return self.gpu_containers.has_idle_container(fqdn);
        }
        ContainerState::Cold
    }

    /// Returns the best possible idle container's [ContainerState] at this time
    /// Not a guarantee it will be available
    pub fn warm_container(&self, fqdn: &str, gpu_token: &GpuToken) -> bool {
        self.gpu_containers.has_gpu_container(fqdn, gpu_token)
    }

    /// Returns the best possible idle container's [ContainerState] at this time
    /// Can be either running or idle, if [ContainerState::Cold], then possibly no container found
    pub fn container_exists(&self, fqdn: &str, compute: Compute) -> ContainerState {
        let mut ret = ContainerState::Cold;
        if compute == Compute::CPU {
            ret = self.cpu_containers.has_container(fqdn);
        } else if compute == Compute::GPU {
            ret = self.gpu_containers.has_container(fqdn);
        }
        if ret == ContainerState::Unhealthy {
            return ContainerState::Cold;
        }
        ret
    }

    /// The number of containers for the given FQDN that are not idle
    /// I.E. they are executing an invocation
    /// 0 if the fqdn is unknown
    pub fn outstanding(&self, fqdn: &str) -> u32 {
        match self.outstanding_containers.get(fqdn) {
            Some(cnt) => (*cnt).load(std::sync::atomic::Ordering::Relaxed),
            None => 0,
        }
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self), fields(tid=%tid)))]
    async fn update_memory_usages(&self, tid: &TransactionId) {
        let old_total_mem = *self.used_mem_mb.read();
        let cpu_mem = self.calc_container_pool_memory_usages(&self.cpu_containers, tid).await;
        let gpu_mem = self.calc_container_pool_memory_usages(&self.gpu_containers, tid).await;
        let new_total_mem = cpu_mem + gpu_mem;
        *self.used_mem_mb.write() = new_total_mem;
        debug!(tid=%tid, old_total=old_total_mem, total=new_total_mem, cpu_pool=cpu_mem, gpu_pool=gpu_mem, "Total container memory usage");
        if new_total_mem < 0 {
            error!(tid=%tid, old_total=old_total_mem, total=new_total_mem, "Container memory usage has gone negative");
        }
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, pool), fields(tid=%tid)))]
    async fn calc_container_pool_memory_usages(&self, pool: &ContainerPool, tid: &TransactionId) -> MemSizeMb {
        debug!(tid=%tid, pool=%pool.pool_name(), "updating container memory usages");
        let mut new_total_mem = 0;
        for container in pool.iter().iter().filter(|c| c.is_healthy()) {
            let old_usage = container.get_curr_mem_usage();
            let new_usage = match self.cont_isolations.get(&container.container_type()) {
                Some(c) => c.update_memory_usage_mb(container, tid).await,
                None => {
                    error!(tid=%tid, iso=?container.container_type(), "Lifecycle for container not supported");
                    continue;
                },
            };
            new_total_mem += new_usage;
            debug!(tid=%tid, container_id=%container.container_id(), new_usage=new_usage, old=old_usage, "updated container memory usage");
        }
        new_total_mem
    }

    /// acquire_container
    /// get a lock on a container for the specified function
    /// will start a function if one is not available
    /// A return type [EventualItem::Future] means a container will have to be started to run the invocation.
    ///    The process to start the container has not begun, and will not until the future is awaited on. A product of Rust's implementation of async/futures.
    /// A return type [EventualItem::Now] means an existing container has been acquired
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, reg, compute), fields(tid=%tid)))]
    pub fn acquire_container<'a>(
        self: &'a Arc<Self>,
        reg: &'a Arc<RegisteredFunction>,
        tid: &'a TransactionId,
        compute: Compute,
    ) -> EventualItem<impl Future<Output = Result<ContainerLock>> + 'a> {
        let cont = self.try_acquire_container(&reg.fqdn, tid, compute);
        let cont = match cont {
            Some(l) => EventualItem::Now(Ok(l)),
            None => {
                // no available container, cold start
                let r = reg.clone();
                EventualItem::Future(self.cold_start(r, tid, compute))
            },
        };
        cont
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, fqdn), fields(tid=%tid)))]
    /// Returns an warmed container if one is available
    fn try_acquire_container(
        self: &Arc<Self>,
        fqdn: &str,
        tid: &TransactionId,
        compute: Compute,
    ) -> Option<ContainerLock> {
        if let Ok(pool) = self.get_resource_pool(compute) {
            return self.acquire_container_from_pool(pool, fqdn, tid);
        }
        None
    }

    fn acquire_container_from_pool(
        self: &Arc<Self>,
        pool: &ContainerPool,
        fqdn: &str,
        tid: &TransactionId,
    ) -> Option<ContainerLock> {
        loop {
            match pool.activate_random_container(fqdn, tid) {
                Some(c) => {
                    if c.is_healthy() {
                        return self.try_lock_container(c, tid);
                    } else if let Err(e) = self.unhealthy_removal_rx.send(c) {
                        error!(tid=%tid, error=%e, "Failed to send unhealthy container for removal");
                    }
                },
                None => return None,
            }
        }
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, reg), fields(tid=%tid)))]
    /// Starts a new container and returns a [ContainerLock] for it to be used
    async fn cold_start(
        self: &Arc<Self>,
        reg: Arc<RegisteredFunction>,
        tid: &TransactionId,
        compute: Compute,
    ) -> Result<ContainerLock> {
        debug!(tid=%tid, fqdn=%reg.fqdn, "Trying to cold start a new container");
        let container = self.launch_container_internal(&reg, tid, compute).await?;
        let rpool = self.get_resource_pool(compute)?;
        rpool.add_running_container(container.clone(), tid);
        self.prioritiy_notify.notify_waiters();
        info!(tid=%tid, container_id=%container.container_id(), "Container cold start completed");
        container.set_state(ContainerState::Cold);
        self.try_lock_container(container, tid)
            .ok_or_else(|| anyhow::anyhow!("Encountered an error making conatiner lock"))
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, container), fields(tid=%tid)))]
    /// Returns a [ContainerLock] for the given container
    /// Returns [None] if the container is unhealthy or an error occurs
    fn try_lock_container(self: &Arc<Self>, container: Container, tid: &TransactionId) -> Option<ContainerLock> {
        if container.is_healthy() {
            debug!(tid=%tid, container_id=%container.container_id(), "Container acquired");
            container.touch();
            if let Some(cnt) = self.outstanding_containers.get(container.fqdn()) {
                (*cnt).fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            Some(ContainerLock::new(container, self, tid))
        } else {
            None
        }
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, container), fields(tid=%tid)))]
    pub fn return_container(&self, container: &Container, tid: &TransactionId) {
        if let Some(cnt) = self.outstanding_containers.get(container.fqdn()) {
            (*cnt).fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
        if !container.is_healthy() {
            warn!(tid=%tid, container_id=%container.container_id(), "Returned container is unhealthy, sending for removal");
            if let Err(e) = self.unhealthy_removal_rx.send(container.clone()) {
                error!(tid=%tid, error=%e, "Failed to send container for removal on return");
            }
            return;
        }
        let resource_pool = match self.get_resource_pool(container.compute_type()) {
            Ok(r) => r,
            Err(_) => {
                error!(tid=%tid, container_id=%container.container_id(), compute=?container.compute_type(), "Unknonwn compute for container");
                return;
            },
        };

        container.set_state(ContainerState::Warm);
        match resource_pool.move_to_idle(container, tid) {
            Ok(_) => (),
            Err(e) => {
                error!(tid=%tid, error=%e, "Error moving container back to idle pool");
                container.mark_unhealthy();
                warn!(tid=%tid, container_id=%container.container_id(), "Marking unhealthy container for removal");
                if let Err(e) = self.unhealthy_removal_rx.send(container.clone()) {
                    error!(tid=%tid, error=%e, "Failed to send container for removal on return");
                }
            },
        };
    }

    fn get_gpu(&self, tid: &TransactionId, compute: Compute) -> Result<Option<GPU>> {
        if compute == Compute::GPU {
            return match self
                .gpu_resources
                .as_ref()
                .ok_or_else(|| anyhow::format_err!("Trying to assign GPU resources when none exist"))?
                .acquire_gpu(tid)
            {
                Some(g) => {
                    info!(tid=%tid, uuid=%g.gpu_uuid, "Assigning GPU to container");
                    Ok(Some(g))
                },
                None => anyhow::bail!(InsufficientGPUError {}),
            };
        }
        Ok(None)
    }

    fn return_gpu(&self, gpu: Option<GPU>, tid: &TransactionId) {
        if let Some(gpu) = gpu {
            if let Some(gpu_man) = self.gpu_resources.as_ref() {
                gpu_man.return_gpu(gpu, tid)
            }
        }
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
            },
        };

        let gpu = self.get_gpu(tid, compute)?;
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
                gpu,
                tid,
            )
            .await;
        let cont = match cont {
            Ok(cont) => cont,
            Err((e, gpu)) => {
                *self.used_mem_mb.write() = i64::max(curr_mem - reg.memory, 0);
                self.return_gpu(gpu, tid);
                return Err(e);
            },
        };

        match cont_lifecycle
            .wait_startup(&cont, self.resources.startup_timeout_ms, tid)
            .await
        {
            Ok(_) => (),
            Err(e) => {
                *self.used_mem_mb.write() = i64::max(curr_mem - reg.memory, 0);
                self.return_gpu(cont.revoke_device(), tid);
                match cont_lifecycle.remove_container(cont, "default", tid).await {
                    Ok(_) => {
                        return Err(e);
                    },
                    Err(inner_e) => anyhow::bail!(
                        "Encountered a second error after startup failed. Primary error: '{}'; inner error: '{}'",
                        e,
                        inner_e
                    ),
                };
            },
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
        match self.try_launch_container(reg, tid, compute).await {
            Ok(c) => Ok(c),
            Err(cause) => {
                if let Some(mem) = cause.downcast_ref::<InsufficientMemoryError>() {
                    debug!(tid=%tid, amount=mem.needed, "Trying to reclaim memory to cold-start a container");
                    self.reclaim_memory(mem.needed, tid).await?;
                    self.try_launch_container(reg, tid, compute).await
                } else if cause.downcast_ref::<InsufficientGPUError>().is_some() {
                    self.reclaim_gpu(tid).await?;
                    self.try_launch_container(reg, tid, compute).await
                } else {
                    Err(cause)
                }
            },
        }
    }

    /// Prewarm a container(s) for the requested function.
    /// Creates one container for each listed `compute`.
    ///
    /// # Errors
    /// Can error if not already registered and full info isn't provided.
    /// Other errors caused by starting/registered the function apply.
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, reg), fields(tid=%tid)))]
    pub async fn prewarm(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId, compute: Compute) -> Result<()> {
        for spec_comp in compute.into_iter() {
            let container = self.launch_container_internal(reg, tid, spec_comp).await?;
            container.set_state(ContainerState::Prewarm);
            let pool = self.get_resource_pool(spec_comp)?;
            pool.add_idle_container(container, tid);
            self.prioritiy_notify.notify_waiters();
            info!(tid=%tid, fqdn=%reg.fqdn, compute=%spec_comp, "function was successfully prewarmed");
        }
        Ok(())
    }

    /// Registers a function using the given request
    pub fn register(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> Result<()> {
        debug!(tid=%tid, function_name=%reg.function_name, function_version=%reg.function_version, fqdn=%reg.fqdn, "Adding new registration to active_containers map");
        self.outstanding_containers.insert(reg.fqdn.clone(), AtomicU32::new(0));
        Ok(())
    }

    /// Delete a container and releases tracked resources for it
    /// Container **must** have already been removed from the container pool
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, container), fields(tid=%tid)))]
    async fn purge_container(&self, container: Container, tid: &TransactionId) -> Result<()> {
        info!(tid=%tid, container_id=%container.container_id(), "Removing container");
        let r = match self.cont_isolations.get(&container.container_type()) {
            Some(c) => c.remove_container(container.clone(), "default", tid).await,
            None => bail_error!(tid=%tid, iso=?container.container_type(), "Lifecycle for container not supported"),
        };
        *self.used_mem_mb.write() -= container.get_curr_mem_usage();
        self.return_gpu(container.revoke_device(), tid);
        container.remove_drop(tid);
        self.prioritiy_notify.notify_waiters();
        r
    }

    fn get_resource_pool(&self, compute: Compute) -> Result<&ContainerPool> {
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
        let mut chosen = None;
        for cont in self.prioritized_gpu_list.read().iter() {
            if cont.is_healthy() && self.gpu_containers.remove_container(cont, tid).is_some() {
                chosen = Some(cont.clone());
                break;
            }
        }
        match chosen {
            Some(c) => self.purge_container(c, tid).await?,
            None => warn!(tid=%tid, "tried to evict a container for a GPU, but was unable"),
        };
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
            if let Some(removed_ctr) = self.cpu_containers.remove_container(container, tid) {
                to_remove.push(removed_ctr.clone());
                reclaimed += removed_ctr.get_curr_mem_usage();
                if reclaimed >= amount_mb {
                    break;
                }
            }
        }
        debug!(tid=%tid, memory=reclaimed, "Memory to be reclaimed");
        for container in to_remove {
            self.purge_container(container, tid).await?;
        }
        Ok(())
    }

    fn order_pool_eviction(&self, tid: &TransactionId, list: &mut Subpool) {
        debug!(tid=%tid, "Computing eviction priorities");
        let comparator = match self.resources.eviction.as_str() {
            "LRU" => ContainerManager::lru_eviction,
            _ => {
                error!(tid=%tid, algorithm=%self.resources.eviction, "Unkonwn eviction algorithm");
                return;
            },
        };
        list.sort_by(comparator);
    }

    fn compute_eviction_priorities(&self, tid: &TransactionId) {
        let mut ordered = self.cpu_containers.iter();
        debug!(tid=%tid, num_containers=%ordered.len(), "Computing CPU eviction priorities");
        self.order_pool_eviction(tid, &mut ordered);
        *self.prioritized_list.write() = ordered;
    }

    fn compute_gpu_eviction_priorities(&self, tid: &TransactionId) {
        let mut ordered = self.gpu_containers.iter();
        debug!(tid=%tid, num_containers=%ordered.len(), "Computing GPU eviction priorities");
        self.order_pool_eviction(tid, &mut ordered);
        *self.prioritized_gpu_list.write() = ordered;
    }

    fn lru_eviction(c1: &Container, c2: &Container) -> Ordering {
        c1.last_used().cmp(&c2.last_used())
    }

    pub async fn remove_idle_containers(&self, tid: &TransactionId) -> Result<std::collections::HashMap<Compute, i32>> {
        let mut ret = std::collections::HashMap::new();
        let mut conts = self.gpu_containers.iter_idle();
        conts.extend(self.cpu_containers.iter_idle());
        for c in conts {
            let compute = c.compute_type();
            let pool = self.get_resource_pool(compute)?;
            if let Some(c) = pool.remove_container(&c, tid) {
                self.purge_container(c, tid).await?;
                match ret.get_mut(&compute) {
                    Some(v) => *v += 1,
                    None => {
                        ret.insert(compute, 1);
                    },
                }
            }
        }
        Ok(ret)
    }

    pub async fn move_to_device(cont: Container, tid: TransactionId) {
        match crate::services::containers::structs::cast::<DockerContainer>(&cont) {
            Ok(c) => match c.client.move_to_device(&tid, &c.container_id).await {
                Ok(()) => c.set_state(ContainerState::Warm),
                Err(e) => error!(tid=%tid, error=%e, "Error moving data to device"),
            },
            Err(e) => error!(tid=%tid, error=%e, "move_to_device Error casting container to DockerContainer"),
        };
    }
    /// Tell all GPU containers of the given function to move memory onto the device
    pub async fn madvise_to_device(&self, fqdn: String, tid: TransactionId) {
        debug!(tid=%tid, fqdn=%fqdn, "moving to device");
        let f = Self::move_to_device;
        self.gpu_containers.iter_fqdn(tid, &fqdn, f).await;
    }
    pub async fn move_off_device(cont: Container, tid: TransactionId) {
        match crate::services::containers::structs::cast::<DockerContainer>(&cont) {
            Ok(c) => {
                match c.client.move_from_device(&tid, &c.container_id).await {
                    // container is "prewarmed" because we need to do work to fully start
                    Ok(()) => c.set_state(ContainerState::Prewarm),
                    Err(e) => error!(tid=%tid, error=%e, "Error moving data from device"),
                }
            },
            Err(e) => error!(tid=%tid, error=%e, "move_off_device Error casting container to DockerContainer"),
        };
    }
    /// Tell all GPU containers of the given function to move memory off of the device
    pub async fn madvise_off_device(&self, fqdn: String, tid: TransactionId) {
        debug!(tid=%tid, fqdn=%fqdn, "moving off device");
        let f = Self::move_off_device;
        self.gpu_containers.iter_fqdn(tid, &fqdn, f).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{services::containers::IsolationFactory, worker_api::worker_config::Configuration};
    use iluvatar_library::transaction::TEST_TID;
    use std::collections::HashMap;
    use std::time::Duration;

    fn cpu_reg() -> Arc<RegisteredFunction> {
        Arc::new(RegisteredFunction {
            function_name: "test".to_string(),
            function_version: "test".to_string(),
            cpus: 1,
            memory: 128,
            parallel_invokes: 1,
            image_name: "docker.io/alfuerst/hello-iluvatar-action:latest".to_string(),
            fqdn: "test-test".into(),
            snapshot_base: "test-test".into(),
            isolation_type: Isolation::DOCKER,
            supported_compute: Compute::CPU,
            historical_runtime_data_sec: HashMap::new(),
        })
    }

    async fn svc(overrides: Option<Vec<(String, String)>>) -> Arc<ContainerManager> {
        let tid: &TransactionId = &iluvatar_library::transaction::SIMULATION_START_TID;
        iluvatar_library::utils::set_simulation(tid).unwrap_or_else(|e| panic!("Failed to make system clock: {:?}", e));
        let cfg = Configuration::boxed(&Some("tests/resources/worker.dev.json"), overrides)
            .unwrap_or_else(|e| panic!("Failed to load config file for sim test: {:?}", e));
        let fac = IsolationFactory::new(cfg.clone())
            .get_isolation_services(&TEST_TID, false)
            .await
            .unwrap_or_else(|e| panic!("Failed to load config file for sim test: {:?}", e));
        ContainerManager::boxed(cfg.container_resources.clone(), fac, None, &TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("Failed to load config file for sim test: {:?}", e))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cpu_unhealthy_removed() {
        let cm = svc(None).await;
        let func = cpu_reg();
        cm.register(&func, &TEST_TID).unwrap();
        let c1 = match cm.acquire_container(&func, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(*cm.used_mem_mb.read(), func.memory);
        let c1_cont = c1.container.clone();
        c1_cont.mark_unhealthy();
        drop(c1);
        cm.prioritiy_notify.notify_waiters();
        tokio::time::sleep(Duration::from_secs(10)).await;
        assert_eq!(cm.gpu_containers.len(), 0, "Unhealthy container should be gone");
        assert_eq!(*cm.used_mem_mb.read(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn purge_container() {
        let cm = svc(None).await;
        let func = cpu_reg();
        cm.register(&func, &TEST_TID).unwrap();
        let c1 = match cm.acquire_container(&func, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(*cm.used_mem_mb.read(), func.memory);
        assert_eq!(cm.cpu_containers.len(), 1, "Container should exist");
        drop(c1);
        cm.remove_idle_containers(&TEST_TID).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        let state = cm.container_exists(
            &calculate_fqdn(&func.function_name, &func.function_version),
            Compute::CPU,
        );
        assert_eq!(state, ContainerState::Cold, "After purging container, should be cold");
        assert_eq!(cm.cpu_containers.len(), 0, "Purged container should be gone");
        assert_eq!(cm.gpu_containers.len(), 0, "Purged container should be gone");
        assert_eq!(*cm.used_mem_mb.read(), 0, "Used memory should be reset to zero");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_tracking() {
        let cm = svc(None).await;
        let func = cpu_reg();
        cm.register(&func, &TEST_TID).unwrap();
        let c1 = match cm.acquire_container(&func, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .unwrap_or_else(|e| panic!("acquire container failed: {:?}", e));
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(*cm.used_mem_mb.read(), func.memory, "first failed");
        let _c2 = match cm.acquire_container(&func, &TEST_TID, Compute::CPU) {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        }
        .unwrap_or_else(|e| panic!("acquire container 2 failed: {:?}", e));
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(*cm.used_mem_mb.read(), func.memory * 2, "second failed");
        drop(c1);
        cm.remove_idle_containers(&TEST_TID)
            .await
            .unwrap_or_else(|e| panic!("remove_idle_containers failed: {:?}", e));
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(*cm.used_mem_mb.read(), func.memory, "thinrd failed");
    }
}
