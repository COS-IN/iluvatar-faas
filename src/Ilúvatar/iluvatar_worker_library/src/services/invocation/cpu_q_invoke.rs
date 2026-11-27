use super::queueing::{
    avail_scale::AvailableScalingQueue,
    cold_priority::ColdPriorityQueue,
    congestion_pricing::{
        CongestionPricingTimingSample, DomainId, PostedPriceCoordinator, PostedPriceInvocationLifecycle,
    },
    fcfs::FCFSQueue,
    minheap::MinHeapQueue,
    minheap_ed::MinHeapEDQueue,
    minheap_iat::MinHeapIATQueue,
    queueless::Queueless,
};
use super::queueing::{DeviceQueue, EnqueuedInvocation, InvokerCpuQueuePolicy};
use crate::services::containers::{
    containermanager::ContainerManager,
    structs::{ContainerState, InsufficientGPUError, InsufficientMemoryError, ParsedResult},
};
#[cfg(feature = "power_cap")]
use crate::services::invocation::energy_limiter::EnergyLimiter;
use crate::services::invocation::{invoke_on_container, QueueLoad};
use crate::services::{registration::RegisteredFunction, resources::cpu::CpuResourceTracker};
use crate::worker_api::worker_config::{CpuQueuePolicy, InvocationConfig};
use anyhow::Result;
use iluvatar_library::char_map::{Chars, WorkerCharMap};
use iluvatar_library::clock::{get_global_clock, now, Clock};
use iluvatar_library::threading::tokio_spawn_thread;
use iluvatar_library::tput_calc::DeviceTput;
use iluvatar_library::{
    threading::tokio_waiter_thread, threading::EventualItem, transaction::TransactionId, types::Compute,
    types::Utilization,
};
use parking_lot::Mutex;
use std::str::FromStr;
use std::{
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{mpsc::UnboundedSender, Notify};
use tokio::time::Instant;
#[cfg(feature = "full_spans")]
use tracing::Instrument;
use tracing::{debug, error, info, warn};

lazy_static::lazy_static! {
  pub static ref INVOKER_CPU_QUEUE_WORKER_TID: TransactionId = "InvokerCPUQueue".to_string();
}

pub struct CpuQueueingInvoker {
    posted_price: Option<Arc<PostedPriceCoordinator>>,
    cont_manager: Arc<ContainerManager>,
    invocation_config: Arc<InvocationConfig>,
    cmap: WorkerCharMap,
    clock: Clock,
    cold_starting: AtomicU32,
    running: AtomicU32,
    fallback_rr: AtomicU64,
    last_memory_warning: Mutex<Instant>,
    cpu: Arc<CpuResourceTracker>,
    _cpu_thread: tokio::task::JoinHandle<()>,
    signal: Notify,
    queue: Arc<dyn InvokerCpuQueuePolicy>,
    _bypass_thread: tokio::task::JoinHandle<()>,
    bypass_rx: UnboundedSender<Arc<EnqueuedInvocation>>,
    #[cfg(feature = "power_cap")]
    energy: Arc<EnergyLimiter>,
    device_tput: Arc<DeviceTput>,
}

#[allow(dyn_drop)]
/// An invoker implementation that enqueues invocations and orders them based on a variety of characteristics
/// Queueing method is configurable
impl CpuQueueingInvoker {
    pub fn new(
        cont_manager: Arc<ContainerManager>,
        invocation_config: Arc<InvocationConfig>,
        tid: &TransactionId,
        cmap: WorkerCharMap,
        cpu: Arc<CpuResourceTracker>,
        #[cfg(feature = "power_cap")] energy: Arc<EnergyLimiter>,
    ) -> Result<Arc<Self>> {
        let (cpu_handle, cpu_tx) = tokio_waiter_thread(
            invocation_config.queue_sleep_ms,
            INVOKER_CPU_QUEUE_WORKER_TID.clone(),
            Self::monitor_queue,
            Some(Self::cpu_wait_on_queue),
        );
        let (bypass_thread, bypass_tx, bypass_rx) = Self::bypass_thread();

        let (queue, posted_price) =
            Self::get_invoker_queue(&invocation_config, &cmap, &cont_manager, &cpu, tid)?;

        let svc = Arc::new(CpuQueueingInvoker {
            posted_price,
            queue,
            cont_manager,
            invocation_config,
            cpu,
            cmap,
            bypass_rx,
            #[cfg(feature = "power_cap")]
            energy,
            _bypass_thread: bypass_thread,
            signal: Notify::new(),
            _cpu_thread: cpu_handle,
            clock: get_global_clock(tid)?,
            running: AtomicU32::new(0),
            cold_starting: AtomicU32::new(0),
            fallback_rr: AtomicU64::new(0),
            last_memory_warning: Mutex::new(now()),
            device_tput: DeviceTput::boxed(),
        });
        let queue_preview = svc.queue.clone();
        svc.cpu.set_cpu_queue_preview(Arc::downgrade(&queue_preview));
        svc.cpu.set_container_manager(Arc::downgrade(&svc.cont_manager));
        cpu_tx.send(svc.clone())?;
        bypass_tx.send(svc.clone())?;
        debug!(tid = tid, "Created CpuQueueingInvoker");
        Ok(svc)
    }

    fn get_invoker_queue(
        invocation_config: &Arc<InvocationConfig>,
        cmap: &WorkerCharMap,
        cont_manager: &Arc<ContainerManager>,
        cpu: &Arc<CpuResourceTracker>,
        tid: &TransactionId,
    ) -> Result<(Arc<dyn InvokerCpuQueuePolicy>, Option<Arc<PostedPriceCoordinator>>)> {
        if let Some(pol) = invocation_config.queue_policies.get(&Compute::CPU) {
            match CpuQueuePolicy::from_str(pol.as_str())? {
                CpuQueuePolicy::None => Ok((Queueless::new()?, None)),
                CpuQueuePolicy::Fcfs => Ok((FCFSQueue::new(cont_manager.clone(), cmap.clone())?, None)),
                CpuQueuePolicy::MinHeap => Ok((MinHeapQueue::new(tid, cmap.clone(), cont_manager.clone())?, None)),
                CpuQueuePolicy::MinheapEd => Ok((MinHeapEDQueue::new(tid, cmap.clone(), cont_manager.clone())?, None)),
                CpuQueuePolicy::MinheapIat => {
                    Ok((MinHeapIATQueue::new(tid, cmap.clone(), cont_manager.clone())?, None))
                },
                CpuQueuePolicy::ColdPriority => {
                    Ok((ColdPriorityQueue::new(cont_manager.clone(), tid, cmap.clone())?, None))
                },
                CpuQueuePolicy::Scaling => {
                    Ok((AvailableScalingQueue::new(cont_manager.clone(), tid, cmap.clone())?, None))
                },
                CpuQueuePolicy::PostedPrice => {
                    let flb_weak = cpu
                        .fineloadbalancing()
                        .map(|flb| Arc::downgrade(&flb))
                        .unwrap_or_default();
                    let fallback_cores = (cpu.cores.round() as u32).max(1);
                    let coordinator = PostedPriceCoordinator::new_with_fallback_cores(
                        invocation_config.posted_price_config(),
                        flb_weak,
                        cmap.clone(),
                        Some(cont_manager.clone()),
                        fallback_cores,
                    );
                    Ok((coordinator.clone(), Some(coordinator)))
                },
            }
        } else {
            anyhow::bail!("No queue policy listed for compute '{:?}'", Compute::CPU)
        }
    }

    /// Wait on the Notify object for the queue or domain capacity to be available again
    async fn cpu_wait_on_queue(self: &Arc<Self>, tid: &TransactionId) {
        self.signal.notified().await;
        debug!(tid = tid, "Invoker waken up by signal");
    }

    fn round_robin_domains(&self) -> Vec<DomainId> {
        let n = self.cpu.num_domains().max(1);
        let start = (self.fallback_rr.fetch_add(1, Ordering::Relaxed) as usize) % n;
        (0..n).map(|off| (start + off) % n).collect()
    }

    /// Late-binding dispatch loop:
    /// 1. Under `posted_price`, checks free capacity before popping so queued items stay sorted
    ///    by `w_0 = s_i^(-beta)` and deferred cold starts never spin-block or HOL-block `WarmIdle`.
    /// 2. Acquires only a brief read lock on `PostedPriceCoordinator` to rank `[d_(1), ..., d_(D)]`,
    ///    releasing the lock BEFORE calling `acquire_cores(&ranked_domains)`.
    /// 3. Immediately attributes `actual_domain` 1:1 on `acquire_cores` return.
    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, _tid), fields(tid=_tid)))]
    async fn monitor_queue(self: &Arc<Self>, _tid: &TransactionId) {
        if let Some(pp) = &self.posted_price {
            loop {
                pp.refresh_pelt_if_due();

                if !self.cpu.has_free_capacity(1) {
                    break;
                }

                let Some(item) = pp.pop_dispatchable() else {
                    break;
                };

                #[cfg(feature = "power_cap")]
                if !self.energy.ok_run_fn(&self.cmap, &item.registration.fqdn) {
                    debug!(tid = item.tid, "Blocking invocation due to power overload");
                    pp.requeue_front(&item);
                    break;
                }

                // Touchpoint 2: Brief read-lock ranking, released BEFORE `acquire_cores`.
                let (ranked_domains, snapshot) = pp.rank_for_item(&item);

                match self
                    .cpu
                    .acquire_cores(&item.registration, &item.tid, &ranked_domains)
                    .await
                {
                    Ok(Some((actual_domain, cpu_permit))) => {
                        let mut permits: Vec<Box<dyn Drop + Send>> = vec![Box::new(cpu_permit)];
                        if self.invocation_config.cold_concurrent > 0
                            && self.cpu.fine_scheduling_enabled()
                            && item.likely_cold.load(Ordering::Relaxed)
                        {
                            let Some(cold_permit) = self.cpu.reserve_likely_cold(
                                &item.tid,
                                &item.registration.fqdn,
                                self.invocation_config.cold_concurrent,
                            ) else {
                                drop(permits);
                                pp.requeue_front(&item);
                                break;
                            };
                            permits.push(Box::new(cold_permit));
                        }

                        if !item.lock() {
                            drop(permits);
                            continue;
                        }

                        // Touchpoint 3: 1:1 attribution to `actual_domain` + dispatch telemetry.
                        let lifecycle = pp.begin_dispatch(
                            &item.tid,
                            &item.registration.fqdn,
                            actual_domain,
                            snapshot.as_ref(),
                            ranked_domains.first().copied(),
                        );
                        self.spawn_tokio_worker(
                            self.clone(),
                            item,
                            Box::new(permits),
                            actual_domain,
                            Some(lifecycle),
                        );
                    },
                    Ok(None) => {
                        pp.requeue_front(&item);
                        break;
                    },
                    Err(e) => {
                        error!(
                            tid = item.tid,
                            error = %e,
                            "CPU Resource Monitor `acquire_cores` returned an error!"
                        );
                        item.mark_error(&e.into());
                    },
                }
            }
            return;
        }

        // Standard path for non-pricing queues (`fcfs`, `minheap`, etc.):
        // pass `&[]` so `fineloadbalancing.lbpolicy` (`least_loaded`, `power_of_two`, etc.) selects the domain.
        while let Some(_peek_item) = self.queue.peek_queue() {
            let item = self.queue.pop_queue();
            loop {
                if let Some((actual_domain, permit)) = self.acquire_resources_to_run(&item, &[]).await {
                    if !item.lock() {
                        break;
                    }
                    self.spawn_tokio_worker(self.clone(), item, permit, actual_domain, None);
                    break;
                } else {
                    tokio::time::sleep(Duration::from_millis(self.invocation_config.queue_sleep_ms.max(1))).await;
                }
            }
        }
    }

    /// Return [true] if the item should bypass concurrency restrictions
    fn should_bypass(&self, reg: &Arc<RegisteredFunction>) -> bool {
        match self.invocation_config.bypass_duration_ms {
            Some(bypass_duration_ms) => {
                let exec_time = self.cmap.get_avg(&reg.fqdn, Chars::CpuExecTime);
                exec_time != 0.0 && exec_time < Duration::from_millis(bypass_duration_ms).as_secs_f64()
            },
            None => false,
        }
    }

    fn bypass_thread() -> (
        tokio::task::JoinHandle<()>,
        std::sync::mpsc::Sender<Arc<Self>>,
        UnboundedSender<Arc<EnqueuedInvocation>>,
    ) {
        let (tx, rx) = std::sync::mpsc::channel();
        let (del_tx, mut del_rx) = tokio::sync::mpsc::unbounded_channel::<Arc<EnqueuedInvocation>>();
        let handle = tokio_spawn_thread(async move {
            let tid: &TransactionId = &INVOKER_CPU_QUEUE_WORKER_TID;
            let service: Arc<Self> = match rx.recv() {
                Ok(cm) => cm,
                Err(e) => {
                    error!(tid=tid, error=%e, "Tokio service thread failed to receive service from channel!");
                    return;
                },
            };
            while let Some(item) = del_rx.recv().await {
                let s_c = service.clone();
                #[cfg(feature = "full_spans")]
                let span = item.span.clone();
                let td = async move {
                    match s_c.bypassing_invoke(&item).await {
                        Ok(true) => (), // bypass happened successfully
                        Ok(false) => {
                            if let Err(cause) = s_c.enqueue_item(&item) {
                                s_c.handle_invocation_error(item, cause);
                            };
                        },
                        Err(cause) => s_c.handle_invocation_error(item, cause),
                    };
                };
                #[cfg(feature = "full_spans")]
                let td = td.instrument(span);
                tokio_spawn_thread(td);
            }
        });

        (handle, tx, del_tx)
    }

    async fn acquire_resources_to_run(
        &self,
        item: &Arc<EnqueuedInvocation>,
        ranked_domains: &[DomainId],
    ) -> Option<(DomainId, Box<dyn Drop + Send>)> {
        debug!(tid = item.tid, "checking resources");
        #[cfg(feature = "power_cap")]
        if !self.energy.ok_run_fn(&self.cmap, &item.registration.fqdn) {
            debug!(tid = item.tid, "Blocking invocation due to power overload");
            return None;
        }
        let mut ret: Vec<Box<dyn Drop + Send>> = vec![];
        match self
            .cpu
            .acquire_cores(&item.registration, &item.tid, ranked_domains)
            .await
        {
            Ok(Some((actual_domain, permit))) => {
                ret.push(Box::new(permit));
                if self.invocation_config.cold_concurrent > 0
                    && self.cpu.fine_scheduling_enabled()
                    && item.likely_cold.load(Ordering::Relaxed)
                {
                    let Some(cold_permit) = self.cpu.reserve_likely_cold(
                        &item.tid,
                        &item.registration.fqdn,
                        self.invocation_config.cold_concurrent,
                    ) else {
                        warn!(
                            tid = item.tid,
                            fqdn = item.registration.fqdn,
                            limit = self.invocation_config.cold_concurrent,
                            "Throttling likely-cold invocation due to per-domain cold concurrency limit"
                        );
                        drop(ret);
                        return None;
                    };
                    ret.push(Box::new(cold_permit));
                }
                Some((actual_domain, Box::new(ret)))
            },
            Ok(None) => None,
            Err(_) => {
                error!(
                    tid = item.tid,
                    "CPU Resource Monitor `acquire_cores` returned an error!"
                );
                None
            },
        }
    }

    /// Runs the specific invocation inside a new tokio worker thread
    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, invoker_svc, item, permit, lifecycle), fields(tid=item.tid)))]
    fn spawn_tokio_worker(
        &self,
        invoker_svc: Arc<Self>,
        item: Arc<EnqueuedInvocation>,
        permit: Box<dyn Drop + Send>,
        actual_domain: DomainId,
        lifecycle: Option<PostedPriceInvocationLifecycle>,
    ) {
        #[cfg(feature = "full_spans")]
        {
            let span = item.span.clone();
            let _handle = tokio_spawn_thread(
                async move {
                    debug!(tid = item.tid, "Launching invocation thread for queued item");
                    invoker_svc
                        .invocation_worker_thread(item, permit, actual_domain, lifecycle)
                        .await;
                }
                .instrument(span),
            );
        }
        #[cfg(not(feature = "full_spans"))]
        {
            let _handle = tokio_spawn_thread(async move {
                debug!(tid = item.tid, "Launching invocation thread for queued item");
                invoker_svc
                    .invocation_worker_thread(item, permit, actual_domain, lifecycle)
                    .await;
            });
        }
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, item, permit, lifecycle), fields(tid=item.tid)))]
    async fn invocation_worker_thread(
        &self,
        item: Arc<EnqueuedInvocation>,
        permit: Box<dyn Drop + Send>,
        actual_domain: DomainId,
        lifecycle: Option<PostedPriceInvocationLifecycle>,
    ) {
        match self.invoke(&item, Some(permit), actual_domain, lifecycle).await {
            Ok((result, duration, compute, state, cpu_utilization)) => {
                item.mark_successful(result, duration, compute, state, cpu_utilization)
            },
            Err(cause) => self.handle_invocation_error(item, cause),
        };
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, item, cause), fields(tid=item.tid)))]
    fn handle_invocation_error(&self, item: Arc<EnqueuedInvocation>, cause: anyhow::Error) {
        if let Some(_mem_err) = cause.downcast_ref::<InsufficientMemoryError>() {
            let mut warn_time = self.last_memory_warning.lock();
            if warn_time.elapsed() > Duration::from_millis(500) {
                warn!(tid = item.tid, "Insufficient memory to run item right now");
                *warn_time = now();
            }
            item.unlock();
            match self.queue.add_item_to_queue(&item, Some(0)) {
                Ok(_) => self.signal.notify_one(),
                Err(e) => {
                    error!(tid=item.tid, error=%e, "Failed to re-queue item in CPU queue after memory exhaustion");
                    item.mark_error(&e);
                },
            };
        } else if let Some(_mem_err) = cause.downcast_ref::<InsufficientGPUError>() {
            warn!(tid = item.tid, "No GPU available to run item right now");
            item.unlock();
        } else {
            error!(tid=item.tid, error=%cause, "Encountered unknown error while trying to run queued invocation");
            if item.increment_error_retry(&cause, self.invocation_config.retries) {
                match self.queue.add_item_to_queue(&item, Some(0)) {
                    Ok(_) => self.signal.notify_one(),
                    Err(e) => {
                        error!(tid=item.tid, error=%e, "Failed to re-queue item after attempt");
                        item.mark_error(&e);
                    },
                };
            }
        }
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, item), fields(tid=item.tid)))]
    async fn bypassing_invoke(&self, item: &Arc<EnqueuedInvocation>) -> Result<bool> {
        info!(tid = item.tid, "Bypassing internal invocation starting");
        if self.cpu.fine_scheduling_enabled() {
            return Ok(false);
        }
        let remove_time = self.clock.now();
        let ctr_lock = match self
            .cont_manager
            .acquire_container(&item.registration, &item.tid, Compute::CPU)
        {
            EventualItem::Future(_) => return Ok(false),
            EventualItem::Now(n) => n?,
        };
        match invoke_on_container(
            &item.registration,
            &item.json_args,
            &item.tid,
            remove_time,
            item.est_completion_time,
            item.insert_time_load,
            &ctr_lock,
            self.clock.format_time(remove_time)?,
            now(),
            &self.cmap,
            &self.clock,
            &self.device_tput,
        )
        .await
        {
            Ok((result, duration, compute, state, cpu_utilization)) => {
                item.mark_successful(result, duration, compute, state, cpu_utilization)
            },
            Err(e) => self.handle_invocation_error(item.clone(), e),
        };
        Ok(true)
    }

    /// Acquires a container on `actual_domain`, updates `container.last_domain` and the BPF
    /// cgroup assignment map with `(actual_domain, s_i, w_0)`, executes the function, and
    /// releases lifecycle guards + core permits cleanly on all exit paths.
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item, permit, lifecycle), fields(tid=item.tid)))]
    async fn invoke<'a>(
        &'a self,
        item: &'a Arc<EnqueuedInvocation>,
        permit: Option<Box<dyn Drop + Send>>,
        actual_domain: DomainId,
        mut lifecycle: Option<PostedPriceInvocationLifecycle>,
    ) -> Result<(ParsedResult, Duration, Compute, ContainerState, Utilization)> {
        debug!(tid = item.tid, "Internal invocation starting");
        let (pricing_queue_ms, timing_started_at) = self
            .posted_price
            .as_ref()
            .map(|pp| pp.take_invocation_timing(&item.tid))
            .unwrap_or_else(|| (0.0, std::time::Instant::now()));
        let remove_time = self.clock.now_str()?;

        let start = now();
        let container_acquire_started_at = std::time::Instant::now();
        let acquire_result = match self.cont_manager.acquire_container_for_domain(
            &item.registration,
            &item.tid,
            Compute::CPU,
            Some(actual_domain),
        ) {
            EventualItem::Future(f) => {
                // Ensure `on_boot_start` is active if an invocation unexpectedly cold-starts.
                if let (Some(pp), Some(lc)) = (&self.posted_price, lifecycle.as_mut()) {
                    if lc.boot_guard.is_none() {
                        let fp = lc.dispatch_guard.footprint_used();
                        lc.boot_guard = Some(pp.start_boot_guard_with_footprint(
                            actual_domain,
                            &item.registration.fqdn,
                            fp,
                        ));
                    }
                }
                self.cold_starting.fetch_add(1, Ordering::Relaxed);
                let result = f.await;
                self.cold_starting.fetch_sub(1, Ordering::Relaxed);
                if let Some(lc) = lifecycle.as_mut() {
                    if let Some(mut bg) = lc.boot_guard.take() {
                        bg.finish_now();
                    }
                }
                result
            },
            EventualItem::Now(n) => {
                // Warm hit: end any speculative boot reservation immediately.
                if let Some(lc) = lifecycle.as_mut() {
                    if let Some(mut bg) = lc.boot_guard.take() {
                        bg.finish_now();
                    }
                }
                n
            },
        };
        let ctr_lock = match acquire_result {
            Ok(lock) => lock,
            Err(error) => {
                drop(lifecycle);
                drop(permit);
                self.signal.notify_one();
                return Err(error);
            },
        };

        // Touchpoint 3: Update container's app-level `last_domain` and BPF cgroup assignment map.
        ctr_lock.container.set_last_domain(Some(actual_domain));
        let (s_i_ms, w_0) = lifecycle
            .as_ref()
            .map(|lc| (Some(lc.s_i_ms), Some(lc.w_0)))
            .unwrap_or((None, None));
        if let Err(error) = self.cpu.cgroup_assigned_to_function(
            ctr_lock.container.cgroup_id(),
            &item.tid,
            item.registration.clone(),
            Some(actual_domain),
            s_i_ms,
            w_0,
        ) {
            drop(ctr_lock);
            drop(lifecycle);
            drop(permit);
            self.signal.notify_one();
            return Err(error);
        }

        let container_acquire_ms = container_acquire_started_at.elapsed().as_secs_f64() * 1000.0;
        self.running.fetch_add(1, Ordering::Relaxed);
        let result = invoke_on_container(
            &item.registration,
            &item.json_args,
            &item.tid,
            item.queue_insert_time,
            item.est_completion_time,
            item.insert_time_load,
            &ctr_lock,
            remove_time,
            start,
            &self.cmap,
            &self.clock,
            &self.device_tput,
        )
        .await;
        let exec_ms = result
            .as_ref()
            .ok()
            .map(|(data, _, _, _, _)| data.duration_sec * 1000.0);
        self.running.fetch_sub(1, Ordering::Relaxed);
        self.cpu
            .cgroup_released_for_function(ctr_lock.container.cgroup_id(), &item.tid, item.registration.clone());

        let exec_end_rho = self
            .posted_price
            .as_ref()
            .map(|pp| pp.predict_domain(actual_domain).effective_rho)
            .unwrap_or(0.0);
        let dispatch_rho = lifecycle.as_ref().map(|lc| lc.dispatch_rho).unwrap_or(0.0);
        if let Some(mut lc) = lifecycle.take() {
            lc.dispatch_guard.complete_now();
        }
        if let Some(pp) = &self.posted_price {
            if let Ok((_, _, _, _, cpu_util_pct)) = &result {
                if *cpu_util_pct > 0
                    || ctr_lock.container.container_type() == iluvatar_library::types::Isolation::DOCKER
                {
                    let cpu_util_cores = (*cpu_util_pct as f64 / 100.0)
                        .clamp(0.05, pp.domain_cores(actual_domain) as f64);
                    pp.on_util_sample(
                        actual_domain,
                        &item.registration.fqdn,
                        cpu_util_cores,
                        dispatch_rho.max(exec_end_rho),
                    );
                }
            }
            let wall_exec_ms = now().duration_since(start).as_secs_f64() * 1000.0;
            let is_cold = matches!(result, Ok((_, _, _, ContainerState::Cold, _)));
            pp.record_execution_observation(
                actual_domain,
                &item.registration.fqdn,
                wall_exec_ms,
                is_cold,
                exec_ms,
            );
        }

        // Return container to warm pool before releasing CPU permit and waking monitor_queue.
        drop(ctr_lock);
        drop(permit);
        self.signal.notify_one();

        if let (Some(pp), Some(exec_ms)) = (&self.posted_price, exec_ms) {
            let total_e2e_ms = timing_started_at.elapsed().as_secs_f64() * 1000.0;
            let post_overhead_ms = total_e2e_ms - pricing_queue_ms - container_acquire_ms - exec_ms;
            pp.record_invocation_timing(
                &item.tid,
                &item.registration.fqdn,
                CongestionPricingTimingSample {
                    pricing_queue_ms,
                    container_acquire_ms,
                    exec_ms,
                    post_overhead_ms,
                },
            );
        }
        result
    }

    fn get_est_completion_time_from_containers(&self, item: &Arc<RegisteredFunction>) -> (f64, ContainerState) {
        let avail = self.cont_manager.container_available(&item.fqdn, Compute::CPU);
        let t = match avail {
            ContainerState::Warm => self.cmap.get_avg(&item.fqdn, Chars::CpuWarmTime),
            ContainerState::Prewarm => self.cmap.get_avg(&item.fqdn, Chars::CpuPreWarmTime),
            _ => self.cmap.get_avg(&item.fqdn, Chars::CpuColdTime),
        };
        (t, avail)
    }
}

impl DeviceQueue for CpuQueueingInvoker {
    fn queue_len(&self) -> usize {
        self.queue.queue_len()
    }
    fn queue_load(&self) -> QueueLoad {
        let load = self.queue.est_queue_time();
        QueueLoad {
            len: self.queue.queue_len(),
            load,
            load_avg: load / self.cpu.cores,
            tput: self.device_tput.get_tput(),
        }
    }
    fn est_completion_time(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> (f64, f64) {
        let qt = if self.queue_len() <= self.cpu.available_cores() {
            0.0
        } else {
            self.queue.est_queue_time() / f64::min(self.cpu.cores, self.queue_len() as f64)
        };
        let (runtime, state) = self.get_est_completion_time_from_containers(reg);
        debug!(tid=tid, queue_time=qt, state=?state, runtime=runtime, "CPU estimated completion time of item");
        (qt + runtime, 0.0)
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, item), fields(tid=item.tid)))]
    fn enqueue_item(&self, item: &Arc<EnqueuedInvocation>) -> Result<()> {
        if self.should_bypass(&item.registration) {
            debug!(tid = item.tid, "CPU queue bypass");
            self.bypass_rx.send(item.clone())?;
            return Ok(());
        }
        debug!(tid = item.tid, "CPU queue item");
        self.queue.add_item_to_queue(item, None)?;
        self.signal.notify_one();
        Ok(())
    }

    fn running(&self) -> u32 {
        self.running.load(Ordering::Relaxed)
    }

    fn cold_starting(&self) -> u32 {
        self.cold_starting.load(Ordering::Relaxed)
    }

    fn warm_hit_probability(&self, reg: &Arc<RegisteredFunction>, _iat: f64) -> f64 {
        let fqdn = &reg.fqdn;
        let cstate = self.cont_manager.container_exists(fqdn, Compute::CPU);
        match cstate {
            ContainerState::Cold => 0.01,
            _ => 1.0 - 0.01,
        }
    }

    fn queue_tput(&self) -> f64 {
        self.device_tput.get_tput()
    }
}
