use super::queueing::{
    avail_scale::AvailableScalingQueue, cold_priority::ColdPriorityQueue, fcfs::FCFSQueue, minheap::MinHeapQueue,
    minheap_ed::MinHeapEDQueue, minheap_iat::MinHeapIATQueue, queueless::Queueless,
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
use crate::worker_api::worker_config::InvocationConfig;
use anyhow::Result;
use iluvatar_library::char_map::{Chars, WorkerCharMap};
use iluvatar_library::clock::{get_global_clock, now, Clock};
use iluvatar_library::tput_calc::DeviceTput;
use iluvatar_library::{
    threading::tokio_waiter_thread, threading::EventualItem, transaction::TransactionId, types::Compute,
};
use parking_lot::Mutex;
use std::{
    sync::{atomic::AtomicU32, Arc},
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
    cont_manager: Arc<ContainerManager>,
    invocation_config: Arc<InvocationConfig>,
    cmap: WorkerCharMap,
    clock: Clock,
    running: AtomicU32,
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

        let svc = Arc::new(CpuQueueingInvoker {
            queue: Self::get_invoker_queue(&invocation_config, &cmap, &cont_manager, tid)?,
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
            last_memory_warning: Mutex::new(now()),
            device_tput: DeviceTput::boxed(),
        });
        cpu_tx.send(svc.clone())?;
        bypass_tx.send(svc.clone())?;
        debug!(tid = tid, "Created CpuQueueingInvoker");
        Ok(svc)
    }

    fn get_invoker_queue(
        invocation_config: &Arc<InvocationConfig>,
        cmap: &WorkerCharMap,
        cont_manager: &Arc<ContainerManager>,
        tid: &TransactionId,
    ) -> Result<Arc<dyn InvokerCpuQueuePolicy>> {
        if let Some(pol) = invocation_config.queue_policies.get(&Compute::CPU) {
            Ok(match pol.as_str() {
                "none" => Queueless::new()?,
                "fcfs" => FCFSQueue::new(cont_manager.clone(), cmap.clone())?,
                "minheap" => MinHeapQueue::new(tid, cmap.clone(), cont_manager.clone())?,
                "minheap_ed" => MinHeapEDQueue::new(tid, cmap.clone(), cont_manager.clone())?,
                "minheap_iat" => MinHeapIATQueue::new(tid, cmap.clone(), cont_manager.clone())?,
                "cold_pri" => ColdPriorityQueue::new(cont_manager.clone(), tid, cmap.clone())?,
                "scaling" => AvailableScalingQueue::new(cont_manager.clone(), tid, cmap.clone())?,
                unknown => anyhow::bail!("Unknown CPU queueing policy '{}'", unknown),
            })
        } else {
            anyhow::bail!("No queue policy listed for compute '{:?}'", Compute::CPU)
        }
    }

    /// Wait on the Notify object for the queue to be available again
    async fn cpu_wait_on_queue(self: &Arc<Self>, tid: &TransactionId) {
        self.signal.notified().await;
        debug!(tid = tid, "Invoker waken up by signal");
    }

    /// Check the invocation queue, running things when there are sufficient resources
    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, _tid), fields(tid=_tid)))]
    async fn monitor_queue(self: &Arc<Self>, _tid: &TransactionId) {
        while let Some(peek_item) = self.queue.peek_queue() {
            if let Some(permit) = self.acquire_resources_to_run(&peek_item) {
                let item = self.queue.pop_queue();
                if !item.lock() {
                    continue;
                }
                self.spawn_tokio_worker(self.clone(), item, permit);
            } else {
                debug!(tid = peek_item.tid, "Insufficient resources to run item");
                break;
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
        let handle = tokio::spawn(async move {
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
                tokio::task::spawn(td);
            }
        });

        (handle, tx, del_tx)
    }

    /// Returns an owned permit if there are sufficient resources to run a function
    /// A return value of [None] means the resources failed to be acquired
    fn acquire_resources_to_run(&self, item: &Arc<EnqueuedInvocation>) -> Option<Box<dyn Drop + Send>> {
        debug!(tid = item.tid, "checking resources");
        #[cfg(feature = "power_cap")]
        if !self.energy.ok_run_fn(&self.cmap, &item.registration.fqdn) {
            debug!(tid = item.tid, "Blocking invocation due to power overload");
            return None;
        }
        let mut ret = vec![];
        match self.cpu.try_acquire_cores(&item.registration, &item.tid) {
            Ok(c) => ret.push(c),
            Err(e) => {
                match e {
                    tokio::sync::TryAcquireError::Closed => {
                        error!(
                            tid = item.tid,
                            "CPU Resource Monitor `try_acquire_cores` returned a closed error!"
                        )
                    },
                    tokio::sync::TryAcquireError::NoPermits => (),
                };
                return None;
            },
        };
        Some(Box::new(ret))
    }

    /// Runs the specific invocation inside a new tokio worker thread
    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, invoker_svc, item, permit), fields(tid=item.tid)))]
    fn spawn_tokio_worker(&self, invoker_svc: Arc<Self>, item: Arc<EnqueuedInvocation>, permit: Box<dyn Drop + Send>) {
        #[cfg(feature = "full_spans")]
        {
            let span = item.span.clone();
            let _handle = tokio::spawn(
                async move {
                    debug!(tid = item.tid, "Launching invocation thread for queued item");
                    invoker_svc.invocation_worker_thread(item, permit).await;
                }
                .instrument(span),
            );
        }
        #[cfg(not(feature = "full_spans"))]
        {
            let _handle = tokio::spawn(async move {
                debug!(tid = item.tid, "Launching invocation thread for queued item");
                invoker_svc.invocation_worker_thread(item, permit).await;
            });
        }
    }

    /// Handle executing an invocation, plus account for its success or failure
    /// On success, the results are moved to the pointer and it is signaled
    /// On failure, [Invoker::handle_invocation_error] is called
    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, item, permit), fields(tid=item.tid)))]
    async fn invocation_worker_thread(&self, item: Arc<EnqueuedInvocation>, permit: Box<dyn Drop + Send>) {
        match self.invoke(&item, Some(permit)).await {
            Ok((result, duration, compute, state)) => item.mark_successful(result, duration, compute, state),
            Err(cause) => self.handle_invocation_error(item, cause),
        };
    }

    /// Handle an error with the given enqueued invocation
    /// By default re-enters item if a resource exhaustion error occurs [InsufficientMemoryError]
    ///   Calls [Self::add_item_to_queue] to do this
    /// Other errors result in exit of invocation if [InvocationConfig.attempts] are made
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
                Ok(_) => self.signal.notify_waiters(),
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
                    Ok(_) => self.signal.notify_waiters(),
                    Err(e) => {
                        error!(tid=item.tid, error=%e, "Failed to re-queue item after attempt");
                        item.mark_error(&e);
                    },
                };
            }
        }
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, item), fields(tid=item.tid)))]
    /// Run an invocation, bypassing any concurrency restrictions
    /// A return value of `false` means that the function would have run cold, and the caller should enqueue it instead
    /// `true` means the invocation was already run successfully
    async fn bypassing_invoke(&self, item: &Arc<EnqueuedInvocation>) -> Result<bool> {
        info!(tid = item.tid, "Bypassing internal invocation starting");
        // take run time now because we may have to wait to get a container
        let remove_time = self.clock.now();
        let ctr_lock = match self
            .cont_manager
            .acquire_container(&item.registration, &item.tid, Compute::CPU)
        {
            EventualItem::Future(_) => return Ok(false), // no bypass
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
            Ok((result, duration, compute, state)) => item.mark_successful(result, duration, compute, state),
            Err(e) => self.handle_invocation_error(item.clone(), e),
        };
        Ok(true)
    }

    /// acquires a container and invokes the function inside it
    /// returns the json result and duration as a tuple
    /// The optional [permit] is dropped to return held resources
    /// Returns
    /// [ParsedResult] A result representing the function output, the user result plus some platform tracking
    /// [Duration]: The E2E latency between the worker and the container
    /// [Compute]: Compute the invocation was run on
    /// [ContainerState]: State the container was in for the invocation
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item, permit), fields(tid=item.tid)))]
    async fn invoke<'a>(
        &'a self,
        item: &'a Arc<EnqueuedInvocation>,
        permit: Option<Box<dyn Drop + Send>>,
    ) -> Result<(ParsedResult, Duration, Compute, ContainerState)> {
        debug!(tid = item.tid, "Internal invocation starting");
        // take run time now because we may have to wait to get a container
        let remove_time = self.clock.now_str()?;

        let start = now();
        let ctr_lock = match self
            .cont_manager
            .acquire_container(&item.registration, &item.tid, Compute::CPU)
        {
            EventualItem::Future(f) => f.await?,
            EventualItem::Now(n) => n?,
        };
        self.running.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (data, duration, compute_type, state) = invoke_on_container(
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
        .await?;
        self.running.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        drop(permit);
        self.signal.notify_waiters();
        Ok((data, duration, compute_type, state))
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
            // If Q is smaller than num of avail CPUs, we don't really have queuing,
            // just a race from item being added recently and not popped
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
        self.signal.notify_waiters();
        Ok(())
    }

    fn running(&self) -> u32 {
        self.running.load(std::sync::atomic::Ordering::Relaxed)
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
