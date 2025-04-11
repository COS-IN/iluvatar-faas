use super::{
    completion_time_tracker::CompletionTimeTracker,
    queueing::{
        fcfs_gpu::FcfsGpuQueue, oldest_gpu::BatchGpuQueue, sized_batches_gpu::SizedBatchGpuQueue, DeviceQueue,
        EnqueuedInvocation, MinHeapEnqueuedInvocation, MinHeapFloat,
    },
    QueueLoad,
};
use crate::services::invocation::queueing::eedf_gpu::EedfGpuQueue;
use crate::services::invocation::queueing::paella::PaellaGpuQueue;
use crate::services::invocation::queueing::sjf_gpu::SjfGpuQueue;
use crate::services::resources::{cpu::CpuResourceTracker, gpu::GpuResourceTracker};
use crate::services::{
    containers::{
        containermanager::ContainerManager,
        structs::{ContainerState, InsufficientGPUError, InsufficientMemoryError, ParsedResult},
    },
    invocation::invoke_on_container,
};
use crate::{
    services::{containers::structs::ContainerLock, registration::RegisteredFunction},
    worker_api::worker_config::{GPUResourceConfig, InvocationConfig},
};
use anyhow::Result;
use iluvatar_library::char_map::{Chars, WorkerCharMap};
use iluvatar_library::clock::{get_global_clock, now, Clock};
use iluvatar_library::tput_calc::DeviceTput;
use iluvatar_library::types::{Compute, DroppableToken};
use iluvatar_library::{threading::tokio_waiter_thread, threading::EventualItem, transaction::TransactionId};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::{
    sync::{atomic::AtomicU32, Arc},
    time::Duration,
};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::Instant;
#[cfg(feature = "full_spans")]
use tracing::Instrument;
use tracing::{debug, error, info, warn};

lazy_static::lazy_static! {
  pub static ref INVOKER_GPU_QUEUE_WORKER_TID: TransactionId = "InvokerGPUQueue".to_string();
}

/// A batch of GPU-based invocations, all of the same function
/// A batch will have at least one invocation in it
/// They are stored in FIFO order
pub struct GpuBatch {
    data: VecDeque<MinHeapFloat>,
    est_time: f64,
}
impl GpuBatch {
    pub fn new(first_item: Arc<EnqueuedInvocation>, est_wall_time: f64) -> Self {
        GpuBatch {
            data: VecDeque::from([MinHeapEnqueuedInvocation::new_f(
                first_item,
                est_wall_time,
                est_wall_time,
            )]),
            est_time: est_wall_time,
        }
    }

    pub fn add(&mut self, item: Arc<EnqueuedInvocation>, est_wall_time: f64) {
        self.est_time += est_wall_time;
        self.data
            .push_back(MinHeapEnqueuedInvocation::new_f(item, est_wall_time, est_wall_time));
    }

    /// The registration for the items in the batch
    pub fn item_registration(&self) -> &Arc<RegisteredFunction> {
        &self.data.front().unwrap().item.registration
    }

    pub fn est_queue_time(&self) -> f64 {
        self.est_time
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn peek(&self) -> &Arc<EnqueuedInvocation> {
        &self.data.front().unwrap().item
    }
}

impl Iterator for GpuBatch {
    type Item = Arc<EnqueuedInvocation>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.data.pop_front() {
            Some(s) => Some(s.item),
            None => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.data.len();
        (len, Some(len))
    }
}

/// A trait representing the functionality a queue policy must implement
pub trait GpuQueuePolicy: Send + Sync {
    /// The total number of items in the queue
    fn queue_len(&self) -> usize;

    /// The estimated time of running everything in the queue
    /// In seconds
    fn est_queue_time(&self) -> f64;

    /// A peek at the first item in the queue.
    /// Returns the [RegisteredFunction] information of the first thing in the queue, if there is anything in the queue, [None] otherwise.
    fn next_batch(&self) -> Option<Arc<RegisteredFunction>>;

    /// Destructively return the first batch in the queue.
    /// This function will only be called if something is known to be un the queue, so using `unwrap` to remove an [Option] is safe
    fn pop_queue(&self) -> Option<GpuBatch>;

    /// Insert an item into the queue
    /// If an error is returned, the item was not put enqueued
    fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>) -> Result<()>;
}

pub struct GpuQueueingInvoker {
    cont_manager: Arc<ContainerManager>,
    invocation_config: Arc<InvocationConfig>,
    cmap: WorkerCharMap,
    clock: Clock,
    running: AtomicU32,
    last_memory_warning: Mutex<Instant>,
    last_gpu_warning: Mutex<Instant>,
    cpu: Arc<CpuResourceTracker>,
    _gpu_thread: JoinHandle<()>,
    gpu: Arc<GpuResourceTracker>,
    signal: Notify,
    queue: Arc<dyn GpuQueuePolicy>,
    /// Track completion time here because the limited number of GPUs and inability to overcommit
    /// means we need to know roughly when one will become available to better predict completion time for incoming invocations
    completion_tracker: Arc<CompletionTimeTracker>,
    gpu_config: Arc<GPUResourceConfig>,
    device_tput: Arc<DeviceTput>,
}

#[allow(dyn_drop)]
/// An invoker implementation that enqueues invocations and orders them based on a variety of characteristics
/// Queueing method is configurable
impl GpuQueueingInvoker {
    pub fn new(
        cont_manager: Arc<ContainerManager>,
        invocation_config: Arc<InvocationConfig>,
        tid: &TransactionId,
        cmap: WorkerCharMap,
        cpu: Arc<CpuResourceTracker>,
        gpu: Option<Arc<GpuResourceTracker>>,
        gpu_config: &Option<Arc<GPUResourceConfig>>,
    ) -> Result<Arc<Self>> {
        let (gpu_handle, gpu_tx) = tokio_waiter_thread(
            invocation_config.queue_sleep_ms,
            INVOKER_GPU_QUEUE_WORKER_TID.clone(),
            Self::monitor_queue,
            Some(Self::gpu_wait_on_queue),
        );

        let q = Self::get_invoker_gpu_queue(&invocation_config, &cmap, &cont_manager, tid);
        let svc = Arc::new(GpuQueueingInvoker {
            cont_manager,
            invocation_config,
            gpu: gpu.ok_or_else(|| anyhow::format_err!("Creating GPU queue with no GPU resources"))?,
            cmap,
            cpu,
            signal: Notify::new(),
            _gpu_thread: gpu_handle,
            clock: get_global_clock(tid)?,
            running: AtomicU32::new(0),
            last_memory_warning: Mutex::new(now()),
            queue: q?,
            last_gpu_warning: Mutex::new(now()),
            completion_tracker: Arc::new(CompletionTimeTracker::new(tid)?),
            gpu_config: gpu_config
                .as_ref()
                .ok_or_else(|| anyhow::format_err!("Creating GPU queue with no GPU config"))?
                .clone(),
            device_tput: DeviceTput::boxed(),
        });
        gpu_tx.send(svc.clone())?;
        info!(tid = tid, "Created GpuQueueingInvoker");
        Ok(svc)
    }

    /// Create the GPU queue to use
    fn get_invoker_gpu_queue(
        invocation_config: &Arc<InvocationConfig>,
        cmap: &WorkerCharMap,
        cont_manager: &Arc<ContainerManager>,
        _tid: &TransactionId,
    ) -> Result<Arc<dyn GpuQueuePolicy>> {
        if let Some(pol) = invocation_config.queue_policies.get(&Compute::GPU) {
            Ok(match pol.as_str() {
                "fcfs" => FcfsGpuQueue::new(cont_manager.clone(), cmap.clone())?,
                "sjf" => SjfGpuQueue::new(cont_manager.clone(), cmap.clone())?,
                "eedf" => EedfGpuQueue::new(cont_manager.clone(), cmap.clone())?,
                "oldest_batch" => BatchGpuQueue::new(cmap.clone())?,
                "sized_batch" => SizedBatchGpuQueue::new(cmap.clone())?,
                "paella" => PaellaGpuQueue::new(cmap.clone())?,
                unknown => anyhow::bail!("Unknown GPU queueing policy '{}'", unknown),
            })
        } else {
            anyhow::bail!("No queue policy listed for compute '{:?}'", Compute::GPU)
        }
    }

    async fn gpu_wait_on_queue(self: &Arc<Self>, tid: &TransactionId) {
        self.signal.notified().await;
        debug!(tid = tid, "Invoker waken up by signal");
    }
    /// Check the invocation queue, running things when there are sufficient resources
    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self), fields(tid=tid)))]
    async fn monitor_queue(self: &Arc<Self>, tid: &TransactionId) {
        while let Some(peek_reg) = self.queue.next_batch() {
            // This async function the only place which decrements running set and resources avail. Implicit assumption that it wont be concurrently invoked.
            if let Some(permit) = self.acquire_resources_to_run(&peek_reg, tid) {
                let b = self.queue.pop_queue();
                match b {
                    None => break,
                    Some(batch) => self.spawn_tokio_worker(self.clone(), batch, permit, tid),
                }
            } else {
                debug!(tid=tid, fqdn=%peek_reg.fqdn, "Insufficient resources to run item");
                break;
            }
        }
    }

    /// Returns an owned permit if there are sufficient resources to run a function
    /// A return value of [None] means the resources failed to be acquired
    fn acquire_resources_to_run(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> Option<DroppableToken> {
        let mut ret: Vec<DroppableToken> = vec![];
        match self.cpu.try_acquire_cores(reg, tid) {
            Ok(Some(c)) => ret.push(Box::new(c)),
            Ok(_) => (),
            Err(e) => {
                match e {
                    tokio::sync::TryAcquireError::Closed => {
                        error!(
                            tid = tid,
                            "CPU Resource Monitor `try_acquire_cores` returned a closed error!"
                        )
                    },
                    tokio::sync::TryAcquireError::NoPermits => {
                        debug!(tid=tid, fqdn=%reg.fqdn, "Not enough CPU permits")
                    },
                };
                return None;
            },
        };
        match self.gpu.try_acquire_resource(None, tid) {
            Ok(c) => ret.push(c.into()),
            Err(e) => {
                match e {
                    tokio::sync::TryAcquireError::Closed => {
                        error!(
                            tid = tid,
                            "GPU Resource Monitor `try_acquire_cores` returned a closed error!"
                        )
                    },
                    tokio::sync::TryAcquireError::NoPermits => {
                        debug!(tid=tid, fqdn=%reg.fqdn, "Not enough GPU permits")
                    },
                };
                return None;
            },
        };
        Some(Box::new(ret))
    }

    /// Runs the specific invocation inside a new tokio worker thread
    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, invoker_svc, batch, permit), fields(tid=tid)))]
    fn spawn_tokio_worker(&self, invoker_svc: Arc<Self>, batch: GpuBatch, permit: DroppableToken, tid: &TransactionId) {
        debug!(tid = tid, "Launching invocation thread for queued item");
        tokio::spawn(async move {
            invoker_svc.invocation_worker_thread(batch, permit).await;
        });
    }

    /// Handle executing an invocation, plus account for its success or failure.
    /// On success, the results are moved to the pointer and it is signaled.
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, batch, permit), fields(fqdn=batch.peek().registration.fqdn)))]
    async fn invocation_worker_thread(&self, batch: GpuBatch, permit: DroppableToken) {
        let tid: &TransactionId = &INVOKER_GPU_QUEUE_WORKER_TID;
        info!(
            tid = tid,
            fqdn = batch.item_registration().fqdn,
            batch_len = batch.len(),
            "Executing batch"
        );
        let curr_time = self.clock.now();
        let est_finish_time = curr_time + time::Duration::seconds_f64(batch.est_queue_time());
        self.completion_tracker.add_item(est_finish_time);
        let mut ctr_lock = None;
        let mut start;
        for item in batch {
            if !item.lock() {
                continue;
            }
            start = now();
            if ctr_lock.is_none() {
                let (lck, was_cold) =
                    match self
                        .cont_manager
                        .acquire_container(&item.registration, &item.tid, Compute::GPU)
                    {
                        EventualItem::Future(f) => (f.await, true),
                        EventualItem::Now(n) => (n, false),
                    };
                match lck {
                    Ok(c) => ctr_lock = Some(c),
                    Err(e) => {
                        error!(tid=item.tid, error=%e, "Failed to get a container to run item");
                        self.handle_invocation_error(&item, &e);
                        continue;
                    },
                };
                if !was_cold && self.gpu_config.send_driver_memory_hints() {
                    // unwrap is safe, we either have a container or will go to the top of the loop
                    let ctr = ctr_lock.as_ref().unwrap().container.clone();
                    let t = item.tid.clone();
                    tokio::spawn(async move { ctr.move_from_device(&t).await });
                }
            }
            // unwrap is safe, we either have a container or will go to the top of the loop
            #[cfg(feature = "full_spans")]
            let fut = self
                .invoke(ctr_lock.as_ref().unwrap(), &item, start)
                .instrument(item.span.clone());
            #[cfg(not(feature = "full_spans"))]
            let fut = self.invoke(ctr_lock.as_ref().unwrap(), &item, start);
            match fut.await {
                Ok((result, duration, compute, container_state)) => {
                    item.mark_successful(result, duration, compute, container_state)
                },
                Err(cause) => {
                    error!(tid=item.tid, error=%cause, "Encountered unknown error while trying to run queued invocation");
                    ctr_lock.as_ref().unwrap().container.mark_unhealthy();
                    ctr_lock = None;
                    if item.increment_error_retry(&cause, self.invocation_config.retries) {
                        item.unlock();
                        match self.queue.add_item_to_queue(&item) {
                            Ok(_) => self.signal.notify_waiters(),
                            Err(e) => {
                                item.mark_error(&cause);
                                error!(tid=item.tid, error=%e, "Failed to re-queue item after attempt");
                            },
                        };
                    }
                    continue;
                },
            };
            // update the container state because the container manager can't do it for us
            ctr_lock.as_ref().unwrap().container.set_state(ContainerState::Warm);
        }
        if self.gpu_config.send_driver_memory_hints() {
            if let Some(ctr_lck) = &ctr_lock {
                let ctr = ctr_lck.container.clone();
                let t = INVOKER_GPU_QUEUE_WORKER_TID.clone();
                tokio::spawn(async move { ctr.move_from_device(&t).await });
            }
        }
        drop(ctr_lock);
        drop(permit);
        self.completion_tracker.remove_item(est_finish_time);
        self.signal.notify_waiters();
    }

    /// Handle an error with the given enqueued invocation
    /// By default re-enters item if a resource exhaustion error occurs [InsufficientMemoryError]
    ///   Calls [Self::add_item_to_queue] to do this
    /// Other errors result in exit of invocation if [InvocationConfig.attempts] are made
    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, item, cause), fields(tid=item.tid)))]
    fn handle_invocation_error(&self, item: &Arc<EnqueuedInvocation>, cause: &anyhow::Error) {
        if let Some(_mem_err) = cause.downcast_ref::<InsufficientMemoryError>() {
            let mut warn_time = self.last_memory_warning.lock();
            if warn_time.elapsed() > Duration::from_millis(500) {
                warn!(tid = item.tid, "Insufficient memory to run item right now");
                *warn_time = now();
            }
            item.unlock();
            match self.queue.add_item_to_queue(item) {
                Ok(_) => self.signal.notify_waiters(),
                Err(e) => {
                    error!(tid=item.tid, error=%e, "Failed to re-queue item in GPU queue after memory exhaustion");
                    item.mark_error(cause);
                },
            };
        } else if let Some(_gpu_err) = cause.downcast_ref::<InsufficientGPUError>() {
            let mut warn_time = self.last_gpu_warning.lock();
            if warn_time.elapsed() > Duration::from_millis(500) {
                warn!(tid = item.tid, "No GPU available to run item right now");
                *warn_time = now();
            }
            item.unlock();
            match self.queue.add_item_to_queue(item) {
                Ok(_) => self.signal.notify_waiters(),
                Err(e) => {
                    error!(tid=item.tid, error=%e, "Failed to re-queue item after GPU exhaustion");
                    item.mark_error(cause);
                },
            };
        } else {
            error!(tid=item.tid, error=%cause, "Encountered unknown error while trying to run queued invocation");
            item.mark_error(cause);
        }
    }

    /// acquires a container and invokes the function inside it
    /// returns the json result and duration as a tuple
    /// The optional [permit] is dropped to return held resources
    /// Returns
    /// [ParsedResult] A result representing the function output, the user result plus some platform tracking
    /// [Duration]: The E2E latency between the worker and the container
    /// [Compute]: Compute the invocation was run on
    /// [ContainerState]: State the container was in for the invocation
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item, ctr_lock, cold_time_start), fields(tid=item.tid)))]
    async fn invoke<'a>(
        &'a self,
        ctr_lock: &'a ContainerLock,
        item: &'a Arc<EnqueuedInvocation>,
        cold_time_start: Instant,
    ) -> Result<(ParsedResult, Duration, Compute, ContainerState)> {
        debug!(tid = item.tid, "Internal invocation starting");
        // take run time now because we may have to wait to get a container
        let remove_time = self.clock.now_str()?;
        self.running.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (data, duration, compute_type, state) = invoke_on_container(
            &item.registration,
            &item.json_args,
            &item.tid,
            item.queue_insert_time,
            item.est_completion_time,
            item.insert_time_load,
            ctr_lock,
            remove_time,
            cold_time_start,
            &self.cmap,
            &self.clock,
            &self.device_tput,
        )
        .await?;
        self.running.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        // self.signal.notify_waiters();
        Ok((data, duration, compute_type, state))
    }

    fn get_est_completion_time_from_containers_gpu(&self, item: &Arc<RegisteredFunction>) -> (f64, ContainerState) {
        let exists = self.cont_manager.container_exists(&item.fqdn, Compute::GPU);
        let t = match exists {
            ContainerState::Warm => self.cmap.get_avg(&item.fqdn, Chars::GpuWarmTime),
            ContainerState::Prewarm => self.cmap.get_avg(&item.fqdn, Chars::GpuPreWarmTime),
            _ => self.cmap.get_avg(&item.fqdn, Chars::GpuColdTime),
        };
        (t, exists)
    }
}

impl DeviceQueue for GpuQueueingInvoker {
    fn queue_len(&self) -> usize {
        self.queue.queue_len()
    }

    fn queue_load(&self) -> QueueLoad {
        let load = self.queue.est_queue_time();
        QueueLoad {
            len: self.queue.queue_len(),
            load,
            load_avg: load / self.gpu.max_concurrency() as f64,
            tput: self.device_tput.get_tput(),
        }
    }

    fn est_completion_time(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> (f64, f64) {
        let est_qt = self.queue.est_queue_time();
        let qt = est_qt / self.gpu.max_concurrency() as f64;
        let (runtime, state) = self.get_est_completion_time_from_containers_gpu(reg);
        debug!(tid=tid, qt=qt, state=?state, runtime=runtime, "GPU estimated completion time of item");
        (qt + runtime, est_qt)
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item), fields(tid=item.tid)))]
    fn enqueue_item(&self, item: &Arc<EnqueuedInvocation>) -> Result<()> {
        self.queue.add_item_to_queue(item)?;
        self.signal.notify_waiters();
        Ok(())
    }

    fn running(&self) -> u32 {
        self.running.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn warm_hit_probability(&self, _reg: &Arc<RegisteredFunction>, _iat: f64) -> f64 {
        0.5 //TODO!
    }

    fn queue_tput(&self) -> f64 {
        self.device_tput.get_tput()
    }
}

#[cfg(test)]
mod gpu_batch_tests {
    use super::*;

    fn item(clock: &Clock) -> Arc<EnqueuedInvocation> {
        let name = "test";
        let rf = Arc::new(RegisteredFunction {
            function_name: name.to_string(),
            function_version: name.to_string(),
            fqdn: name.to_string(),
            image_name: name.to_string(),
            memory: 1,
            cpus: 1,
            parallel_invokes: 1,
            ..Default::default()
        });
        Arc::new(EnqueuedInvocation::new(
            rf,
            name.to_string(),
            name.to_string(),
            clock.now(),
            0.0,
            0.0,
        ))
    }

    #[test]
    fn one_item_correct() {
        let clock = get_global_clock(&"clock".to_string()).unwrap();
        let b = GpuBatch::new(item(&clock), 1.0);
        assert_eq!(b.len(), 1);
        assert_eq!(b.est_queue_time(), 1.0);
    }

    #[test]
    fn added_items_correct() {
        let clock = get_global_clock(&"clock".to_string()).unwrap();
        let mut b = GpuBatch::new(item(&clock), 1.0);

        for _ in 0..3 {
            b.add(item(&clock), 1.5);
        }
        assert_eq!(b.len(), 4);
        assert_eq!(b.est_queue_time(), 5.5);
    }
}
