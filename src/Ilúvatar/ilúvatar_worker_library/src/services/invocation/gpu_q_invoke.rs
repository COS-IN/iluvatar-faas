use super::{
    completion_time_tracker::CompletionTimeTracker,
    queueing::{
        fcfs_gpu::FcfsGpuQueue, oldest_gpu::BatchGpuQueue, DeviceQueue, EnqueuedInvocation, MinHeapEnqueuedInvocation,
        MinHeapFloat,
    },
};
use crate::services::containers::{
    containermanager::ContainerManager,
    structs::{ContainerLock, ContainerState, InsufficientGPUError, InsufficientMemoryError, ParsedResult},
};
use crate::services::registration::RegisteredFunction;
use crate::services::resources::{cpu::CpuResourceTracker, gpu::GpuResourceTracker};
use crate::worker_api::worker_config::{FunctionLimits, InvocationConfig};
use anyhow::Result;
use iluvatar_library::characteristics_map::{Characteristics, CharacteristicsMap, Values};
use iluvatar_library::{
    logging::LocalTime, threading::tokio_runtime, threading::EventualItem, transaction::TransactionId, types::Compute,
};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::{
    sync::{atomic::AtomicU32, Arc},
    time::Duration,
};
use time::{Instant, OffsetDateTime};
use tokio::sync::Notify;
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
            data: VecDeque::from([
                MinHeapEnqueuedInvocation::new_f(first_item.clone(), est_wall_time, est_wall_time).into(),
            ]),
            est_time: est_wall_time,
        }
    }

    pub fn add(&mut self, item: Arc<EnqueuedInvocation>, est_wall_time: f64) {
        self.est_time += est_wall_time;
        self.data
            .push_back(MinHeapEnqueuedInvocation::new_f(item.clone(), est_wall_time, est_wall_time).into());
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

#[tonic::async_trait]
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
    fn pop_queue(&self) -> GpuBatch;

    /// Insert an item into the queue
    /// If an error is returned, the item was not put enqueued
    fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>) -> Result<()>;
}

pub struct GpuQueueingInvoker {
    cont_manager: Arc<ContainerManager>,
    invocation_config: Arc<InvocationConfig>,
    cmap: Arc<CharacteristicsMap>,
    clock: LocalTime,
    running: AtomicU32,
    last_memory_warning: Mutex<Instant>,
    cpu: Arc<CpuResourceTracker>,
    _gpu_thread: std::thread::JoinHandle<()>,
    gpu: Arc<GpuResourceTracker>,
    signal: Notify,
    queue: Arc<dyn GpuQueuePolicy>,
    /// Track completion time here because the limited number of GPUs and inability to overcommit
    /// means we need to know roughly when one will become available to better predict completion time for incoming invocations
    completion_tracker: CompletionTimeTracker,
}

#[allow(dyn_drop)]
/// An invoker implementation that enqueues invocations and orders them based on a variety of characteristics
/// Queueing method is configurable
impl GpuQueueingInvoker {
    pub fn new(
        cont_manager: Arc<ContainerManager>,
        function_config: Arc<FunctionLimits>,
        invocation_config: Arc<InvocationConfig>,
        tid: &TransactionId,
        cmap: Arc<CharacteristicsMap>,
        cpu: Arc<CpuResourceTracker>,
        gpu: Arc<GpuResourceTracker>,
    ) -> Result<Arc<Self>> {
        let (gpu_handle, gpu_tx) = tokio_runtime(
            invocation_config.queue_sleep_ms,
            INVOKER_GPU_QUEUE_WORKER_TID.clone(),
            Self::monitor_queue,
            Some(Self::gpu_wait_on_queue),
            Some(function_config.cpu_max as usize),
        )?;
        let svc = Arc::new(GpuQueueingInvoker {
            queue: Self::get_invoker_gpu_queue(&invocation_config, &cmap, &cont_manager, tid)?,
            cont_manager,
            invocation_config,
            gpu,
            cmap,
            cpu,
            signal: Notify::new(),
            _gpu_thread: gpu_handle,
            clock: LocalTime::new(tid)?,
            running: AtomicU32::new(0),
            last_memory_warning: Mutex::new(Instant::now()),
            completion_tracker: CompletionTimeTracker::new(),
        });
        gpu_tx.send(svc.clone())?;
        debug!(tid=%tid, "Created GpuQueueingInvoker");
        Ok(svc)
    }

    /// Create the GPU queue to use
    fn get_invoker_gpu_queue(
        invocation_config: &Arc<InvocationConfig>,
        cmap: &Arc<CharacteristicsMap>,
        cont_manager: &Arc<ContainerManager>,
        _tid: &TransactionId,
    ) -> Result<Arc<dyn GpuQueuePolicy>> {
        if let Some(pol) = invocation_config.queue_policies.get(&(&Compute::GPU).try_into()?) {
            Ok(match pol.as_str() {
                "fcfs" => FcfsGpuQueue::new(cont_manager.clone(), cmap.clone())?,
                "oldest_batch" => BatchGpuQueue::new(cmap.clone())?,
                unknown => anyhow::bail!("Unknown queueing policy '{}'", unknown),
            })
        } else {
            anyhow::bail!("No queue policy listed for compute '{:?}'", Compute::GPU)
        }
    }

    async fn gpu_wait_on_queue(invoker_svc: Arc<GpuQueueingInvoker>, tid: TransactionId) {
        invoker_svc.signal.notified().await;
        debug!(tid=%tid, "Invoker waken up by signal");
    }
    /// Check the invocation queue, running things when there are sufficient resources
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self), fields(tid=%tid)))]
    async fn monitor_queue(self: Arc<Self>, tid: TransactionId) {
        loop {
            if let Some(peek_reg) = self.queue.next_batch() {
                if let Some(permit) = self.acquire_resources_to_run(&peek_reg, &tid) {
                    let batch = self.queue.pop_queue();
                    self.spawn_tokio_worker(self.clone(), batch, permit, &tid);
                } else {
                    debug!(tid=%tid, fqdn=%peek_reg.fqdn, "Insufficient resources to run item");
                    break;
                }
            } else {
                // nothing can be run, or nothing to run
                break;
            }
        }
    }

    /// Returns an owned permit if there are sufficient resources to run a function
    /// A return value of [None] means the resources failed to be acquired
    fn acquire_resources_to_run(
        &self,
        reg: &Arc<RegisteredFunction>,
        tid: &TransactionId,
    ) -> Option<Box<dyn Drop + Send>> {
        let mut ret = vec![];
        match self.cpu.try_acquire_cores(&reg, &tid) {
            Ok(c) => ret.push(c),
            Err(e) => {
                match e {
                    tokio::sync::TryAcquireError::Closed => {
                        error!(tid=%tid, "CPU Resource Monitor `try_acquire_cores` returned a closed error!")
                    }
                    tokio::sync::TryAcquireError::NoPermits => {
                        debug!(tid=%tid, fqdn=%reg.fqdn, "Not enough CPU permits")
                    }
                };
                return None;
            }
        };
        match self.gpu.try_acquire_resource() {
            Ok(c) => ret.push(Some(c)),
            Err(e) => {
                match e {
                    tokio::sync::TryAcquireError::Closed => {
                        error!(tid=%tid, "GPU Resource Monitor `try_acquire_cores` returned a closed error!")
                    }
                    tokio::sync::TryAcquireError::NoPermits => {
                        debug!(tid=%tid, fqdn=%reg.fqdn, "Not enough GPU permits")
                    }
                };
                return None;
            }
        };
        Some(Box::new(ret))
    }

    /// Runs the specific invocation inside a new tokio worker thread
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, invoker_svc, batch, permit), fields(tid=%tid)))]
    fn spawn_tokio_worker(
        &self,
        invoker_svc: Arc<Self>,
        batch: GpuBatch,
        permit: Box<dyn Drop + Send>,
        tid: &TransactionId,
    ) {
        debug!(tid=%tid, "Launching invocation thread for queued item");
        tokio::spawn(async move {
            invoker_svc.invocation_worker_thread(batch, permit).await;
        });
    }

    /// Handle executing an invocation, plus account for its success or failure
    /// On success, the results are moved to the pointer and it is signaled
    /// On failure, [Invoker::handle_invocation_error] is called
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, batch, permit), fields(fqdn=batch.peek().registration.fqdn)))]
    async fn invocation_worker_thread(&self, batch: GpuBatch, permit: Box<dyn Drop + Send>) {
        let now = OffsetDateTime::now_utc();
        let est_finish_time = now + time::Duration::seconds_f64(batch.est_queue_time());
        self.completion_tracker.add_item(est_finish_time);
        for item in batch {
            if !item.lock() {
                continue;
            }
            match self
                .invoke(&item.registration, &item.json_args, &item.tid, item.queue_insert_time)
                .await
            {
                Ok((result, duration, compute, container_state)) => {
                    item.mark_successful(result, duration, compute, container_state)
                }
                Err(cause) => self.handle_invocation_error(item, cause),
            };
        }
        drop(permit);
        self.completion_tracker.remove_item(est_finish_time);
        self.signal.notify_waiters();
    }

    /// Handle an error with the given enqueued invocation
    /// By default re-enters item if a resource exhaustion error occurs [InsufficientMemoryError]
    ///   Calls [Self::add_item_to_queue] to do this
    /// Other errors result in exit of invocation if [InvocationConfig.attempts] are made
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item, cause), fields(tid=%item.tid)))]
    fn handle_invocation_error(&self, item: Arc<EnqueuedInvocation>, cause: anyhow::Error) {
        if let Some(_mem_err) = cause.downcast_ref::<InsufficientMemoryError>() {
            let mut warn_time = self.last_memory_warning.lock();
            if warn_time.elapsed() > Duration::from_millis(500) {
                warn!(tid=%item.tid, "Insufficient memory to run item right now");
                *warn_time = Instant::now();
            }
            item.unlock();
            match self.queue.add_item_to_queue(&item) {
                Ok(_) => self.signal.notify_waiters(),
                Err(e) => {
                    error!(tid=item.tid, error=%e, "Failed to re-queue item in GPU queue after memory exhaustion")
                }
            };
        } else if let Some(_mem_err) = cause.downcast_ref::<InsufficientGPUError>() {
            warn!(tid=%item.tid, "No GPU available to run item right now");
            item.unlock();
            match self.queue.add_item_to_queue(&item) {
                Ok(_) => self.signal.notify_waiters(),
                Err(e) => {
                    error!(tid=item.tid, error=%e, "Failed to re-queue item after GPU exhaustion")
                }
            };
        } else {
            error!(tid=%item.tid, error=%cause, "Encountered unknown error while trying to run queued invocation");
            if item.increment_error_retry(cause, self.invocation_config.retries) {
                match self.queue.add_item_to_queue(&item) {
                    Ok(_) => self.signal.notify_waiters(),
                    Err(e) => {
                        error!(tid=item.tid, error=%e, "Failed to re-queue item after attempt")
                    }
                };
            }
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
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, reg, json_args, queue_insert_time), fields(tid=%tid)))]
    async fn invoke<'a>(
        &'a self,
        reg: &'a Arc<RegisteredFunction>,
        json_args: &'a String,
        tid: &'a TransactionId,
        queue_insert_time: OffsetDateTime,
    ) -> Result<(ParsedResult, Duration, Compute, ContainerState)> {
        debug!(tid=%tid, "Internal invocation starting");
        // take run time now because we may have to wait to get a container
        let remove_time = self.clock.now_str()?;

        let start = Instant::now();
        let ctr_lock = match self.cont_manager.acquire_container(reg, tid, Compute::GPU) {
            EventualItem::Future(f) => f.await?,
            EventualItem::Now(n) => n?,
        };
        self.invoke_on_container(reg, json_args, tid, queue_insert_time, ctr_lock, remove_time, start)
            .await
    }

    /// Returns
    /// [ParsedResult] A result representing the function output, the user result plus some platform tracking
    /// [Duration]: The E2E latency between the worker and the container
    /// [Compute]: Compute the invocation was run on
    /// [ContainerState]: State the container was in for the invocation
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, reg, json_args, queue_insert_time, ctr_lock, remove_time,cold_time_start) fields(tid=%tid)))]
    async fn invoke_on_container<'a>(
        &'a self,
        reg: &'a Arc<RegisteredFunction>,
        json_args: &'a String,
        tid: &'a TransactionId,
        queue_insert_time: OffsetDateTime,
        ctr_lock: ContainerLock<'a>,
        remove_time: String,
        cold_time_start: Instant,
    ) -> Result<(ParsedResult, Duration, Compute, ContainerState)> {
        info!(tid=%tid, insert_time=%self.clock.format_time(queue_insert_time)?, remove_time=%remove_time, "Item starting to execute");
        self.running.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (data, duration) = ctr_lock.invoke(json_args).await?;
        self.running.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        match ctr_lock.container.state() {
            ContainerState::Warm => self.cmap.add(
                &reg.fqdn,
                Characteristics::GpuWarmTime,
                Values::F64(data.duration_sec),
                true,
            ),
            ContainerState::Prewarm => self.cmap.add(
                &reg.fqdn,
                Characteristics::GpuPreWarmTime,
                Values::F64(data.duration_sec),
                true,
            ),
            _ => self.cmap.add(
                &reg.fqdn,
                Characteristics::GpuColdTime,
                Values::F64(cold_time_start.elapsed().as_seconds_f64()),
                true,
            ),
        };
        self.cmap.add(
            &reg.fqdn,
            Characteristics::GpuExecTime,
            Values::F64(data.duration_sec),
            true,
        );
        let (compute, state) = (ctr_lock.container.compute_type(), ctr_lock.container.state());
        drop(ctr_lock);
        Ok((data, duration, compute, state))
    }

    fn get_est_completion_time_from_containers_gpu(&self, item: &Arc<RegisteredFunction>) -> (f64, ContainerState) {
        let exists = self
            .cont_manager
            .container_exists(&item.fqdn, iluvatar_library::types::Compute::GPU);
        let t = match exists {
            ContainerState::Warm => self.cmap.get_gpu_warm_time(&item.fqdn),
            ContainerState::Prewarm => self.cmap.get_gpu_prewarm_time(&item.fqdn),
            _ => self.cmap.get_gpu_cold_time(&item.fqdn),
        };
        (t, exists)
    }
}

#[tonic::async_trait]
impl DeviceQueue for GpuQueueingInvoker {
    fn queue_len(&self) -> usize {
        self.queue.queue_len()
    }

    fn est_completion_time(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> f64 {
        let qt = self.queue.est_queue_time();
        let tracked = self.completion_tracker.next_avail().as_seconds_f64();
        let (runtime, state) = self.get_est_completion_time_from_containers_gpu(reg);
        debug!(tid=%tid, qt=qt, state=?state, tracked=tracked, runtime=runtime, "GPU estimated completion time of item");
        qt + tracked + runtime
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item), fields(tid=%item.tid)))]
    fn enqueue_item(&self, item: &Arc<EnqueuedInvocation>) -> Result<()> {
        self.queue.add_item_to_queue(item)?;
        self.signal.notify_waiters();
        Ok(())
    }

    fn running(&self) -> u32 {
        self.running.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
mod gpu_batch_tests {
    use super::*;
    use iluvatar_library::logging::LocalTime;

    fn item(clock: &LocalTime) -> Arc<EnqueuedInvocation> {
        let name = "test";
        let rf = Arc::new(RegisteredFunction {
            function_name: name.to_string(),
            function_version: name.to_string(),
            fqdn: name.to_string(),
            image_name: name.to_string(),
            memory: 1,
            cpus: 1,
            snapshot_base: "".to_string(),
            parallel_invokes: 1,
            isolation_type: iluvatar_library::types::Isolation::CONTAINERD,
            supported_compute: iluvatar_library::types::Compute::CPU,
        });
        Arc::new(EnqueuedInvocation::new(
            rf,
            name.to_string(),
            name.to_string(),
            clock.now(),
        ))
    }

    #[test]
    fn one_item_correct() {
        let clock = LocalTime::new(&"clock".to_string()).unwrap();
        let b = GpuBatch::new(item(&clock), 1.0);
        assert_eq!(b.len(), 1);
        assert_eq!(b.est_queue_time(), 1.0);
    }

    #[test]
    fn added_items_correct() {
        let clock = LocalTime::new(&"clock".to_string()).unwrap();
        let mut b = GpuBatch::new(item(&clock), 1.0);

        for _ in 0..3 {
            b.add(item(&clock), 1.5);
        }
        assert_eq!(b.len(), 4);
        assert_eq!(b.est_queue_time(), 5.5);
    }
}
