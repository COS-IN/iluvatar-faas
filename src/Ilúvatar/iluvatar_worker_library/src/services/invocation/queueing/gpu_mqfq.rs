use super::{DeviceQueue, EnqueuedInvocation};
use crate::rpc::ContainerState;
use crate::services::containers::{containermanager::ContainerManager, structs::ParsedResult};
use crate::services::invocation::completion_time_tracker::CompletionTimeTracker;
use crate::services::invocation::invoke_on_container;
use crate::services::registration::RegisteredFunction;
use crate::services::resources::cpu::CpuResourceTracker;
use crate::services::resources::gpu::{GpuResourceTracker, GpuToken};
use crate::worker_api::worker_config::{GPUResourceConfig, InvocationConfig};
use anyhow::Result;
use dashmap::{mapref::multiple::RefMutMulti, DashMap};
use iluvatar_library::characteristics_map::CharacteristicsMap;
use iluvatar_library::logging::LocalTime;
use iluvatar_library::threading::{tokio_runtime, EventualItem};
use iluvatar_library::transaction::TransactionId;
use iluvatar_library::types::{Compute, DroppableToken};
use iluvatar_library::utils::missing_default;
use parking_lot::RwLock;
use serde::Deserialize;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use time::{Instant, OffsetDateTime};
use tokio::sync::Notify;
use tracing::{debug, error, info, warn};

lazy_static::lazy_static! {
  pub static ref MQFQ_GPU_QUEUE_WORKER_TID: TransactionId = "MQFQ_GPU_Queue".to_string();
  pub static ref MQFQ_GPU_QUEUE_BKG_TID: TransactionId = "MQFQ_GPU_Bkg".to_string();
}

#[derive(Debug, Deserialize)]
#[allow(unused)]
pub struct MqfqConfig {
    /// maximum allowed flow overrun, in seconds, default 10 sec if missing
    pub allowed_overrun: Option<f64>,
    /// in-flight execution cap, unused
    pub in_flight: Option<u32>,
    /// enable dynamic weights for queues, unused
    pub dynamic_weights: Option<bool>,
    /// VT time increment per invocation
    /// If [None] or 0.0, uses avg execution time for the function
    pub service_average: Option<f64>,
    /// TTL for active flow turning to inactive, default to 2.0 if [None]
    pub ttl_sec: Option<f64>,
}

/// Multi-Queue Fair Queueing.
/// Refer to ATC '19 paper by Hedayati et.al.
/// Key modifications:
///   1. Concurrency with D tokens.
///   2. Grace period for anticipatory batching.
/// Each function is its own flow.
#[derive(PartialEq, Debug, Copy, Clone)]
pub enum MQState {
    /// Non-empty queues are active
    Active,
    /// Non-empty queue but not considered for scheduling
    Throttled,
    /// Empty queue
    Inactive,
}

#[allow(unused)]
enum MQEvent {
    GraceExpired,
    RequestDispatched,
    NewRequest,
    RequestCancelled,
}

// TODO: Average completion time using little's law, and other estimates.

pub struct MQRequest {
    pub invok: Arc<EnqueuedInvocation>,
    // Do we maintain a backward pointer to FlowQ? qid atleast?
    pub start_time_virt: f64,
    pub finish_time_virt: f64,
}

impl MQRequest {
    pub fn new(invok: Arc<EnqueuedInvocation>, start_t_virt: f64, finish_t_virt: f64) -> Arc<Self> {
        Arc::new(Self {
            invok: invok,
            start_time_virt: start_t_virt,
            finish_time_virt: finish_t_virt,
        })
    }
}

/// A single queue of entities (invocations) of the same priority/locality class
pub struct FlowQ {
    /// Q name for indexing/debugging etc
    pub fqdn: String,
    /// Simple FIFO for now
    pub queue: VecDeque<Arc<MQRequest>>,
    /// (0,1]
    weight: f64,
    pub state: MQState,
    /// Virtual start time. S = max(vitual_time, flow.F) on insert
    pub start_time_virt: f64,
    /// Virtual finish time. F = S + service_avg/Wt
    pub finish_time_virt: f64,
    /// Number concurrently executing, to enforce cap?
    in_flight: i32,
    /// Keep-alive. Seconds to wait for next arrival if queue is empty
    ttl_sec: f64,
    pub last_serviced: OffsetDateTime,
    /// avg function execution time in seconds
    service_avg: Option<f64>,
    /// Max service this flow can be ahead of others
    allowed_overrun: f64,
    /// Inactive -> Active transition timestamp. Use to compute active period (eviction time) when going back from Active -> Inactive.
    active_start_t: OffsetDateTime,
    /// Avg active wall_t in seconds
    avg_active_t: f64,
    num_active_periods: i32,

    cont_manager: Arc<ContainerManager>,
    gpu_config: Arc<GPUResourceConfig>,
    cmap: Arc<CharacteristicsMap>,
}

impl FlowQ {
    pub fn new(
        fqdn: String,
        start_time_virt: f64,
        weight: f64,
        cont_manager: &Arc<ContainerManager>,
        gpu_config: &Arc<GPUResourceConfig>,
        q_config: &Arc<MqfqConfig>,
        cmap: &Arc<CharacteristicsMap>,
    ) -> Self {
        Self {
            queue: VecDeque::new(),
            state: MQState::Inactive,
            start_time_virt,
            finish_time_virt: start_time_virt,
            in_flight: 0,
            ttl_sec: missing_default(q_config.ttl_sec, 2.0),
            last_serviced: OffsetDateTime::now_utc(),
            service_avg: q_config.service_average,
            allowed_overrun: missing_default(q_config.allowed_overrun, 10.0),
            active_start_t: OffsetDateTime::now_utc(),
            avg_active_t: 0.0,
            num_active_periods: 0,
            cont_manager: cont_manager.clone(),
            fqdn,
            weight,
            gpu_config: gpu_config.clone(),
            cmap: cmap.clone(),
        }
    }

    fn update_state(&mut self, new_state: MQState) {
        if new_state != self.state {
            debug!(queue=%self.fqdn, old_state=?self.state, new_state=?new_state, "Switching state");
            if self.state == MQState::Active && self.gpu_config.send_driver_memory_hints() {
                let ctr = self.cont_manager.clone();
                let fname = self.fqdn.clone();
                tokio::spawn(async move {
                    ctr.madvise_off_device(fname, MQFQ_GPU_QUEUE_BKG_TID.clone()).await;
                });
            }
            self.state = new_state;
        }
    }

    fn service_avg(&self, item: &Arc<EnqueuedInvocation>) -> f64 {
        let avg = match self.service_avg {
            Some(avg) if avg != 0.0 => avg,
            _ => self.cmap.avg_gpu_exec_t(&item.registration.fqdn),
        };
        if avg <= 0.0 {
            10.0 // no record in cmap yet
        } else {
            avg
        }
    }

    /// Return True if should update the global time
    pub fn push_flow(&mut self, item: Arc<EnqueuedInvocation>, vitual_time: f64) -> bool {
        let start_t = f64::max(vitual_time, self.finish_time_virt); // cognizant of weights
        let service_avg = self.service_avg(&item);
        let finish_t = start_t + (service_avg / self.weight);
        let req = MQRequest::new(item, start_t, finish_t);
        let req_finish_virt = req.finish_time_virt;

        self.queue.push_back(req);

        self.start_time_virt = f64::max(self.start_time_virt, start_t); // if this was 0?
        self.finish_time_virt = f64::max(req_finish_virt, self.finish_time_virt); // always needed

        if self.queue.len() == 1 && self.state == MQState::Inactive {
            // We just turned active, so mark the time
            self.active_start_t = OffsetDateTime::now_utc();
            self.num_active_periods += 1;
            self.update_state(MQState::Active);
        }
        self.queue.len() == 1
    }

    /// Remove oldest item. No other svc state update.
    pub fn pop_flow(&mut self, vitual_time: f64) -> Option<Arc<MQRequest>> {
        let r = self.queue.pop_front();
        self.update_dispatched(vitual_time);
        // MQFQ should remove from the active list if not ready
        r
    }

    /// Check if the start time is ahead of global time by allowed overrun
    pub fn update_dispatched(&mut self, vitual_time: f64) {
        self.last_serviced = OffsetDateTime::now_utc();
        self.in_flight += 1;
        if let Some(next_item) = self.queue.front() {
            self.start_time_virt = next_item.start_time_virt;
            // start timer for grace period?
        } else {
            // queue is empty
            self.check_empty_q_grace_period();
        }
        let gap = self.start_time_virt - vitual_time; // vitual_time is old start_time_virt, but is start_time_virt updated?
        if gap >= self.allowed_overrun {
            self.update_state(MQState::Throttled);
        }
    }

    fn check_empty_q_grace_period(&mut self) {
        if self.state == MQState::Active {
            let ttl_remaining = (OffsetDateTime::now_utc() - self.last_serviced).as_seconds_f64();
            if ttl_remaining > self.ttl_sec {
                self.update_state(MQState::Inactive);
                // Update the active period/eviction time
                let active_t = (OffsetDateTime::now_utc() - self.active_start_t).as_seconds_f64();
                let n = self.num_active_periods as f64;
                let prev_avg = self.avg_active_t;
                let new_avg = (n * prev_avg) + active_t / (n + 1.0);
                self.avg_active_t = new_avg;
            }
        }
    }

    /// The vitual_time may have advanced, so reset throttle. Call on dispatch
    pub fn set_idle_throttled(&mut self, vitual_time: f64) {
        // check grace period
        if self.queue.is_empty() {
            return self.check_empty_q_grace_period();
        }

        let gap = self.start_time_virt - vitual_time; // vitual_time is old start_time_virt, but is start_time_virt updated?
        if gap <= self.allowed_overrun {
            self.update_state(MQState::Active);
            return;
        }
        self.update_state(MQState::Throttled);
    }

    /// Estimated q wait time, assumes weight = 1
    fn est_flow_wait(&self) -> f64 {
        self.finish_time_virt - self.start_time_virt
    }
}

/// TODO: Semaphore impl?
struct TokenBucket {
    capacity: i32,
    current: i32,
}
#[allow(unused)]
impl TokenBucket {
    fn new(capacity: i32) -> Arc<Self> {
        Arc::new(TokenBucket {
            capacity: capacity,
            current: 0,
        })
    }
    fn get_tok(&self) -> bool {
        //can return none if none available?
        let b = self.capacity - self.current;
        b > 0
    }

    fn add_tok(&mut self) {
        self.current += 1;
    }
}

pub struct MQFQ {
    /// Keyed by function name  (qid)
    mqfq_set: DashMap<String, FlowQ>,
    /// System-wide logical clock for resources consumed
    vitual_time: RwLock<f64>,

    ///Remaining passed by gpu_q_invoke
    cont_manager: Arc<ContainerManager>,
    cmap: Arc<CharacteristicsMap>,
    /// Use this as a token bucket
    ctrack: Arc<CompletionTimeTracker>,

    signal: Notify,
    cpu: Arc<CpuResourceTracker>,
    _thread: std::thread::JoinHandle<()>,
    gpu: Arc<GpuResourceTracker>,
    gpu_config: Arc<GPUResourceConfig>,
    clock: LocalTime,
    q_config: Arc<MqfqConfig>,
}

/// TODO: Pass concurrency semaphore from gpu_q_invoke
/// TODO: config with D, T, wts, etc.
/// TODO: limit number active queues via GPU memory sizing
#[allow(dyn_drop)]
impl MQFQ {
    pub fn new(
        cont_manager: Arc<ContainerManager>,
        cmap: Arc<CharacteristicsMap>,
        invocation_config: Arc<InvocationConfig>,
        cpu: Arc<CpuResourceTracker>,
        gpu: &Option<Arc<GpuResourceTracker>>,
        gpu_config: &Option<Arc<GPUResourceConfig>>,
        tid: &TransactionId,
    ) -> Result<Arc<Self>> {
        let q_config = invocation_config
            .mqfq_config
            .clone()
            .ok_or_else(|| anyhow::format_err!("Tried to create MQFQ without a MqfqConfig"))?;

        let (gpu_handle, gpu_tx) = tokio_runtime(
            invocation_config.queue_sleep_ms,
            MQFQ_GPU_QUEUE_WORKER_TID.clone(),
            Self::monitor_queue,
            Some(Self::gpu_wait_on_queue),
            None,
        )?;

        let svc = Arc::new(MQFQ {
            mqfq_set: DashMap::new(),
            vitual_time: RwLock::new(0.0),
            ctrack: Arc::new(CompletionTimeTracker::new()),
            signal: Notify::new(),
            _thread: gpu_handle,
            gpu: gpu
                .as_ref()
                .ok_or_else(|| anyhow::format_err!("Creating GPU queue invoker with no GPU resources"))?
                .clone(),
            clock: LocalTime::new(tid)?,
            cpu,
            cmap,
            cont_manager,
            gpu_config: gpu_config
                .as_ref()
                .ok_or_else(|| anyhow::format_err!("Creating GPU queue invoker with no GPU config"))?
                .clone(),
            q_config,
        });
        gpu_tx.send(svc.clone())?;
        info!(tid=%tid, "Created MQFQ");
        Ok(svc)
    }

    async fn gpu_wait_on_queue(invoker_svc: Arc<Self>, tid: TransactionId) {
        invoker_svc.signal.notified().await;
        debug!(tid=%tid, "Invoker waken up by signal");
    }
    /// Check the invocation queue, running things when there are sufficient resources
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self), fields(tid=%tid)))]
    async fn monitor_queue(self: Arc<Self>, tid: TransactionId) {
        while let Some((next_item, gpu_token)) = self.dispatch(&tid) {
            // This async function the only place which decrements running set and resources avail. Implicit assumption that it wont be concurrently invoked.
            if let Some(cpu_token) = self.acquire_resources_to_run(&next_item.invok.registration, &tid) {
                let svc = self.clone();
                tokio::spawn(async move {
                    svc.invocation_worker_thread(next_item, cpu_token, gpu_token).await;
                });
            } else {
                warn!(tid=%tid, fqdn=%next_item.invok.registration.fqdn, "Insufficient resources to run item");
                break;
            }
        }
    }

    /// Handle executing an invocation, plus account for its success or failure
    /// On success, the results are moved to the pointer and it is signaled
    /// On failure, [Invoker::handle_invocation_error] is called
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, batch, permit), fields(fqdn=batch.peek().registration.fqdn)))]
    async fn invocation_worker_thread(
        &self,
        item: Arc<MQRequest>,
        cpu_token: DroppableToken,
        gpu_token: DroppableToken,
    ) {
        let ct = OffsetDateTime::now_utc();
        self.ctrack.add_item(ct);
        if item.invok.lock() {
            match self
                .invoke(
                    &item.invok.registration,
                    &item.invok.json_args,
                    &item.invok.tid,
                    item.invok.queue_insert_time,
                    cpu_token,
                    gpu_token,
                )
                .await
            {
                Ok((result, duration, compute, container_state)) => {
                    item.invok.mark_successful(result, duration, compute, container_state)
                }
                Err(cause) => self.handle_invocation_error(item.invok.clone(), cause),
            };
        }
        self.signal.notify_waiters();
        self.ctrack.remove_item(ct);
    }

    /// Handle an error with the given enqueued invocation
    /// By default re-enters item if a resource exhaustion error occurs [InsufficientMemoryError]
    ///   Calls [Self::add_item_to_queue] to do this
    /// Other errors result in exit of invocation if [InvocationConfig.attempts] are made
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item, cause), fields(tid=%item.tid)))]
    fn handle_invocation_error(&self, item: Arc<EnqueuedInvocation>, cause: anyhow::Error) {
        debug!(tid=%item.tid, error=%cause, "Marking invocation as error");
        item.mark_error(cause);
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
        json_args: &'a str,
        tid: &'a TransactionId,
        queue_insert_time: OffsetDateTime,
        cpu_token: DroppableToken,
        gpu_token: DroppableToken,
    ) -> Result<(ParsedResult, Duration, Compute, ContainerState)> {
        debug!(tid=%tid, "Internal invocation starting");
        // take run time now because we may have to wait to get a container
        let remove_time = self.clock.now_str()?;

        let start = Instant::now();
        let ctr_lock = match self.cont_manager.acquire_container(reg, tid, Compute::GPU) {
            EventualItem::Future(f) => f.await?,
            EventualItem::Now(n) => {
                let n = n?;
                if self.gpu_config.send_driver_memory_hints() {
                    let ctr = n.container.clone();
                    let t = tid.clone();
                    tokio::spawn(ContainerManager::move_to_device(ctr, t));
                }
                n
            }
        };
        match invoke_on_container(
            reg,
            json_args,
            tid,
            queue_insert_time,
            &ctr_lock,
            remove_time,
            start,
            &self.cmap,
            &self.clock,
        )
        .await
        {
            Ok(c) => {
                drop(cpu_token);
                Ok(c)
            }
            Err(e) => {
                debug!(tid=%tid, error=%e, container_id=%ctr_lock.container.container_id(), "Error on container invoke");
                if !ctr_lock.container.is_healthy() {
                    debug!(tid=%tid, container_id=%ctr_lock.container.container_id(), "Adding gpu token to drop_on_remove for container");
                    // container will be removed, but holds onto GPU until deleted
                    ctr_lock.container.add_drop_on_remove(gpu_token, tid);
                }
                Err(e)
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
                        error!(tid=%tid, "CPU Resource Monitor `try_acquire_cores` returned a closed error!")
                    }
                    tokio::sync::TryAcquireError::NoPermits => {
                        debug!(tid=%tid, fqdn=%reg.fqdn, "Not enough CPU permits")
                    }
                };
                return None;
            }
        };
        Some(Box::new(ret))
    }

    /// Get or create FlowQ
    fn add_invok_to_flow(&self, item: Arc<EnqueuedInvocation>) {
        let vitual_time = *self.vitual_time.read();
        match self.mqfq_set.get_mut(&item.registration.fqdn) {
            Some(mut fq) => {
                if fq.value_mut().push_flow(item, vitual_time) {
                    let mut lck = self.vitual_time.write();
                    *lck = f64::min(*lck, fq.start_time_virt);
                }
            }
            None => {
                let fname = item.registration.fqdn.clone();
                let mut qguard = FlowQ::new(
                    fname.clone(),
                    vitual_time,
                    1.0,
                    &self.cont_manager,
                    &self.gpu_config,
                    &self.q_config,
                    &self.cmap,
                );
                if qguard.push_flow(item, vitual_time) {
                    let mut lck = self.vitual_time.write();
                    *lck = f64::min(*lck, qguard.start_time_virt);
                }
                self.mqfq_set.insert(fname, qguard);
            }
        };
    }

    /// Earliest eligible flow
    fn next_flow<'a>(&'a self, tid: &'a TransactionId, token: &GpuToken) -> Option<RefMutMulti<'_, String, FlowQ>> {
        let vitual_time = *self.vitual_time.read();
        let mut min_time = f64::MAX;
        let mut min_q = None;
        let mut min_time_with_cont = f64::MAX;
        let mut min_q_with_cont = None;
        for mut q in self.mqfq_set.iter_mut() {
            let val = q.value_mut();
            val.set_idle_throttled(vitual_time);
            if val.state != MQState::Inactive {
                // Active, not throttled, and lowest start_time_virt
                if val.queue.is_empty() {
                    debug!(tid=%tid, qid=%val.fqdn, val.start_time_virt, "flow is empty");
                    continue;
                }
                if self.cont_manager.warm_container(&val.fqdn, token) {
                    if min_q_with_cont.is_none() {
                        debug!(tid=%tid, qid=%val.fqdn, "first active Q, matched to GPU");
                        min_time_with_cont = q.start_time_virt;
                        min_q_with_cont = Some(q);
                    } else if q.start_time_virt <= min_time_with_cont {
                        debug!(tid=%tid, qid=%q.fqdn, old_t=min_time_with_cont, new_t=q.start_time_virt, "new min Q, matched to GPU");
                        min_time_with_cont = q.start_time_virt;
                        min_q_with_cont = Some(q);
                    }
                } else if min_q.is_none() {
                    debug!(tid=%tid, qid=%val.fqdn, "first active Q");
                    min_time = q.start_time_virt;
                    min_q = Some(q);
                } else if q.start_time_virt <= min_time {
                    debug!(tid=%tid, qid=%q.fqdn, old_t=min_time, new_t=q.start_time_virt, "new min Q");
                    min_time = q.start_time_virt;
                    min_q = Some(q);
                }
            }
        }
        if min_time != f64::MAX {
            *self.vitual_time.write() = min_time;
        }
        if min_q_with_cont.is_some() {
            return min_q_with_cont;
        }
        min_q
    }

    // Invoked functions automatically increase the count, conversely for finished functions
    fn get_token(&self, tid: &TransactionId) -> Option<GpuToken> {
        self.gpu.try_acquire_resource(None, tid).ok()
    }

    /// Main
    fn dispatch(&self, tid: &TransactionId) -> Option<(Arc<MQRequest>, DroppableToken)> {
        // Filter by active queues, and select with lowest start time.
        let vitual_time = *self.vitual_time.read();
        let qlen = self.queue_len();
        if qlen == 0 {
            debug!(tid=%tid, qlen=qlen, "Empty queue");
            return None;
        }

        match self.get_token(tid) {
            Some(token) => {
                if let Some(mut chosen_q) = self.next_flow(tid, &token) {
                    if let Some(i) = chosen_q.pop_flow(vitual_time) {
                        let updated_vitual_time = f64::max(vitual_time, i.start_time_virt); // dont want it to go backwards
                        *self.vitual_time.write() = updated_vitual_time;
                        chosen_q.update_dispatched(updated_vitual_time);
                        Some((i, token.into()))
                    } else {
                        debug!(tid=%tid, chosen_q=%chosen_q.fqdn, qlen=qlen, "Empty flow chosen");
                        None
                    }
                } else {
                    debug!(tid=%tid, qlen=qlen, "No chosen flow");
                    None
                }
            }
            None => {
                debug!(tid=%tid, qlen=qlen, "No token");
                None
            }
        }
    }
} // END MQFQ

impl DeviceQueue for MQFQ {
    fn queue_len(&self) -> usize {
        // sum(self.mqfq_set.iter().map(|x| x.len()))
        let per_flow_q_len = self.mqfq_set.iter().map(|x| x.value().queue.len());
        per_flow_q_len.sum::<usize>()
    }

    fn est_completion_time(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> f64 {
        // sum_q (q_F-q_S) / max_in_flight
        let per_flow_wait_times = self.mqfq_set.iter().map(|x| x.value().est_flow_wait());
        let total_wait: f64 = per_flow_wait_times.sum();

        debug!(tid=%tid, qt=total_wait, runtime=0.0, "GPU estimated completion time of item");

        (total_wait / self.gpu.total_gpus() as f64) + self.cmap.get_gpu_exec_time(&reg.fqdn)
    }

    fn enqueue_item(&self, item: &Arc<EnqueuedInvocation>) -> Result<()> {
        self.add_invok_to_flow(item.clone());
        Ok(())
    }

    fn running(&self) -> u32 {
        self.ctrack.get_inflight() as u32
    }

    fn warm_hit_probability(&self, reg: &Arc<RegisteredFunction>, iat: f64) -> f64 {
        // if flowq doesnt exist or inactive, 0
        // else (active or throttled), but no guarantees
        // Average eviction time for the queue? eviction == q becomes inactive
        // 1 - e^-(AET/iat)
        let fname = &reg.fqdn;
        let f = self.mqfq_set.get(fname);
        match f {
            Some(fq) => {
                let aet = fq.value().avg_active_t;
                let r = -aet / iat;
                1.0 - r.exp()
            }
            None => 0.0,
        }
    }
}
