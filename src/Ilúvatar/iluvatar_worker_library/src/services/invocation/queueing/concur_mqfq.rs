use super::gpu_mqfq::{MQRequest, MQState, MqfqConfig};
use super::{DeviceQueue, EnqueuedInvocation};
use crate::services::containers::containermanager::ContainerManager;
use crate::services::containers::structs::ContainerLock;
use crate::services::invocation::completion_time_tracker::CompletionTimeTracker;
use crate::services::invocation::{invoke_on_container, QueueLoad};
use crate::services::registration::RegisteredFunction;
use crate::services::resources::cpu::CpuResourceTracker;
use crate::services::resources::gpu::{GpuResourceTracker, GPU};
use crate::worker_api::worker_config::{GPUResourceConfig, InvocationConfig};
use anyhow::Result;
use dashmap::DashMap;
use iluvatar_library::characteristics_map::CharacteristicsMap;
use iluvatar_library::clock::{get_global_clock, now, Clock};
use iluvatar_library::mindicator::Mindicator;
use iluvatar_library::threading::EventualItem;
use iluvatar_library::transaction::TransactionId;
use iluvatar_library::types::{Compute, DroppableToken};
use iluvatar_library::utils::missing_default;
use parking_lot::RwLock;
use std::collections::VecDeque;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::sync::Notify;
use tracing::{debug, error, info};

lazy_static::lazy_static! {
  pub static ref MQFQ_STATE_MON: TransactionId = "MQFQ_State_Mon".to_string();
}

struct Flow {
    mindicator: Arc<Mindicator>,
    cpu: Arc<CpuResourceTracker>,
    gpu: Arc<GpuResourceTracker>,
    ctrack: Arc<CompletionTimeTracker>,
    queue: SharedQueue,
    cont_manager: Arc<ContainerManager>,
    registration: Arc<RegisteredFunction>,
    clock: Clock,
    cmap: Arc<CharacteristicsMap>,
    queue_signal: Arc<Notify>,
}
impl Flow {
    pub fn new(
        queue: &SharedQueue,
        mindicator: &Arc<Mindicator>,
        cpu: &Arc<CpuResourceTracker>,
        gpu: &Arc<GpuResourceTracker>,
        ctrack: &Arc<CompletionTimeTracker>,
        cont_manager: &Arc<ContainerManager>,
        registration: &Arc<RegisteredFunction>,
        cmap: &Arc<CharacteristicsMap>,
        tid: &TransactionId,
        signal: Arc<Notify>,
    ) -> Result<Self> {
        Ok(Self {
            queue: queue.clone(),
            mindicator: mindicator.clone(),
            gpu: gpu.clone(),
            cpu: cpu.clone(),
            ctrack: ctrack.clone(),
            cont_manager: cont_manager.clone(),
            registration: registration.clone(),
            clock: get_global_clock(tid)?,
            cmap: cmap.clone(),
            queue_signal: signal,
        })
    }

    /// Handle an error with the given enqueued invocation
    /// By default re-enters item if a resource exhaustion error occurs [InsufficientMemoryError]
    ///   Calls [Self::add_item_to_queue] to do this
    /// Other errors result in exit of invocation if [InvocationConfig.attempts] are made
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item, cause), fields(tid=%item.tid)))]
    fn handle_invocation_error(&self, item: Arc<EnqueuedInvocation>, cause: anyhow::Error) {
        debug!(tid=%item.tid, error=%cause, "Marking invocation as error");
        item.mark_error(&cause);
    }

    /// Returns an owned permit if there are sufficient resources to run a function
    /// A return value of [None] means the resources failed to be acquired
    fn acquire_resources_to_run(
        &self,
        reg: &Arc<RegisteredFunction>,
        tid: &TransactionId,
        gpu: Option<&GPU>,
    ) -> Option<DroppableToken> {
        let mut ret: Vec<DroppableToken> = vec![];
        match self.cpu.try_acquire_cores(reg, tid) {
            Ok(Some(c)) => ret.push(Box::new(c)),
            Ok(_) => (),
            Err(e) => {
                match e {
                    tokio::sync::TryAcquireError::Closed => {
                        error!(tid=%tid, "CPU Resource Monitor `try_acquire_cores` returned a closed error!")
                    },
                    tokio::sync::TryAcquireError::NoPermits => {
                        // debug!(tid=%tid, fqdn=%reg.fqdn, "Not enough CPU permits")
                    },
                };
                return None;
            },
        };
        match self.gpu.try_acquire_resource(gpu, tid) {
            Ok(c) => ret.push(c.into()),
            Err(e) => {
                match e {
                    tokio::sync::TryAcquireError::Closed => {
                        error!(tid=%tid, "GPU Resource Monitor `try_acquire_cores` returned a closed error!")
                    },
                    tokio::sync::TryAcquireError::NoPermits => {
                        // debug!(tid=%tid, fqdn=%reg.fqdn, "Not enough GPU permits")
                    },
                };
                return None;
            },
        };
        Some(Box::new(ret))
    }

    pub async fn get_container(&self, tid: &TransactionId) -> Option<ContainerLock> {
        let res_lck = match self
            .cont_manager
            .acquire_container(&self.registration, tid, Compute::GPU)
        {
            EventualItem::Future(f) => f.await,
            EventualItem::Now(n) => n,
        };
        match res_lck {
            Ok(lck) => Some(lck),
            Err(cause) => {
                if cause
                    .downcast_ref::<crate::services::containers::structs::InsufficientGPUError>()
                    .is_some()
                {
                    None
                } else {
                    error!(tid=%tid, error=%cause, "Error getting new container");
                    None
                }
            },
        }
    }

    pub async fn invoke(
        self: &Arc<Self>,
        ctr_lck: ContainerLock,
        item: Arc<MQRequest>,
        remove_time: OffsetDateTime,
        tokens: DroppableToken,
        tid: TransactionId,
    ) {
        let start = now();
        if item.invoke.lock() {
            let ct = self.clock.now();
            self.ctrack.add_item(ct);
            match invoke_on_container(
                &self.registration,
                &item.invoke.json_args,
                &item.invoke.tid,
                item.invoke.queue_insert_time,
                item.invoke.est_completion_time,
                item.invoke.insert_time_load,
                &ctr_lck,
                self.clock.format_time(remove_time).unwrap_or_else(|_| "".to_string()),
                start,
                &self.cmap,
                &self.clock,
            )
            .await
            {
                Ok((result, duration, compute, container_state)) => {
                    info!(tid=%tid, "!!invoke done!!");
                    item.invoke.mark_successful(result, duration, compute, container_state);
                },
                Err(cause) => {
                    debug!(tid=%item.invoke.tid, error=%cause, container_id=%ctr_lck.container.container_id(), "Error on container invoke");
                    self.handle_invocation_error(item.invoke.clone(), cause);
                    if !ctr_lck.container.is_healthy() {
                        debug!(tid=%item.invoke.tid, container_id=%ctr_lck.container.container_id(), "Adding gpu token to drop_on_remove for container");
                        // container will be removed, but holds onto GPU until deleted
                        ctr_lck.container.add_drop_on_remove(tokens, &item.invoke.tid);
                    }
                },
            }
            self.ctrack.remove_item(ct);
            self.queue_signal.notify_waiters();
        }
    }

    pub async fn run(self: &Arc<Self>, tid: &TransactionId) {
        loop {
            self.queue.set_idle_throttled(tid);
            while self.queue.allowed_to_run() {
                let mv_tid = tid.clone();
                let svc = self.clone();
                // TODO: madvise memory to device
                let ctr_lck = match svc.get_container(&mv_tid).await {
                    Some(c) => c,
                    None => continue,
                };
                let gpu = ctr_lck.container.device_resource();
                if let Some(has_tokens) = self.acquire_resources_to_run(&self.registration, &mv_tid, gpu.as_ref()) {
                    drop(gpu);
                    let min: f64 = self.mindicator.min();
                    if let Some(item) = self.queue.pop_flow(&mv_tid, min) {
                        let remove_time = self.clock.now();
                        tokio::task::spawn(async move {
                            svc.invoke(ctr_lck, item, remove_time, has_tokens, mv_tid).await;
                        });
                    } else {
                        break;
                    }
                };
            }
            tokio::select! {
              _ = self.queue_signal.notified() => continue,
              _ = self.queue.global_signal.notified() => continue
            }
        }
    }
}

/// A single queue of entities (invocations) of the same priority/locality class
struct FuncQueue {
    /// Q name for indexing/debugging etc
    pub registration: Arc<RegisteredFunction>,
    /// Simple FIFO for now
    pub queue: RwLock<VecDeque<Arc<MQRequest>>>,
    /// (0,1]
    weight: f64,
    pub state: RwLock<MQState>,
    /// Virtual start time. S = max(vitual_time, flow.F) on insert
    pub start_time_virt: RwLock<f64>,
    /// Virtual finish time. F = S + service_avg/Wt
    pub finish_time_virt: RwLock<f64>,
    /// Keep-alive. Seconds to wait for next arrival if queue is empty
    ttl_sec: f64,
    pub last_serviced: RwLock<OffsetDateTime>,
    /// avg function execution time in seconds
    service_avg: f64,
    /// Max service this flow can be ahead of others
    pub allowed_overrun: f64,

    cont_manager: Arc<ContainerManager>,
    mindicator: Arc<Mindicator>,
    mindicator_id: usize,
    cpu: Arc<CpuResourceTracker>,
    gpu: Arc<GpuResourceTracker>,
    ctrack: Arc<CompletionTimeTracker>,
    cmap: Arc<CharacteristicsMap>,
    flow_tid: TransactionId,
    pub global_signal: Arc<Notify>,
    pub queue_signal: Arc<Notify>,
    clock: Clock,
}
type SharedQueue = Arc<FuncQueue>;
impl FuncQueue {
    pub fn new(
        registration: Arc<RegisteredFunction>,
        weight: f64,
        cont_manager: &Arc<ContainerManager>,
        q_config: &Arc<MqfqConfig>,
        mindicator: &Arc<Mindicator>,
        mindicator_id: usize,
        cpu: &Arc<CpuResourceTracker>,
        gpu: &Arc<GpuResourceTracker>,
        ctrack: &Arc<CompletionTimeTracker>,
        cmap: &Arc<CharacteristicsMap>,
        signal: &Arc<Notify>,
        clock: &Clock,
    ) -> SharedQueue {
        let start_time_virt = mindicator.min();
        Arc::new(Self {
            flow_tid: format!("flow-{}", registration.fqdn),
            queue: RwLock::new(VecDeque::new()),
            state: RwLock::new(MQState::Inactive),
            start_time_virt: RwLock::new(start_time_virt),
            finish_time_virt: RwLock::new(start_time_virt),
            ttl_sec: 20.0,
            last_serviced: RwLock::new(clock.now()),
            service_avg: 10.0,
            allowed_overrun: missing_default(&q_config.allowed_overrun, 10.0),
            registration: registration.clone(),
            weight,
            mindicator: mindicator.clone(),
            mindicator_id,
            gpu: gpu.clone(),
            cpu: cpu.clone(),
            ctrack: ctrack.clone(),
            cont_manager: cont_manager.clone(),
            cmap: cmap.clone(),
            global_signal: signal.clone(),
            queue_signal: Arc::new(Notify::new()),
            clock: clock.clone(),
        })
    }

    fn start_new_flow(self: &Arc<Self>, _tid: &TransactionId) {
        let tid: TransactionId = format!("flow-{}", self.registration.fqdn);
        match Flow::new(
            self,
            &self.mindicator,
            &self.cpu,
            &self.gpu,
            &self.ctrack,
            &self.cont_manager,
            &self.registration,
            &self.cmap,
            &tid,
            self.queue_signal.clone(),
        ) {
            Ok(flow) => {
                tokio::spawn(async move {
                    Arc::new(flow).run(&tid).await;
                });
            },
            Err(e) => {
                error!(tid=%tid, error=%e, fqdn=%self.registration.fqdn, "Failed to make new Flow queue thread");
            },
        }
    }

    fn update_state(self: &Arc<Self>, tid: &TransactionId, new_state: MQState) {
        if new_state != *self.state.read() {
            let mut state = self.state.write();
            info!(tid=%tid, queue=%self.registration.fqdn, old_state=?*state, new_state=?new_state, "Switching state");
            *state = new_state;
            self.global_signal.notify_waiters();
        }
    }

    pub fn allowed_to_run(&self) -> bool {
        let state = *self.state.read();
        state == MQState::Active && !self.queue.read().is_empty()
    }

    pub fn push_flow(self: &Arc<Self>, item: Arc<EnqueuedInvocation>) {
        let finish_time_virt = *self.finish_time_virt.read();
        let start_t = f64::max(self.mindicator.min(), finish_time_virt); // cognizant of weights
                                                                         // TODO: Update the service_avg regularly from cmap
        let finish_t = start_t + (self.service_avg / self.weight);
        let req = MQRequest::new(item, start_t, finish_t);
        let req_finish_virt = req.finish_time_virt;

        self.queue.write().push_back(req);

        let mut start_time_virt = self.start_time_virt.write();
        *start_time_virt = f64::max(*start_time_virt, start_t); // if this was 0?
        drop(start_time_virt);
        *self.finish_time_virt.write() = f64::max(req_finish_virt, finish_time_virt); // always needed
                                                                                      // info!("pushing flow");
        self.queue_signal.notify_waiters();
        self.update_state(&self.flow_tid, MQState::Active);
    }

    /// Remove oldest item. No other svc state update.
    pub fn pop_flow(self: &Arc<Self>, tid: &TransactionId, glob_virt_time: f64) -> Option<Arc<MQRequest>> {
        let r: Option<Arc<MQRequest>> = self.queue.write().pop_front();
        self.update_dispatched(tid, glob_virt_time);
        // MQFQ should remove from the active list if not ready
        r
    }

    /// Check if the start time is ahead of global time by allowed overrun
    pub fn update_dispatched(self: &Arc<Self>, tid: &TransactionId, glob_virt_time: f64) {
        *self.last_serviced.write() = self.clock.now();
        let mut new_start_time_virt = *self.start_time_virt.read();
        if let Some(next_item) = self.queue.read().front() {
            new_start_time_virt = next_item.start_time_virt;
            *self.start_time_virt.write() = next_item.start_time_virt;
            match self.mindicator.insert(self.mindicator_id, new_start_time_virt) {
                Ok(_) => (),
                Err(e) => error!(error=%e, queue=%self.registration.fqdn, "Inserted NaN into mindicator"),
            };
            // start timer for grace period?
        } else {
            // queue is empty
            self.mindicator.remove(self.mindicator_id);
            // info!("dispatch queue empty");
            self.update_state(tid, MQState::Inactive);
        }
        let gap = new_start_time_virt - glob_virt_time; // vitual_time is old start_time_virt, but is start_time_virt updated?
        if gap >= self.allowed_overrun {
            // info!("dispatch queue overrun");
            self.update_state(tid, MQState::Throttled);
        }
    }

    // /// The vitual_time may have advanced, so reset throttle. Call on dispatch
    pub fn set_idle_throttled(self: &Arc<Self>, tid: &TransactionId) {
        let gap = *self.start_time_virt.read() - self.mindicator.min();
        if gap <= self.allowed_overrun {
            // info!("set_idle_throttled under overrun");
            self.update_state(tid, MQState::Active);
            return;
        }
        // check grace period
        if self.queue.read().is_empty() {
            let ttl_remaining = (self.clock.now() - *self.last_serviced.read()).as_seconds_f64();
            if ttl_remaining > self.ttl_sec {
                // info!("set_idle_throttled ttl overshtot");
                self.update_state(tid, MQState::Inactive);
            }
        }
    }

    /// Estimated q wait time, assumes weight = 1
    fn est_flow_wait(&self) -> f64 {
        (*self.finish_time_virt.read() - *self.start_time_virt.read()) * self.weight
    }
}

pub struct ConcurMqfq {
    /// Keyed by function name  (qid)
    queues: DashMap<String, SharedQueue>,

    ///Remaining passed by gpu_q_invoke
    cont_manager: Arc<ContainerManager>,
    cmap: Arc<CharacteristicsMap>,
    /// Use this as a token bucket
    ctrack: Arc<CompletionTimeTracker>,

    cpu: Arc<CpuResourceTracker>,
    gpu: Arc<GpuResourceTracker>,
    q_config: Arc<MqfqConfig>,
    mindicator: Arc<Mindicator>,
    signal: Arc<Notify>,
    clock: Clock,
}

#[allow(dyn_drop)]
impl ConcurMqfq {
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
        gpu_config
            .as_ref()
            .ok_or_else(|| anyhow::format_err!("Creating GPU queue invoker with no GPU config"))?;
        let svc = Arc::new(ConcurMqfq {
            queues: DashMap::new(),
            ctrack: Arc::new(CompletionTimeTracker::new(tid)?),
            gpu: gpu
                .as_ref()
                .ok_or_else(|| anyhow::format_err!("Creating GPU queue invoker with no GPU resources"))?
                .clone(),
            cpu,
            cmap,
            cont_manager,
            q_config,
            mindicator: Mindicator::boxed(0),
            signal: Arc::new(Notify::new()),
            clock: get_global_clock(tid)?,
        });
        info!(tid=%tid, "Created ConcurMqfq");
        Ok(svc)
    }

    /// Get or create FlowQ
    fn add_invok_to_flow(&self, item: Arc<EnqueuedInvocation>) {
        info!("adding item to flow");
        match self.queues.get(&item.registration.fqdn) {
            Some(fq) => {
                info!("inserting into existing flow");
                fq.push_flow(item);
                // fq.global_signal.notify_waiters();
            },
            None => {
                info!("making new flow");
                let fname = item.registration.fqdn.clone();
                let inserts = 1;
                let mindicator_id = self.mindicator.add_procs(inserts) - inserts;
                let qguard = FuncQueue::new(
                    item.registration.clone(),
                    1.0,
                    &self.cont_manager,
                    &self.q_config,
                    &self.mindicator,
                    mindicator_id,
                    &self.cpu,
                    &self.gpu,
                    &self.ctrack,
                    &self.cmap,
                    &self.signal,
                    &self.clock,
                );
                // let sig = qguard.global_signal.clone();
                qguard.start_new_flow(&item.tid);
                qguard.push_flow(item);
                self.queues.insert(fname, qguard);
                // sig.notify_waiters();
            },
        };
    }
}

impl DeviceQueue for ConcurMqfq {
    fn queue_len(&self) -> usize {
        // sum(self.mqfq_set.iter().map(|x| x.len()))
        let per_flow_q_len = self.queues.iter().map(|x| x.value().queue.read().len());
        per_flow_q_len.sum::<usize>()
    }
    fn queue_load(&self) -> QueueLoad {
        QueueLoad::default()
    }
    fn est_completion_time(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> (f64, f64) {
        // sum_q (q_F-q_S) / max_in_flight
        let per_flow_wait_times = self.queues.iter().map(|x| x.value().est_flow_wait());
        let total_wait: f64 = per_flow_wait_times.sum();

        debug!(tid=%tid, qt=total_wait, runtime=0.0, "GPU estimated completion time of item");

        (
            (total_wait / self.gpu.total_gpus() as f64) + self.cmap.get_gpu_exec_time(&reg.fqdn),
            total_wait,
        )
    }

    fn enqueue_item(&self, item: &Arc<EnqueuedInvocation>) -> Result<()> {
        self.add_invok_to_flow(item.clone());
        Ok(())
    }

    fn running(&self) -> u32 {
        self.ctrack.get_inflight() as u32
    }

    fn warm_hit_probability(&self, _reg: &Arc<RegisteredFunction>, _iat: f64) -> f64 {
        // if flowq doesnt exist or inactive, 0
        // else (active or throttled), but no guarantees
        // Average eviction time for the queue? eviction == q becomes inactive
        // 1 - e^-(AET/iat)
        // let fname = &reg.fqdn;
        // let f = self.queues.get(fname);
        // match f {
        //     Some(fq) => {
        //         let aet = fq.value().avg_active_t;
        //         let r = -aet / iat;
        //         1.0 - r.exp()
        //     }
        //     None => 0.0,
        // }
        todo!("warm_hit_probability for ConcurMqfq not implemented, only GPU support");
    }
}
