#![allow(unused)]

use crate::services::containers::containermanager::ContainerManager;
use crate::services::invocation::completion_time_tracker::CompletionTimeTracker;
use crate::services::resources::cpu::CpuResourceTracker;
use crate::services::resources::gpu::GpuResourceTracker;
use crate::services::registration::RegisteredFunction;
use crate::worker_api::worker_config::InvocationConfig;
use anyhow::Result;
use dashmap::{DashMap, mapref::multiple::RefMulti};
use iluvatar_library::characteristics_map::CharacteristicsMap;
use iluvatar_library::threading::tokio_runtime;
use iluvatar_library::transaction::TransactionId;
use parking_lot::{Mutex, RwLock};
use std::collections::VecDeque;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::sync::Notify;
use tracing::{debug, error};
use super::{DeviceQueue, EnqueuedInvocation};


lazy_static::lazy_static! {
  pub static ref MQFQ_GPU_QUEUE_WORKER_TID: TransactionId = "MQFQ_GPU_Queue".to_string();
}

/// Multi-Queue Fair Queueing.
/// Refer to ATC '19 paper by Hedayati et.al.
/// Key modifications:
///   1. Concurrency with D tokens.
///   2. Grace period for anticipatory batching.
/// Each function is its own flow.
#[derive(PartialEq)]
pub enum MQState {
    /// Non-empty queues are active
    Active,
    /// Non-empty but not considered for scheduling
    Throttled,
    /// Empty queue
    Inactive,
}

enum MQEvent {
    GraceExpired,
    RequestDispatched,
    NewRequest,
    RequestCancelled,
}

// TODO: Average completion time using little's law, and other estimates.

pub struct MQRequest {
    invok: Arc<EnqueuedInvocation>,
    // Do we maintain a backward pointer to FlowQ? qid atleast?
    start_time_virt: f64,
    finish_time_virt: f64,
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
    qid: String,
    /// Simple FIFO for now
    queue: VecDeque<Arc<MQRequest>>,
    /// (0,1]
    weight: f64,
    state: MQState,
    /// Virtual start time. S = max(vitual_time, flow.F) on insert
    start_time_virt: f64,
    /// Virtual finish time. F = S + service_avg/Wt
    finish_time_virt: f64,
    /// Number concurrently executing, to enforce cap?
    in_flight: i32,
    /// Keep-alive. Seconds to wait for next arrival if queue is empty
    ttl_sec: f64,
    last_serviced: OffsetDateTime,
    /// avg function execution time in seconds 
    service_avg: f64,
    /// Max service this flow can be ahead of others
    allowed_overrun: f64,
    /// Inactive -> Active transition timestamp. Use to compute active period (eviction time) when going back from Active -> Inactive. 
    active_start_t: OffsetDateTime,
    /// Avg active wall_t in seconds 
    avg_active_t: f64,
    num_active_periods:i32,
}

impl FlowQ {
    pub fn new(qid: String, start_time_virt: f64, weight: f64) -> Self {
        Self {
            qid: qid,
            queue: VecDeque::new(),
            weight: weight,
            state: MQState::Inactive,
            start_time_virt,
            finish_time_virt: 0.0,
            in_flight: 0,
            ttl_sec: 20.0,
            last_serviced: OffsetDateTime::now_utc(),
            service_avg: 10.0,
            allowed_overrun: 10.0,
	    active_start_t:OffsetDateTime::now_utc(),
	    avg_active_t: 0.0,
	    num_active_periods:0
        }
    }

    /// Return True if should update the global time
    pub fn push_flow(&mut self, item: Arc<EnqueuedInvocation>, vitual_time: f64) -> bool {
        let start_t = f64::max(vitual_time, self.finish_time_virt); // cognizant of weights
	// Update the service_avg regularly from cmap

        let finish_t = start_t + (self.service_avg / self.weight);
        let req = MQRequest::new(item, start_t, finish_t);
        let req_finish_virt = req.finish_time_virt;

        self.queue.push_back(req);

        self.start_time_virt = f64::max(self.start_time_virt, start_t); // if this was 0?
        self.finish_time_virt = f64::max(req_finish_virt, self.finish_time_virt); // always needed

	if self.queue.len() == 1 {
	    if self.state == MQState::Inactive {
		// We just turned active, so mark the time
		self.active_start_t = OffsetDateTime::now_utc();
		self.num_active_periods += 1;
	    }
	}
        self.state = MQState::Active;	
        self.queue.len() == 1
        //self.start_time_virt = r.start_time_virt; // only if the first element!
    }

    /// Remove oldest item. No other svc state update.
    pub fn pop_flow(&mut self, vitual_time: f64) -> Option<Arc<MQRequest>> {
        let r = self.queue.pop_front();

        // already handled in set_idle_throttled 	
        // if self.queue.is_empty() {
        //     self.state = MQState::Inactive;
        //     self.finish_time_virt = 0.0; // Clears the history if empty queue?
        //     self.start_time_virt = 0.0;	    
        // }
	
        self.update_dispatched(vitual_time);
        // MQFQ should remove from the active list if not ready
        r
    }

    /// Check if the start time is ahead of global time by allowed overrun
    pub fn update_dispatched(&mut self, vitual_time: f64) {
        self.last_serviced = OffsetDateTime::now_utc();
        self.in_flight = self.in_flight + 1;
        // let next_item = self.queue.front();
        match self.queue.front() {
            Some(next_item) => {
                self.start_time_virt = next_item.start_time_virt;
            }
            None => (),
        };
        // start timer for grace period?
        let gap = self.start_time_virt - vitual_time; // vitual_time is old start_time_virt, but is start_time_virt updated?
        if gap > self.allowed_overrun {
            self.state = MQState::Throttled;
        }
    }

    /// The vitual_time may have advanced, so reset throttle. Call on dispatch
    pub fn set_idle_throttled(&mut self, vitual_time: f64) {
        let gap = self.start_time_virt - vitual_time; // vitual_time is old start_time_virt, but is start_time_virt updated?
        if gap <= self.allowed_overrun {
            self.state = MQState::Active;
            return;
        }
        // check grace period
        if self.queue.is_empty() {
            let ttl_remaining = (OffsetDateTime::now_utc() - self.last_serviced).as_seconds_f64();
            if ttl_remaining > self.ttl_sec {
                self.state = MQState::Inactive;
		self.finish_time_virt = 0.0; // Clears the history if empty queue?
		self.start_time_virt = 0.0;
		// Update the active period/eviction time 
		let active_t = (OffsetDateTime::now_utc() - self.active_start_t).as_seconds_f64();
		let n = self.num_active_periods as f64;
		let prev_avg = self.avg_active_t; 
		let new_avg = (n*prev_avg) + active_t/(n+1.0) ;
		self.avg_active_t = new_avg ; 
            }
        }
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
        let svc = Arc::new(TokenBucket {
            capacity: capacity,
            current: 0,
        });
        svc
    }
    fn get_tok(&self) -> bool {
        //can return none if none available?
        let b = self.capacity - self.current;
        b > 0
    }

    fn add_tok(&mut self) -> () {
        self.current = self.current + 1;
    }
}

pub struct MQFQ {
    /// Keyed by function name  (qid)
    mqfq_set: DashMap<String, Arc<Mutex<FlowQ>>>,
    /// System-wide logical clock for resources consumed
    vitual_time: RwLock<f64>,
    /// TODO: Configurable param
    max_inflight: i32,
    /// TODO: Ignored for now
    est_time: Mutex<f64>,

    ///Remaining passed by gpu_q_invoke
    cont_manager: Arc<ContainerManager>,
    cmap: Arc<CharacteristicsMap>,
    /// Use this as a token bucket
    ctrack: Arc<CompletionTimeTracker>,

    signal: Notify,
    cpu: Arc<CpuResourceTracker>,
    _thread: std::thread::JoinHandle<()>,
    gpu: Arc<GpuResourceTracker>,
}

/// TODO: Pass concurrency semaphore from gpu_q_invoke
/// TODO: config with D, T, wts, etc.
#[allow(dyn_drop)]
impl MQFQ {
    pub fn new(
        cont_manager: Arc<ContainerManager>,
        cmap: Arc<CharacteristicsMap>,
        invocation_config: Arc<InvocationConfig>,
        cpu: Arc<CpuResourceTracker>,
        gpu: &Option<Arc<GpuResourceTracker>>,
    ) -> Result<Arc<Self>> {
        let (gpu_handle, gpu_tx) = tokio_runtime(
            invocation_config.queue_sleep_ms,
            MQFQ_GPU_QUEUE_WORKER_TID.clone(),
            Self::monitor_queue,
            Some(Self::gpu_wait_on_queue),
            None,
        )?;

        let svc = Arc::new(MQFQ {
            mqfq_set: DashMap::new(),
            est_time: Mutex::new(0.0),
            vitual_time: RwLock::new(0.0),
            max_inflight: 4,
            ctrack: Arc::new(CompletionTimeTracker::new()),
            signal: Notify::new(),
            _thread: gpu_handle,
            gpu: gpu.as_ref().ok_or_else(|| anyhow::format_err!("Creating GPU queue invoker with no GPU resources"))?.clone(),
            cpu,
            cmap,
            cont_manager,
        });
        gpu_tx.send(svc.clone())?;
        Ok(svc)
    }

    async fn gpu_wait_on_queue(invoker_svc: Arc<Self>, tid: TransactionId) {
        invoker_svc.signal.notified().await;
        debug!(tid=%tid, "Invoker waken up by signal");
    }
    /// Check the invocation queue, running things when there are sufficient resources
    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self), fields(tid=%tid)))]
    async fn monitor_queue(self: Arc<Self>, tid: TransactionId) {
        while let Some(peek_reg) = self.dispatch() {
            // This async function the only place which decrements running set and resources avail. Implicit assumption that it wont be concurrently invoked.
            if let Some(permit) = self.acquire_resources_to_run(&peek_reg.invok.registration, &tid) {
                // self.spawn_tokio_worker(self.clone(), batch, permit, &tid);
                todo!();
            } else {
                debug!(tid=%tid, fqdn=%peek_reg.invok.registration.fqdn, "Insufficient resources to run item");
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
        match self.cpu.try_acquire_cores(reg, tid) {
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

    /// Get or create FlowQ
    fn add_invok_to_flow(&self, item: Arc<EnqueuedInvocation>) {
        let fname = &item.registration.fqdn;
        let vitual_time = *self.vitual_time.read();
        //        let qret:Arc<FlowQ>;
        // Lookup flow if exists
        if self.mqfq_set.contains_key(fname) {
            let fq = self.mqfq_set.get_mut(fname).unwrap();
	    fq.value().lock().service_avg = self.cmap.avg_gpu_exec_t(fname); // hope this doesnt starve? 
            let mut qret = fq.value().lock().push_flow(item, vitual_time);
            // qret //? Always do that here?
        }
        // else, create the FlowQ, add to set, and add item to flow and
        else {
            let fname = item.registration.fqdn.clone();
            let qguard = Arc::new(Mutex::new(FlowQ::new(fname.clone(), 0.0, 1.0)));
            let mut qret = qguard.lock().push_flow(item, vitual_time);
            // qret.push_flowQ(item, vitual_time); //? Always do that here?
            // let qret = qguard.lock();
            self.mqfq_set.insert(fname, qguard);
        }
    }

    /// Earliest eligible flow
    fn next_flow(&self) -> Option<Arc<Mutex<FlowQ>>> {
        let vitual_time = *self.vitual_time.read();
        // TODO: Should be <Mutex<Arc<FlowQ>> ?
        let filt = |x: &RefMulti<'_, String, Arc<Mutex<FlowQ>>>| {
            // update state and check for active at same time
            let mut val = x.value().lock();
            val.set_idle_throttled(vitual_time);
            val.state == MQState::Active
        };
        // Active, not throttled, and lowest start_time_virt
        let avail_flows = self.mqfq_set.iter().filter(filt).map(|x| x.value().clone());
	// TODO: do we need the clone? 

        let chosen_q =
            avail_flows.min_by(|x, y| x.lock().start_time_virt.partial_cmp(&y.lock().start_time_virt).unwrap());
        match chosen_q {
            Some(chosen_q) => Some(chosen_q.clone()),
            None => None,
        }
    }

    // Invoked functions automatically increase the count, conversely for finished functions
    fn enough_tokens(&self) -> bool {
        self.ctrack.get_inflight() < self.max_inflight
    }

    /// Main
    fn dispatch(&self) -> Option<Arc<MQRequest>> {
        // Filter by active queues, and select with lowest start time.
        // How to avoid hoarding of the tokens? Want round-robin.
        let vitual_time = *self.vitual_time.read();

        if !self.enough_tokens() {
            return None;
        }

        match self.next_flow() {
            None => None,
            Some(cq) => {
                let mut chosen_q = cq.lock();
                let item = chosen_q.pop_flow(vitual_time);
                match item {
                    Some(i) => {
                        let updated_vitual_time = f64::max(vitual_time, i.start_time_virt); // dont want it to go backwards
                        *self.vitual_time.write() = updated_vitual_time;
                        chosen_q.update_dispatched(updated_vitual_time);
                        Some(i)
                    }
                    None => None,
                }
            }
        }
        // Update MQFQ State
    }

    /// Function just finished running. Completion call-back. Add tokens?
    fn charge_fn(efn: EnqueuedInvocation) -> () {}

} // END MQFQ 


//   fn pop_queue(&self) -> Option<GpuBatch> {
//     let to_run = self.dispatch();
//     match to_run {
//         Some(t) => {
//             let i = &t.invok;
//             let g = GpuBatch::new(i.clone(), 1.0);
//             Some(g)
//         }
//         None => {
//             debug!("Nothing in queue to run");
//             None
//         } // Asked to run something, but are throttled. Return None?
//     }
// }

impl DeviceQueue for MQFQ {
    fn queue_len(&self) -> usize {
	//sum(self.mqfq_set.iter().map(|x| x.len()))
	let per_flow_q_len = self.mqfq_set.iter().map(|x| x.value().lock().queue.len());
	let s:usize = per_flow_q_len.sum();
	s 
    }

    fn est_completion_time(&self, reg: &Arc<RegisteredFunction>, tid: &TransactionId) -> f64 {
	// sum_q (q_F-q_S) / max_in_flight
	let per_flow_wait_times = self.mqfq_set.iter().map(|x| x.value().lock().est_flow_wait());
	
	let total_wait: f64 = per_flow_wait_times.sum();
	let C = self.max_inflight as f64;
	
	total_wait / C 	
    }

    fn enqueue_item(&self, item: &Arc<EnqueuedInvocation>) -> Result<()> {
        self.add_invok_to_flow(item.clone());
        Ok(())
    }

    fn running(&self) -> u32 {
        self.ctrack.get_inflight() as u32
    }


    fn WarmHitP(&self, reg: &Arc<RegisteredFunction>, iat:f64) -> f64 {
	// if flowq doesnt exist or inactive, 0
	// else (active or throttled), but no guarantees
	// Average eviction time for the queue? eviction == q becomes inactive
	// 1 - e^-(AET/iat)
	let fname = &reg.fqdn ; 
	let f = self.mqfq_set.get(fname);
	match f {
	    Some(fq) => {
		let aet = fq.value().lock().avg_active_t;
		let r = -aet/iat;
		return 1.0 - r.exp();		
	    },
	    None => return 0.0,
	}
	return 0.0
    }
    
}
