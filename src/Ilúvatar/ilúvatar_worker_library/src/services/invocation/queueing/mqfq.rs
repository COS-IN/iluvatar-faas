#![allow(non_snake_case, unused)]

use crate::services::containers::containermanager::ContainerManager;
use crate::services::invocation::completion_time_tracker::CompletionTimeTracker;
use crate::services::{
    invocation::gpu_q_invoke::{GpuBatch, GpuQueuePolicy},
    registration::RegisteredFunction,
};
use anyhow::Result;
use dashmap::mapref::multiple::RefMulti;
use dashmap::DashMap;
use iluvatar_library::characteristics_map::CharacteristicsMap;
use parking_lot::{Mutex, RwLock};
use std::cmp;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, VecDeque};
use std::ops::Deref;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::debug;

use super::{EnqueuedInvocation, InvokerCpuQueuePolicy, MinHeapEnqueuedInvocation};

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
    /// Actual service time may be different. Updated after function completion somehow?
    /// Is the TTL Keep-alive duration
    in_flight: i32,
    /// s to wait for next arrival if empty
    TTL_s: f64,
    last_serviced: OffsetDateTime,
    /// avg service time in s
    service_avg: f64,
    /// Max service this flow can be ahead of others
    allowed_overrun: f64,
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
            TTL_s: 20.0,
            last_serviced: OffsetDateTime::now_utc(),
            service_avg: 10.0,
            allowed_overrun: 10.0,
        }
    }

    /// Return True if should update the global time
    pub fn push_flowQ(&mut self, item: Arc<EnqueuedInvocation>, vitual_time: f64) -> bool {
        let start_t = f64::max(vitual_time, self.finish_time_virt); // cognizant of weights
        let finish_t = start_t + (self.service_avg / self.weight);
        let r = MQRequest::new(item, start_t, finish_t);
        let rFv = r.finish_time_virt;

        self.queue.push_back(r);

        self.state = MQState::Active;
        self.start_time_virt = f64::max(self.start_time_virt, start_t); // if this was 0?
        self.finish_time_virt = f64::max(rFv, self.finish_time_virt); // always needed

        self.queue.len() == 1
        //self.start_time_virt = r.start_time_virt; // only if the first element!
    }

    /// Remove oldest item. No other svc state update.
    pub fn pop_flowQ(&mut self, vitual_time: f64) -> Option<Arc<MQRequest>> {
        let r = self.queue.pop_front();

        if self.queue.is_empty() {
            // Turn inactive
            self.state = MQState::Inactive;
            self.finish_time_virt = 0.0; // This clears the history if empty queue. Do we want that?
            self.start_time_virt = 0.0;
        }
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
            let TTL_remaining = (OffsetDateTime::now_utc() - self.last_serviced).as_seconds_f64();
            if TTL_remaining > self.TTL_s {
                self.state = MQState::Inactive;
            }
        }
    }
}

/// TODO: Semaphore impl?
struct TokenBucket {
    capacity: i32,
    current: i32,
}

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
}

/// TODO: Pass concurrency semaphore from gpu_q_invoke
/// TODO: config with D, T, wts, etc.

impl MQFQ {
    pub fn new(
        cont_manager: Arc<ContainerManager>,
        cmap: Arc<CharacteristicsMap>,
        ctrack: Arc<CompletionTimeTracker>,
    ) -> Result<Arc<Self>> {
        let svc = Arc::new(MQFQ {
            mqfq_set: DashMap::new(),
            est_time: Mutex::new(0.0),
            vitual_time: RwLock::new(0.0),
            max_inflight: 4,
            cmap,
            cont_manager,
            ctrack,
        });
        Ok(svc)
    }

    /// Get or create FlowQ
    fn add_invok_to_flow(&self, item: Arc<EnqueuedInvocation>) -> () {
        let fname = &item.registration.fqdn;
        let vitual_time = *self.vitual_time.read();
        //        let qret:Arc<FlowQ>;
        // Lookup flow if exists
        if self.mqfq_set.contains_key(fname) {
            let fq = self.mqfq_set.get_mut(fname).unwrap();
            let mut qret = fq.value().lock().push_flowQ(item, vitual_time);
            // qret //? Always do that here?
        }
        // else, create the FlowQ, add to set, and add item to flow and
        else {
            let fname = item.registration.fqdn.clone();
            let qguard = Arc::new(Mutex::new(FlowQ::new(fname.clone(), 0.0, 1.0)));
            let mut qret = qguard.lock().push_flowQ(item, vitual_time);
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

        let nq = self.next_flow();
        //if nq.is_none() {
        //    return None
        //}

        match nq {
            None => None,
            Some(cq) => {
                let mut chosen_q = cq.lock();
                let item = chosen_q.pop_flowQ(vitual_time);
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
}

#[tonic::async_trait]
impl GpuQueuePolicy for MQFQ {
    /// This is limited to the five functions add, peek, pop, len, time
    /// Of which, we len/time are easy. Add should inject function into the right queue based on some property. pop/peek both select the right ``current'' queue and the function within this queue.

    /// Main request dispatch.
    fn pop_queue(&self) -> Option<GpuBatch> {
        let to_run = self.dispatch();
        match to_run {
            Some(t) => {
                let i = &t.invok;
                let g = GpuBatch::new(i.clone(), 1.0);
                Some(g)
            }
            None => {
                debug!("Nothing in queue to run");
                None
            } // Asked to run something, but are throttled. Return None?
        }
    }

    fn queue_len(&self) -> usize {
        1
    }
    fn est_queue_time(&self) -> f64 {
        *self.est_time.lock()
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item, _index), fields(tid = % item.tid)))]
    fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>) -> Result<()> {
        // Add mutex lock guard for all these trait methods . Coarse-grained, but MQFQ state machine a bit too complex
        self.add_invok_to_flow(item.clone());
        Ok(())
    }

    fn next_batch(&self) -> Option<Arc<RegisteredFunction>> {
        todo!()
        // This is used for checking available resources. Check the token bucket here and return None
    }

    fn queue_compress(&self) -> () {
        todo!()
    }
}
