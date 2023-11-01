use crate::services::containers::containermanager::ContainerManager;
use anyhow::Result;
use iluvatar_library::characteristics_map::CharacteristicsMap;
use parking_lot::{Mutex, RwLock};
use std::collections::{BinaryHeap, VecDeque};
use std::sync::Arc;
use std::cmp;
use std::cmp::Ordering;
use std::ops::Deref;
use dashmap::DashMap;
use time::OffsetDateTime;
use tracing::debug;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use crate::services::{
    invocation::gpu_q_invoke::{GpuBatch, GpuQueuePolicy},
    registration::RegisteredFunction,
};
use dashmap::mapref::multiple::RefMulti;
use crate::services::invocation::completion_time_tracker::CompletionTimeTracker;

use super::{EnqueuedInvocation, InvokerCpuQueuePolicy, MinHeapEnqueuedInvocation};

/// Multi-Queue Fair Queueing.
/// Refer to ATC '19 paper by Hedayati et.al.
/// Key modifications:
///   1. Concurrency with D tokens.
///   2. Grace period for anticipatory batching.
/// Each function is its own flow. 

pub enum MQState {
    Active,
    /// Non-empty queues are active
    Throttled,
    /// Non-empty but not considered for scheduling
    Inactive,
}

enum MQEvent {
    GraceExpired,
    RequestDispatched,
    NewRequest,
    RequestCancelled,
}

struct TimerEvent {
    deadline: OffsetDateTime,
    // When the timer should fire
    flow: Arc<FlowQ>,
    ev_type: MQEvent,
}

struct TimerWheel {
    twheel: Vec<TimerEvent>,
}


pub struct MQRequest {
    invok: Arc<EnqueuedInvocation>,
    // Do we maintain a backward pointer to FlowQ? qid atleast? 
    Sv: f64,
    Fv: f64,
}

impl MQRequest {
    pub fn new(invok: Arc<EnqueuedInvocation>,
               S: f64,
               F: f64) -> Arc<Self> {
        let svc = Arc::new(MQRequest {
            invok: invok,
            Sv: S,
            Fv: F,
        });
        svc
    }
}

/// A single queue of entities (invocations) of the same priority/locality class 
pub struct FlowQ {
    qid: String,
    // Q name for indexing/debugging etc
    queue: VecDeque<Arc<MQRequest>>,
    // Simple FIFO for now
    weight: f64,
    // (0,1]
    state: MQState,
    Sv: f64,
    // Virtual start time. S = max(VT, flow.F) on insert
    Fv: f64,
    // Virtual finish time. F = S + service_avg/Wt
    in_flight: i32,
    // Number concurrently executing, to enforce cap?
    // Actual service time may be different. Updated after function completion somehow?
    grace_period: f64,
    // s to wait for next arrival if empty
    last_serviced: OffsetDateTime,
    service_avg: f64,
    // avg service time in s
    allowed_overrun: f64,
    // Max service this flow can be ahead of others
}

impl FlowQ {
    pub fn new(qid: String,
               Sv: f64,
               weight: f64,
    ) -> Self {
        let svc = //Arc::new(
	    FlowQ {
            qid: qid,
            queue: VecDeque::new(),
            weight: weight,
            state: MQState::Inactive,
            Sv,
            Fv: 0.0,
            in_flight: 0,
            grace_period: 20.0,
            last_serviced: OffsetDateTime::now_utc(),
            service_avg: 10.0,
            allowed_overrun: 10.0,
            };
    //);
        svc
    }

    /// Return True if should update the global time 
    pub fn push_flowQ(&mut self, item: Arc<EnqueuedInvocation>,
                      VT: f64) -> bool {
        let S = f64::max(VT, self.Fv); // cognizant of weights
        let F = S + (self.service_avg / self.weight);
        let r = MQRequest::new(item, S, F);
        let rFv = r.Fv;

        self.queue.push_back(r.clone());

        self.state = MQState::Active;
        self.Sv = f64::max(self.Sv, S);  // if this was 0?
        self.Fv = f64::max(rFv, self.Fv); // always needed

        self.queue.len() == 1
        //self.Sv = r.Sv; // only if the first element!
    }

    /// Remove oldest item. No other svc state update. 
    pub fn pop_flowQ(&mut self, VT: f64) -> Option<Arc<MQRequest>> {
        let r = self.queue.pop_front();

        if self.queue.len() == 0 {
            // Turn inactive
            self.state = MQState::Inactive;
            self.Fv = 0.0; // This clears the history if empty queue. Do we want that?
            self.Sv = 0.0;
        }
        self.update_dispatched(VT);
        // MQFQ should remove from the active list if not ready
        r
    }

    /// Check if the start time is ahead of global time by allowed overrun 
    pub fn update_dispatched(&mut self, VT: f64) -> () {
        self.last_serviced = OffsetDateTime::now_utc();
        self.in_flight = self.in_flight + 1;
        let next_item = self.queue.front();
        match next_item {
            Some(next_item) => { self.Sv = next_item.Sv; }
            None => {}
        }
        // start timer for grace period?
        let gap = self.Sv - VT; // VT is old Sv, but is Sv updated?
        if gap > self.allowed_overrun {
            self.state = MQState::Throttled;
        }
    }

    /// The VT may have advanced, so reset throttle. Call on dispatch 
    pub fn set_idle_throttled(&mut self, VT: f64) -> () {
        let gap = self.Sv - VT; // VT is old Sv, but is Sv updated?
        // check grace period
        if self.queue.len() == 0 {
            let grace_remaining = (OffsetDateTime::now_utc() - self.last_serviced).as_seconds_f64();
            if grace_remaining > self.grace_period {
                self.state = MQState::Inactive;
            }
        }
        if gap <= self.allowed_overrun {
            self.state = MQState::Active;
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
    /// TODO: Concurent MQFQ. mqfq_set can be behind mutex. token bucket is separate semaphore.
    /// This leaves VT Can be left unprotected since its only modified on add/remove protected by the main mqfq mutex?
    /// We can have a separate one for VT, but not going to help?
    mqfq_set: DashMap<String, Arc<Mutex<FlowQ>>>,
    /// Keyed by function name  (qid)

    VT: RwLock<f64>,
    /// System-wide logical clock for resources consumed
    max_inflight: i32,
    /// At most this many number of queues to dispatch from
    /// With k-parallelism, does this have to be a k-vector?

    est_time: Mutex<f64>,
    /// Convenience for the trait methods?

    ///Remaining passed by gpu_q_invoke
    cont_manager: Arc<ContainerManager>,
    cmap: Arc<CharacteristicsMap>,
    /// Use this as a token bucket?
    ctrack: Arc<CompletionTimeTracker>,
}

/// TODO: Pass concurrency semaphore from gpu_q_invoke
/// TODO: config with D, T, wts, etc.

impl MQFQ {
    pub fn new(cont_manager: Arc<ContainerManager>,
               cmap: Arc<CharacteristicsMap>,
               ctrack: Arc<CompletionTimeTracker>,
    ) -> Result<Arc<Self>> {
        let svc = Arc::new(MQFQ {
            mqfq_set: DashMap::new(),
            est_time: Mutex::new(0.0),
            VT: RwLock::new(0.0),
            max_inflight: 4,
            cmap,
            cont_manager,
            ctrack,
        });
        Ok(svc)
    }

    /// Get or create FlowQ 
    fn add_invok_to_flow(&self, item: Arc<EnqueuedInvocation>) -> () {
        let fname = item.registration.fqdn.clone();
        let vt = self.VT.read().clone();
        //        let qret:Arc<FlowQ>;
        // Lookup flow if exists
        if self.mqfq_set.contains_key(fname.as_str()) {
            let fq = self.mqfq_set.get_mut(fname.as_str()).unwrap();
            let mut qret = fq.value().lock();
            qret.push_flowQ(item, vt); //? Always do that here?
        } // else, create the FlowQ, add to set, and add item to flow and
        else {
            let qguard = Arc::new(Mutex::new(FlowQ::new(fname.clone(), 0.0, 1.0)));
	    let mut qret = qguard.lock(); 
	    qret.push_flowQ(item, vt); //? Always do that here?
	    // let qret = qguard.lock();
	    
            self.mqfq_set.insert(fname.clone(), qguard.clone());
        }
    }


    /// Earliest eligible flow 
    fn next_flow(&self) -> Option<Arc<Mutex<FlowQ>>> {
        let vrg = self.VT.read();
        let vt = vrg.clone();
        drop(vrg);
	// TODO: Should be <Mutex<Arc<FlowQ>> ? 
        fn filter_avail_flow(x: &RefMulti<'_, String, Arc<Mutex<FlowQ>>>) -> bool {
            let flow = x.value().lock();
            let out = match flow.state {
                MQState::Active => true,
                _ => false
            };
            out
        }

        // reset throttle for all flows
        for x in self.mqfq_set.iter_mut() {
            x.value().lock().set_idle_throttled(vt);
        }

        // Active, not throttled, and lowest Sv
        let avail_flows = self.mqfq_set.iter().filter(filter_avail_flow);

        // filter(|(&k, &v)| {match v.state {
        // 	MQState::Active => true ,
        // 	_ => false
        // }});

        let chosen_q = avail_flows.min_by(|x, y| x.value().lock().Sv.partial_cmp(& y.deref().lock().Sv).unwrap()).unwrap();
        let cq = chosen_q.value();
        Some(cq.clone())
    }

    // Invoked functions automatically increase the count, conversely for finished functions
    fn enough_tokens(&self) -> bool {
        self.ctrack.get_inflight() < self.max_inflight
    }

    /// Main 
    fn dispatch(&mut self) -> Option<Arc<MQRequest>> {
        /// Filter by active queues, and select with lowest start time.
        // How to avoid hoarding of the tokens? Want round-robin.
        let vrg = self.VT.read();
        let vt = vrg.clone();
        drop(vrg);

        if !self.enough_tokens() {
            return None;
        }
	
        let nq = self.next_flow();
	//if nq.is_none() {
	//    return None 
	//}

	match nq {
	    None => {None}
	    Some(cq) => {
		let mut chosen_q = cq.lock();
		let item = chosen_q.pop_flowQ(vt);
		match item {
		    Some(i) => {
			let updated_vt = f64::max(vt, i.Sv); // dont want it to go backwards
			let mut vwg= self.VT.write();
			*vwg = updated_vt.clone();
			drop(vwg);
			chosen_q.update_dispatched(updated_vt);
			Some(i)
		    }
		    None => { None }
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
    // TODO: Can return None, refactor the GpuQpolicy trait and gpu_q_invok
    fn pop_queue(&self) -> GpuBatch {
        let vrg = self.VT.read();
        let vt = vrg.clone();
        drop(vrg);

	// TODO: critical to fix. 
        // if !self.enough_tokens() {
        //     return None;
        // }
	
        let nq = self.next_flow();

	let to_run = match nq {
	    None => {None}
	    Some(cq) => {
		let mut chosen_q = cq.lock();
		let item = chosen_q.pop_flowQ(vt);
		match item {
		    Some(i) => {
			let updated_vt = f64::max(vt, i.Sv); // dont want it to go backwards
			let mut vwg= self.VT.write();
			*vwg = updated_vt.clone();
			drop(vwg);
			chosen_q.update_dispatched(updated_vt);
			Some(i)
		    }
		    None => { None }
		}
	    }
	};
	
        match to_run {
            Some(t) => {
                let i = &t.invok;
                let g = GpuBatch::new(i.clone(), 1.0);
                g 
            }
            None => { panic!("Nothing in queue to run") }
            // Asked to run something, but are throttled. Return None?
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


