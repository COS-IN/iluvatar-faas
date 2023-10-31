use crate::services::containers::containermanager::ContainerManager;
use anyhow::Result;
use iluvatar_library::characteristics_map::CharacteristicsMap;
use parking_lot::Mutex;
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

use super::{EnqueuedInvocation, InvokerCpuQueuePolicy, MinHeapEnqueuedInvocation};

/// Multi-Queue Fair Queueing.
/// Refer to ATC '19 paper by Hedayati et.al.
/// Key modifications:
///   1. Concurrency with D tokens.
///   2. Grace period for anticipatory batching.
/// Each function is its own flow. 

enum MQState {
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


struct MQRequest {
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
    // ms to wait for next arrival if empty
    last_serviced: OffsetDateTime,
    service_avg: f64,
    // avg service time in ms
    allowed_overrun: f64,
    // Max service this flow can be ahead of others
}

impl FlowQ {
    pub fn new(qid: String,
               Sv: f64,
               weight: f64,
    ) -> Arc<Self> {
        let svc = Arc::new(FlowQ {
            qid: qid,
            queue: VecDeque::new(),
            weight: weight,
            state: MQState::Inactive,
            Sv,
            Fv: 0.0,
            in_flight: 0,
            grace_period: 100.0,
            last_serviced: OffsetDateTime::now_utc(),
            service_avg: 0.1,
            allowed_overrun: 10.0,
        });
        svc
    }

    /// Return True if should update the global time 
    pub fn push_flowQ(&mut self, item: Arc<EnqueuedInvocation>,
                      VT: f64) -> bool {
        let S = f64::max(VT, self.Fv); // cognizant of weights
        let F = S + (self.service_avg / self.weight);
        let r = MQRequest::new(item, S, F);

        self.queue.push_back(r);

        self.state = MQState::Active;
        self.Sv = f64::max(self.Sv, S);  // if this was 0?
        self.Fv = f64::max(r.Fv, self.Fv); // always needed

        self.queue.len() == 1
        //self.Sv = r.Sv; // only if the first element!
    }

    /// Remove oldest item. No other svc state update. 
    pub fn pop_flowQ(&mut self, VT:f64) -> Option<Arc<MQRequest>> {
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
    pub fn update_dispatched(&mut self, VT:f64) -> () {
	self.last_serviced = OffsetDateTime::now_utc();
	self.in_flight = self.in_flight + 1 ;
	let next_item = self.queue.front();
	match next_item {
	    Some(next_item) =>  {self.Sv = next_item.Sv;}
	    None => { } 
	}
	
	let gap  = self.Sv - VT; // VT is old Sv, but is Sv updated?
	if gap > self.allowed_overrun {
	    self.state = MQState::Throttled; 
	}
    }

    /// The VT may have advanced, so reset throttle. Call on dispatch 
    pub fn reset_throttle(&mut self, VT:f64) -> () {
	let gap  = self.Sv - VT; // VT is old Sv, but is Sv updated?
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
    mqfq_set: DashMap<String, Arc<FlowQ>>,
    /// Keyed by function name  (qid)

    VT: f64,
    /// System-wide logical clock for resources consumed
    tokens: Arc<TokenBucket>,
    /// At most this many number of queues to dispatch from
    /// With k-parallelism, does this have to be a k-vector?

    est_time: Mutex<f64>,
    /// Convenience for the trait methods?

    ///Remaining passed by gpu_q_invoke
    cont_manager: Arc<ContainerManager>,
    cmap: Arc<CharacteristicsMap>,
}

/// TODO: Pass concurrency semaphore from gpu_q_invoke
/// TODO: config with D, T, wts, etc.

impl MQFQ {
    pub fn new(cont_manager: Arc<ContainerManager>,
               cmap: Arc<CharacteristicsMap>,
    ) -> Result<Arc<Self>> {
        let svc = Arc::new(MQFQ {
            mqfq_set: DashMap::new(),
            est_time: Mutex::new(0.0),
            VT: 0.0,
            tokens: TokenBucket::new(3),
            cmap,
            cont_manager,
        });
        Ok(svc)
    }


    /// Earliest eligible flow 
    fn next_flow(&self) -> &Arc<FlowQ> {
	
        fn filter_avail_flow(x:&RefMulti<'_,String, Arc<FlowQ>>) -> bool {
	    let flow = *x.value() ; 
	    let out = match flow.state {
		MQState::Active => true ,
		_ => false
	    };
	    out 
        }

	// reset throttle for all flows 
	for x in self.mqfq_set.iter() {
	    x.value().reset_throttle(self.VT); 
	}
	
        // Active, not throttled, and lowest Sv
        let avail_flows = self.mqfq_set.iter().filter(filter_avail_flow);
	
	    // filter(|(&k, &v)| {match v.state {
	    // 	MQState::Active => true ,
	    // 	_ => false
	    // }});

        let chosen_q = avail_flows.min_by(|x, y| x.Sv.partial_cmp(&y.Sv).unwrap()).unwrap();
        let cq = chosen_q.deref();
        cq
    }

    /// Main 
    fn dispatch(&self) -> Option<Arc<MQRequest>> {
        /// Filter by active queues, and select with lowest start time.
        // How to avoid hoarding of the tokens? Want round-robin.
        if !self.tokens.get_tok() {
            return None ;
        }
        let mut chosen_q = self.next_flow();
        let item = chosen_q.pop_flowQ(self.VT);
	match item {
	    Some(i) => {
		self.VT = f64::max(self.VT, i.Sv) ; // dont want it to go backwards
		chosen_q.update_dispatched(self.VT);
		Some(i) 
	    }
	    None => {None}
	}
	// Update MQFQ State 
    }


    /// Function just finished running. Completion call-back. Add tokens? 
    fn charge_fn(efn: EnqueuedInvocation) -> () {}

    fn add_invok_to_flow(&self, item: Arc<EnqueuedInvocation>) -> &Arc<FlowQ> {
        let fname = item.registration.fqdn.clone();
        let mut qret ;
        // Lookup flow if exists
        if self.mqfq_set.contains_key(fname.as_str()) {
            let mut fq = self.mqfq_set.get_mut(fname.as_str()).unwrap();
            qret = fq.value();
        } // else, create the FlowQ, add to set, and add item to flow and
        else {
            let mut newq = FlowQ::new(fname.clone(), 0.0, 1.0);
            self.mqfq_set.insert(fname.clone(), newq);
            qret = &newq
        }
        qret.push_flowQ(item, self.VT); //? Always do that here?
        qret
    }
}

#[tonic::async_trait]
impl GpuQueuePolicy for MQFQ {
    /// This is limited to the five functions add, peek, pop, len, time
    /// Of which, we len/time are easy. Add should inject function into the right queue based on some property. pop/peek both select the right ``current'' queue and the function within this queue.


    /// Main request dispatch.
    // TODO: Can return None, refactor the GpuQpolicy trait and gpu_q_invok
    fn pop_queue(&self) -> Option<GpuBatch> {
	//Arc<EnqueuedInvocation>> {
        let to_run = self.dispatch();
        match to_run {
            Some(t) => {
		let i = &t.invok;
		let g = GpuBatch::new(*i, 1.0);
		Some(g)
	   },
            None => {None }
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


