use crate::services::containers::containermanager::ContainerManager;
use anyhow::Result;
use iluvatar_library::characteristics_map::CharacteristicsMap;
use parking_lot::Mutex;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::cmp;
use time::OffsetDateTime;
use tracing::debug;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::{EnqueuedInvocation, InvokerCpuQueuePolicy, MinHeapEnqueuedInvocation};

/// Multi-Queue Fair Queueing.
/// Refer to ATC '19 paper by Hedayati et.al.
/// Key modifications:
///   1. Concurrency with D tokens.
///   2. Grace period for anticipatory batching.
/// Each function is its own flow. 

enum MQState {
    Active,  /// Non-empty queues are active 
    Throttled, /// Non-empty but not considered for scheduling 
    Inactive 
}

enum MQEvent {
    GraceExpired,
    RequestDispatched,
    NewRequest,
    RequestCancelled
}

struct TimerEvent {
    deadline: OffsetDateTime, /// When the timer should fire
    flow: Arc<FlowQ>,
    ev_type: MQEvent 
}

struct TimerWheel {
    twheel: Vec<TimerEvent>, /// Probably keep it sorted by deadline? 
}


struct MQRequest {
    invok: Arc<EnqueuedInvocation>,
    /// Do we maintain a backward pointer to FlowQ? qid atleast? 
    Sv: f64,
    Fv: f64,
}

impl MQRequest {
    pub fn new(invok: Arc<EnqueuedInvocation>,
	       S:f64,
	       F:F64) -> Result<Arc<Self>> {
	let svc = Arc::new(MQRequest {
	    invok: invok,
	    Sv:S,
	    Fv:F
	});
	Ok(svc)		   
    }
}

/// A single queue of entities (invocations) of the same priority/locality class 
pub struct FlowQ {
    qid: String, /// Q name for indexing/debugging etc 
    queue: VecDeque<MQRequest>, /// Simple FIFO for now 
    weight: f64, /// (0,1] 
    state: MQState,
    Sv: f64, /// Virtual start time. S = max(VT, flow.F) on insert 
    Fv: f64, /// Virtual finish time. F = S + service_avg/Wt
    in_flight: i32, /// Number concurrently executing, to enforce cap? 
    max_in_flight: i32, /// Avoiding monopolization? 
    FReal: f64, /// Actual service time may be different. Updated after function completion somehow? 
    budget: f64, /// Is the time/service allocation 
    /// Lazily update it on completion etc? Can be negative if function takes longer than estimated . Dont schedule another entity if we still have positive budget, even if other entities have items enqueued. This allows anticipatory scheduling (non work-conserving) for locality/priority preservation.
    grace_period: f64, /// ms to wait for next arrival if empty
    service_avg: f64, /// avg service time in ms 
    remaining_budget:f64, /// Remaining service time
    last_epoch:i64, /// Useful to keep track of most/least recently scheduled class?  
}

impl FlowQ {
    pub fn new(qid: String,
	       Sv:f64,
	       weight: f64,
    ) -> Result<Arc<Self>> {
	let svc = Arc::new(FlowQ {
	    qid: qid,
	    queue:Vec::new(),
	    weight:weight,
	    state:MQState::Inactive,
	    grace_period: 100.0,
	    service_avg: 0.1,
	});
	Ok(svc)			   
    }

    /// Return True if should update the global time 
    pub fn push_flowQ(&self, item:Arc<EnqueuedInvocation>,
		     VT: f64) -> bool {
	let S  = cmp::max(VT, self.Fv) ; // cognizant of weights 
	let F = S + (self.service_avg/self.weight);
	let r = MQRequest::new(item, S, F);
	
	self.queue.push_back(r);

	self.state = MQState::Active;
	self.Sv = cmp::max(self.Sv, S);  // if this was 0? 
	self.Fv = cmp::max(r.Fv, self.Fv); // always needed 
	
	self.queue.len() == 1 
	//self.Sv = r.Sv; // only if the first element! 
    }

    /// Remove oldest item. No other svc state update. 
    pub fn pop_flowQ(&self) -> Arc<MQRequest> {
	let r = self.queue.pop_front();
	
	if self.queue.len() == 0 {
	    /// Turn inactive 
	    self.state = MQState::Inactive; 
	    self.Fv = 0; // This clears the history if empty queue. Do we want that? 
	    self.Sv = 0; 
	}
	/// MQFQ should remove from the active list if not ready
	r
    }
}


/// TODO: How to handle task parallelism? Esp for CPUs, granting exclusive access to one EntityQ wont work. CPU schedulers tackle this with one runQ per CPU. Except BFS, which is one global runQ and virtual deadlines and some __ CPU assignment?
/// Makes no sense for CPUs, just use single runQ sorted by priority+arrival+completion for priorities. 
/// 

pub struct MQFQ {
    mqfq_set: DashMap<String, FlowQ>, /// Keyed by qid. 
    num_flows: i32,
    max_concurrency: i32, /// At most this many number of queues to dispatch from 
    qlb_fn: String, /// How to assign functions to queues

    /// WFQ State 
    VT: f64, /// System-wide logical clock for resources consumed
    overrun: f64, /// how much a single flow can lead VT by 
    /// With k-parallelism, does this have to be a k-vector? 
    active_flows: Vec<Arc<FlowQ>>, /// qid. Vector for multi-head ? 
    tquantum: f32,
    epoch:i64, /// Each active entity class scheduled is one epoch. 
    
    
    est_time: Mutex<f64>,
    cont_manager: Arc<ContainerManager>,
    cmap: Arc<CharacteristicsMap>,
    concurrency_semaphore: Option<Arc<Semaphore>>,
    mqlock: Mutex<u32>,
    /// Separate locks for main mqfq and token-bucket may lead to capacity violations, but better than the contention overhead? 
}

impl MQFQ {
    pub fn new(cont_manager: Arc<ContainerManager>,
	       cmap: Arc<CharacteristicsMap>,
	       num_classes:i32,
	       /// TODO: Pass concurrency semaphore from gpu_q_invoke?
	       /// Need it for decrementing function completions 
	       qlb_fn:wfq_type) -> Result<Arc<Self>> {

        let sem = match config.count {
            0 => None,
            p => Some(Arc::new(Semaphore::new(p as usize))),
        };
	
        let svc = Arc::new(WFQueue {
	    VService:0.0,
	    num_classes:num_classes,
	    epoch: 0,
	    wfq_set:mk_wfq_set(num_classes), /// 
	    qlb_fn:qlb_fn,
            est_time: Mutex::new(0.0),
            cmap,
            cont_manager,
	    concurrency_semaphore: sem,
        });
        Ok(svc)
    }

    /// Main 
    fn dispatch(&self) -> Arc<MQRequest> {
	/// Filter by active queues, and select with lowest start time.
	// How to avoid hoarding of the tokens? Want round-robin.
	let tok = self.get_token() ;
	if Some(tok){
	    let active_mqs = self.mqfq_set.filter_by(MQState::Active) ;
	    let chosen_q = active_mqs.filter_by(q.Sv < self.VT + self.overrun).min();
	    // Check if this exists, else None 
	    let item = chosen_q.pop_flowQ();
	    self.VT = item.Sv; // Minimum of all start times, which this is.
	}
	else { // No tokens: return none. 
	    None;
	}
	    
	
    }
    

    /// Returning a string allows easier decoupling?
    /// What about the 'misc' class?
    /// Needs separate lookup + insert 
    fn fn_to_EQ(efn:EnqueuedInvocation) -> String {
	let fname: String = efn.registration.fqdn;
	fname //Keep the same name, just for easier debugging? 
    }
    
    /// EntityQ * num_classes but arranged somehow. 
    fn mk_wfq_set(num_classes:i32) -> () {
	
    }

    /// Function just finished running. Completion call-back. Add tokens? 
    fn charge_fn(efn: EnqueuedInvocation) -> () {

    }

    
    fn get_flow_for_invok(&self, item: Arc<EnqueuedInvocation>) -> Arc<FlowQ> {
	let fname = item.registration.fqdn;
	if let newq = Some(self.mqfq_set(fname)) {
	}
	else {
	    let newq = Arc::new(FlowQ::new({fname, 0, 1.0}));
	    newq.push_flowQ(item); //? Always do that here? 
	}
	newq 
    }
    
}

#[tonic::async_trait]
impl InvokerGpuQueuePolicy for WFQueue {
    /// This is limited to the five functions add, peek, pop, len, time
    /// Of which, we len/time are easy. Add should inject function into the right queue based on some property. pop/peek both select the right ``current'' queue and the function within this queue.
    
    fn peek_queue(&self) -> Option<Arc<EnqueuedInvocation>> {
        let r = self.invoke_queue.lock();
        let r = r.peek()?;
        Some(r.item.clone())
    }

    /// Main request dispatch.
    /// V = V + expected service? 
    fn pop_queue(&self) -> Arc<EnqueuedInvocation> {
        let mut invoke_queue = self.invoke_queue.lock();
        let v = invoke_queue.pop().unwrap();
        *self.est_time.lock() -= v.est_wall_time;
        let v = v.item;
        let mut func_name = "empty";
        if let Some(e) = invoke_queue.peek() {
            func_name = e.item.registration.function_name.as_str();
        }
        debug!(tid=%v.tid,  "Popped item from queue fcfs heap - len: {} popped: {} top: {} ",
           invoke_queue.len(), v.registration.function_name, func_name );
        v
    }
    fn queue_len(&self) -> usize {
        self.invoke_queue.lock().len()
    }
    fn est_queue_time(&self) -> f64 {
        *self.est_time.lock()
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item, _index), fields(tid=%item.tid)))]
    fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>, _index: Option<usize>) -> Result<()> {

	let flow = self.get_flow_for_invok(item);

        Ok(())
    }
}


