use crate::services::containers::containermanager::ContainerManager;
use anyhow::Result;
use iluvatar_library::characteristics_map::CharacteristicsMap;
use parking_lot::Mutex;
use std::collections::BinaryHeap;
use std::sync::Arc;
use time::OffsetDateTime;
use tracing::debug;

use super::{EnqueuedInvocation, InvokerCpuQueuePolicy, MinHeapEnqueuedInvocation};

/// A single queue of entities (invocations) of the same priority/locality class 
pub struct EntityQ {
    qid: String, /// Q name for indexing/debugging etc 
    queue: Vec<EnqueuedInvocation>, /// Simple FIFO for now 
    weight: f64,
    Sv: f64, /// Virtual start time. S = max(V, F) on insert 
    Fv: f64, /// Virtual finish time. F = S + Budget/W
    FReal: f64, /// Actual service time may be different. Updated after function completion somehow? 
    budget: f64, /// Is the time/service allocation 
    /// Lazily update it on completion etc? Can be negative if function takes longer than estimated . Dont schedule another entity if we still have positive budget, even if other entities have items enqueued. This allows anticipatory scheduling (non work-conserving) for locality/priority preservation.
    remaining_budget:f64, /// Remaining service time
    last_epoch:i64, /// Useful to keep track of most/least recently scheduled class?  
	
}

enum wfq_type {
    locality, /// each function in its own class, mainly for GPUs. 
    priority /// High/low priority CPU functions. Not suitable. 
}

/// TODO: How to handle task parallelism? Esp for CPUs, granting exclusive access to one EntityQ wont work. CPU schedulers tackle this with one runQ per CPU. Except BFS, which is one global runQ and virtual deadlines and some __ CPU assignment?
/// Makes no sense for CPUs, just use single runQ sorted by priority+arrival+completion for priorities. 
/// 

pub struct WFQueue {
    wfq_set: DashMap<String, EntityQ>, /// Keyed by qid. 
    num_classes: i32,
    qlb_fn: wfq_type, /// Supposed to assign functions to queues

    /// WFQ State 
    VService: f64, /// System-wide logical clock for resources consumed
    /// With k-parallelism, does this have to be a k-vector? 
    active_class: Arc<EntityQ>, /// qid. Vector for multi-head ? 
    tquantum: f32,
    epoch:i64, /// Each active entity class scheduled is one epoch. 
    
    est_time: Mutex<f64>,
    cont_manager: Arc<ContainerManager>,
    cmap: WorkerCharMap,
}

impl WFQueue {
    pub fn new(cont_manager: Arc<ContainerManager>,
	       cmap: WorkerCharMap,
	       num_classes:i32,
	       qlb_fn:wfq_type) -> Result<Arc<Self>> {
        let svc = Arc::new(WFQueue {
	    VService:0.0,
	    num_classes:num_classes,
	    epoch: 0,
	    wfq_set:mk_wfq_set(num_classes), /// 
	    qlb_fn:qlb_fn,
            est_time: Mutex::new(0.0),
            cmap,
            cont_manager,
        });
        Ok(svc)
    }

    /// Returning a string allows easier decoupling?
    /// What about the 'misc' class?
    /// Needs separate lookup + insert 
    fn fn_to_EQ(efn:EnqueuedInvocation) -> String {
	let fname: String = efn.registration.fqdn;
	fname //Keep the same name, just for easier debugging? 
    }
    
    /// EntityQ * num_classes but arranged somehow. 
    fn mk_wfq_set(num_classes:i32) {
	
    }

    /// Current class if there are credits remaining. Else find next class (by some sorting metric) and return it 
    fn get_next_class(WFQueue:&WFQueue) -> Arc<EntityQ> {

    }

    /// Function just finished running. Completion call-back. 
    // fn charge_fn(efn: EnqueuedInvocation) {

    // }
    
}

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
        debug!(tid=v.tid,  "Popped item from queue fcfs heap - len: {} popped: {} top: {} ",
           invoke_queue.len(), v.registration.function_name, func_name );
        v
    }
    fn queue_len(&self) -> usize {
        self.invoke_queue.lock().len()
    }
    fn est_queue_time(&self) -> f64 {
        *self.est_time.lock()
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, item, _index), fields(tid=item.tid)))]
    fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>, _index: Option<usize>) -> Result<()> {
        let est_wall_time = self.est_wall_time(item, &self.cont_manager, &self.cmap)?;
        *self.est_time.lock() += est_wall_time;
        let mut queue = self.invoke_queue.lock();
        queue.push(MinHeapEnqueuedInvocation::new(
            item.clone(),
            item.queue_insert_time,
            est_wall_time,
        ));
        Ok(())
    }
}
