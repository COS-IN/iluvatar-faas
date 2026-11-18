use crate::services::containers::containermanager::ContainerManager;
use crate::services::containers::structs::ContainerState;
use anyhow::Result;
use iluvatar_library::transaction::TransactionId;
use parking_lot::Mutex;
use std::sync::Arc;
use tracing::debug;

use super::{
    preview_heap_fqdn_counts, EnqueuedInvocation, InvokerCpuQueuePolicy, MinHeapEnqueuedInvocation, MinHeapFloat,
};
use iluvatar_library::char_map::{Chars, WorkerCharMap};
use iluvatar_library::clock::now;
use std::collections::BinaryHeap;
use tokio::time::Instant;

/// Earliest-deadline CPU queue with enqueue-time estimates for cold starts.
///
/// The estimate tries to avoid treating every invocation in a burst as warm just
/// because a warm container is idle at the instant the burst arrives. For each
/// function, queued entries consume the current snapshot of idle warm containers,
/// then prewarm containers; later entries are estimated as cold. This gives the
/// queue a rough view of the work the burst is likely to create and lets those
/// longer estimates influence its ordering.
///
/// This is deliberately best-effort. The snapshot is taken only when an entry is
/// enqueued: popped entries waiting elsewhere are not counted, and queued estimates
/// are not updated when a container later becomes idle. A cold estimate is only a
/// scheduling estimate, not a requirement to cold-start; if a container returns
/// before dispatch, the invocation can still use it.
pub struct MinHeapEDQueue {
    invoke_queue: Arc<Mutex<BinaryHeap<MinHeapFloat>>>,
    cmap: WorkerCharMap,
    est_time: Mutex<f64>,
    cont_manager: Arc<ContainerManager>,
    creation: Instant,
}

impl MinHeapEDQueue {
    pub fn new(tid: &TransactionId, cmap: WorkerCharMap, cont_manager: Arc<ContainerManager>) -> Result<Arc<Self>> {
        let svc = Arc::new(MinHeapEDQueue {
            invoke_queue: Arc::new(Mutex::new(BinaryHeap::new())),
            est_time: Mutex::new(0.0),
            cmap,
            cont_manager,
            creation: now(),
        });
        debug!(tid = tid, "Created MinHeapEDInvoker");
        Ok(svc)
    }
    fn time_since_creation(&self) -> f64 {
        now().duration_since(self.creation).as_secs_f64()
    }
}

impl InvokerCpuQueuePolicy for MinHeapEDQueue {
    fn peek_queue(&self) -> Option<Arc<EnqueuedInvocation>> {
        let r = self.invoke_queue.lock();
        let r = r.peek()?;
        Some(r.item.clone())
    }
    fn preview_fqdn_counts(&self, limit: usize) -> std::collections::HashMap<String, usize> {
        preview_heap_fqdn_counts(&self.invoke_queue.lock(), limit)
    }
    fn pop_queue(&self) -> Arc<EnqueuedInvocation> {
        let mut invoke_queue = self.invoke_queue.lock();
        let v = invoke_queue.pop().unwrap();
        *self.est_time.lock() -= v.est_wall_time;
        let v = v.item;
        let mut func_name = "empty";
        if let Some(e) = invoke_queue.peek() {
            func_name = e.item.registration.function_name.as_str();
        }
        debug!(
            tid = v.tid,
            component = "minheap",
            "Popped item from queue minheap - len: {} popped: {} top: {} ",
            invoke_queue.len(),
            v.registration.function_name,
            func_name
        );
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
        let mut queue = self.invoke_queue.lock();
        let fqdn = &item.registration.fqdn;
        // Use the number of same-function entries already waiting as a rough
        // reservation against the idle containers observed at this enqueue.
        let queued_for_function = queue
            .iter()
            .filter(|queued| queued.item.registration.fqdn == *fqdn)
            .count();
        let (warm_containers, prewarm_containers) = self.cont_manager.idle_cpu_container_state_counts(fqdn);
        let anticipated_state = if queued_for_function < warm_containers {
            ContainerState::Warm
        } else if queued_for_function < warm_containers + prewarm_containers {
            ContainerState::Prewarm
        } else {
            ContainerState::Cold
        };
        item.likely_cold.store(
            anticipated_state == ContainerState::Cold,
            std::sync::atomic::Ordering::Relaxed,
        );
        let est_wall_time = match anticipated_state {
            ContainerState::Warm => self.cmap.get_avg(fqdn, Chars::CpuWarmTime),
            ContainerState::Prewarm => self.cmap.get_avg(fqdn, Chars::CpuPreWarmTime),
            _ => self.cmap.get_avg(fqdn, Chars::CpuColdTime),
        };
        let deadline = est_wall_time + self.time_since_creation();
        queue.push(MinHeapEnqueuedInvocation::new_f(item.clone(), deadline, est_wall_time));
        *self.est_time.lock() += est_wall_time;
        debug!(
            tid = item.tid,
            component = "minheap",
            anticipated_state = ?anticipated_state,
            queued_for_function,
            warm_containers,
            prewarm_containers,
            est_wall_time,
            "Added item to queue minheap - len: {} arrived: {} top: {} ",
            queue.len(),
            item.registration.function_name,
            queue.peek().unwrap().item.registration.function_name
        );
        Ok(())
    }
}
