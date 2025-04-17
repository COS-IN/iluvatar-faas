use super::{EnqueuedInvocation, InvokerCpuQueuePolicy, MinHeapEnqueuedInvocation, MinHeapFloat};
use crate::services::containers::{containermanager::ContainerManager, structs::ContainerState};
use anyhow::Result;
use iluvatar_library::char_map::{Chars, WorkerCharMap};
use iluvatar_library::transaction::TransactionId;
use parking_lot::Mutex;
use std::collections::BinaryHeap;
use std::sync::Arc;
use tracing::debug;

/// An invoker that scales concurrency based on system load
/// Prioritizes based on container availability
pub struct AvailableScalingQueue {
    cont_manager: Arc<ContainerManager>,
    invoke_queue: Arc<Mutex<BinaryHeap<MinHeapFloat>>>,
    cmap: WorkerCharMap,
    est_time: Mutex<f64>,
}

impl AvailableScalingQueue {
    pub fn new(cont_manager: Arc<ContainerManager>, tid: &TransactionId, cmap: WorkerCharMap) -> Result<Arc<Self>> {
        let svc = Arc::new(AvailableScalingQueue {
            cont_manager,
            invoke_queue: Arc::new(Mutex::new(BinaryHeap::new())),
            cmap,
            est_time: Mutex::new(0.0),
        });
        debug!(tid = tid, "Created AvailableScalingInvoker");
        Ok(svc)
    }
}

impl InvokerCpuQueuePolicy for AvailableScalingQueue {
    fn queue_len(&self) -> usize {
        self.invoke_queue.lock().len()
    }
    fn est_queue_time(&self) -> f64 {
        *self.est_time.lock()
    }

    fn peek_queue(&self) -> Option<Arc<EnqueuedInvocation>> {
        let r = self.invoke_queue.lock();
        let r = r.peek()?;
        let r = r.item.clone();
        Some(r)
    }
    fn pop_queue(&self) -> Arc<EnqueuedInvocation> {
        let mut invoke_queue = self.invoke_queue.lock();
        let v = invoke_queue.pop().unwrap();
        *self.est_time.lock() -= v.est_wall_time;
        v.item
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, item, _index), fields(tid=item.tid)))]
    fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>, _index: Option<usize>) -> Result<()> {
        let mut priority = 0.0;
        if self.cont_manager.outstanding(&item.registration.fqdn) == 0 {
            priority = self.cmap.get_avg(&item.registration.fqdn, Chars::CpuWarmTime);
        }
        priority = match self
            .cont_manager
            .container_available(&item.registration.fqdn, iluvatar_library::types::Compute::CPU)
        {
            ContainerState::Warm => priority,
            ContainerState::Prewarm => self.cmap.get_avg(&item.registration.fqdn, Chars::CpuPreWarmTime),
            _ => self.cmap.get_avg(&item.registration.fqdn, Chars::CpuColdTime),
        };
        *self.est_time.lock() += priority;
        let mut queue = self.invoke_queue.lock();
        queue.push(MinHeapEnqueuedInvocation::new_f(item.clone(), priority, priority));
        debug!(
            tid = item.tid,
            component = "minheap",
            "Added item to front of queue minheap - len: {} arrived: {} top: {} ",
            queue.len(),
            item.registration.fqdn,
            queue.peek().unwrap().item.registration.fqdn
        );
        Ok(())
    }
}
