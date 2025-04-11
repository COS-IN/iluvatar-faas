use super::{EnqueuedInvocation, InvokerCpuQueuePolicy};
use anyhow::Result;
use std::{collections::VecDeque, sync::Arc};
use tracing::debug;

/// This implements a simple FCFS queue for all invocations
/// To turn it into a no-queue policy, it must be combined with setting [crate::worker_api::worker_config::ComputeResourceConfig::count] for CPU to `0`
///   This stops the worker from controlling CPU concurrency, allowing all invocations to run immediately
pub struct Queueless {
    async_queue: parking_lot::RwLock<VecDeque<Arc<EnqueuedInvocation>>>,
}
impl Queueless {
    pub fn new() -> Result<Arc<Self>> {
        let svc = Arc::new(Queueless {
            async_queue: parking_lot::RwLock::new(VecDeque::new()),
        });
        Ok(svc)
    }
}

#[allow(dyn_drop)]
impl InvokerCpuQueuePolicy for Queueless {
    fn queue_len(&self) -> usize {
        0
    }
    fn est_queue_time(&self) -> f64 {
        0.0
    }

    fn peek_queue(&self) -> Option<Arc<EnqueuedInvocation>> {
        let q = self.async_queue.read();
        if let Some(r) = q.front() {
            return Some(r.clone());
        }
        None
    }
    fn pop_queue(&self) -> Arc<EnqueuedInvocation> {
        self.async_queue.write().pop_front().unwrap()
    }

    #[cfg_attr(feature = "full_spans", tracing::instrument(level="debug", skip(self, item, _index), fields(tid=item.tid)))]
    fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>, _index: Option<usize>) -> Result<()> {
        let mut queue = self.async_queue.write();
        queue.push_back(item.clone());
        debug!(tid = item.tid, "Added item to front of queue; waking worker thread");
        Ok(())
    }
}
