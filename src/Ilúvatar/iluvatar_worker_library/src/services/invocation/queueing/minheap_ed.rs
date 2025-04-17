use crate::services::containers::containermanager::ContainerManager;
use anyhow::Result;
use iluvatar_library::transaction::TransactionId;
use parking_lot::Mutex;
use std::sync::Arc;
use tracing::debug;

use super::{EnqueuedInvocation, InvokerCpuQueuePolicy, MinHeapEnqueuedInvocation, MinHeapFloat};
use iluvatar_library::char_map::{Chars, WorkerCharMap};
use iluvatar_library::clock::now;
use std::collections::BinaryHeap;
use tokio::time::Instant;

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
        let est_wall_time = self.est_wall_time(item, &self.cont_manager, &self.cmap)?;
        *self.est_time.lock() += est_wall_time;
        let mut queue = self.invoke_queue.lock();
        let deadline = self.cmap.get_avg(&item.registration.fqdn, Chars::CpuExecTime) + self.time_since_creation();
        queue.push(MinHeapEnqueuedInvocation::new_f(item.clone(), deadline, est_wall_time));
        debug!(
            tid = item.tid,
            component = "minheap",
            "Added item to front of queue minheap - len: {} arrived: {} top: {} ",
            queue.len(),
            item.registration.function_name,
            queue.peek().unwrap().item.registration.function_name
        );
        Ok(())
    }
}
