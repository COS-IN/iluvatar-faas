use crate::services::containers::containermanager::ContainerManager;
use anyhow::Result;
use iluvatar_library::{characteristics_map::CharacteristicsMap, transaction::TransactionId};
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::debug;

use super::{EnqueuedInvocation, InvokerCpuQueuePolicy, MinHeapEnqueuedInvocation, MinHeapFloat};
use std::collections::BinaryHeap;

fn time_since_epoch() -> f64 {
    let start = SystemTime::now();
    start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs_f64()
}

pub struct MinHeapEDQueue {
    invoke_queue: Arc<Mutex<BinaryHeap<MinHeapFloat>>>,
    cmap: Arc<CharacteristicsMap>,
    est_time: Mutex<f64>,
    cont_manager: Arc<ContainerManager>,
}

impl MinHeapEDQueue {
    pub fn new(
        tid: &TransactionId,
        cmap: Arc<CharacteristicsMap>,
        cont_manager: Arc<ContainerManager>,
    ) -> Result<Arc<Self>> {
        let svc = Arc::new(MinHeapEDQueue {
            invoke_queue: Arc::new(Mutex::new(BinaryHeap::new())),
            est_time: Mutex::new(0.0),
            cmap,
            cont_manager,
        });
        debug!(tid=%tid, "Created MinHeapEDInvoker");
        Ok(svc)
    }
}

#[tonic::async_trait]
impl InvokerCpuQueuePolicy for MinHeapEDQueue {
    fn peek_queue(&self) -> Option<Arc<EnqueuedInvocation>> {
        let r = self.invoke_queue.lock();
        let r = r.peek()?;
        Some(r.item.clone())
    }
    fn pop_queue(&self) -> Arc<EnqueuedInvocation> {
        let mut invoke_queue = self.invoke_queue.lock();
        let v = invoke_queue.pop().unwrap();
        *self.est_time.lock() += v.est_wall_time;
        let v = v.item.clone();
        let mut func_name = "empty";
        if let Some(e) = invoke_queue.peek() {
            func_name = e.item.registration.function_name.as_str();
        }
        debug!(tid=%v.tid,  component="minheap", "Popped item from queue minheap - len: {} popped: {} top: {} ",
           invoke_queue.len(),
           v.registration.function_name,
           func_name );
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
        let est_wall_time = self.est_wall_time(item, &self.cont_manager, &self.cmap)?;
        *self.est_time.lock() += est_wall_time;
        let mut queue = self.invoke_queue.lock();
        let deadline = self.cmap.get_exec_time(&item.registration.fqdn) + time_since_epoch();
        queue.push(MinHeapEnqueuedInvocation::new_f(item.clone(), deadline, est_wall_time));
        debug!(tid=%item.tid,  component="minheap", "Added item to front of queue minheap - len: {} arrived: {} top: {} ", 
                        queue.len(),
                        item.registration.function_name,
                        queue.peek().unwrap().item.registration.function_name );
        Ok(())
    }
}
