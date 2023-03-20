use std::collections::BinaryHeap;
use std::sync::Arc;
use anyhow::Result;
use parking_lot::Mutex;
use time::OffsetDateTime;
use tracing::debug;
use super::InvokerQueuePolicy;
use super::invoker_structs::{MinHeapEnqueuedInvocation, EnqueuedInvocation};

pub struct FCFSQueue {
  invoke_queue: Arc<Mutex<BinaryHeap<MinHeapEnqueuedInvocation<OffsetDateTime>>>>,
  est_time: Mutex<f64>
}

impl FCFSQueue {
  pub fn new() -> Result<Arc<Self>> {
    let svc = Arc::new(FCFSQueue {
      invoke_queue: Arc::new(Mutex::new(BinaryHeap::new())),
      est_time: Mutex::new(0.0),
    });
    Ok(svc)
  }
}

#[tonic::async_trait]
impl InvokerQueuePolicy for FCFSQueue {
  fn peek_queue(&self) -> Option<Arc<EnqueuedInvocation>> {
    let r = self.invoke_queue.lock();
    let r = r.peek()?;
    Some(r.item.clone())
  }
  fn pop_queue(&self) -> Arc<EnqueuedInvocation> {
    let mut invoke_queue = self.invoke_queue.lock();
    let v = invoke_queue.pop().unwrap();
    let v = v.item.clone();
    let mut func_name = "empty"; 
    if let Some(e) = invoke_queue.peek() {
      func_name = e.item.registration.function_name.as_str();
    }
    debug!(tid=%v.tid,  "Popped item from queue fcfs heap - len: {} popped: {} top: {} ",
           invoke_queue.len(), v.registration.function_name, func_name );
    v
  }
  fn queue_len(&self) -> usize { self.invoke_queue.lock().len() }
  fn est_queue_time(&self) -> f64 { 
    *self.est_time.lock() 
  }
  
  #[cfg_attr(feature = "full_spans", tracing::instrument(skip(self, item, _index), fields(tid=%item.tid)))]
  fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>, _index: Option<usize>) -> Result<()> {
    *self.est_time.lock() += item.est_execution_time;
    let mut queue = self.invoke_queue.lock();
    queue.push(MinHeapEnqueuedInvocation::new(item.clone(), item.queue_insert_time));
    Ok(())
  }
}
