use std::sync::Arc;
use iluvatar_library::{transaction::TransactionId, characteristics_map::CharacteristicsMap};
use anyhow::Result;
use parking_lot::Mutex;
use tracing::debug;
use crate::services::invocation::InvokerQueuePolicy;
use crate::services::invocation::{EnqueuedInvocation, MinHeapEnqueuedInvocation, MinHeapFloat};
use std::collections::BinaryHeap;

pub struct MinHeapIATQueue {
  invoke_queue: Arc<Mutex<BinaryHeap<MinHeapFloat>>>,
  cmap: Arc<CharacteristicsMap>,
  est_time: Mutex<f64>
}

impl MinHeapIATQueue {
  pub fn new(tid: &TransactionId, cmap: Arc<CharacteristicsMap>) -> Result<Arc<Self>> {
    let svc = Arc::new(MinHeapIATQueue {
      invoke_queue: Arc::new(Mutex::new(BinaryHeap::new())),
      est_time: Mutex::new(0.0),
      cmap,
    });
    debug!(tid=%tid, "Created MinHeapIATInvoker");
    Ok(svc)
  }
}

#[tonic::async_trait]
impl InvokerQueuePolicy for MinHeapIATQueue {
  fn peek_queue(&self) -> Option<Arc<EnqueuedInvocation>> {
    let r = self.invoke_queue.lock();
    let r = r.peek()?;
    let r = r.item.clone();
    return Some(r);
  }
  fn pop_queue(&self) -> Arc<EnqueuedInvocation> {
    let mut invoke_queue = self.invoke_queue.lock();
    let v = invoke_queue.pop().unwrap();
    let v = v.item.clone();
    let top = invoke_queue.peek();
    let func_name; 
    match top {
        Some(e) => func_name = e.item.registration.function_name.clone(),
        None => func_name = "empty".to_string()
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
  
  fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>, _index: Option<usize>) -> Result<()> {
    *self.est_time.lock() += item.est_execution_time;
    let mut queue = self.invoke_queue.lock();
    let iat = self.cmap.get_iat( &item.registration.fqdn );
    queue.push(MinHeapEnqueuedInvocation::new_f(item.clone(), iat ));
    debug!(tid=%item.tid,  component="minheap", "Added item to front of queue minheap - len: {} arrived: {} top: {} ", 
                        queue.len(),
                        item.registration.function_name,
                        queue.peek().unwrap().item.registration.function_name );
    Ok(())
  }
}
