use std::sync::Arc;
use crate::services::invocation::create_concurrency_semaphore;
use crate::services::invocation::invoker_structs::MinHeapEnqueuedInvocation;
use crate::worker_api::worker_config::InvocationConfig;
use iluvatar_library::{transaction::TransactionId, characteristics_map::CharacteristicsMap};
use anyhow::Result;
use parking_lot::Mutex;
use tokio::sync::Semaphore;
use tracing::debug;
use super::InvokerQueuePolicy;
use super::invoker_structs::{EnqueuedInvocation, MinHeapFloat};
use std::collections::BinaryHeap;

pub struct MinHeapInvoker {
  invoke_queue: Arc<Mutex<BinaryHeap<MinHeapFloat>>>,
  pub cmap: Arc<CharacteristicsMap>,
  concurrency_semaphore: Arc<Semaphore>,
}

impl MinHeapInvoker {
  pub fn new(invocation_config: Arc<InvocationConfig>, tid: &TransactionId, cmap: Arc<CharacteristicsMap>) -> Result<Arc<Self>> {
    let svc = Arc::new(MinHeapInvoker {
      concurrency_semaphore: create_concurrency_semaphore(invocation_config.concurrent_invokes)?.ok_or(anyhow::anyhow!("Must provide `concurrent_invokes`"))?,
      invoke_queue: Arc::new(Mutex::new(BinaryHeap::new())),
      cmap,
    });
    debug!(tid=%tid, "Created MinHeapInvoker");
    Ok(svc)
  }
}

#[tonic::async_trait]
impl InvokerQueuePolicy for MinHeapInvoker {
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
  fn concurrency_semaphore(&self) -> Option<&Arc<Semaphore>> {
    Some(&self.concurrency_semaphore)
  }
  
  fn add_item_to_queue(&self, item: &Arc<EnqueuedInvocation>, _index: Option<usize>) {
    let mut queue = self.invoke_queue.lock();
    queue.push(MinHeapEnqueuedInvocation::new_f(item.clone(), self.cmap.get_exec_time(&item.registration.fqdn)));
    debug!(tid=%item.tid,  component="minheap", "Added item to front of queue minheap - len: {} arrived: {} top: {} ", 
                        queue.len(),
                        item.registration.function_name,
                        queue.peek().unwrap().item.registration.function_name );
  }
}
