use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::services::invocation::create_concurrency_semaphore;
use crate::worker_api::worker_config::InvocationConfig;
use iluvatar_library::{transaction::TransactionId, characteristics_map::CharacteristicsMap};
use anyhow::Result;
use parking_lot::Mutex;
use tokio::sync::Semaphore;
use tracing::debug;
use super::InvokerQueuePolicy;
use super::invoker_structs::{EnqueuedInvocation, MinHeapEnqueuedInvocation, MinHeapFloat};
use std::collections::BinaryHeap;

fn time_since_epoch() -> f64 {
    let start = SystemTime::now();
    start.duration_since( UNIX_EPOCH )
         .expect("Time went backwards")
         .as_secs_f64()
}

pub struct MinHeapEDInvoker {
  invoke_queue: Arc<Mutex<BinaryHeap<MinHeapFloat>>>,
  cmap: Arc<CharacteristicsMap>,
  concurrency_semaphore: Arc<Semaphore>,
}

impl MinHeapEDInvoker {
  pub fn new(invocation_config: Arc<InvocationConfig>, tid: &TransactionId, cmap: Arc<CharacteristicsMap>) -> Result<Arc<Self>> {
    let svc = Arc::new(MinHeapEDInvoker {
      concurrency_semaphore: create_concurrency_semaphore(invocation_config.concurrent_invokes)?.ok_or(anyhow::anyhow!("Must provide `concurrent_invokes`"))?,
      invoke_queue: Arc::new(Mutex::new(BinaryHeap::new())),
      cmap,
    });
    debug!(tid=%tid, "Created MinHeapEDInvoker");
    Ok(svc)
  }
}

#[tonic::async_trait]
impl InvokerQueuePolicy for MinHeapEDInvoker {
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
    let deadline = self.cmap.get_exec_time(&item.registration.fqdn) + time_since_epoch();
    queue.push(MinHeapEnqueuedInvocation::new_f(item.clone(), deadline ));
    debug!(tid=%item.tid,  component="minheap", "Added item to front of queue minheap - len: {} arrived: {} top: {} ", 
                        queue.len(),
                        item.registration.function_name,
                        queue.peek().unwrap().item.registration.function_name );
  }
}
